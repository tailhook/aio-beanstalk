import os
import time
import abc
import logging
import asyncio
import argparse
import signal
import json

import yaml
try:
    from yaml.cyaml import CSafeLoader as YamlLoader
except ImportError:
    from yaml import SafeLoader as YamlLoader

from .proto import Client
from .packets import Reserved, Ok
from .exceptions import DeadlineSoon, TimedOut


log = logging.getLogger(__name__)


class Bury(Exception):
    """This exception bury task silently"""


class Release(Exception):
    """This exception release task in a timeout"""

    def __init__(self, priority, delay):
        self.priority = priority
        self.delay = delay
        super().__init__(priority, delay)


class Signature(object):
    """A class which holds method call with args,
       and prints nice representation

       Mostly used for logs
    """

    def __init__(self, name, args, kwargs):
        self.name = name
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        astr = [repr(a) for a in self.args]
        astr.extend(k + '=' + repr(self.kwargs[k])
                    for k in sorted(self.kwargs))
        return "{}({})".format(self.name, ', '.join(astr))



class AbstractWorker(metaclass=abc.ABCMeta):

    def __init__(self, servers, queues, concurrency, loop=None):
        self.servers = servers
        self.queues = queues
        self.concurrency = concurrency
        self._loop = loop or asyncio.get_event_loop()


    @asyncio.coroutine
    def start(self):
        self._terminating = False
        self._connectors = [asyncio.Task(self._client(host, port))
                            for (host, port) in self.servers]
        self._tasks = set()
        self._current_task = None

    @asyncio.coroutine
    def _client(self, host, port):
        try:
            while not self._terminating:
                try:
                    yield from self._connect(host, port)
                except (OSError, EOFError):
                    log.info("Connection to %s:%d closed. Reconnecting",
                        host, port, exc_info=1)
        except asyncio.CancelledError:
            log.info("Client is cancelled")
        except Exception:
            log.exception("Unexpected error in worker loop")

    @asyncio.coroutine
    def _connect(self, host, port):
        try:
            start = time.time()
            cli = yield from Client.connect(host, port)
        except OSError as e:
            log.warning("Error establishing connection. Will retry...")
            yield from asyncio.sleep(max(0, start + 0.1 - time.time()),
                                     loop=self._loop)
            raise e
        log.info("Established connection to %s:%s", host, port)
        try:
            for q in self.queues:
                res = yield from cli.send_command('watch', q)
                if isinstance(res, Exception):
                    raise res
            if 'default' not in self.queues:
                res = yield from cli.send_command('ignore', 'default')
                if isinstance(res, Exception):
                    raise res

            while not self._terminating:
                while len(self._tasks) >= self.concurrency:
                    try:
                        yield from asyncio.wait(self._tasks,
                            return_when=asyncio.FIRST_COMPLETED,
                            loop=self._loop)
                    except asyncio.CancelledError:
                        pass

                try:
                    if self._tasks:
                        #  When there is a task we shouldn't wait forever
                        #  because our waiting blocks release/bury/delete
                        #  commands to proceed for task
                        task = yield from cli.send_command(
                            'reserve-with-timeout', 1)
                    else:
                        task = yield from cli.send_command('reserve')
                    if not isinstance(task, Reserved):
                        if isinstance(task, DeadlineSoon):
                            log.warning("Deadline is soon. Stopping accepting"
                                " tasks for safety period 1 sec")
                            yield from asyncio.sleep(1, loop=self._loop)
                            continue
                        if isinstance(task, TimedOut):
                            continue
                        log.error("Unexpected response %r", task)
                        continue
                    log.debug("Received task %d: %r", task.job_id, task.data)
                    yield from self._start_task(cli, task)
                except EOFError:
                    break
        finally:
            if self._tasks:
                yield from asyncio.wait(self._tasks, loop=self._loop)
            cli.close()

    @asyncio.coroutine
    def release_task(self, cli, reserved, stats):
        yield from cli.send_command('release',
            reserved.job_id, stats['pri'], 0)

    @asyncio.coroutine
    def _start_task(self, cli, reserved):
        stats = yield from cli.send_command('stats-job', reserved.job_id)
        if not isinstance(stats, Ok):
            #  Presumably task is deleted by administrator
            return
        stats = yaml.load(stats.data, Loader=YamlLoader)
        if len(self._tasks) >= self.concurrency:
            #  Worker already have a task, probably from another client
            yield from self.release_task(cli, reserved, stats)
            return
        task = asyncio.shield(self.task_wrapper(cli, reserved, stats),
                              loop=self._loop)
        task.add_done_callback(self._tasks.remove)
        self._tasks.add(task)

    @abc.abstractmethod
    @asyncio.coroutine
    def run_task(self, reserved, stats):
        pass

    @asyncio.coroutine
    def task_wrapper(self, cli, reserved, stats):
        try:
            yield from self.run_task(reserved, stats)
        except Bury:
            log.debug("Burying task %d", reserved.job_id)
            yield from cli.send_command('bury', reserved.job_id, stats['pri'])
        except Release as rel:
            yield from cli.send_command('release',
                reserved.job_id, rel.priority, rel.delay)
        except Exception:
            log.exception("Exception running task %d. Burying...",
                reserved.job_id)
            yield from cli.send_command('bury', reserved.job_id, stats['pri'])
        else:
            yield from cli.send_command('delete', reserved.job_id)

    @asyncio.coroutine
    def wait_stopped(self):
        yield from asyncio.wait(self._connectors, loop=self._loop)
        while self._tasks:
            yield from asyncio.wait(self._tasks, loop=self._loop)
        log.debug("No connectors and tasks left")

    def terminate(self):
        log.info("Starting normal termination")
        for c in self._connectors:
            c.cancel()
        self._terminating = True


class JsonTaskWorker(AbstractWorker):

    def __init__(self, *args, retry_times=3, retry_delay=2, **kw):
        super().__init__(*args, **kw)
        self.tasks = self.initial_tasks()
        self.retry_delay = retry_delay
        self.retry_times = retry_times

    def initial_tasks(self):
        """These ones useful for debugging. Remove them in subclass
        if they make you feel incomfortable
        """
        return {
            'fail': lambda *args, **kw: 1/0,
            'nop': lambda *args, **kw: None,
            'echo': lambda *args, **kw: "echo(*%r, **%r)" % (args, kw),
            }

    @asyncio.coroutine
    def run_task(self, reserved, stats):
        name, kwargs, *args = json.loads(reserved.data.decode('utf-8'))
        sig = Signature(name, args, kwargs)
        if name not in self.tasks:
            log.error("Task %r not found", name)
            raise Bury()
        try:
            log.info("Calling (id: %d) %s", reserved.job_id, sig)
            yield from self.tasks[name](*args, **kwargs)
        except Exception:
            if stats['reserves'] > self.retry_times:
                log.exception(
                    "Task %d has failed to much times (%d/%d). Burying...",
                    reserved.job_id, stats['reserves'], self.retry_times)
                raise Bury()
            else:
                delay = self._calc_delay(stats)
                log.exception("Task %d has failed %d/%d. Will retry in %d...",
                    reserved.job_id,
                    stats['reserves'], self.retry_times, delay)
                raise self.release_task(sig, stats)

    def _calc_delay(self, stats):
        return self.retry_delay*stats['reserves']

    def release_task(self, signature, stats):
        """Adjusts the task priority and delay on failure"""
        # Make lower priority for retried tasks
        # And use exponential delay
        return Release(stats['pri'] + 1, self._calc_delay(stats))



@asyncio.coroutine
def run(options, add_tasks, Worker=JsonTaskWorker):
    conn = [('localhost', 11300)]
    if options.connect:
        conn = [(h, int(p))
                for h, p in (a.split(':', 1)
                             for a in options.connect)]

    worker = Worker(conn, options.queues or ['default'],
        retry_times=options.retry_times,
        retry_delay=options.retry_delay,
        concurrency=options.concurrency)
    add_tasks(worker)
    asyncio.get_event_loop().add_signal_handler(signal.SIGTERM,
        worker.terminate)
    asyncio.get_event_loop().add_signal_handler(signal.SIGINT,
        worker.terminate)
    yield from worker.start()
    log.info("Worker successfully started")
    yield from worker.wait_stopped()


def worker_options(ap):
    ap.add_argument('-c', '--connect', metavar="HOST:PORT",
        help="Add beanstalkd server to connect to (repeatable)",
        default=[], action="append")
    ap.add_argument('--retry-times',
        help="Number of times for task to be retried (default %(default)d)",
        default=3, type=int)
    ap.add_argument('--retry-delay',
        help="Exponential delay for retries (default %(default)d)",
        default=2, type=int)
    ap.add_argument('-q', '--queue', metavar="HOST:PORT",
        help="Add queue (tube) to receive messages from (repeatable)",
        dest='queues', default=[], action="append")
    ap.add_argument('--concurrency',
        help="Number of task to run simultaneously. Note they are run as"
             " asyncio tasks in same thread and process"
             " (default %(default)d)",
        default=1, type=int)


def main(add_tasks=lambda w: None):
    ap = argparse.ArgumentParser()
    worker_options(ap)
    ap.add_argument('--log-level',
        help="The base log level",
        default="WARNING")
    ap.add_argument('--debug-asyncio',
        help="Enable debugging of asyncio (too verbose)",
        default=False, action="store_true")
    options = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, options.log_level),
        format='%(asctime)-15s %(name)s %(levelname)s %(message)s',
        )
    if not options.debug_asyncio:
        logging.getLogger('asyncio').setLevel(logging.WARNING)
    asyncio.get_event_loop().run_until_complete(run(options, add_tasks))


if __name__ == '__main__':
    from .worker import main  # flake8: noqa
    main()
