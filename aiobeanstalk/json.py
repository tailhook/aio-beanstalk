import argparse
import asyncio
import logging
import json
import time
import random
from urllib.parse import urlparse

from .proto import Client
from .exceptions import Draining
from .packets import Inserted


log = logging.getLogger(__name__)


class CallingError(Exception):
    """We tried to put a task, but not sure if it failed

    When we did `put` but didn't receive a reply, we are not sure whether task
    is queued and we failed to get reply, or task has not been put at all.
    """


class Caller(object):

    def __init__(self, hosts):
        assert hosts, hosts
        self._hosts = hosts
        self._clients = {}
        self._events = {}
        self._tasks = [self._start_connection(h, p) for h, p in hosts]

    def _start_connection(self, host, port):
        ev = asyncio.Event()
        self._events[host, port] = ev
        return asyncio.Task(self._reconnect(host, port, ev))

    @asyncio.coroutine
    def _reconnect(self, host, port, event):
        try:
            while True:
                cli = yield from self._connect(host, port)
                self._clients[host, port] = cli
                event.set()
                try:
                    yield from cli.wait_closed()
                except EOFError:
                    fut = asyncio.Future()
                    self._connectors.add(fut)
                    continue
                except asyncio.CancelledError:
                    return
                finally:
                    event.clear()
                    # May already be removed in case of draining
                    self._clients.pop((host, port), None)
        except Exception:
            log.exception("Abnormal termination for client thread")

    @asyncio.coroutine
    def _connect(self, host, port):
        log.debug("Connecting to %r:%d", host, port)
        while True:
            try:
                start = time.time()
                cli = yield from Client.connect(host, port)
            except OSError:
                log.warning("Error establishing connection. Will retry...")
                yield from asyncio.sleep(max(0, start + 0.1 - time.time()))
            else:
                return cli

    @asyncio.coroutine
    def call(self, name, args, kwargs, *,
        priority=2**31, ttr=120, delay=0, tube='default'):
        lst = [name, kwargs]
        lst.extend(args)
        body = json.dumps(lst, ensure_ascii=False).encode('utf-8')
        while True:
            while not self._clients:
                yield from asyncio.wait(
                    [ev.wait() for ev in self._events.values()],
                    return_when=asyncio.FIRST_COMPLETED)
            cli = random.choice(list(self._clients.values()))
            try:
                # Must pipeline these two
                use = cli.send_command('use', tube)
                put = cli.send_command('put', priority, delay, ttr, body=body)
                yield from use
                val = yield from put
            except EOFError:
                raise CallingError()
            if isinstance(val, Draining):
                self._clients.pop((cli.host, cli.port), None)
                log.info("Server is draining, trying next")
                continue
            elif isinstance(val, Exception):
                raise val
            assert isinstance(val, Inserted)
            return 'beanstalk://{}:{}/{}'.format(
                cli.host, cli.port, val.job_id)

    def close(self):
        for i in self._tasks:
            i.cancel()

    def wait_closed(self):
        yield from asyncio.wait(self._tasks)


@asyncio.coroutine
def run(options):
    import yaml
    conn = [('localhost', 11300)]
    if options.connect:
        conn = [(h, int(p))
                for h, p in (a.split(':', 1)
                             for a in options.connect)]
    args = tuple(map(yaml.safe_load, options.arguments))
    kwargs = {k: yaml.safe_load(v) for k, v in options.kwargs}

    caller = Caller(conn)
    reserve = yield from caller.call(options.task_name, args, kwargs,
        priority=options.priority,
        ttr=options.ttr,
        delay=options.delay,
        tube=options.queue,
        )
    caller.close()
    yield from caller.wait_closed()
    print(reserve)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('-c', '--connect', metavar="HOST:PORT",
        help="Add beanstalkd server to connect to (repeatable)",
        default=[], action="append")
    ap.add_argument('--log-level',
        help="The base log level",
        default="WARNING")
    ap.add_argument('-p', '--pri', '--priority',
        help="Priority for the task (default %(default)s)",
        default=2 ** 31, type=int, dest='priority')
    ap.add_argument('-t', '--ttr', '--time-to-release',
        help="The TTR (time to release) for task (default %(default)s)",
        default=120, type=int, dest='ttr')
    ap.add_argument('-q', '--queue',
        help="The queue (tube) for task (default %(default)s)",
        default='default')
    ap.add_argument('-d', '--delay',
        help="Delay to put task with in seconds (default %(default)s)",
        default=0, type=int)
    ap.add_argument('--debug-asyncio',
        help="Enable debugging of asyncio (too verbose)",
        default=False, action="store_true")
    ap.add_argument('task_name',
        help="Full name of the task to queue")
    ap.add_argument('arguments', metavar="ARG", nargs="*",
        help="Full name of the task to queue")
    ap.add_argument('-K', nargs=2, metavar=("NAME", "VALUE"),
        help="Keyword argument",
        dest="kwargs", action="append", default=[])
    options = ap.parse_args()

    logging.basicConfig(
        level=getattr(logging, options.log_level),
        format='%(asctime)-15s %(name)s %(levelname)s %(message)s',
        )
    if not options.debug_asyncio:
        logging.getLogger('asyncio').setLevel(logging.WARNING)
    asyncio.get_event_loop().run_until_complete(run(options))


if __name__ == '__main__':
    from .json import main  # flake8: noqa
    main()
