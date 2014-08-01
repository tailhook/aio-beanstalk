=============
aio-beanstalk
=============

aio-beanstalk is a asyncio client for beanstalk

Status: Low level protocol is fully implemented but not well tested.
Higher-level interface is in prototype stage.


Low-Level Interface
-------------------


Basically it looks like:

.. code-block:: python

   import asyncio
   from aiobeanstalk.proto import Client

   @asyncio.coroutine
   def main():
       client = yield from Client.connect('localhost', 11300)
       yield from client.send_command('use', 'mytesttube')
       res = yield from client.send_command('put',
            100,  # priority
            0,    # delay
            10,   # TTR = time to run
            body=b'task body')
       if isinstance(res, Exception):
           raise res
       print("Job queued with id", res.job_id)
       client.close()

   if __name__ == '__main__':
       asyncio.get_event_loop().run_until_complete(main())


The low-level interface is intentionally has no methods for each command, and
does not raise exceptions. Higher-level interface fill-in the gaps, and
provides usual consumer-producer abstractions (At low level both the process
which sends the tasks and worker that processes them share same protocol. It's
how beanstalk is designed).

Client methods:

``connect(host, port)``
    A classmethod coroutine that returns ``Client`` object that is connected
    to beanstalkd server on the specified host and port.

``send_command(cmd, *args, body=None)``
    A coroutine that sends command to the beanstalkd server and waits for the
    reply.

    Reply is returned as object from ``aiobeanstalk.packets`` or
    ``aiobeanstalk.exceptions``. Exceptions are returned rather than raised,
    so you must always check result. But, ``EOFError`` may be *raised* in the
    case connection is closed before receiving a reply.

    It's *safe* to call it from many coroutines simultaneously, requests will
    be pipelined. However, it's probably useless to call any command
    simultaneously with ``reserve``, because the latter blocks on server.


``close()``
    Closes socket.


High Level Interface
====================

TBD



