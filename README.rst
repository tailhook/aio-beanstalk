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


High Level Interface
====================

TBD



