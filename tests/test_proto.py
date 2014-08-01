import asyncio
import unittest
from unittest.mock import ANY

from aiobeanstalk.proto import Client
from aiobeanstalk.packets import Using, Inserted


def btalk_test(fun):

    fun = asyncio.coroutine(fun)

    def wrapper(self):

        @asyncio.coroutine
        def full_test():
            cli = yield from Client.connect('localhost', 11300)
            try:
                yield from fun(self, cli)
            finally:
                cli.close()

        self.loop.run_until_complete(full_test())

    return wrapper


class TestCase(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.get_event_loop()

    @btalk_test
    def testPut(self, btalk):
        self.assertEqual((yield from btalk.send_command('use', 'test.q1')),
            Using('test.q1'))
        self.assertEqual((yield from btalk.send_command(
            'put', 0, 0, 30,
            body=b'hello world')),
            Inserted(ANY))
