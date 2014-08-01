import asyncio
from collections import deque

from .packets import BasePacket
from . import exceptions  # flake8: noqa


class ProtocolError(Exception):
    pass


class Client(object):

    def __init__(self, reader, writer):
        self._reader = reader
        self._writer = writer
        self._queue = deque([])
        self._task = asyncio.Task(self._reader_task())

    @classmethod
    @asyncio.coroutine
    def connect(Client, host, port):
        reader, writer = yield from asyncio.open_connection(host, port)
        return Client(reader, writer)

    #  It's not a coroutine, it must return future to allow pipelining
    def send_command(self, *args, body=None):
        chunks = []
        for arg in args:
            arg = str(arg)
            assert not (' ' in arg or '\r' in arg or '\n' in arg), args
            chunks.append(arg.encode('ascii'))
        if body is not None:
            if isinstance(body, str):
                body = body.encode('utf-8')
            assert isinstance(body, bytes), body
            chunks.append(str(len(body)).encode('ascii'))
        req = b' '.join(chunks) + b'\r\n'
        if body is not None:
            req += body + b'\r\n'
        fut = asyncio.Future()

        self._writer.write(req)
        self._queue.append(fut)

        return fut

    @asyncio.coroutine
    def _reader_task(self):
        try:
            while True:
                header = yield from self._reader.readline()
                if not header:  # connection closed
                    raise EOFError()
                cmd, *args = header.split()
                Packet = BasePacket.registry[cmd]
                if Packet.fields and Packet.fields[-1][1] == bytes:
                    blen = int(args[-1])
                    args[-1] = yield from self._reader.readexactly(blen)
                    endline = yield from self._reader.readexactly(2)
                    if endline != b'\r\n':
                        raise ProtocolError()
                if len(args) != len(Packet.fields):
                    raise ProtocolError()
                packet = Packet(*(typ(x)
                                  for (_, typ), x in zip(Packet.fields, args)))
                self._queue.popleft().set_result(packet)
        finally:
            self._writer.transport.close()
            for f in self._queue:
                f.set_exception(EOFError())

    def close(self):
        self._writer.transport.close()

    def wait_closed(self):
        return asyncio.shield(self._task)
