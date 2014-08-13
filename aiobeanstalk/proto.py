import asyncio
import logging
from collections import deque

from .packets import BasePacket
from . import exceptions  # flake8: noqa


log = logging.getLogger(__name__)


def _convert(value, type):
    if type is str:
        return value.decode('utf-8')
    return type(value)


class ProtocolError(Exception):
    pass


class Client(object):

    def __init__(self, reader, writer, loop=None):
        self._reader = reader
        self._writer = writer
        self._loop = loop or asyncio.get_event_loop()
        self._queue = deque([])
        self._task = asyncio.Task(self._reader_task(), loop=self._loop)

    @classmethod
    @asyncio.coroutine
    def connect(Client, host, port, loop=None):
        reader, writer = yield from asyncio.open_connection(host, port,
                                                            loop=loop)
        cli =  Client(reader, writer, loop=loop)
        cli.host = host
        cli.port = port
        return cli

    #  It's not a coroutine, it must return future to allow pipelining
    def send_command(self, *args, body=None):
        log.debug("Sending %r (%r)", args, body)
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
        fut = asyncio.Future(loop=self._loop)

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
                try:
                    Packet = BasePacket.registry[cmd, len(args)]
                except KeyError:
                    raise ProtocolError("Unexpected response {!r} {}"
                        .format(cmd, args))
                if Packet.fields and Packet.fields[-1][1] == bytes:
                    blen = int(args[-1])
                    args[-1] = yield from self._reader.readexactly(blen)
                    endline = yield from self._reader.readexactly(2)
                    if endline != b'\r\n':
                        raise ProtocolError()
                packet = Packet(*(_convert(x, typ)
                                  for (_, typ), x in zip(Packet.fields, args)))
                log.debug("Got reply %r", packet)
                self._queue.popleft().set_result(packet)
        except EOFError:
            log.debug("Connection to %s:%d closed", self.host, self.port)
        except Exception:
            log.exception("Unexpected error in connection %s:%d",
                self.host, self.port, exc_info=1)
        finally:
            self._writer.transport.close()
            for f in self._queue:
                f.set_exception(EOFError())

    def close(self):
        self._writer.transport.close()

    def wait_closed(self):
        return asyncio.shield(self._task, loop=self._loop)
