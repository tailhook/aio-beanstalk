import argparse
import asyncio
from sys import exit

from .proto import Client


def do_stats(caller, options):
    res = yield from caller.send_command('stats')
    if isinstance(res, Exception):
        print(res)
        exit(1)
    print(res.data.decode('ascii'))


def do_stats_tube(caller, options):
    for tube in options.tube:
        res = yield from caller.send_command('stats-tube', tube)
        if isinstance(res, Exception):
            print(res)
            exit(1)
        print(res.data.decode('ascii'))


@asyncio.coroutine
def run(options):
    conn = [('localhost', 11300)]
    if options.connect:
        conn = [(h, int(p))
                for h, p in (a.split(':', 1)
                             for a in options.connect)]
    assert len(conn) == 1, 'Only single connection supported so far'
    cli = yield from Client.connect(*conn[0])
    yield from options.action(cli, options)


def main():
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument('-c', '--connect', metavar="HOST:PORT",
        help="Add beanstalkd server to connect to (repeatable)",
        default=[], action="append")
    ap = argparse.ArgumentParser(parents=[common])
    sub = ap.add_subparsers()

    sap = sub.add_parser("stats", parents=[common])
    sap.set_defaults(action=do_stats)

    tap = sub.add_parser("tube", parents=[common],
        aliases=['stats-tube', 'queue', 'stats-queue'])
    tap.add_argument("tube", metavar="NAME", default=["default"], nargs='*')
    tap.set_defaults(action=do_stats_tube)

    options = ap.parse_args()

    asyncio.get_event_loop().run_until_complete(run(options))

if __name__ == '__main__':
    from .stats import main  # flake8: noqa
    main()
