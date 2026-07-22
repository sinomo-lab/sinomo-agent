#!/usr/bin/env python3
"""Accept TCP connections and hold them open without answering.

The Docker routing scenario uses this to inject stale healthy direct-endpoint
evidence and verify that a timed-out direct attempt is demoted.
"""

import argparse
import socket
import socketserver
import sys


BUFFER_SIZE = 64 * 1024


def stall_client(client: socket.socket) -> None:
    client.settimeout(1.0)
    while True:
        try:
            data = client.recv(BUFFER_SIZE)
        except socket.timeout:
            continue
        except OSError:
            return
        if not data:
            return


class DirectStallHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        peer = f"{self.client_address[0]}:{self.client_address[1]}"
        print(f"{peer} direct RPC stalling", flush=True)
        stall_client(self.request)


class ThreadedTcpServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--listen-port", type=int, required=True)
    args = parser.parse_args()

    server = ThreadedTcpServer((args.listen_host, args.listen_port), DirectStallHandler)
    print(
        f"direct stall proxy listening on {args.listen_host}:{args.listen_port}",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
