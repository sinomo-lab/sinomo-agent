#!/usr/bin/env python3
"""TCP proxy used by docker/test-routing-scenario.sh.

The proxy relays normal daemon object-RPC connections to a real daemon, but
when the first request on a connection is the TCP tunnel open route it relays
the open handshake and then stops forwarding tunnel frames. This reproduces a
stale persistent tunnel while keeping the same endpoint usable for direct RPC
fallback. With --stall-direct it accepts ordinary daemon RPC connections and
stalls them without forwarding, which reproduces stale healthy direct endpoint
evidence.
"""

import argparse
import select
import socket
import socketserver
import sys
import threading
import time


TUNNEL_OPEN_ROUTE = b"/cluster/tcp-tunnel/open"
BUFFER_SIZE = 64 * 1024
FIRST_REQUEST_TIMEOUT = 2.0
RESPONSE_IDLE_SECONDS = 0.05
STALL_SLEEP_SECONDS = 3600.0


def log(message: str) -> None:
    print(message, flush=True)


def read_initial(sock: socket.socket) -> bytes:
    sock.setblocking(False)
    deadline = time.monotonic() + FIRST_REQUEST_TIMEOUT
    chunks: list[bytes] = []
    while time.monotonic() < deadline:
        readable, _, _ = select.select([sock], [], [], 0.05)
        if not readable:
            if chunks:
                break
            continue
        chunk = sock.recv(BUFFER_SIZE)
        if not chunk:
            break
        chunks.append(chunk)
        joined = b"".join(chunks)
        if TUNNEL_OPEN_ROUTE in joined:
            return joined
    return b"".join(chunks)


def drain_response_until_idle(sock: socket.socket) -> bytes:
    sock.setblocking(False)
    chunks: list[bytes] = []
    while True:
        readable, _, _ = select.select([sock], [], [], RESPONSE_IDLE_SECONDS)
        if not readable:
            break
        chunk = sock.recv(BUFFER_SIZE)
        if not chunk:
            break
        chunks.append(chunk)
    return b"".join(chunks)


def relay_bidirectional(left: socket.socket, right: socket.socket) -> tuple[int, int, str, float]:
    started = time.monotonic()
    left_to_right = 0
    right_to_left = 0
    sockets = [left, right]
    for sock in sockets:
        sock.setblocking(False)
    while sockets:
        readable, _, errored = select.select(sockets, [], sockets, 1.0)
        if errored:
            return left_to_right, right_to_left, "select-error", time.monotonic() - started
        for sock in readable:
            try:
                data = sock.recv(BUFFER_SIZE)
            except OSError:
                return left_to_right, right_to_left, "recv-error", time.monotonic() - started
            if not data:
                return left_to_right, right_to_left, "eof", time.monotonic() - started
            other = right if sock is left else left
            try:
                other.sendall(data)
            except OSError:
                return left_to_right, right_to_left, "send-error", time.monotonic() - started
            if sock is left:
                left_to_right += len(data)
            else:
                right_to_left += len(data)
    return left_to_right, right_to_left, "closed", time.monotonic() - started


def stall_client(client: socket.socket) -> None:
    client.settimeout(1.0)
    while True:
        try:
            data = client.recv(BUFFER_SIZE)
        except socket.timeout:
            time.sleep(STALL_SLEEP_SECONDS)
            continue
        except OSError:
            return
        if not data:
            return


class StallProxyHandler(socketserver.BaseRequestHandler):
    target_host: str
    target_port: int | None
    stall_direct: bool

    def handle(self) -> None:
        peer = f"{self.client_address[0]}:{self.client_address[1]}"
        try:
            initial = read_initial(self.request)
            if not initial:
                log(f"{peer} closed before first request")
                return
            is_tunnel_open = TUNNEL_OPEN_ROUTE in initial
            log(
                f"{peer} first_request_bytes={len(initial)} "
                f"tunnel_open={str(is_tunnel_open).lower()}"
            )
            if self.stall_direct and not is_tunnel_open:
                log(f"{peer} direct rpc stalling")
                stall_client(self.request)
                return
            if self.target_port is None:
                log(f"{peer} no proxy target configured; stalling")
                stall_client(self.request)
                return
            with socket.create_connection(
                (self.target_host, self.target_port), timeout=5.0
            ) as target:
                target.sendall(initial)
                if is_tunnel_open:
                    response = drain_response_until_idle(target)
                    log(f"{peer} tunnel_open_response_bytes={len(response)}; stalling")
                    if response:
                        self.request.sendall(response)
                    stall_client(self.request)
                    return
                log(f"{peer} relaying direct rpc")
                sent, received, reason, elapsed = relay_bidirectional(self.request, target)
                log(
                    f"{peer} direct rpc relay done reason={reason} "
                    f"client_to_target_bytes={sent} target_to_client_bytes={received} "
                    f"elapsed_ms={elapsed * 1000:.1f}"
                )
        except Exception as exc:
            log(f"{peer} proxy error: {exc!r}")


class ThreadedTcpServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--listen-host", default="127.0.0.1")
    parser.add_argument("--listen-port", type=int, required=True)
    parser.add_argument("--target-host", default="127.0.0.1")
    parser.add_argument("--target-port", type=int)
    parser.add_argument("--stall-direct", action="store_true")
    args = parser.parse_args()

    StallProxyHandler.target_host = args.target_host
    StallProxyHandler.target_port = args.target_port
    StallProxyHandler.stall_direct = args.stall_direct
    server = ThreadedTcpServer((args.listen_host, args.listen_port), StallProxyHandler)
    target = (
        "direct-stall"
        if args.stall_direct
        else f"{args.target_host}:{args.target_port}"
    )
    print(
        f"tcp tunnel stall proxy listening on "
        f"{args.listen_host}:{args.listen_port} -> {target}",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
