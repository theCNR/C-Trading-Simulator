"""POSIX-only integration tests for the local TCP/UDP simulator."""

from __future__ import annotations

import ctypes
import signal
import socket
import struct
import subprocess
import tempfile
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LONG_MAX = (1 << (ctypes.sizeof(ctypes.c_long) * 8 - 1)) - 1


def reserve_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Peer:
    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.pending = bytearray()

    def send(self, data: bytes) -> None:
        self.sock.sendall(data)

    def command(self, line: str) -> None:
        self.send(line.encode("ascii") + b"\n")

    def read_line(self, timeout: float = 3.0) -> str:
        self.sock.settimeout(timeout)
        deadline = time.monotonic() + timeout
        while b"\n" not in self.pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError("timed out waiting for a TCP response")
            self.sock.settimeout(remaining)
            chunk = self.sock.recv(4096)
            if not chunk:
                raise AssertionError("server closed before sending a complete line")
            self.pending.extend(chunk)

        end = self.pending.index(b"\n")
        line = bytes(self.pending[:end]).rstrip(b"\r")
        del self.pending[: end + 1]
        return line.decode("ascii")

    def close(self) -> None:
        self.sock.close()

    def reset_close(self) -> None:
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        self.sock.close()


class SimulatorIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(prefix="stock-simulator-")
        self.temp_path = Path(self.temp.name)
        self.stock_file = self.temp_path / "stocks.txt"
        self.stock_file.write_text("AAPL 100.00\nLOW 0.01\nMSFT 50.00\n", encoding="ascii")
        self.tcp_port = reserve_tcp_port()

        self.udp_receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_receiver.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.udp_receiver.bind(("0.0.0.0", 0))
        self.udp_port = self.udp_receiver.getsockname()[1]

        self.app_log_path = self.temp_path / "server.stdout.log"
        self.app_log = self.app_log_path.open("w", encoding="utf-8")
        self.server_log_path = self.temp_path / "server.log"
        self.server = subprocess.Popen(
            [
                str(ROOT / "ServerTrd"),
                "-p",
                str(self.tcp_port),
                "-u",
                str(self.udp_port),
                "-s",
                str(self.stock_file),
                "-l",
                str(self.server_log_path),
            ],
            cwd=ROOT,
            stdout=self.app_log,
            stderr=subprocess.STDOUT,
        )

        deadline = time.monotonic() + 4.0
        while time.monotonic() < deadline:
            if self.server.poll() is not None:
                self.fail(f"server exited during startup: {self._read_app_log()}")
            self.app_log.flush()
            if "SERVER_START" in self._read_app_log():
                break
            time.sleep(0.02)
        else:
            self.fail("server did not report startup")

    def tearDown(self) -> None:
        if self.server.poll() is None:
            self.server.send_signal(signal.SIGINT)
            try:
                self.server.wait(timeout=4.0)
            except subprocess.TimeoutExpired:
                self.server.kill()
                self.server.wait(timeout=2.0)
        self.udp_receiver.close()
        self.app_log.close()
        self.temp.cleanup()

    def _read_app_log(self) -> str:
        try:
            return self.app_log_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return ""

    def connect(self, role: str, username: str) -> Peer:
        sock = socket.create_connection(("127.0.0.1", self.tcp_port), timeout=2.0)
        peer = Peer(sock)
        peer.command(f"JOIN {role} {username}")
        self.assertEqual(peer.read_line(), f"OK JOIN {username}")
        return peer

    def wait_for_udp(self, expected: bytes, timeout: float = 3.0) -> bytes:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.udp_receiver.settimeout(max(0.05, deadline - time.monotonic()))
            message, _ = self.udp_receiver.recvfrom(8192)
            if expected in message:
                return message
        self.fail(f"did not receive UDP message containing {expected!r}")

    def test_buy_sell_validation_limits_and_udp_broadcast(self) -> None:
        trader = self.connect("TRADER", "alice")
        analyst = self.connect("ANALYST", "viewer")
        try:
            trader.command("BUY AAPL 10")
            self.assertEqual(trader.read_line(), "OK BUY AAPL 10 100.10")
            self.assertIn(b"PRICE_UPDATE AAPL:100.10", self.wait_for_udp(b"AAPL:100.10"))

            trader.command("PORTFOLIO")
            self.assertEqual(trader.read_line(), "OK PORTFOLIO AAPL:10")

            trader.command("SELL AAPL 4")
            self.assertEqual(trader.read_line(), "OK SELL AAPL 4 100.06")
            trader.command("PORTFOLIO")
            self.assertEqual(trader.read_line(), "OK PORTFOLIO AAPL:6")

            trader.command("SELL AAPL 7")
            self.assertEqual(trader.read_line(), "ERR INSUFFICIENT_SHARES")
            trader.command("BUY AAPL 0")
            self.assertEqual(trader.read_line(), "ERR UNKNOWN BUY")
            trader.command("BUY AAPL -1")
            self.assertEqual(trader.read_line(), "ERR UNKNOWN BUY")
            trader.command("BUY AAPL 1x")
            self.assertEqual(trader.read_line(), "ERR UNKNOWN BUY")
            trader.command(f"BUY AAPL {LONG_MAX}")
            self.assertEqual(trader.read_line(), "ERR UNKNOWN BUY")
            trader.command(f"BUY AAPL {LONG_MAX + 1}")
            self.assertEqual(trader.read_line(), "ERR UNKNOWN BUY")
            trader.command("SELL AAPL 0")
            self.assertEqual(trader.read_line(), "ERR UNKNOWN SELL")
            trader.command("SELL AAPL -1")
            self.assertEqual(trader.read_line(), "ERR UNKNOWN SELL")
            trader.command(f"SELL AAPL {LONG_MAX}")
            self.assertEqual(trader.read_line(), "ERR INSUFFICIENT_SHARES")

            trader.command("BUY MISSING 1")
            self.assertEqual(trader.read_line(), "ERR UNKNOWN_SYMBOL")
            trader.command("BUY AAPL")
            self.assertEqual(trader.read_line(), "ERR UNKNOWN BUY")
            trader.command("UNRECOGNIZED")
            self.assertEqual(trader.read_line(), "ERR UNKNOWN UNRECOGNIZED")

            analyst.command("BUY AAPL 1")
            self.assertEqual(analyst.read_line(), "ERR UNAUTHORIZED")
            analyst.command("PRICE AAPL")
            self.assertEqual(analyst.read_line(), "OK PRICE AAPL 100.06")

            trader.command("BUY LOW 2")
            self.assertEqual(trader.read_line(), "OK BUY LOW 2 0.03")
            trader.command("SELL LOW 2")
            self.assertEqual(trader.read_line(), "OK SELL LOW 2 0.01")
        finally:
            trader.close()
            analyst.close()

    def test_fragmented_and_overlong_tcp_lines(self) -> None:
        trader = self.connect("TRADER", "fragmented")
        try:
            trader.send(b"BUY AA")
            trader.send(b"PL 2\n")
            self.assertEqual(trader.read_line(), "OK BUY AAPL 2 100.02")

            trader.send(b"UNKNOWN" + b" " * 505 + b"\n")
            self.assertEqual(trader.read_line(), "ERR UNKNOWN UNKNOWN")
            trader.send(b"X" * 513 + b"\n")
            self.assertEqual(trader.read_line(), "ERR TOOLONG")
            trader.command("PORTFOLIO")
            self.assertEqual(trader.read_line(), "OK PORTFOLIO AAPL:2")
        finally:
            trader.close()

    def test_abrupt_client_disconnect_does_not_kill_server(self) -> None:
        for number in range(20):
            peer = self.connect("TRADER", f"reset{number}")
            peer.send(b"PORTFOLIO\n" * 40)
            peer.reset_close()
            time.sleep(0.01)
            self.assertIsNone(self.server.poll(), "server exited after an abortive disconnect")

        replacement = self.connect("TRADER", "replacement")
        replacement.close()

    def test_client_survives_server_reset_during_handshake(self) -> None:
        for number in range(10):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
                listener.bind(("127.0.0.1", 0))
                listener.listen(1)
                port = listener.getsockname()[1]
                client = subprocess.Popen(
                    [str(ROOT / "trader"), "127.0.0.1", str(port), f"sigpipe{number}"],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                conn, _ = listener.accept()
                conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                conn.close()
                try:
                    result = client.wait(timeout=2.0)
                except subprocess.TimeoutExpired:
                    client.kill()
                    client.wait(timeout=2.0)
                    self.fail("client hung after the peer reset the TCP connection")
                self.assertNotEqual(result, -signal.SIGPIPE, "client was terminated by SIGPIPE")

    def test_periodic_udp_snapshot(self) -> None:
        message = self.wait_for_udp(b"PRICE_UPDATE AAPL:100.00", timeout=7.0)
        self.assertIn(b"LOW:0.01", message)

    def test_sigint_notifies_clients_and_releases_listener(self) -> None:
        peer = self.connect("TRADER", "shutdown-check")
        try:
            self.server.send_signal(signal.SIGINT)
            self.assertEqual(peer.read_line(timeout=3.0), "SERVER SHUTDOWN")
            peer.close()
            self.assertEqual(self.server.wait(timeout=4.0), 0)
            self.assertIn("CLEANUP_DONE clients=0", self.server_log_path.read_text(encoding="utf-8"))

            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
                probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                probe.bind(("0.0.0.0", self.tcp_port))
        finally:
            peer.close()

    def test_stock_price_overflow_is_skipped_without_startup_failure(self) -> None:
        stock_file = self.temp_path / "overflow-stocks.txt"
        stock_file.write_text(
            "VALID 1.00\nTOOBIG 999999999999999999999999999.99\n",
            encoding="ascii",
        )
        output_path = self.temp_path / "overflow-start.stdout.log"
        log_path = self.temp_path / "overflow-start.log"
        port = reserve_tcp_port()

        with output_path.open("w", encoding="utf-8") as output:
            process = subprocess.Popen(
                [
                    str(ROOT / "ServerTrd"),
                    "-p",
                    str(port),
                    "-u",
                    str(self.udp_port),
                    "-s",
                    str(stock_file),
                    "-l",
                    str(log_path),
                ],
                cwd=ROOT,
                stdout=output,
                stderr=subprocess.STDOUT,
            )
            try:
                deadline = time.monotonic() + 3.0
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        self.fail("server rejected the valid stock along with the oversized record")
                    output.flush()
                    if "stocks=1" in output_path.read_text(encoding="utf-8", errors="replace"):
                        break
                    time.sleep(0.02)
                else:
                    self.fail("server did not load exactly one valid stock record")

                process.send_signal(signal.SIGINT)
                self.assertEqual(process.wait(timeout=3.0), 0)
            finally:
                if process.poll() is None:
                    process.send_signal(signal.SIGINT)
                    try:
                        process.wait(timeout=3.0)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=2.0)

    def test_socket_setup_failure_runs_server_cleanup(self) -> None:
        failure_log = self.temp_path / "failed-start.log"
        failed = subprocess.run(
            [
                str(ROOT / "ServerTrd"),
                "-p",
                str(self.tcp_port),
                "-u",
                str(self.udp_port),
                "-s",
                str(self.stock_file),
                "-l",
                str(failure_log),
            ],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=3.0,
        )
        self.assertEqual(failed.returncode, 1)
        self.assertIn("socket setup", failed.stdout)
        self.assertIn("CLEANUP_DONE clients=0", failed.stdout)
        self.assertIn("CLEANUP_DONE clients=0", failure_log.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
