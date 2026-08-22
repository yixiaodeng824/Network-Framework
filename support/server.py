import signal
import socket
import subprocess
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from enum import Enum
from typing import BinaryIO

from support.config import HOST, TIMEOUT

# ========================================
# 服务器生命周期控制
# ========================================


class ServerLogMode(Enum):
    CAPTURE = "capture"
    INHERIT = "inherit"
    DISCARD = "discard"


class RunningServer:
    def __init__(
        self,
        process: subprocess.Popen,
        host: str,
        port: int,
        connect_timeout: float,
        log_file: BinaryIO | None,
    ) -> None:
        self._process = process
        self._connect_timeout = connect_timeout
        self._log_file = log_file
        self._host = host
        self._port = port

    @property
    def address(self) -> tuple[str, int]:
        return self._host, self._port

    def connect(self, timeout: float | None = None) -> socket.socket:
        return socket.create_connection(
            self.address,
            timeout=self._connect_timeout if timeout is None else timeout,
        )

    def stop(self, timeout: float = 3.0) -> int:
        returncode = self._process.poll()
        if returncode is not None:
            return returncode

        self._process.send_signal(signal.SIGINT)
        return self._process.wait(timeout=timeout)

    def _wait_until_ready(self, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            returncode = self._process.poll()
            if returncode is not None:
                raise RuntimeError(
                    self._failure_message(
                        f"server exited before becoming ready with code {returncode}"
                    )
                )

            try:
                with self.connect(timeout=0.1):
                    return
            except OSError:
                time.sleep(0.05)

        raise TimeoutError(
            self._failure_message(
                f"server did not become ready at {self._host}:{self._port} "
                f"within {timeout:.1f}s"
            )
        )

    def _ensure_stopped(self, timeout: float = 3.0) -> None:
        try:
            self.stop(timeout)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait()

    def _failure_message(self, message: str) -> str:
        logs = self._read_logs()
        if not logs:
            return message
        return f"{message}\n--- server output ---\n{logs}"

    def _read_logs(self) -> str:
        if self._log_file is None:
            return ""

        position = self._log_file.tell()
        self._log_file.seek(0)
        output = self._log_file.read().decode("utf-8", errors="replace").strip()
        self._log_file.seek(position)
        return output

    def _close_log(self) -> None:
        if self._log_file is not None:
            self._log_file.close()


class ServerLauncher:
    def __init__(
        self,
        binary: str,
        *,
        host: str = HOST,
        connect_timeout: float = TIMEOUT,
    ) -> None:
        self._binary = binary
        self._host = host
        self._connect_timeout = connect_timeout

    @contextmanager
    def running(
        self,
        *,
        port: int | None = None,
        threads: int | None = None,
        heartbeat_timeout: int | None = None,
        startup_timeout: float = 5.0,
        shutdown_timeout: float = 3.0,
        log_mode: ServerLogMode = ServerLogMode.CAPTURE,
    ) -> Iterator[RunningServer]:
        selected_port = self._available_port() if port is None else port
        command = self._command(selected_port, threads, heartbeat_timeout)
        process, log_file = self._start(command, log_mode)
        server = RunningServer(
            process,
            self._host,
            selected_port,
            self._connect_timeout,
            log_file,
        )

        try:
            server._wait_until_ready(startup_timeout)
            yield server
        except BaseException as exc:
            logs = server._read_logs()
            if logs and hasattr(exc, "add_note"):
                exc.add_note(f"server output:\n{logs}")
            raise
        finally:
            server._ensure_stopped(shutdown_timeout)
            server._close_log()

    def _available_port(self) -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((self._host, 0))
            return sock.getsockname()[1]

    def _command(
        self,
        port: int,
        threads: int | None,
        heartbeat_timeout: int | None,
    ) -> list[str]:
        command = [self._binary, "-p", str(port)]
        if threads is not None:
            command.extend(("-t", str(threads)))
        if heartbeat_timeout is not None:
            command.extend(("-h", str(heartbeat_timeout)))
        return command

    def _start(
        self, command: list[str], log_mode: ServerLogMode
    ) -> tuple[subprocess.Popen, BinaryIO | None]:
        if log_mode is ServerLogMode.INHERIT:
            return subprocess.Popen(command), None
        if log_mode is ServerLogMode.DISCARD:
            return (
                subprocess.Popen(
                    command,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.STDOUT,
                ),
                None,
            )

        log_file = tempfile.TemporaryFile()  # noqa: SIM115 - owned by RunningServer
        try:
            process = subprocess.Popen(
                command,
                stdout=log_file,
                stderr=subprocess.STDOUT,
            )
        except BaseException:
            log_file.close()
            raise
        return process, log_file
