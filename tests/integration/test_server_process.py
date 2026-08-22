import sys

import pytest

from support.protocol import roundtrip
from support.server import ServerLauncher


def test_server_instances_use_independent_ports(
    server_launcher: ServerLauncher,
) -> None:
    with server_launcher.running() as first, server_launcher.running() as second:
        assert first.address != second.address

        with first.connect() as first_socket, second.connect() as second_socket:
            assert roundtrip(first_socket, b"first") == b"first"
            assert roundtrip(second_socket, b"second") == b"second"


def test_startup_failure_includes_process_output() -> None:
    invalid_launcher = ServerLauncher(sys.executable)

    with (
        pytest.raises(RuntimeError) as error,
        invalid_launcher.running(startup_timeout=1.0),
    ):
        pass

    message = str(error.value)
    assert "server exited before becoming ready" in message
    assert "--- server output ---" in message
