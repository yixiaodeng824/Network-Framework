import signal
import subprocess
import os
import pytest
from tests.common import PORT, wait_for_server, stop_server

SERVER_BINARY = os.getenv("NF_SERVER_BINARY")
if SERVER_BINARY is None:
    pytest.exit("NF_SERVER_BINARY is not set; run tests through CTest")

SERVER_MODE = os.getenv("NF_SERVER_MODE")


@pytest.fixture(scope="session", autouse=True)
def running_server():
    server_args = [SERVER_BINARY, "-p", str(PORT)]
    if SERVER_MODE:
        server_args.extend(["-m", SERVER_MODE])

    process = subprocess.Popen(server_args)
    wait_for_server(process)
    yield
    stop_server(process)
