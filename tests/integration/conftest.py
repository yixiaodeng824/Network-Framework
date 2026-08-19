import signal
import subprocess
import os
import pytest
from tests.common import PORT, wait_for_server, stop_server

SERVER_BINARY = os.getenv("NF_SERVER_BINARY")
if SERVER_BINARY is None:
    pytest.exit("NF_SERVER_BINARY is not set; run tests through CTest")


@pytest.fixture(scope="session", autouse=True)
def running_server():
    process = subprocess.Popen([SERVER_BINARY, "-p", str(PORT)])
    wait_for_server(process)
    yield
    stop_server(process)
