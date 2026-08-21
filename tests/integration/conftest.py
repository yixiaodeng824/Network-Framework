import pytest

from support.config import PORT, SERVER_BINARY
from support.server import running_server

if SERVER_BINARY is None:
    pytest.exit("NF_SERVER_BINARY is not set; run tests through CTest")


@pytest.fixture(scope="session", autouse=True)
def server():
    with running_server(SERVER_BINARY, "-p", str(PORT)):
        yield
