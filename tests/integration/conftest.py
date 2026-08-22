import pytest

from support.config import SERVER_BINARY
from support.server import ServerLauncher


@pytest.fixture(scope="session")
def server_launcher() -> ServerLauncher:
    if SERVER_BINARY is None:
        pytest.fail(
            "NF_SERVER_BINARY is not set; run tests through CTest "
            "or export NF_SERVER_BINARY=/path/to/server",
            pytrace=False,
        )
    return ServerLauncher(SERVER_BINARY)


@pytest.fixture(scope="module")
def server(server_launcher: ServerLauncher):
    with server_launcher.running() as running_server:
        yield running_server
