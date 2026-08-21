import os

HOST = os.getenv("NF_HOST", "127.0.0.1")
PORT = int(os.getenv("NF_PORT", "8888"))
TIMEOUT = float(os.getenv("NF_TIMEOUT", "3"))
SERVER_BINARY = os.getenv("NF_SERVER_BINARY")
