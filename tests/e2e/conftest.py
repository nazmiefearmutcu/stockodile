import os
import sys
from pathlib import Path

# Prevent flock deadlocks on the default shared IPC file
os.environ["CUSTOM_POOLS_IPC_FILE"] = str(
    Path(__file__).parent / f".test_custom_pools_ipc_{os.getpid()}.json"
)

import socket
import subprocess
import time
from collections.abc import AsyncGenerator, Generator

import aiohttp
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent.resolve()))
from mock_rpc_server import start_mock_server


def get_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        return int(s.getsockname()[1])


@pytest.fixture(scope="function")
async def mock_rpc() -> AsyncGenerator[tuple[str, int], None]:
    # Start Mock RPC server on dynamic port (passing 0 allows OS to select a free port atomically)
    runner, actual_port = await start_mock_server(host="127.0.0.1", port=0)
    rpc_url = f"http://127.0.0.1:{actual_port}"

    yield rpc_url, actual_port

    await runner.cleanup()


@pytest.fixture(scope="function")
def api_server(mock_rpc: tuple[str, int], tmp_path: Path) -> Generator[str, None, None]:
    rpc_url, _ = mock_rpc

    max_attempts = 5
    for attempt in range(max_attempts):
        port = get_free_port()

        # Isolate the payment DB file for each test function
        payments_file = tmp_path / f"payments_db_{attempt}.json"

        # Run API server subprocess overriding BASE_RPC_URL and setting PYTHONPATH
        env = os.environ.copy()
        env["BASE_RPC_URL"] = rpc_url
        env["PYTHONPATH"] = os.path.abspath("src") + os.pathsep + os.environ.get("PYTHONPATH", "")
        env["PAYMENTS_FILE"] = str(payments_file)
        # API server runs as a subprocess (no pytest in sys.modules); enable simulation for e2e.
        env["ALLOW_SIMULATION"] = "true"

        proc = subprocess.Popen(
            [
                sys.executable,
                "/Users/nazmi/Desktop/Stockodile/.venv/bin/stockodile_runner.py",
                str(port),
            ],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Wait for FastAPI to start
        start_time = time.time()
        api_url = f"http://127.0.0.1:{port}"
        success = False

        while time.time() - start_time < 45.0:
            if proc.poll() is not None:
                # Server crashed (e.g. port collision), break to try next port
                break
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                    success = True
                    break
            except OSError:
                time.sleep(0.1)

        if success:
            yield api_url
            proc.terminate()
            proc.wait()
            return
        else:
            try:
                proc.terminate()
                proc.wait()
            except Exception:
                pass
    else:
        raise RuntimeError("API server failed to start on any ports after multiple retries.")


@pytest.fixture(scope="function")
def mcp_server_client(mock_rpc: tuple[str, int]) -> Generator[subprocess.Popen[str], None, None]:
    rpc_url, _ = mock_rpc
    env = os.environ.copy()
    env["BASE_RPC_URL"] = rpc_url
    env["PYTHONPATH"] = os.path.abspath("src") + os.pathsep + os.environ.get("PYTHONPATH", "")

    # Run MCP server subprocess (over stdin/stdout) using python code execution
    proc = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import asyncio; from stockodile.mcp_server import serve_stdio; "
                "asyncio.run(serve_stdio())"
            ),
        ],
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )

    # Verify it doesn't crash immediately
    time.sleep(0.5)
    if proc.poll() is not None:
        _stdout, stderr = proc.communicate()
        raise RuntimeError(f"MCP server failed to start. Stderr: {stderr}")

    yield proc

    proc.terminate()
    proc.wait()


@pytest.fixture(autouse=True)
async def clear_mock_rpc_state(mock_rpc: tuple[str, int]) -> None:
    rpc_url, _ = mock_rpc
    # Clear and reset state of mock RPC server between tests
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(f"{rpc_url}/control/reset") as resp:
                await resp.text()
        except Exception:
            pass


def is_localhost_blocked() -> bool:
    return False


def pytest_runtest_setup(item: pytest.Item) -> None:
    test_path = getattr(item, "path", None) or getattr(item, "fspath", None)
    if test_path and "tests/e2e" in str(test_path):
        if is_localhost_blocked():
            pytest.skip("Localhost port binding is blocked.")


@pytest.fixture(scope="session", autouse=True)
def cleanup_test_ipc_file() -> Generator[None, None, None]:
    yield
    ipc_file = os.environ.get("CUSTOM_POOLS_IPC_FILE")
    if ipc_file:
        for path_str in [ipc_file, ipc_file + ".lock", ipc_file + ".tmp"]:
            try:
                os.remove(path_str)
            except Exception:
                pass
