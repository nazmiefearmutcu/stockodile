import sys
from unittest.mock import MagicMock

# Mock yfinance to prevent slow pandas imports and network requests in sandbox env
mock_yf = MagicMock()
mock_ticker = MagicMock()
mock_history = MagicMock()
mock_history.empty = False
mock_close = MagicMock()
mock_close.iloc = [150.0]
mock_history.__getitem__.side_effect = lambda key: mock_close if key == "Close" else MagicMock()
mock_ticker.history.return_value = mock_history
mock_yf.Ticker.return_value = mock_ticker
sys.modules["yfinance"] = mock_yf

import asyncio
from stockodile.mcp_server import AsyncWeb3, get_onchain_price


async def main() -> None:
    print("Testing AsyncWeb3...")
    # Mock AsyncBaseProvider
    from web3.providers.async_base import AsyncBaseProvider
    mock_provider = MagicMock(spec=AsyncBaseProvider)
    async with AsyncWeb3(mock_provider) as w3:
        assert w3 is not None
    print("AsyncWeb3 OK.")

    print("Testing get_onchain_price stock fallback...")
    res = await get_onchain_price("AAPL")
    print("Result:", res)
    assert "price" in res
    assert res["price"] > 0
    print("get_onchain_price stock fallback OK.")

    print("All tests OK!")


if __name__ == "__main__":
    asyncio.run(main())
