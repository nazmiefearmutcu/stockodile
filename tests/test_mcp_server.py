import sys
from unittest.mock import AsyncMock, MagicMock, patch

# Mock yfinance to prevent importing pandas which is extremely slow in the test environment
mock_yf = MagicMock()
sys.modules["yfinance"] = mock_yf

import pytest
from stockodile.mcp_server import AsyncWeb3, get_base_market_data, get_onchain_price
from typing import Any, Generator

class AwaitableValue:
    def __init__(self, val: Any) -> None:
        self.val = val

    def __await__(self) -> Generator[Any, None, Any]:
        async def _async_val() -> Any:
            if isinstance(self.val, Exception):
                raise self.val
            return self.val

        return _async_val().__await__()


@pytest.mark.asyncio
async def test_async_web3_export() -> None:
    """Verify AsyncWeb3 is exported and can be instantiated."""
    assert AsyncWeb3 is not None
    # Verify we can use it as context manager
    from web3.providers.async_base import AsyncBaseProvider
    mock_provider = MagicMock(spec=AsyncBaseProvider)
    mock_provider.disconnect = AsyncMock()

    async with AsyncWeb3(mock_provider) as w3:
        assert w3 is not None

    mock_provider.disconnect.assert_called_once()


@pytest.mark.asyncio
async def test_get_onchain_price_stock_fallback() -> None:
    """Verify get_onchain_price falls back to yfinance for stock symbols."""
    mock_ticker = MagicMock()
    mock_history = MagicMock()
    mock_history.empty = False
    mock_close = MagicMock()
    mock_close.iloc = [150.0]
    mock_history.__getitem__.side_effect = lambda key: mock_close if key == "Close" else MagicMock()

    mock_ticker.history.return_value = mock_history
    with patch("yfinance.Ticker") as mock_ticker_class:
        mock_ticker_class.return_value = mock_ticker

        res = await get_onchain_price("AAPL")
        assert res["symbol"] == "AAPL"
        assert res["pool_address"] == "equity_feed"
        assert res["price"] == 150.0
        assert res["pool_type"] == "equity_market"


@pytest.mark.asyncio
async def test_get_base_market_data_stock_fallback() -> None:
    """Verify get_base_market_data falls back to yfinance for stock symbols."""
    mock_ticker = MagicMock()
    mock_history = MagicMock()
    mock_history.empty = False

    # mock Close prices and Volumes
    mock_close = MagicMock()
    mock_close.iloc = [150.0]

    mock_vol = MagicMock()
    mock_vol.iloc = [7000000.0]

    mock_history.__getitem__.side_effect = lambda key: mock_close if key == "Close" else (mock_vol if key == "Volume" else MagicMock())

    mock_ticker.history.return_value = mock_history
    with patch("yfinance.Ticker") as mock_ticker_class:
        mock_ticker_class.return_value = mock_ticker

        res = await get_base_market_data("AAPL/USD")
        assert res["symbol"] == "AAPL-USD"
        assert res["price"] == 150.0
        assert res["volume_1h_base"] == 1000000.0
        assert res["volume_1h_quote"] == 150000000.0
