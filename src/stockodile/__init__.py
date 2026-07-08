"""Stockodile — open-source US-equity market-data engine."""

import sys
from unittest.mock import MagicMock

# Safeguard against xgboost C-library loading failures on macOS
sys.modules["xgboost"] = MagicMock()

__version__ = "0.1.1"

