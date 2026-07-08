import os
import sys
from unittest.mock import MagicMock

# Prevent OpenMP and OpenBLAS multithreading deadlocks/slowness on macOS Apple Silicon
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"

# Safeguard against xgboost C-library loading failures on macOS
sys.modules["xgboost"] = MagicMock()
