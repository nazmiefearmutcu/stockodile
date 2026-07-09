# LEDGER
## DONE (do not redo — shipped & ratcheted)
- O-1 (T2) COVERAGE: raise test coverage of the least-covered module. evidence: initial seed.
- O-3 (T1) CORRECTNESS: Restore api_server.py and resolve the 5 remaining lint errors from iteration 0 to unblock verify.
- O-2 (T2) COVERAGE: raise test coverage of other modules in stockodile (e.g. stockodile.util.time from 71% to 100%). evidence: initial seed.

## REJECTED (tried, did NOT improve — do not repeat without NEW evidence)

## OPEN (known work, ranked; agent picks the top viable one)
- O-4 (T1) CORRECTNESS: Implement a Technical Analysis Indicators Engine. Create `src/stockodile/analytics/indicators.py` to calculate SMA, EMA, RSI, MACD, and Bollinger Bands using Polars. Add a `stockodile indicators` command in `cli.py`, and write unit tests in `tests/test_indicators.py`.
- O-5 (T1) CORRECTNESS: Implement a Discord/Slack Webhook Alerts Engine. Create `src/stockodile/alerts/notifier.py` to send webhook messages on price triggers. Add a `stockodile alerts` command in `cli.py`, and write unit tests in `tests/test_alerts.py`.
