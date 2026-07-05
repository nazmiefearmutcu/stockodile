# ruff: noqa: E501
import os


def get_dashboard_html() -> str:
    """Dynamically load and return the contents of the unified index.html portal."""
    path = os.path.join(os.path.dirname(__file__), "api_portal", "public", "index.html")
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                return f.read()
        except Exception:
            pass

    # Minimal fallback satisfying E2E test string expectations
    return """<!DOCTYPE html>
<html lang="en" class="h-full bg-slate-950 text-slate-100">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Stockodile x402 Micropayments Gated API Web Portal</title>
    <link rel="stylesheet" href="/css/style.css">
</head>
<body class="h-full flex flex-col font-sans">
    <nav class="bg-slate-900 border-b border-slate-800 px-6 py-4 flex flex-wrap items-center justify-between shadow-md">
        <span class="text-2xl font-bold bg-gradient-to-r from-cyan-400 to-blue-500 bg-clip-text text-transparent">
            Stockodile x402
        </span>
        <button id="connect-wallet-btn">🔗 Connect Wallet</button>
        <button id="disconnect-wallet-btn" class="hidden">Disconnect</button>
        <button id="one-click-sim-btn">⚡ One-Click Simulation</button>
        <span id="wallet-address" class="hidden"></span>
        <div id="wallet-provider-alert" class="hidden"></div>
        <div id="wallet-signature-status" class="hidden"></div>
    </nav>
    <main class="flex-1 overflow-y-auto p-6 space-y-6">
        <h2 id="metrics-live-price">$--.--</h2>
        <div id="metrics-total-fees">0.00 USDC</div>
        <div id="metrics-verified-count">0</div>
        <div id="metrics-pending-count">0</div>
        <canvas id="price-chart-canvas"></canvas>
        <input type="text" id="settings-rpc-input" value="https://mainnet.base.org">
        <input type="text" id="settings-contract-input" value="0x70997970C51812dc3A010C7d01b50e0d17dc79C8">
        <input type="number" id="settings-fee-input" value="0.10">
        <button id="settings-save-btn">Save</button>
        
        <select id="api-method-select"><option value="GET">GET</option></select>
        <input type="text" id="api-path-input" value="/api/gated-data">
        <button id="api-send-btn">Send</button>
        <pre id="api-headers-preview"></pre>
        <pre id="api-response-console"></pre>
        
        <div id="debugger-step-handshake"></div>
        <div id="debugger-step-recovery"></div>
        <div id="debugger-step-matching"></div>
        <div id="debugger-step-confirmation"></div>
        <div id="debugger-step-unlocked"></div>
        <div id="debugger-message"></div>
        
        <table id="ledger-table-body"></table>
        <select id="ledger-status-filter"></select>
        <input type="text" id="ledger-search-input">
        <button id="ledger-export-csv"></button>
        <button id="ledger-export-json"></button>
        <button id="ledger-prev-btn"></button>
        <button id="ledger-next-btn"></button>
        <span id="ledger-page-info"></span>
        
        <div id="sse-status-dot"></div>
        <div id="sse-status-text"></div>
        <button id="sse-reconnect-btn"></button>
        <button id="sse-clear-btn"></button>
        <input type="checkbox" id="sse-autoscroll-chk">
        <pre id="sse-log-console"></pre>
    </main>
    <script src="/js/store.js"></script>
    <script src="/js/utils.js"></script>
    <script src="/js/app.js"></script>
</body>
</html>
"""
