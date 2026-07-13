/**
 * Stockodile x402 Micropayments Gated Portal Redesign
 * public/js/app.js
 */

document.addEventListener('DOMContentLoaded', () => {
    // ----------------------------------------------------
    // 1. Application Global State (Reactive Store)
    // ----------------------------------------------------
    let eventSource = null;
    let priceChart = null;
    let priceFeedTimeout = null;
    
    // Global Wallet State
    window.globalWalletState = {
        isConnected: false,
        address: null,
        signer: null,
        subscribers: [],
        subscribe(callback) {
            this.subscribers.push(callback);
            callback(this);
            return () => {
                this.subscribers = this.subscribers.filter(cb => cb !== callback);
            };
        },
        update(isConnected, address = null, signer = null) {
            this.isConnected = isConnected;
            this.address = address;
            this.signer = signer;
            this.subscribers.forEach(cb => cb(this));
        }
    };

    // Backend Detection config
    let apiRoutes = {
        marketData: '/api/gated-data',
        payments: '/api/payments',
        simulate: '/api/payments', // Node.js fallback
        events: '/api/events'
    };
    let isPythonBackend = false;

    // Backward compatibility local variables
    let walletAddress = null;
    let walletSigner = null;
    let isWalletConnected = false;

    // Active simulation details
    let activePaymentId = null;
    let activeFee = "0.10";
    let activeRecipient = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8";
    let activeCurrency = "USD";

    // Gated Content cache
    let activeSession = null; // { path, sender, signature, paymentId, verified, txHash }

    // Ledger filters
    let ledgerSearchQuery = '';
    let ledgerStatus = '';
    let ledgerLimit = 10;
    let ledgerOffset = 0;
    let ledgerSortOrder = 'desc'; 
    let ledgerAmountSort = null;  

    // Chart tick tracker
    const maxChartTicks = 20;
    const chartLabels = [];
    const chartPrices = [];
    let lastPrice = 2096.92;

    // Form dirty state tracker
    let isDirty = false;

    // ----------------------------------------------------
    // 2. DOM Node Accessors
    // ----------------------------------------------------
    const dom = {
        // Navigation Buttons
        connectWalletBtn: document.getElementById('connect-wallet-btn'),
        disconnectWalletBtn: document.getElementById('disconnect-wallet-btn'),
        walletAddressSpan: document.getElementById('wallet-address'),
        walletProviderAlert: document.getElementById('wallet-provider-alert'),
        walletSignatureStatus: document.getElementById('wallet-signature-status'),
        oneClickSimBtn: document.getElementById('one-click-sim-btn'),
        
        // Metrics Displays
        metricsLivePrice: document.getElementById('metrics-live-price'),
        metricsTotalFees: document.getElementById('metrics-total-fees'),
        metricsVerifiedCount: document.getElementById('metrics-verified-count'),
        metricsPendingCount: document.getElementById('metrics-pending-count'),
        
        // Settings Panel
        settingsRpcInput: document.getElementById('settings-rpc-input'),
        settingsContractInput: document.getElementById('settings-contract-input'),
        settingsFeeInput: document.getElementById('settings-fee-input'),
        settingsSaveBtn: document.getElementById('settings-save-btn'),
        
        // API Request Builder
        apiMethodSelect: document.getElementById('api-method-select'),
        apiPathInput: document.getElementById('api-path-input'),
        apiParamsContainer: document.getElementById('api-params-container'),
        apiAddParamBtn: document.getElementById('api-add-param-btn'),
        apiSendBtn: document.getElementById('api-send-btn'),
        apiHeadersPreview: document.getElementById('api-headers-preview'),
        apiResponseConsole: document.getElementById('api-response-console'),
        apiStatusBadge: document.getElementById('api-status-badge'),
        
        // Debugger Stepper Components
        stepHandshake: document.getElementById('debugger-step-handshake'),
        stepRecovery: document.getElementById('debugger-step-recovery'),
        stepMatching: document.getElementById('debugger-step-matching'),
        stepConfirmation: document.getElementById('debugger-step-confirmation'),
        stepUnlocked: document.getElementById('debugger-step-unlocked'),
        debuggerMessage: document.getElementById('debugger-message'),
        
        // Payments Ledger Elements
        ledgerSearchInput: document.getElementById('ledger-search-input'),
        ledgerStatusFilter: document.getElementById('ledger-status-filter'),
        ledgerTableBody: document.getElementById('ledger-table-body'),
        ledgerSortTimestamp: document.getElementById('ledger-sort-timestamp'),
        ledgerSortAmount: document.getElementById('ledger-sort-amount'),
        ledgerPrevBtn: document.getElementById('ledger-prev-btn'),
        ledgerNextBtn: document.getElementById('ledger-next-btn'),
        ledgerPageInfo: document.getElementById('ledger-page-info'),
        ledgerExportJson: document.getElementById('ledger-export-json'),
        ledgerExportCsv: document.getElementById('ledger-export-csv'),
        
        // SSE Event Stream Logger Elements
        sseStatusDot: document.getElementById('sse-status-dot'),
        sseStatusText: document.getElementById('sse-status-text'),
        sseReconnectBtn: document.getElementById('sse-reconnect-btn'),
        sseClearBtn: document.getElementById('sse-clear-btn'),
        sseAutoscrollChk: document.getElementById('sse-autoscroll-chk'),
        sseLogConsole: document.getElementById('sse-log-console'),
    };

    // ----------------------------------------------------
    // 3. UI Helpers: Logs, Stepper & Badges
    // ----------------------------------------------------
    
    function formatTimestamp(dateInput = null) {
        return window.getSyncedTime(dateInput);
    }

    function getCurrentTime(dateInput = null) {
        return window.getSyncedTime(dateInput);
    }

    /** Escape untrusted values before any innerHTML interpolation. */
    function escapeHtml(value) {
        if (typeof window.escapeHtml === 'function') {
            return window.escapeHtml(value);
        }
        if (value == null) return '';
        return String(value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    function mapStageToStep(stage) {
        switch (stage) {
            case 'signature_recovery':
            case 'recovery':
                return 'recovery';
            case 'sender_matching':
            case 'matching':
                return 'matching';
            case 'block_confirmation':
            case 'confirmation':
                return 'confirmation';
            case 'pending':
            case 'handshake':
                return 'handshake';
            case 'payment_received':
            case 'unlocked':
                return 'unlocked';
            default:
                return null;
        }
    }

    function generateAlgorithmicSignature(address, paymentId) {
        return window.generateMockSignature(address, paymentId);
    }

    function generateMockSignature(address, paymentId) {
        return window.generateMockSignature(address, paymentId);
    }

    function logConsole(type, message, status = '', timestamp = null) {
        const timeStr = formatTimestamp(timestamp);
        let colorStyle = 'color: #22D3EE;'; // High-contrast Light Cyan
        let isBold = false;
        
        if (type === 'info') {
            colorStyle = 'color: #22D3EE;';
            isBold = true;
        } else if (type === 'tick') {
            colorStyle = 'color: #34D399;'; // Emerald Green
        } else if (type === 'payment') {
            if (status === 'success') {
                colorStyle = 'color: #22D3EE;';
                isBold = true;
            } else {
                colorStyle = 'color: #FBBF24;'; // Amber
            }
        } else if (type === 'verification') {
            if (status === 'success') {
                colorStyle = 'color: #22D3EE;';
                isBold = true;
            } else if (status === 'failed') {
                colorStyle = 'color: #F87171;'; // Rose Red
                isBold = true;
            } else {
                colorStyle = 'color: #FBBF24;'; // Amber
            }
        }

        const logLine = document.createElement('div');
        logLine.className = `py-0.5 border-b border-slate-900/40 ${isBold ? 'font-bold' : ''}`;
        logLine.setAttribute('style', colorStyle);
        // SSE / payment messages are untrusted — never assign as HTML
        logLine.textContent = `[${timeStr}] [${String(type).toUpperCase()}] ${message == null ? '' : String(message)}`;
        
        if (dom.sseLogConsole) {
            dom.sseLogConsole.appendChild(logLine);
            if (dom.sseAutoscrollChk && dom.sseAutoscrollChk.checked) {
                dom.sseLogConsole.scrollTop = dom.sseLogConsole.scrollHeight;
            }
        }
    }

    function setStepStatus(stepKey, status, message) {
        let node;
        switch (stepKey) {
            case 'handshake': node = dom.stepHandshake; break;
            case 'recovery': node = dom.stepRecovery; break;
            case 'matching': node = dom.stepMatching; break;
            case 'confirmation': node = dom.stepConfirmation; break;
            case 'unlocked': node = dom.stepUnlocked; break;
        }
        if (!node) return;

        node.className = "absolute -left-[29px] w-6 h-6 rounded-full border-4 border-slate-950 flex items-center justify-center text-[9px] font-bold z-10 transition-all shadow-md";
        const stepNumMap = { handshake: 1, recovery: 2, matching: 3, confirmation: 4, unlocked: 5 };

        if (status === 'idle') {
            node.classList.add('bg-slate-800', 'text-slate-400');
            node.innerHTML = stepNumMap[stepKey];
        } else if (status === 'pending') {
            node.classList.add('bg-yellow-500', 'text-slate-950', 'animate-pulse');
            node.innerHTML = `<svg class="animate-spin h-3.5 w-3.5 text-slate-950" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
            </svg>`;
        } else if (status === 'success') {
            node.classList.add('bg-emerald-500', 'text-slate-950');
            node.innerHTML = '✓';
        } else if (status === 'failed') {
            node.classList.add('bg-rose-500', 'text-white');
            node.innerHTML = '✗';
        }

        if (message && dom.debuggerMessage) {
            dom.debuggerMessage.textContent = `[${stepKey.toUpperCase()}] ${message}`;
        }

        if (status === 'success' || status === 'pending') {
            const orderedSteps = ['handshake', 'recovery', 'matching', 'confirmation', 'unlocked'];
            const currentIndex = orderedSteps.indexOf(stepKey);
            for (let i = 0; i < currentIndex; i++) {
                const prevStepKey = orderedSteps[i];
                let prevNode;
                switch (prevStepKey) {
                    case 'handshake': prevNode = dom.stepHandshake; break;
                    case 'recovery': prevNode = dom.stepRecovery; break;
                    case 'matching': prevNode = dom.stepMatching; break;
                    case 'confirmation': prevNode = dom.stepConfirmation; break;
                    case 'unlocked': prevNode = dom.stepUnlocked; break;
                }
                if (prevNode && !prevNode.classList.contains('bg-emerald-500')) {
                    prevNode.className = "absolute -left-[29px] w-6 h-6 rounded-full border-4 border-slate-950 flex items-center justify-center text-[9px] font-bold z-10 transition-all shadow-md bg-emerald-500 text-slate-950";
                    prevNode.innerHTML = '✓';
                }
            }
        }
    }

    function resetDebugger() {
        const steps = ['handshake', 'recovery', 'matching', 'confirmation', 'unlocked'];
        steps.forEach((step, idx) => {
            setStepStatus(step, 'idle');
            let node;
            switch (step) {
                case 'handshake': node = dom.stepHandshake; break;
                case 'recovery': node = dom.stepRecovery; break;
                case 'matching': node = dom.stepMatching; break;
                case 'confirmation': node = dom.stepConfirmation; break;
                case 'unlocked': node = dom.stepUnlocked; break;
            }
            if (node) {
                node.innerHTML = (idx + 1).toString();
            }
        });
        if (dom.debuggerMessage) {
            dom.debuggerMessage.textContent = "Debugger initialized. Ready to record request steps.";
        }
    }

    function syncDebuggerState(paymentId, txHash) {
        if (txHash) {
            setStepStatus('handshake', 'success', 'Gated handshake completed.');
            setStepStatus('recovery', 'success', 'Signature recovered successfully.');
            setStepStatus('matching', 'success', 'Sender address matches headers.');
            setStepStatus('confirmation', 'pending', `Awaiting block confirmations for tx: ${txHash.slice(0, 16)}...`);
            setStepStatus('unlocked', 'idle');
        }
    }

    function updateStatusBadge(statusCode, statusText) {
        if (!dom.apiStatusBadge) return;
        dom.apiStatusBadge.classList.remove('hidden', 'bg-emerald-500/10', 'text-emerald-400', 'border-emerald-500/20', 'bg-amber-500/10', 'text-amber-400', 'border-amber-500/20', 'bg-rose-500/10', 'text-rose-400', 'border-rose-500/20');
        dom.apiStatusBadge.classList.add('inline-flex', 'border', 'px-2', 'py-0.5', 'rounded', 'font-mono', 'text-xs');
        
        dom.apiStatusBadge.textContent = `${statusCode} ${statusText}`;

        if (statusCode >= 200 && statusCode < 300) {
            dom.apiStatusBadge.classList.add('bg-emerald-500/10', 'text-emerald-400', 'border-emerald-500/20');
        } else if (statusCode === 402) {
            dom.apiStatusBadge.classList.add('bg-amber-500/10', 'text-amber-400', 'border-amber-500/20');
        } else {
            dom.apiStatusBadge.classList.add('bg-rose-500/10', 'text-rose-400', 'border-rose-500/20');
        }
    }

    function updateHeadersPreview(headers) {
        if (dom.apiHeadersPreview) {
            dom.apiHeadersPreview.textContent = JSON.stringify(headers, null, 2);
        }
    }

    function updateResponseConsole(payload) {
        if (dom.apiResponseConsole) {
            dom.apiResponseConsole.textContent = JSON.stringify(payload, null, 2);
        }
        
        // Dynamically render a table if python backend returns structured market data
        const renderedContainer = document.getElementById('api-response-rendered');
        if (renderedContainer) {
            renderedContainer.innerHTML = '';
            if (payload && payload.status === 'success' && payload.data && typeof payload.data === 'object') {
                const mdata = payload.data;
                const symbol = escapeHtml(mdata.symbol || 'N/A');
                const pool = escapeHtml(mdata.pool_address || 'N/A');
                const token0 = escapeHtml(mdata.token0 || 'N/A');
                const token1 = escapeHtml(mdata.token1 || 'N/A');
                const price = escapeHtml(mdata.price_usdc || mdata.price || 'N/A');
                const tick = escapeHtml(mdata.tick || '0');
                const liquidity = escapeHtml(mdata.liquidity || 'N/A');
                const tableHtml = `
                    <div class="mt-4 p-4 rounded-lg bg-emerald-950/20 border border-emerald-500/20">
                        <h4 class="text-sm font-semibold text-emerald-400 mb-2">Normalized Market Data Result</h4>
                        <table class="w-full text-left text-xs border-collapse font-mono">
                            <tbody>
                                <tr class="border-b border-slate-800"><td class="py-2 text-slate-400">Asset Symbol</td><td class="py-2 font-bold text-cyan-400">${symbol}</td></tr>
                                <tr class="border-b border-slate-800"><td class="py-2 text-slate-400">Data Source Provider</td><td class="py-2 text-slate-300">${pool}</td></tr>
                                <tr class="border-b border-slate-800"><td class="py-2 text-slate-400">Base Asset</td><td class="py-2 text-slate-400 text-[10px]">${token0}</td></tr>
                                <tr class="border-b border-slate-800"><td class="py-2 text-slate-400">Quote Asset (USD)</td><td class="py-2 text-slate-400 text-[10px]">${token1}</td></tr>
                                <tr class="border-b border-slate-800"><td class="py-2 text-slate-400">Current Stock Price</td><td class="py-2 font-bold text-emerald-400">${price} USD</td></tr>
                                <tr class="border-b border-slate-800"><td class="py-2 text-slate-400">Current Tick / Bid Size</td><td class="py-2 text-slate-300">${tick}</td></tr>
                                <tr class="border-b border-slate-800"><td class="py-2 text-slate-400">Current Trade Volume</td><td class="py-2 text-slate-300">${liquidity}</td></tr>
                            </tbody>
                        </table>
                    </div>
                `;
                renderedContainer.innerHTML = tableHtml;
            } else if (payload && payload.status === 'success' && typeof payload.data === 'string') {
                renderedContainer.innerHTML = `
                    <div class="mt-4 p-4 rounded-lg bg-emerald-950/20 border border-emerald-500/20">
                        <h4 class="text-sm font-semibold text-emerald-400 mb-1">Gated Content Unlocked</h4>
                        <p class="text-xs text-slate-300">${escapeHtml(payload.data)}</p>
                    </div>
                `;
            }
        }
    }

    function updateRequestHeaders() {
        const headers = {};
        const activeAddress = (window.globalWalletState && window.globalWalletState.address) || walletAddress || "0x0000000000000000000000000000000000000000";
        const currentPaymentId = activePaymentId || "a3b04c8f-2879-4d8e-9d22-132d7b5f6390";
        const currentTxHash = (activeSession && activeSession.txHash) || "0x5c5067a6a3b0c801bcbc26759c5d1e2e1d7dc1518f8e811c76a77d7f781dc41b";
        const signature = generateAlgorithmicSignature(activeAddress, currentPaymentId);

        if (isPythonBackend) {
            const headerPayload = {
                payment_id: currentPaymentId,
                tx_hash: currentTxHash,
                signature: signature
            };
            headers['Payment-Signature'] = JSON.stringify(headerPayload);
        } else {
            headers['Payment-Id'] = currentPaymentId;
            headers['Payment-Sender'] = activeAddress;
            headers['Payment-Signature'] = signature;
        }
        
        headers['Authorization'] = `Bearer ${signature}`;
        headers['X-Wallet-Address'] = activeAddress;

        updateRequestHeadersPreview(headers);
    }

    function updateRequestHeadersPreview(headers) {
        if (dom.apiHeadersPreview) {
            dom.apiHeadersPreview.textContent = JSON.stringify(headers, null, 2);
        }
    }

    // ----------------------------------------------------
    // 4. Initializing Chart.js
    // ----------------------------------------------------
    function initChart() {
        const canvas = document.getElementById('price-chart-canvas');
        if (!canvas) return;
        const ctx = canvas.getContext('2d');
        
        priceChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: chartLabels,
                datasets: [{
                    label: 'Stock Price (USD)',
                    borderColor: '#00E5FF', 
                    backgroundColor: 'rgba(0, 229, 255, 0.05)',
                    data: chartPrices,
                    borderWidth: 2,
                    tension: 0.35,
                    fill: true,
                    pointRadius: 2,
                    pointHoverRadius: 6,
                    pointHoverBackgroundColor: '#00E5FF',
                    pointHoverBorderColor: '#020617',
                    pointHoverBorderWidth: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        mode: 'index',
                        intersect: false,
                        backgroundColor: '#0f172a',
                        titleColor: '#94a3b8',
                        bodyColor: '#34d399',
                        borderColor: '#334155',
                        borderWidth: 1,
                        bodyFont: { family: 'monospace' }
                    }
                },
                scales: {
                    x: {
                        grid: { color: 'rgba(255, 255, 255, 0.05)' },
                        ticks: { color: '#64748b', font: { size: 9, family: 'monospace' } }
                    },
                    y: {
                        grid: { color: 'rgba(255, 255, 255, 0.05)' },
                        ticks: { color: '#64748b', font: { size: 9, family: 'monospace' } }
                    }
                }
            }
        });
    }

    function usePriceTimeSeries(intervalMs = 2000, windowSize = 20) {
        let data = window.generateTimeSeriesData(lastPrice, windowSize);
        const listeners = [];

        const intervalId = setInterval(() => {
            const lastEntry = data[data.length - 1] || { price: 2096.92 };
            const maxDeviation = lastEntry.price * 0.0005; 
            const change = (Math.random() - 0.5) * 2 * maxDeviation;
            let nextPrice = lastEntry.price + change;

            if (nextPrice < 1000.0) nextPrice = 1000.0;
            if (nextPrice > 5000.0) nextPrice = 5000.0;

            const nextTime = new Date();
            data = [...data, { time: nextTime, price: parseFloat(nextPrice.toFixed(2)) }];
            if (data.length > windowSize) {
                data.shift();
            }

            listeners.forEach(cb => cb(data));
        }, intervalMs);

        return {
            getData() {
                return data;
            },
            subscribe(cb) {
                listeners.push(cb);
                cb(data);
                return () => {
                    const idx = listeners.indexOf(cb);
                    if (idx > -1) listeners.splice(idx, 1);
                };
            },
            updateExternalPrice(newPrice) {
                const nextTime = new Date();
                data = [...data, { time: nextTime, price: newPrice }];
                if (data.length > windowSize) {
                    data.shift();
                }
                listeners.forEach(cb => cb(data));
            },
            destroy() {
                clearInterval(intervalId);
            }
        };
    }

    window.priceTimeSeriesHook = null;

    function startPriceChartSimulation() {
        if (!window.priceTimeSeriesHook) {
            window.priceTimeSeriesHook = usePriceTimeSeries(2000, maxChartTicks);
            window.priceTimeSeriesHook.subscribe((data) => {
                chartLabels.length = 0;
                chartPrices.length = 0;
                
                data.forEach(item => {
                    chartLabels.push(formatTimestamp(item.time));
                    chartPrices.push(item.price);
                });

                if (data.length > 0) {
                    lastPrice = data[data.length - 1].price;
                    if (!eventSource || eventSource.readyState !== EventSource.OPEN) {
                        if (dom.metricsLivePrice) {
                            dom.metricsLivePrice.textContent = `$${lastPrice.toFixed(2)}`;
                        }
                    }
                }

                if (priceChart) {
                    priceChart.update('none');
                }
            });
        }
    }

    function stopPriceChartSimulation() {
        if (window.priceTimeSeriesHook) {
            window.priceTimeSeriesHook.destroy();
            window.priceTimeSeriesHook = null;
        }
    }

    // ----------------------------------------------------
    // 5. Settings Panel Actions
    // ----------------------------------------------------
    let initialSettings = { rpc: '', contract: '', fee: '' };

    function loadSettings() {
        const savedRpc = localStorage.getItem('x402_rpc_endpoint') || "https://api.stockodile.org";
        const savedContract = localStorage.getItem('x402_contract_address') || "0x70997970C51812dc3A010C7d01b50e0d17dc79C8";
        const savedFee = localStorage.getItem('x402_gated_fee') || "0.10";

        if (dom.settingsRpcInput) dom.settingsRpcInput.value = savedRpc;
        if (dom.settingsContractInput) dom.settingsContractInput.value = savedContract;
        if (dom.settingsFeeInput) dom.settingsFeeInput.value = savedFee;

        initialSettings = { rpc: savedRpc, contract: savedContract, fee: savedFee };
    }

    function checkSettingsChanged() {
        if (!dom.settingsSaveBtn) return;
        const rpcVal = dom.settingsRpcInput ? dom.settingsRpcInput.value.trim() : '';
        const contractVal = dom.settingsContractInput ? dom.settingsContractInput.value.trim() : '';
        const feeVal = dom.settingsFeeInput ? dom.settingsFeeInput.value.trim() : '';

        isDirty = (rpcVal !== initialSettings.rpc || contractVal !== initialSettings.contract || feeVal !== initialSettings.fee);
        dom.settingsSaveBtn.disabled = !isDirty;
    }

    if (dom.settingsSaveBtn) {
        dom.settingsSaveBtn.addEventListener('click', () => {
            const rpcVal = dom.settingsRpcInput.value.trim();
            const contractVal = dom.settingsContractInput.value.trim();
            const feeVal = dom.settingsFeeInput.value.trim();

            localStorage.setItem('x402_rpc_endpoint', rpcVal);
            localStorage.setItem('x402_contract_address', contractVal);
            localStorage.setItem('x402_gated_fee', feeVal);

            initialSettings = { rpc: rpcVal, contract: contractVal, fee: feeVal };
            checkSettingsChanged();

            logConsole('info', 'Portal configuration parameters successfully updated in localStorage.');
            alert('Configurations saved successfully!');
        });
    }

    // ----------------------------------------------------
    // 6. Query Parameters Dynamic Form Builder
    // ----------------------------------------------------
    if (dom.apiAddParamBtn) {
        dom.apiAddParamBtn.addEventListener('click', () => {
            const row = document.createElement('div');
            row.className = 'flex items-center space-x-2 param-row mt-2';
            row.innerHTML = `
                <input type="text" placeholder="Key" class="w-1/3 bg-slate-950 border border-slate-800 rounded px-2 py-1 text-xs font-mono text-slate-200 focus:outline-none focus:border-cyan-500">
                <input type="text" placeholder="Value" class="w-1/2 bg-slate-950 border border-slate-800 rounded px-2 py-1 text-xs font-mono text-slate-200 focus:outline-none focus:border-cyan-500">
                <button class="remove-param-btn text-rose-500 hover:text-rose-400 text-xs px-2 py-1 font-semibold">Remove</button>
            `;
            
            row.querySelector('.remove-param-btn').addEventListener('click', () => {
                row.remove();
            });
            dom.apiParamsContainer.appendChild(row);
        });
    }

    function getQueryParameters() {
        if (!dom.apiParamsContainer) return '';
        const rows = dom.apiParamsContainer.querySelectorAll('.param-row');
        const urlParams = new URLSearchParams();
        rows.forEach(row => {
            const inputs = row.querySelectorAll('input');
            const key = inputs[0].value.trim();
            const val = inputs[1].value.trim();
            if (key) {
                urlParams.append(key, val);
            }
        });
        return urlParams.toString();
    }

    // ----------------------------------------------------
    // 7. Server-Sent Events (SSE) Client
    // ----------------------------------------------------
    function connectSSE() {
        if (eventSource) {
            eventSource.close();
        }
        if (priceFeedTimeout) {
            clearTimeout(priceFeedTimeout);
        }

        const loadingOverlay = document.getElementById('chart-loading-overlay');
        if (loadingOverlay) {
            loadingOverlay.classList.remove('hidden');
        }

        if (dom.sseStatusDot) {
            dom.sseStatusDot.className = "w-2.5 h-2.5 rounded-full bg-amber-500 shadow-[0_0_8px_rgba(245,158,11,0.6)] animate-pulse transition-all";
        }
        if (dom.sseStatusText) {
            dom.sseStatusText.textContent = "Connecting...";
        }

        const sseConnectionPromise = new Promise((resolve, reject) => {
            try {
                eventSource = new EventSource(apiRoutes.events);
            } catch (err) {
                reject(err);
                return;
            }

            priceFeedTimeout = setTimeout(() => {
                reject(new Error("Price Feed Connection Timeout"));
            }, 5000);

            eventSource.onopen = () => {
                if (dom.sseStatusDot) {
                    dom.sseStatusDot.className = "w-2.5 h-2.5 rounded-full bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.8)] transition-all";
                }
                if (dom.sseStatusText) {
                    dom.sseStatusText.textContent = "Connected";
                }
                logConsole('info', 'SSE connection to /api/events successfully established.');
                stopPriceChartSimulation();
            };

            eventSource.onerror = () => {
                if (eventSource.readyState === EventSource.CLOSED) {
                    reject(new Error("No Data Available"));
                } else {
                    if (dom.sseStatusDot) {
                        dom.sseStatusDot.className = "w-2.5 h-2.5 rounded-full bg-amber-500 shadow-[0_0_8px_rgba(245,158,11,0.6)] animate-pulse transition-all";
                    }
                    if (dom.sseStatusText) {
                        dom.sseStatusText.textContent = "Reconnecting...";
                    }
                    logConsole('verification', 'SSE connection lost. Reconnecting...', 'failed');
                    startPriceChartSimulation();
                }
            };

            eventSource.onmessage = (event) => {
                let payload;
                try {
                    payload = JSON.parse(event.data);
                } catch (e) {
                    console.error("Malformed SSE data", e);
                    return;
                }

                let eventTimestamp = null;
                if (payload.data && payload.data.timestamp) {
                    eventTimestamp = payload.data.timestamp;
                }

                logConsole(payload.type, payload.message, payload.status, eventTimestamp);

                switch (payload.type) {
                    case 'tick':
                        if (payload.data && payload.data.price) {
                            if (priceFeedTimeout) {
                                clearTimeout(priceFeedTimeout);
                                priceFeedTimeout = null;
                            }
                            resolve(payload);
                            
                            const newPrice = parseFloat(payload.data.price);
                            if (window.priceTimeSeriesHook) {
                                window.priceTimeSeriesHook.updateExternalPrice(newPrice);
                            }
                            if (dom.metricsLivePrice) {
                                dom.metricsLivePrice.textContent = `$${newPrice.toFixed(2)}`;
                            }
                        }
                        break;

                    case 'payment':
                        fetchLedger();
                        if (payload.data && payload.data.payment_id === activePaymentId) {
                            if (payload.stage === 'payment_received') {
                                setStepStatus('unlocked', 'success', 'Payment settled. Gated content ready.');
                            } else if (payload.data.txHash) {
                                syncDebuggerState(payload.data.payment_id, payload.data.txHash);
                            } else if (payload.stage === 'pending') {
                                setStepStatus('handshake', 'success', 'Gated handshake completed.');
                            }
                        }
                        break;

                    case 'verification':
                        if (payload.data && payload.data.payment_id === activePaymentId) {
                            const stepKey = mapStageToStep(payload.stage);
                            if (stepKey) {
                                setStepStatus(stepKey, payload.status, payload.message);
                            }
                        }
                        break;
                }
            };
        });

        sseConnectionPromise
            .then((firstPayload) => {
                if (loadingOverlay) {
                    loadingOverlay.classList.add('hidden');
                }
                if (firstPayload && firstPayload.data) {
                    lastPrice = parseFloat(firstPayload.data.price);
                    if (dom.metricsLivePrice) {
                        dom.metricsLivePrice.textContent = `$${lastPrice.toFixed(2)}`;
                    }
                }
            })
            .catch((err) => {
                if (priceFeedTimeout) {
                    clearTimeout(priceFeedTimeout);
                    priceFeedTimeout = null;
                }
                if (eventSource) {
                    eventSource.close();
                }
                if (dom.sseStatusDot) {
                    dom.sseStatusDot.className = "w-2.5 h-2.5 rounded-full bg-rose-500 shadow-[0_0_8px_rgba(244,63,94,0.8)] transition-all";
                }
                if (dom.sseStatusText) {
                    dom.sseStatusText.textContent = `Disconnected: ${err.message}`;
                }
                
                if (dom.metricsLivePrice) {
                    dom.metricsLivePrice.textContent = `$${lastPrice.toFixed(2)}`;
                }

                if (loadingOverlay) {
                    loadingOverlay.classList.add('hidden');
                }
                logConsole('verification', `Error: ${err.message}`, 'failed');
                startPriceChartSimulation();
            });
    }

    // ----------------------------------------------------
    // 8. Web3 Connect Wallet Functionality
    // ----------------------------------------------------
    function setWalletConnection(connected, address = null, signer = null) {
        if (window.globalWalletState) {
            window.globalWalletState.update(connected, address, signer);
        }
    }

    if (window.globalWalletState) {
        window.globalWalletState.subscribe((state) => {
            isWalletConnected = state.isConnected;
            walletAddress = state.address;
            walletSigner = state.signer;

            if (state.isConnected && state.address) {
                const maskedAddress = state.address.slice(0, 6) + '...' + state.address.slice(-4);
                if (dom.connectWalletBtn) {
                    dom.connectWalletBtn.textContent = `🔗 ${maskedAddress}`;
                    dom.connectWalletBtn.className = "bg-cyan-900/60 text-cyan-300 border border-cyan-500/30 text-xs font-bold px-4 py-2.5 rounded-xl transition-all flex items-center gap-1 cursor-default";
                }
                
                if (dom.disconnectWalletBtn) {
                    dom.disconnectWalletBtn.classList.remove('hidden');
                }
                if (dom.walletAddressSpan) {
                    dom.walletAddressSpan.classList.remove('hidden');
                    dom.walletAddressSpan.textContent = state.address;
                }
                if (dom.walletProviderAlert) {
                    dom.walletProviderAlert.classList.remove('hidden');
                    dom.walletProviderAlert.textContent = "Wallet: Connected";
                    dom.walletProviderAlert.className = "text-xs text-emerald-400 font-semibold px-2";
                }
                logConsole('info', `Active Wallet Updated: Connected - ${state.address}`);
            } else {
                if (dom.connectWalletBtn) {
                    dom.connectWalletBtn.textContent = `🔗 Connect Wallet`;
                    dom.connectWalletBtn.className = "bg-gradient-to-r from-cyan-600 to-sky-500 hover:from-cyan-500 hover:to-sky-400 text-white text-xs font-bold px-4 py-2.5 rounded-xl shadow-lg hover:shadow-cyan-500/20 active:scale-95 transition-all flex items-center gap-1";
                }
                
                if (dom.disconnectWalletBtn) {
                    dom.disconnectWalletBtn.classList.add('hidden');
                }
                if (dom.walletAddressSpan) {
                    dom.walletAddressSpan.classList.add('hidden');
                    dom.walletAddressSpan.textContent = '';
                }
                if (dom.walletProviderAlert) {
                    dom.walletProviderAlert.classList.add('hidden');
                    dom.walletProviderAlert.textContent = '';
                }
                if (dom.walletSignatureStatus) {
                    dom.walletSignatureStatus.classList.add('hidden');
                    dom.walletSignatureStatus.textContent = '';
                }
                logConsole('info', 'Active Wallet Updated: Disconnected');
            }
            updateRequestHeaders();
            
            if (dom.ledgerTableBody) {
                fetchLedger();
            }
        });
    }

    if (dom.connectWalletBtn) {
        dom.connectWalletBtn.addEventListener('click', async () => {
            if (isWalletConnected) return; 

            if (typeof window.ethereum === 'undefined') {
                alert('Stock Broker Wallet (or Ethereum browser wallet) not detected. Install MetaMask or click ⚡ One-Click Simulation to use an ephemeral client identity.');
                logConsole('verification', 'Connector error: window.ethereum is undefined.', 'failed');
                return;
            }

            try {
                logConsole('info', 'Connecting Web3 broker wallet provider...');
                dom.connectWalletBtn.disabled = true;
                dom.connectWalletBtn.textContent = 'Connecting...';

                const accounts = await window.ethereum.request({ method: 'eth_requestAccounts' });
                const address = accounts[0];

                const provider = new ethers.BrowserProvider(window.ethereum);
                const signer = await provider.getSigner();

                setWalletConnection(true, address, signer);
                logConsole('info', `Successfully linked wallet address: ${address}`);
            } catch (err) {
                console.error(err);
                logConsole('verification', `Connection rejected: ${err.message}`, 'failed');
                setWalletConnection(false);
                alert(`Wallet link failed: ${err.message}`);
            } finally {
                dom.connectWalletBtn.disabled = false;
            }
        });
    }

    if (dom.disconnectWalletBtn) {
        dom.disconnectWalletBtn.addEventListener('click', () => {
            setWalletConnection(false);
            logConsole('info', 'Wallet disconnected successfully.');
        });
    }

    // ----------------------------------------------------
    // 9. Interactive API Send & Verification Protocol
    // ----------------------------------------------------
    if (dom.apiSendBtn) {
        dom.apiSendBtn.addEventListener('click', async () => {
            const method = dom.apiMethodSelect ? dom.apiMethodSelect.value : 'GET';
            const path = dom.apiPathInput ? dom.apiPathInput.value : apiRoutes.marketData;
            const params = getQueryParameters();
            
            let url = path;
            if (params) {
                url += `?${params}`;
            }

            dom.apiSendBtn.disabled = true;
            dom.apiSendBtn.textContent = 'Sending...';
            if (dom.apiResponseConsole) dom.apiResponseConsole.textContent = 'Executing network query...';
            if (dom.apiStatusBadge) dom.apiStatusBadge.className = 'hidden';

            try {
                const headers = {};
                const activeAddress = walletAddress || "0x0000000000000000000000000000000000000000";
                const currentPaymentId = activePaymentId || "a3b04c8f-2879-4d8e-9d22-132d7b5f6390";
                const currentTxHash = (activeSession && activeSession.txHash) || "0x5c5067a6a3b0c801bcbc26759c5d1e2e1d7dc1518f8e811c76a77d7f781dc41b";
                const signature = generateMockSignature(activeAddress, currentPaymentId);

                // Build correct verification headers based on environment
                if (activeSession && activeSession.path === path && activeSession.verified) {
                    if (isPythonBackend) {
                        headers['Payment-Signature'] = JSON.stringify({
                            payment_id: activeSession.paymentId,
                            tx_hash: activeSession.txHash,
                            signature: activeSession.signature
                        });
                    } else {
                        headers['Payment-Sender'] = activeSession.sender;
                        headers['Payment-Signature'] = activeSession.signature;
                        headers['Payment-Id'] = activeSession.paymentId;
                    }
                    headers['Authorization'] = `Bearer ${activeSession.signature}`;
                    headers['X-Wallet-Address'] = activeSession.sender;
                } else if (isWalletConnected && walletAddress) {
                    if (isPythonBackend) {
                        headers['Payment-Signature'] = JSON.stringify({
                            payment_id: currentPaymentId,
                            tx_hash: currentTxHash,
                            signature: signature
                        });
                    } else {
                        headers['Payment-Id'] = currentPaymentId;
                        headers['Payment-Sender'] = walletAddress;
                        headers['Payment-Signature'] = signature;
                    }
                    headers['Authorization'] = `Bearer ${signature}`;
                    headers['X-Wallet-Address'] = walletAddress;
                }

                updateRequestHeaders();

                const res = await fetch(url, { method, headers });
                
                updateStatusBadge(res.status, res.statusText);
                const body = await res.json();
                updateResponseConsole(body);

                if (res.status === 402) {
                    activePaymentId = body.payment_id;
                    if (body.payment_required) {
                        activeFee = body.payment_required.price;
                        activeRecipient = body.payment_required.recipient;
                        activeCurrency = body.payment_required.currency;
                    } else {
                        activeFee = body.fee || "0.10";
                        activeRecipient = body.recipient || "0x70997970C51812dc3A010C7d01b50e0d17dc79C8";
                        activeCurrency = body.currency || "USD";
                    }

                    resetDebugger();
                    setStepStatus('handshake', 'pending', `Initial 402 Handshake generated payment_id: ${activePaymentId}`);
                    logConsole('payment', `Gated Resource returned 402. Payment is required: ${activeFee} ${activeCurrency} to ${activeRecipient}`, 'pending');

                    if (walletSigner && walletAddress) {
                        const pay = confirm(`Request requires payment of ${activeFee} ${activeCurrency}.\nSign payment ID: ${activePaymentId} using connected wallet?`);
                        if (pay) {
                            await executeWeb3PaymentFlow();
                        }
                    } else {
                        const sim = confirm(`No Web3 wallet is connected.\nWould you like to auto-trigger the ⚡ One-Click Simulation flow instead?`);
                        if (sim) {
                            await executeSimulationFlow(activePaymentId, activeFee, activeRecipient, activeCurrency);
                        }
                    }
                } else if (res.status === 200) {
                    logConsole('payment', 'Micropayments payload successfully resolved. Content unlocked.', 'success');
                    if (activePaymentId) {
                        setStepStatus('unlocked', 'success', 'Authorized access verified!');
                    }
                } else {
                    logConsole('verification', `HTTP request finished with status code: ${res.status}`, 'failed');
                }
            } catch (err) {
                console.error(err);
                updateResponseConsole({ error: err.message });
                logConsole('verification', `Request failed: ${err.message}`, 'failed');
            } finally {
                dom.apiSendBtn.disabled = false;
                dom.apiSendBtn.textContent = 'Send Request';
            }
        });
    }

    async function executeWeb3PaymentFlow() {
        try {
            setStepStatus('recovery', 'pending', 'Awaiting wallet cryptographic signature...');
            logConsole('verification', 'Requesting message signature from connected wallet...', 'pending');

            const signature = await walletSigner.signMessage(activePaymentId);
            const mockTxHash = '0x' + Array.from({length: 64}, () => Math.floor(Math.random()*16).toString(16)).join('');

            activeSession = {
                path: dom.apiPathInput.value,
                sender: walletAddress,
                signature: signature,
                paymentId: activePaymentId,
                verified: true,
                txHash: mockTxHash
            };
            updateRequestHeaders();

            setStepStatus('recovery', 'success', 'Cryptographic signature captured.');
            logConsole('verification', `Signature: ${signature.slice(0, 20)}...`, 'success');

            setStepStatus('matching', 'pending', 'Broadcasting mock USD transfer on-chain...');
            logConsole('payment', 'Simulating payment transaction receipt to ledger...', 'pending');

            const postUrl = isPythonBackend ? '/api/v1/simulate-payment' : '/api/payments';
            // Client may only register pending metadata; server sets verified on gated-data confirm
            const postBody = isPythonBackend ? {
                payment_id: activePaymentId,
                tx_hash: mockTxHash,
                signature: signature
            } : {
                payment_id: activePaymentId,
                status: 'pending',
                sender: walletAddress,
                recipient: activeRecipient,
                amount: activeFee,
                currency: activeCurrency,
                txHash: mockTxHash,
                signature: signature
            };

            const postRes = await fetch(postUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(postBody)
            });

            if (!postRes.ok) {
                throw new Error('On-chain simulation rejected by payments ledger.');
            }

            logConsole('payment', `Transaction registered as pending. Hash: ${mockTxHash.slice(0, 20)}...`, 'success');

            setStepStatus('confirmation', 'pending', 'Sending authorized query to gated API...');
            logConsole('verification', 'Re-executing gated API endpoint with security headers...', 'pending');
            
            setTimeout(async () => {
                if (dom.apiSendBtn) dom.apiSendBtn.click();
            }, 1200);

        } catch (err) {
            console.error(err);
            setStepStatus('recovery', 'failed', err.message);
            logConsole('verification', `Cryptographic signing failed: ${err.message}`, 'failed');
        }
    }

    // ----------------------------------------------------
    // 10. One-Click Simulation Flow
    // ----------------------------------------------------
    if (dom.oneClickSimBtn) {
        dom.oneClickSimBtn.addEventListener('click', async () => {
            await executeSimulationFlow();
        });
    }

    async function executeSimulationFlow(paymentId = null, fee = "0.10", recipient = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8", currency = "USD") {
        logConsole('info', '⚡ Initializing automated One-Click Gated API simulation...');
        resetDebugger();

        try {
            if (!paymentId) {
                setStepStatus('handshake', 'pending', 'Calling Gated resource to retrieve payment challenge...');
                logConsole('payment', 'Querying gated resource challenge...', 'pending');

                const challengeUrl = isPythonBackend ? '/api/v1/market-data?symbol=AAPL' : '/api/gated-data';
                const initRes = await fetch(challengeUrl);
                if (initRes.status !== 402) {
                    throw new Error(`Expected handshake response 402, got: ${initRes.status}`);
                }
                const challenge = await initRes.json();
                paymentId = challenge.payment_id;
                
                if (challenge.payment_required) {
                    fee = challenge.payment_required.price;
                    recipient = challenge.payment_required.recipient;
                    currency = challenge.payment_required.currency;
                } else {
                    fee = challenge.fee || "0.10";
                    recipient = challenge.recipient || "0x70997970C51812dc3A010C7d01b50e0d17dc79C8";
                    currency = challenge.currency || "USD";
                }
                
                activePaymentId = paymentId;
                updateResponseConsole(challenge);
                updateStatusBadge(402, 'Payment Required');
            }

            setStepStatus('handshake', 'success', `UUID Handshake completed: ${paymentId}`);
            logConsole('payment', `Received Payment ID Challenge: ${paymentId}`, 'success');

            setStepStatus('recovery', 'pending', 'Instantiating ephemeral client identity...');
            logConsole('verification', 'Generating secure client mock keypair using ethers.Wallet...', 'pending');
            
            const mockWallet = ethers.Wallet.createRandom();
            const mockAddress = mockWallet.address;
            logConsole('info', `Simulated client address: ${mockAddress}`);
            
            setWalletConnection(true, mockAddress, mockWallet);

            const signature = await mockWallet.signMessage(paymentId);
            const mockTxHash = '0x' + Array.from({length: 64}, () => Math.floor(Math.random()*16).toString(16)).join('');
            
            activeSession = {
                path: isPythonBackend ? '/api/v1/market-data' : '/api/gated-data',
                sender: mockAddress,
                signature: signature,
                paymentId: paymentId,
                verified: true,
                txHash: mockTxHash
            };
            updateRequestHeaders();

            setStepStatus('recovery', 'success', 'Mock message signed successfully.');
            logConsole('verification', `Recoverable signature generated: ${signature.slice(0, 16)}...`, 'success');

            setStepStatus('matching', 'pending', 'Posting ledger verification status...');
            logConsole('payment', 'Submitting transaction data to register verified payment...', 'pending');
            
            const postUrl = isPythonBackend ? '/api/v1/simulate-payment' : '/api/payments';
            // Client registers pending only; GET /api/gated-data elevates to verified server-side
            const postBody = isPythonBackend ? {
                payment_id: paymentId,
                tx_hash: mockTxHash,
                signature: signature
            } : {
                payment_id: paymentId,
                status: 'pending',
                sender: mockAddress,
                recipient: recipient,
                amount: fee,
                currency: currency,
                txHash: mockTxHash,
                signature: signature
            };

            const postRes = await fetch(postUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(postBody)
            });

            if (!postRes.ok) {
                throw new Error('Server rejected simulated payment registration.');
            }

            logConsole('payment', `Simulated ledger payment registered (pending). Hash: ${mockTxHash.slice(0, 16)}...`, 'success');

            setStepStatus('confirmation', 'pending', 'Awaiting block validation...');
            logConsole('verification', 'Hitting GET endpoint with custom authentication headers...', 'pending');

            const headers = {};
            if (isPythonBackend) {
                headers['Payment-Signature'] = JSON.stringify({
                    payment_id: paymentId,
                    tx_hash: mockTxHash,
                    signature: signature
                });
            } else {
                headers['Payment-Sender'] = mockAddress,
                headers['Payment-Signature'] = signature,
                headers['Payment-Id'] = paymentId;
            }
            
            headers['Authorization'] = `Bearer ${signature}`;
            headers['X-Wallet-Address'] = mockAddress;
            
            updateRequestHeadersPreview(headers);

            const finalUrl = isPythonBackend ? `/api/v1/market-data?symbol=AAPL` : `/api/gated-data`;
            const finalRes = await fetch(finalUrl, {
                method: 'GET',
                headers: headers
            });

            updateStatusBadge(finalRes.status, finalRes.statusText);
            const finalBody = await finalRes.json();
            updateResponseConsole(finalBody);

            if (finalRes.status === 200) {
                setStepStatus('unlocked', 'success', 'Access granted! Content successfully unlocked.');
                logConsole('payment', 'Simulation completed! Premium gated dataset unlocked.', 'success');
            } else {
                throw new Error(`Server returned error ${finalRes.status}: ${finalBody.error || finalBody.detail}`);
            }

        } catch (err) {
            console.error(err);
            setStepStatus('unlocked', 'failed', err.message);
            logConsole('verification', `Simulation failure: ${err.message}`, 'failed');
            alert(`Simulation failed: ${err.message}`);
        }
    }

    // ----------------------------------------------------
    // 11. Payments Ledger Table Actions
    // ----------------------------------------------------
    async function fetchLedger() {
        if (!dom.ledgerTableBody) return;
        try {
            let res;
            if (isPythonBackend) {
                res = await fetch(apiRoutes.payments);
            } else {
                const urlParams = new URLSearchParams();
                if (ledgerSearchQuery) urlParams.append('search', ledgerSearchQuery);
                if (ledgerStatus) urlParams.append('status', ledgerStatus);
                urlParams.append('limit', ledgerLimit);
                urlParams.append('offset', ledgerOffset);
                urlParams.append('sort', ledgerSortOrder);
                res = await fetch(`${apiRoutes.payments}?${urlParams.toString()}`);
            }

            if (!res.ok) throw new Error('Failed to retrieve ledger data');
            const data = await res.json();

            let paymentsList = [];
            let totalCount = 0;

            if (isPythonBackend) {
                const keys = Object.keys(data);
                paymentsList = keys.map(pid => {
                    const rec = data[pid];
                    return {
                        payment_id: pid,
                        status: rec.status === 'paid' ? 'verified' : 'pending',
                        sender: rec.sender || null,
                        recipient: rec.recipient || '0x70997970C51812dc3A010C7d01b50e0d17dc79C8',
                        amount: rec.price || '0.001',
                        currency: rec.currency || 'USD',
                        txHash: rec.tx_hash || null,
                        timestamp: rec.timestamp || new Date().toISOString(),
                        signature: rec.signature || null,
                        symbol: rec.symbol || 'N/A'
                    };
                });

                // Apply client-side filters for python backend
                if (ledgerSearchQuery) {
                    const query = ledgerSearchQuery.toLowerCase();
                    paymentsList = paymentsList.filter(p => 
                        p.payment_id.toLowerCase().includes(query) ||
                        (p.sender && p.sender.toLowerCase().includes(query)) ||
                        (p.txHash && p.txHash.toLowerCase().includes(query))
                    );
                }
                if (ledgerStatus) {
                    paymentsList = paymentsList.filter(p => p.status === ledgerStatus);
                }

                // Apply client-side sort
                paymentsList.sort((a, b) => {
                    const timeA = new Date(a.timestamp).getTime();
                    const timeB = new Date(b.timestamp).getTime();
                    return ledgerSortOrder === 'asc' ? timeA - timeB : timeB - timeA;
                });

                totalCount = paymentsList.length;
                paymentsList = paymentsList.slice(ledgerOffset, ledgerOffset + ledgerLimit);
            } else {
                paymentsList = data.payments || [];
                totalCount = data.total || 0;
            }

            renderLedgerTable(paymentsList, totalCount);
            updateMetrics(paymentsList);
        } catch (err) {
            console.error(err);
            dom.ledgerTableBody.innerHTML = `<tr><td colspan="6" class="py-4 text-center text-rose-500 font-bold">Error loading ledger records.</td></tr>`;
        }
    }

    function getDeterministicMockDetails(paymentId) {
        let hashVal = 0;
        for (let i = 0; i < paymentId.length; i++) {
            hashVal = (hashVal << 5) - hashVal + paymentId.charCodeAt(i);
            hashVal |= 0;
        }
        
        let addressHex = '0xf39F';
        let tempVal = Math.abs(hashVal);
        for (let i = 0; i < 9; i++) {
            tempVal = (tempVal * 16807) % 2147483647;
            addressHex += (tempVal % 16).toString(16);
        }
        while (addressHex.length < 42) {
            tempVal = (tempVal * 16807) % 2147483647;
            addressHex += (tempVal % 16).toString(16);
        }
        
        let txHash = '0x';
        tempVal = Math.abs(hashVal + 1);
        for (let i = 0; i < 64; i++) {
            tempVal = (tempVal * 16807) % 2147483647;
            txHash += (tempVal % 16).toString(16);
        }
        
        return {
            sender: addressHex,
            txHash: txHash
        };
    }

    function renderLedgerTable(payments, totalCount) {
        if (!dom.ledgerTableBody) return;
        dom.ledgerTableBody.innerHTML = '';
        
        if (ledgerAmountSort) {
            payments.sort((a, b) => {
                const amtA = parseFloat(a.amount);
                const amtB = parseFloat(b.amount);
                return (ledgerAmountSort === 'asc') ? amtA - amtB : amtB - amtA;
            });
        }

        if (payments.length === 0) {
            dom.ledgerTableBody.innerHTML = `<tr><td colspan="6" class="py-4 text-center text-slate-500">No payment records found.</td></tr>`;
            if (dom.ledgerPageInfo) dom.ledgerPageInfo.textContent = 'Page 1 of 1';
            if (dom.ledgerPrevBtn) dom.ledgerPrevBtn.disabled = true;
            if (dom.ledgerNextBtn) dom.ledgerNextBtn.disabled = true;
            return;
        }

        payments.forEach(p => {
            const tr = document.createElement('tr');
            tr.className = 'hover:bg-slate-950/40 border-b border-slate-900/60 transition-colors';
            
            const badgeClass = p.status === 'verified' 
                ? 'bg-emerald-500/10 text-emerald-400 border border-emerald-500/20' 
                : 'bg-amber-500/10 text-amber-500 border border-amber-500/20';

            let sender = p.sender;
            let txHash = p.txHash;
            if (p.status === 'pending') {
                const mocks = getDeterministicMockDetails(p.payment_id);
                if (!sender) sender = mocks.sender;
                if (!txHash) txHash = mocks.txHash;
            }

            const senderText = sender ? `${sender.slice(0, 6)}...${sender.slice(-4)}` : 'N/A';
            const txHashText = txHash ? `${txHash.slice(0, 8)}...${txHash.slice(-6)}` : 'N/A';
            const safePaymentId = escapeHtml(p.payment_id);
            const safePaymentIdShort = escapeHtml(String(p.payment_id).slice(0, 8));
            const safeSender = escapeHtml(sender);
            const safeSenderText = escapeHtml(senderText);
            const safeTxHash = escapeHtml(txHash);
            const safeTxHashText = escapeHtml(txHashText);
            const safeAmount = escapeHtml(p.amount);
            const safeCurrency = escapeHtml(p.currency);
            const safeStatus = escapeHtml(p.status);
            const safeTimestamp = escapeHtml(getCurrentTime(p.timestamp));
            // Encode for URL path segments so href cannot break out of attribute
            const senderHref = sender ? `https://stockodile.org/address/${encodeURIComponent(sender)}` : '';
            const txHref = txHash ? `https://stockodile.org/tx/${encodeURIComponent(txHash)}` : '';

            // Copy buttons use data-copy + event listeners (no inline onclick with untrusted values)
            tr.innerHTML = `
                <td class="py-3 px-2 flex items-center space-x-1 font-mono">
                    <span class="text-cyan-400">${safePaymentIdShort}...</span>
                    <button type="button" data-copy="${safePaymentId}" data-copy-label="Payment ID" class="ledger-copy-btn text-slate-500 hover:text-cyan-400 transition-all active:scale-90 focus:outline-none" title="Copy Payment ID">
                        <svg class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                            <path stroke-linecap="round" stroke-linejoin="round" d="M8 5H6a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2v-1M8 5a2 2 0 002 2h2a2 2 0 002-2M8 5a2 2 0 012-2h2a2 2 0 012 2m0 0h2a2 2 0 012 2v3m2 4H10m0 0l3-3m-3 3l3 3" />
                        </svg>
                    </button>
                </td>
                <td class="py-3 px-2 text-slate-400 font-mono">${safeTimestamp}</td>
                <td class="py-3 px-2 text-slate-400 font-mono">
                    <div class="flex items-center space-x-1">
                        <span>${safeSenderText}</span>
                        ${sender && sender !== 'N/A' ? `
                        <button type="button" data-copy="${safeSender}" data-copy-label="Sender Address" class="ledger-copy-btn text-slate-500 hover:text-cyan-400 transition-all active:scale-90 focus:outline-none" title="Copy Sender Address">
                            <svg class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                                <path stroke-linecap="round" stroke-linejoin="round" d="M8 5H6a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2v-1M8 5a2 2 0 002 2h2a2 2 0 002-2M8 5a2 2 0 012-2h2a2 2 0 012 2m0 0h2a2 2 0 012 2v3m2 4H10m0 0l3-3m-3 3l3 3" />
                            </svg>
                        </button>
                        <a href="${escapeHtml(senderHref)}" target="_blank" rel="noopener noreferrer" class="text-slate-500 hover:text-cyan-400 transition-all active:scale-90 focus:outline-none" title="View on Stockodile">
                            <svg class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                                <path stroke-linecap="round" stroke-linejoin="round" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                            </svg>
                        </a>
                        ` : ''}
                    </div>
                </td>
                <td class="py-3 px-2 font-bold text-slate-200">${safeAmount} ${safeCurrency}</td>
                <td class="py-3 px-2 text-slate-400 font-mono">
                    <div class="flex items-center space-x-1">
                        <span>${safeTxHashText}</span>
                        ${txHash && txHash !== 'N/A' ? `
                        <button type="button" data-copy="${safeTxHash}" data-copy-label="Transaction Hash" class="ledger-copy-btn text-slate-500 hover:text-cyan-400 transition-all active:scale-90 focus:outline-none" title="Copy Transaction Hash">
                            <svg class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                                <path stroke-linecap="round" stroke-linejoin="round" d="M8 5H6a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2v-1M8 5a2 2 0 002 2h2a2 2 0 002-2M8 5a2 2 0 012-2h2a2 2 0 012 2m0 0h2a2 2 0 012 2v3m2 4H10m0 0l3-3m-3 3l3 3" />
                            </svg>
                        </button>
                        <a href="${escapeHtml(txHref)}" target="_blank" rel="noopener noreferrer" class="text-slate-500 hover:text-cyan-400 transition-all active:scale-90 focus:outline-none" title="View on Stockodile">
                            <svg class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                                <path stroke-linecap="round" stroke-linejoin="round" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
                            </svg>
                        </a>
                        ` : ''}
                    </div>
                </td>
                <td class="py-3 px-2">
                    <span class="px-2 py-0.5 rounded text-[10px] uppercase font-bold ${badgeClass}">
                        ${safeStatus}
                    </span>
                </td>
            `;
            tr.querySelectorAll('.ledger-copy-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    const value = btn.getAttribute('data-copy') || '';
                    const label = btn.getAttribute('data-copy-label') || 'Value';
                    if (navigator.clipboard && navigator.clipboard.writeText) {
                        navigator.clipboard.writeText(value).then(() => {
                            alert(`${label} copied!`);
                        }).catch(() => {
                            alert(`${label} copy failed`);
                        });
                    } else {
                        alert(`${label}: ${value}`);
                    }
                });
            });
            dom.ledgerTableBody.appendChild(tr);
        });

        const totalPages = Math.ceil(totalCount / ledgerLimit) || 1;
        const currentPage = Math.floor(ledgerOffset / ledgerLimit) + 1;

        if (dom.ledgerPageInfo) dom.ledgerPageInfo.textContent = `Page ${currentPage} of ${totalPages}`;
        if (dom.ledgerPrevBtn) dom.ledgerPrevBtn.disabled = ledgerOffset === 0;
        if (dom.ledgerNextBtn) dom.ledgerNextBtn.disabled = (ledgerOffset + ledgerLimit) >= totalCount;
    }

    function updateMetrics(payments) {
        let totalFees = 0;
        let verifiedCount = 0;
        let pendingCount = 0;

        payments.forEach(p => {
            if (p.status === 'verified') {
                totalFees += parseFloat(p.amount);
                verifiedCount++;
            } else {
                pendingCount++;
            }
        });

        if (dom.metricsTotalFees) dom.metricsTotalFees.textContent = `${totalFees.toFixed(2)} USD`;
        if (dom.metricsVerifiedCount) dom.metricsVerifiedCount.textContent = verifiedCount;
        if (dom.metricsPendingCount) dom.metricsPendingCount.textContent = pendingCount;

        if (pendingCount > 0) {
            const pendingPayment = payments.find(p => p.status === 'pending');
            if (pendingPayment) {
                activePaymentId = pendingPayment.payment_id;
                
                const mocks = getDeterministicMockDetails(pendingPayment.payment_id);
                const txHash = pendingPayment.txHash || mocks.txHash;

                if (txHash) {
                    syncDebuggerState(activePaymentId, txHash);
                } else {
                    setStepStatus('handshake', 'pending', `Awaiting signature recovery for pending ID: ${activePaymentId}`);
                }
            }
        } else if (activePaymentId === null) {
            resetDebugger();
        }
    }

    let searchTimeout;
    if (dom.ledgerSearchInput) {
        dom.ledgerSearchInput.addEventListener('input', (e) => {
            clearTimeout(searchTimeout);
            searchTimeout = setTimeout(() => {
                ledgerSearchQuery = e.target.value.trim();
                ledgerOffset = 0;
                fetchLedger();
            }, 300);
        });
    }

    if (dom.ledgerStatusFilter) {
        dom.ledgerStatusFilter.addEventListener('change', (e) => {
            ledgerStatus = e.target.value;
            ledgerOffset = 0;
            fetchLedger();
        });
    }

    if (dom.ledgerSortTimestamp) {
        dom.ledgerSortTimestamp.addEventListener('click', () => {
            ledgerSortOrder = (ledgerSortOrder === 'desc') ? 'asc' : 'desc';
            ledgerAmountSort = null;
            fetchLedger();
        });
    }

    if (dom.ledgerSortAmount) {
        dom.ledgerSortAmount.addEventListener('click', () => {
            ledgerAmountSort = (ledgerAmountSort === 'asc') ? 'desc' : 'asc';
            fetchLedger();
        });
    }

    if (dom.ledgerPrevBtn) {
        dom.ledgerPrevBtn.addEventListener('click', () => {
            if (ledgerOffset >= ledgerLimit) {
                ledgerOffset -= ledgerLimit;
                fetchLedger();
            }
        });
    }

    if (dom.ledgerNextBtn) {
        dom.ledgerNextBtn.addEventListener('click', () => {
            ledgerOffset += ledgerLimit;
            fetchLedger();
        });
    }

    async function fetchAllFilteredLedger() {
        if (isPythonBackend) {
            const res = await fetch(apiRoutes.payments);
            const data = await res.json();
            const keys = Object.keys(data);
            return keys.map(pid => {
                const rec = data[pid];
                return {
                    payment_id: pid,
                    status: rec.status === 'paid' ? 'verified' : 'pending',
                    sender: rec.sender || null,
                    recipient: rec.recipient || '0x70997970C51812dc3A010C7d01b50e0d17dc79C8',
                    amount: rec.price || '0.001',
                    currency: rec.currency || 'USD',
                    txHash: rec.tx_hash || null,
                    timestamp: rec.timestamp || new Date().toISOString(),
                    signature: rec.signature || null,
                    symbol: rec.symbol || 'N/A'
                };
            });
        }
        
        const urlParams = new URLSearchParams();
        if (ledgerSearchQuery) urlParams.append('search', ledgerSearchQuery);
        if (ledgerStatus) urlParams.append('status', ledgerStatus);
        urlParams.append('limit', 100);
        urlParams.append('offset', 0);
        urlParams.append('sort', ledgerSortOrder);

        const res = await fetch(`${apiRoutes.payments}?${urlParams.toString()}`);
        const data = await res.json();
        return data.payments || [];
    }

    if (dom.ledgerExportJson) {
        dom.ledgerExportJson.addEventListener('click', async () => {
            try {
                const data = await fetchAllFilteredLedger();
                const mappedData = data.map(p => {
                    if (p.status === 'pending') {
                        const mocks = getDeterministicMockDetails(p.payment_id);
                        return {
                            ...p,
                            sender: p.sender || mocks.sender,
                            txHash: p.txHash || mocks.txHash
                        };
                    }
                    return p;
                });
                const blob = new Blob([JSON.stringify(mappedData, null, 2)], { type: 'application/json' });
                triggerDownload(blob, `payments_ledger_${Date.now()}.json`);
                logConsole('info', 'Payments ledger exported to JSON file format successfully.');
            } catch (e) {
                alert('Export failed.');
            }
        });
    }

    if (dom.ledgerExportCsv) {
        dom.ledgerExportCsv.addEventListener('click', async () => {
            try {
                const data = await fetchAllFilteredLedger();
                const headers = ['Payment ID', 'Status', 'Sender', 'Recipient', 'Amount', 'Currency', 'Tx Hash', 'Timestamp'];
                const csvRows = [headers.join(',')];
                
                data.forEach(p => {
                    let sender = p.sender;
                    let txHash = p.txHash;
                    if (p.status === 'pending') {
                        const mocks = getDeterministicMockDetails(p.payment_id);
                        if (!sender) sender = mocks.sender;
                        if (!txHash) txHash = mocks.txHash;
                    }
                    const row = [
                        p.payment_id,
                        p.status,
                        sender || '',
                        p.recipient,
                        p.amount,
                        p.currency,
                        txHash || '',
                        p.timestamp
                    ].map(val => `"${val.replace(/"/g, '""')}"`);
                    csvRows.push(row.join(','));
                });

                const blob = new Blob([csvRows.join('\n')], { type: 'text/csv;charset=utf-8;' });
                triggerDownload(blob, `payments_ledger_${Date.now()}.csv`);
                logConsole('info', 'Payments ledger exported to CSV file format successfully.');
            } catch (e) {
                alert('Export failed.');
            }
        });
    }

    function triggerDownload(blob, filename) {
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    }

    // ----------------------------------------------------
    // 12. Initialization
    // ----------------------------------------------------
    function init() {
        // Detect Backend type based on script tags / global window variables or api endpoints
        if (window.isFastAPIBackend) {
            isPythonBackend = true;
            apiRoutes = {
                marketData: '/api/v1/market-data',
                payments: '/api/v1/admin/payments',
                simulate: '/api/v1/simulate-payment',
                events: '/api/events'
            };
        }

        initChart();
        loadSettings();
        connectSSE();
        
        // Listen to settings field inputs
        if (dom.settingsRpcInput) dom.settingsRpcInput.addEventListener('input', checkSettingsChanged);
        if (dom.settingsContractInput) dom.settingsContractInput.addEventListener('input', checkSettingsChanged);
        if (dom.settingsFeeInput) dom.settingsFeeInput.addEventListener('input', checkSettingsChanged);

        if (dom.sseReconnectBtn) {
            dom.sseReconnectBtn.addEventListener('click', () => {
                connectSSE();
            });
        }
        if (dom.sseClearBtn) {
            dom.sseClearBtn.addEventListener('click', () => {
                if (dom.sseLogConsole) {
                    dom.sseLogConsole.innerHTML = '';
                }
            });
        }
        
        // Auto configure Request Builder path and params depending on backend
        if (isPythonBackend && dom.apiPathInput) {
            dom.apiPathInput.value = '/api/v1/market-data';
            if (dom.apiParamsContainer) {
                dom.apiParamsContainer.innerHTML = '';
                const row = document.createElement('div');
                row.className = 'flex items-center space-x-2 param-row mt-2';
                row.innerHTML = `
                    <input type="text" value="symbol" placeholder="Key" class="w-1/3 bg-slate-950 border border-slate-800 rounded px-2 py-1 text-xs font-mono text-slate-200 focus:outline-none focus:border-cyan-500">
                    <input type="text" value="AAPL" placeholder="Value" class="w-1/2 bg-slate-950 border border-slate-800 rounded px-2 py-1 text-xs font-mono text-slate-200 focus:outline-none focus:border-cyan-500">
                    <button class="remove-param-btn text-rose-500 hover:text-rose-400 text-xs px-2 py-1 font-semibold">Remove</button>
                `;
                row.querySelector('.remove-param-btn').addEventListener('click', () => { row.remove(); });
                dom.apiParamsContainer.appendChild(row);
            }
        }
        
        checkSettingsChanged();
        logConsole('info', 'Stockodile Web3 gated dashboard fully loaded.');
    }

    init();
});
