document.addEventListener('DOMContentLoaded', () => {
    console.log("DOM fully loaded. Starting script initialization.");

    // --- STAN APLIKACJI ---
    const state = {
        phase1: [], phase2: [], phase3: [],
        portfolio: [],
        transactions: [],
        liveQuotes: {},
        workerStatus: { status: 'IDLE', phase: 'NONE', progress: { processed: 0, total: 0 } },
        discardedSignalCount: 0,
        activePortfolioPolling: null,
        activeCountdownPolling: null, 
        profitAlertsSent: {}, 
        snoozedAlerts: {},
        activeAIOptimizerPolling: null,
        currentReportPage: 1,
        activeH3DeepDivePolling: null
    };

    // --- SELEKTORY UI ---
    const ui = {
        loginScreen: document.getElementById('login-screen'),
        dashboardScreen: document.getElementById('dashboard'),
        loginButton: document.getElementById('login-button'),
        loginStatusText: document.getElementById('login-status-text'),
        mainContent: document.getElementById('main-content'),
        startBtn: document.getElementById('start-btn'),
        pauseBtn: document.getElementById('pause-btn'),
        resumeBtn: document.getElementById('resume-btn'),
        apiStatus: document.getElementById('api-status'),
        workerStatusText: document.getElementById('worker-status-text'),
        dashboardLink: document.getElementById('dashboard-link'),
        portfolioLink: document.getElementById('portfolio-link'),
        transactionsLink: document.getElementById('transactions-link'),
        agentReportLink: document.getElementById('agent-report-link'),
        heartbeatStatus: document.getElementById('heartbeat-status'),
        alertContainer: document.getElementById('system-alert-container'),
        phase1: { list: document.getElementById('phase-1-list'), count: document.getElementById('phase-1-count') },
        phase2: { list: document.getElementById('phase-2-list'), count: document.getElementById('phase-2-count') },
        phase3: { list: document.getElementById('phase-3-list'), count: document.getElementById('phase-3-count') },
        
        // Modale Transakcyjne
        buyModal: { 
            backdrop: document.getElementById('buy-modal'), 
            tickerSpan: document.getElementById('buy-modal-ticker'), 
            quantityInput: document.getElementById('buy-quantity'), 
            priceInput: document.getElementById('buy-price'),
            cancelBtn: document.getElementById('buy-cancel-btn'),
            confirmBtn: document.getElementById('buy-confirm-btn')
        },
        sellModal: { 
            backdrop: document.getElementById('sell-modal'), 
            tickerSpan: document.getElementById('sell-modal-ticker'), 
            maxQuantitySpan: document.getElementById('sell-max-quantity'), 
            quantityInput: document.getElementById('sell-quantity'), 
            priceInput: document.getElementById('sell-price'),
            cancelBtn: document.getElementById('sell-cancel-btn'),
            confirmBtn: document.getElementById('sell-confirm-btn')
        },
        
        // Modal Raportu AI
        aiReportModal: {
            backdrop: document.getElementById('ai-report-modal'),
            content: document.getElementById('ai-report-content'),
            closeBtn: document.getElementById('ai-report-close-btn')
        },

        // Modal H3 Deep Dive (Analiza Porażek)
        h3DeepDiveModal: {
            backdrop: document.getElementById('h3-deep-dive-modal'),
            yearInput: document.getElementById('h3-deep-dive-year-input'),
            runBtn: document.getElementById('run-h3-deep-dive-btn'),
            statusMsg: document.getElementById('h3-deep-dive-status-message'),
            content: document.getElementById('h3-deep-dive-report-content'),
            closeBtn: document.getElementById('h3-deep-dive-close-btn')
        },

        // Modal Pulpitu Strategii H3 (Dynamiczne Parametry)
        h3StrategyModal: {
            backdrop: document.getElementById('h3-strategy-modal'),
            yearInput: document.getElementById('h3-strategy-year'),
            percentileInput: document.getElementById('h3-param-percentile'),
            mSqInput: document.getElementById('h3-param-m-sq'),
            tpInput: document.getElementById('h3-param-tp'),
            slInput: document.getElementById('h3-param-sl'),
            holdInput: document.getElementById('h3-param-hold'),
            runBtn: document.getElementById('run-h3-strategy-btn'),
            statusMsg: document.getElementById('h3-strategy-status'),
            closeBtn: document.getElementById('h3-strategy-close-btn')
        },

        // Sidebar Mobilny
        sidebar: document.getElementById('app-sidebar'),
        sidebarBackdrop: document.getElementById('sidebar-backdrop'),
        mobileMenuBtn: document.getElementById('mobile-menu-btn'),
        mobileSidebarCloseBtn: document.getElementById('mobile-sidebar-close'),
        sidebarNav: document.querySelector('#app-sidebar nav'),
        sidebarPhasesContainer: document.getElementById('phases-container')
    };

    // --- KONFIGURACJA API ---
    const API_BASE_URL = "https://apex-predator-api-x0l8.onrender.com"; // Upewnij się, że to poprawny URL twojego backendu
    
    const PORTFOLIO_QUOTE_POLL_INTERVAL = 30000; // 30s
    const ALERT_POLL_INTERVAL = 7000; // 7s
    const AI_OPTIMIZER_POLL_INTERVAL = 5000; // 5s
    const H3_DEEP_DIVE_POLL_INTERVAL = 5000; // 5s
    const PROFIT_ALERT_THRESHOLD = 1.02; // +2%
    const REPORT_PAGE_SIZE = 200;

    const logger = {
        error: (message, ...args) => console.error(message, ...args),
        info: (message, ...args) => console.log(message, ...args),
        warn: (message) => console.warn(message)
    };

    // --- WARSTWA KOMUNIKACJI Z API ---
    const apiRequest = async (endpoint, options = {}) => {
        const url = endpoint ? `${API_BASE_URL}/${endpoint}` : API_BASE_URL;
        try {
            const response = await fetch(url, options);
            if (ui.apiStatus) ui.apiStatus.innerHTML = '<span class="h-2 w-2 rounded-full bg-green-500 mr-2"></span>Online';
            if (!response.ok) {
                let errorText = response.statusText;
                try {
                    const errorJson = await response.json();
                    errorText = errorJson.detail || errorText;
                } catch (e) {
                    errorText = await response.text() || errorText;
                }
                if (response.status === 404) throw new Error(`404 - Nie znaleziono zasobu`);
                if (response.status === 409) throw new Error(`409 - Konflikt: Worker jest zajęty.`);
                throw new Error(`Błąd serwera (${response.status}): ${errorText}`);
            }
            if (response.status === 204 || response.headers.get("Content-Length") === "0") return null;
            return await response.json();
        } catch (error) {
             logger.error(`API Error for ${url}:`, error.message);
             if (ui.apiStatus) ui.apiStatus.innerHTML = '<span class="h-2 w-2 rounded-full bg-red-500 mr-2"></span>Offline';
             throw error;
        }
    };

    const api = {
        getApiRootStatus: () => apiRequest(''),
        getWorkerStatus: () => apiRequest('api/v1/worker/status'),
        sendWorkerControl: (action) => apiRequest(`api/v1/worker/control/${action}`, { method: 'POST' }),
        getPhase1Candidates: () => apiRequest('api/v1/candidates/phase1'),
        getPhase2Results: () => apiRequest('api/v1/results/phase2'),
        getPhase3Signals: () => apiRequest('api/v1/signals/phase3'),
        getDiscardedCount: () => apiRequest('api/v1/signals/discarded-count-24h'),
        getLiveQuote: (ticker) => apiRequest(`api/v1/quote/${ticker}`),
        getSystemAlert: () => apiRequest('api/v1/system/alert'),
        getPortfolio: () => apiRequest('api/v1/portfolio'),
        buyStock: (data) => apiRequest('api/v1/portfolio/buy', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) }),
        sellStock: (data) => apiRequest('api/v1/portfolio/sell', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) }),
        getTransactionHistory: () => apiRequest('api/v1/transactions'),
        
        // Raporty i Analiza
        getVirtualAgentReport: (page = 1, pageSize = REPORT_PAGE_SIZE) => apiRequest(`api/v1/virtual-agent/report?page=${page}&page_size=${pageSize}`),
        
        // Backtest (Zaktualizowany o parametry)
        requestBacktest: (year, parameters = null) => apiRequest('api/v1/backtest/request', { 
            method: 'POST', 
            headers: { 'Content-Type': 'application/json' }, 
            body: JSON.stringify({ year: year, parameters: parameters })
        }),
        
        // AI Optimizer
        requestAIOptimizer: () => apiRequest('api/v1/ai-optimizer/request', {
            method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({})
        }),
        getAIOptimizerReport: () => apiRequest('api/v1/ai-optimizer/report'),
        
        // H3 Deep Dive
        requestH3DeepDive: (year) => apiRequest('api/v1/analysis/h3-deep-dive', {
            method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ year: year })
        }),
        getH3DeepDiveReport: () => apiRequest('api/v1/analysis/h3-deep-dive-report'),
    };

    // --- OBSŁUGA SIDEBARA (MOBILE) ---
    function openSidebar() {
        if (ui.sidebar) { ui.sidebar.classList.remove('-translate-x-full'); ui.sidebar.classList.add('translate-x-0'); }
        if (ui.sidebarBackdrop) ui.sidebarBackdrop.classList.remove('hidden');
    }
    function closeSidebar() {
        if (ui.sidebar) { ui.sidebar.classList.add('-translate-x-full'); ui.sidebar.classList.remove('translate-x-0'); }
        if (ui.sidebarBackdrop) ui.sidebarBackdrop.classList.add('hidden');
    }
    if (ui.mobileMenuBtn) ui.mobileMenuBtn.addEventListener('click', openSidebar);
    if (ui.mobileSidebarCloseBtn) ui.mobileSidebarCloseBtn.addEventListener('click', closeSidebar);
    if (ui.sidebarBackdrop) ui.sidebarBackdrop.addEventListener('click', closeSidebar);
    if (ui.sidebarNav) ui.sidebarNav.addEventListener('click', (e) => { if (e.target.closest('a')) closeSidebar(); });


    // --- RENDERERY HTML ---
    const renderers = {
        loading: (text) => `<div class="text-center py-10"><div class="flex flex-col items-center"><i data-lucide="loader-2" class="w-8 h-8 text-sky-500 animate-spin mb-2"></i><p class="text-sky-400">${text}</p></div></div>`,
        
        phase1List: (candidates) => candidates.map(c => `<div class="candidate-item flex justify-between items-center text-xs p-2 rounded-md cursor-default transition-colors phase-1-text"><span class="font-bold">${c.ticker}</span></div>`).join('') || `<p class="text-xs text-gray-500 p-2">Brak wyników.</p>`,
        phase2List: (results) => results.map(r => `<div class="candidate-item flex justify-between items-center text-xs p-2 rounded-md cursor-default transition-colors phase-2-text"><span class="font-bold">${r.ticker}</span><span>Score: ${r.total_score}/10</span></div>`).join('') || `<p class="text-xs text-gray-500 p-2">Brak wyników.</p>`,
        phase3List: (signals) => signals.map(s => {
            const color = s.status === 'ACTIVE' ? 'text-green-400' : (s.status === 'PENDING' ? 'text-yellow-400' : 'text-gray-500');
            return `<div class="candidate-item flex items-center text-xs p-2 rounded-md cursor-default transition-colors ${color}"><span class="font-bold mr-2">${s.ticker}</span><span class="ml-auto">${s.status}</span></div>`;
        }).join('') || `<p class="text-xs text-gray-500 p-2">Brak sygnałów.</p>`,
        
        dashboard: () => `
            <div id="dashboard-view" class="max-w-4xl mx-auto">
                <h2 class="text-2xl font-bold text-sky-400 mb-6 border-b border-gray-700 pb-2">Panel Kontrolny Systemu</h2>
                <div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
                    <div class="bg-[#161B22] p-4 rounded-lg shadow-lg border border-gray-700">
                        <h3 class="font-semibold text-gray-400 flex items-center"><i data-lucide="cpu" class="w-4 h-4 mr-2 text-sky-400"></i>Status Silnika</h3>
                        <p id="dashboard-worker-status" class="text-4xl font-extrabold mt-2 text-green-500">IDLE</p>
                        <p id="dashboard-current-phase" class="text-sm text-gray-500 mt-1">Faza: NONE</p>
                    </div>
                    <div class="bg-[#161B22] p-4 rounded-lg shadow-lg border border-gray-700">
                        <h3 class="font-semibold text-gray-400 flex items-center"><i data-lucide="bar-chart-2" class="w-4 h-4 mr-2 text-yellow-400"></i>Postęp Skanowania</h3>
                        <div class="mt-2"><span id="progress-text" class="text-2xl font-extrabold">0 / 0</span><span class="text-gray-500 text-sm"> tickery</span></div>
                        <div class="w-full bg-gray-700 rounded-full h-2.5 mt-2"><div id="progress-bar" class="bg-sky-600 h-2.5 rounded-full transition-all duration-500" style="width: 0%"></div></div>
                    </div>
                    <div class="bg-[#161B22] p-4 rounded-lg shadow-lg border border-gray-700">
                        <h3 class="font-semibold text-gray-400 flex items-center"><i data-lucide="trending-up" class="w-4 h-4 mr-2 text-red-500"></i>Sygnały (Aktywne / Wyrzucone)</h3>
                        <div class="flex items-baseline gap-x-4 gap-y-2 mt-2">
                            <div><p id="dashboard-active-signals" class="text-4xl font-extrabold text-red-400">0</p><p class="text-sm text-gray-500 mt-1">Aktywne</p></div>
                            <div class="border-l border-gray-700 pl-4"><p id="dashboard-discarded-signals" class="text-4xl font-extrabold text-gray-500">0</p><p class="text-sm text-gray-500 mt-1">Wyrzucone (24h)</p></div>
                        </div>
                    </div>
                </div>
                <h3 class="text-xl font-bold text-gray-300 mb-4 border-b border-gray-700 pb-1">Logi Silnika</h3>
                <div id="scan-log-container" class="bg-[#161B22] p-4 rounded-lg shadow-inner h-96 overflow-y-scroll border border-gray-700">
                    <pre id="scan-log" class="text-xs text-gray-300 whitespace-pre-wrap font-mono">Czekam na rozpoczęcie skanowania...</pre>
                </div>
            </div>`,
        
        portfolio: (holdings, quotes) => {
            let totalVal = 0, totalPL = 0;
            const rows = holdings.map(h => {
                const quote = quotes[h.ticker];
                let price = 0, pl = 0, val = 0;
                if (quote && quote['05. price']) {
                    price = parseFloat(quote['05. price']);
                    val = h.quantity * price;
                    pl = val - (h.quantity * h.average_buy_price);
                    totalVal += val; totalPL += pl;
                }
                const plColor = pl >= 0 ? 'text-green-500' : 'text-red-500';
                return `<tr class="border-b border-gray-800 hover:bg-[#1f2937]">
                    <td class="p-3 font-bold text-sky-400">${h.ticker}</td>
                    <td class="p-3 text-right">${h.quantity}</td>
                    <td class="p-3 text-right">${h.average_buy_price.toFixed(2)}</td>
                    <td class="p-3 text-right">${price ? price.toFixed(2) : '---'}</td>
                    <td class="p-3 text-right text-cyan-400 font-bold">${h.take_profit ? h.take_profit.toFixed(2) : '---'}</td>
                    <td class="p-3 text-right ${plColor}">${pl ? pl.toFixed(2) : '---'} USD</td>
                    <td class="p-3 text-right"><button data-ticker="${h.ticker}" data-quantity="${h.quantity}" class="sell-stock-btn text-xs bg-red-600/20 hover:bg-red-600/40 text-red-300 py-1 px-3 rounded">Sprzedaj</button></td>
                </tr>`;
            }).join('');
            const totalPLColor = totalPL >= 0 ? 'text-green-500' : 'text-red-500';
            return `<div id="portfolio-view" class="max-w-6xl mx-auto">
                <h2 class="text-2xl font-bold text-sky-400 mb-6 border-b border-gray-700 pb-2 flex justify-between items-center">
                    Portfel Inwestycyjny <span class="text-lg text-gray-400">Wartość: ${totalVal.toFixed(2)} USD | Z/S: <span class="${totalPLColor}">${totalPL.toFixed(2)} USD</span></span>
                </h2>
                ${holdings.length === 0 ? '<p class="text-center text-gray-500 py-10">Portfel pusty.</p>' : 
                `<div class="overflow-x-auto bg-[#161B22] rounded-lg border border-gray-700"><table class="w-full text-sm text-left text-gray-300"><thead class="text-xs text-gray-400 uppercase bg-[#0D1117]"><tr><th class="p-3">Ticker</th><th class="p-3 text-right">Ilość</th><th class="p-3 text-right">Śr. Cena</th><th class="p-3 text-right">Cena</th><th class="p-3 text-right">Cel</th><th class="p-3 text-right">Z/S</th><th class="p-3 text-right">Akcja</th></tr></thead><tbody>${rows}</tbody></table></div>`}
            </div>`;
        },
        
        transactions: (transactions) => {
             const rows = transactions.map(t => {
                const color = t.transaction_type === 'BUY' ? 'text-green-400' : 'text-red-400';
                const plColor = t.profit_loss_usd >= 0 ? 'text-green-500' : 'text-red-500';
                return `<tr class="border-b border-gray-800 hover:bg-[#1f2937]"><td class="p-3 text-gray-400 text-xs">${new Date(t.transaction_date).toLocaleString()}</td><td class="p-3 font-bold text-sky-400">${t.ticker}</td><td class="p-3 font-semibold ${color}">${t.transaction_type}</td><td class="p-3 text-right">${t.quantity}</td><td class="p-3 text-right">${t.price_per_share.toFixed(4)}</td><td class="p-3 text-right ${plColor}">${t.profit_loss_usd ? t.profit_loss_usd.toFixed(2) + ' USD' : '---'}</td></tr>`;
            }).join('');
            return `<div id="transactions-view" class="max-w-6xl mx-auto"><h2 class="text-2xl font-bold text-sky-400 mb-6 border-b border-gray-700 pb-2">Historia Transakcji</h2>${transactions.length === 0 ? '<p class="text-center text-gray-500 py-10">Brak historii.</p>' : `<div class="overflow-x-auto bg-[#161B22] rounded-lg border border-gray-700"><table class="w-full text-sm text-left text-gray-300"><thead class="text-xs text-gray-400 uppercase bg-[#0D1117]"><tr><th class="p-3">Data</th><th class="p-3">Ticker</th><th class="p-3">Typ</th><th class="p-3 text-right">Ilość</th><th class="p-3 text-right">Cena</th><th class="p-3 text-right">Z/S</th></tr></thead><tbody>${rows}</tbody></table></div>`}</div>`;
        },

        agentReport: (report) => {
            const stats = report.stats;
            // Sekcja Kart Statystyk
            const createStatCard = (l, v, i) => `<div class="bg-[#161B22] p-4 rounded-lg shadow-lg border border-gray-700"><h3 class="font-semibold text-gray-400 flex items-center text-sm"><i data-lucide="${i}" class="w-4 h-4 mr-2 text-sky-400"></i>${l}</h3><p class="text-3xl font-extrabold mt-2 text-white">${v}</p></div>`;
            
            // Tabela Strategii
            const setupRows = Object.entries(stats.by_setup).map(([k, v]) => `
                <tr class="border-b border-gray-800 hover:bg-[#1f2937]">
                    <td class="p-3 text-sky-400 font-semibold">${k}</td>
                    <td class="p-3 text-right">${v.total_trades}</td>
                    <td class="p-3 text-right ${v.win_rate_percent >= 50 ? 'text-green-400' : 'text-red-400'}">${v.win_rate_percent.toFixed(1)}%</td>
                    <td class="p-3 text-right font-bold ${v.total_p_l_percent >= 0 ? 'text-green-400' : 'text-red-400'}">${v.total_p_l_percent.toFixed(2)}%</td>
                </tr>`).join('');

            // Panele sterowania (Backtest / AI / Deep Dive / Export)
            const toolsPanel = `
                <div class="grid grid-cols-1 md:grid-cols-2 gap-6 mt-8">
                    <!-- Backtest & H3 Strategy -->
                    <div class="bg-[#161B22] p-6 rounded-lg border border-gray-700">
                        <h4 class="text-lg font-semibold text-gray-300 mb-3">Symulacja Historyczna</h4>
                        <div class="flex gap-2 mb-2">
                            <input type="number" id="backtest-year-input" class="modal-input w-32 !mb-0" placeholder="Rok" min="2000" max="2099">
                            <button id="run-backtest-year-btn" class="modal-button modal-button-primary flex-1"><i data-lucide="play" class="w-4 h-4 mr-2"></i>Start</button>
                        </div>
                        <button id="open-h3-strategy-modal-btn" class="w-full text-xs text-sky-400 hover:text-sky-300 py-2 border border-dashed border-gray-600 rounded hover:border-sky-400 transition-colors mt-2">
                            + Otwórz Pulpit Strategii H3 (Parametry)
                        </button>
                        <div id="backtest-status-message" class="text-sm mt-2 h-4"></div>
                    </div>

                    <!-- Mega Agent AI -->
                    <div class="bg-[#161B22] p-6 rounded-lg border border-gray-700">
                        <h4 class="text-lg font-semibold text-gray-300 mb-3">Mega Agent AI</h4>
                        <div class="flex gap-2">
                            <button id="run-ai-optimizer-btn" class="modal-button modal-button-primary flex-1"><i data-lucide="brain-circuit" class="w-4 h-4 mr-2"></i>Analizuj</button>
                            <button id="view-ai-report-btn" class="modal-button modal-button-secondary"><i data-lucide="file-text" class="w-4 h-4"></i>Raport</button>
                        </div>
                        <div id="ai-optimizer-status-message" class="text-sm mt-2 h-4"></div>
                    </div>

                    <!-- H3 Deep Dive -->
                    <div class="bg-[#161B22] p-6 rounded-lg border border-gray-700">
                        <h4 class="text-lg font-semibold text-gray-300 mb-3">H3 Deep Dive</h4>
                        <button id="run-h3-deep-dive-modal-btn" class="modal-button modal-button-primary w-full"><i data-lucide="search" class="w-4 h-4 mr-2"></i>Analiza Porażek</button>
                        <div id="h3-deep-dive-main-status" class="text-sm mt-2 h-4"></div>
                    </div>

                    <!-- Export CSV -->
                    <div class="bg-[#161B22] p-6 rounded-lg border border-gray-700">
                        <h4 class="text-lg font-semibold text-gray-300 mb-3">Eksport Danych</h4>
                        <button id="run-csv-export-btn" class="modal-button modal-button-secondary w-full"><i data-lucide="download" class="w-4 h-4 mr-2"></i>Pobierz CSV</button>
                        <div id="csv-export-status-message" class="text-sm mt-2 h-4"></div>
                    </div>
                </div>
            `;

            // Tabela Transakcji (uproszczona dla czytelności, pełna w CSV)
            const tradeRows = report.trades.map(t => `
                <tr class="border-b border-gray-800 text-xs font-mono hover:bg-[#1f2937]">
                    <td class="p-2 text-gray-400">${new Date(t.open_date).toLocaleDateString()}</td>
                    <td class="p-2 font-bold text-sky-400">${t.ticker}</td>
                    <td class="p-2 text-gray-300">${t.setup_type.replace('BACKTEST_', '')}</td>
                    <td class="p-2 text-right ${t.status.includes('TP') ? 'text-green-400' : 'text-red-400'}">${t.status}</td>
                    <td class="p-2 text-right font-bold ${t.final_profit_loss_percent >= 0 ? 'text-green-400' : 'text-red-400'}">${t.final_profit_loss_percent ? t.final_profit_loss_percent.toFixed(2) + '%' : '---'}</td>
                    <td class="p-2 text-right text-yellow-300 font-bold">${t.metric_aqm_score_h3 ? t.metric_aqm_score_h3.toFixed(3) : '-'}</td>
                </tr>
            `).join('');

            return `<div id="agent-report-view" class="max-w-6xl mx-auto">
                <h2 class="text-2xl font-bold text-sky-400 mb-6 border-b border-gray-700 pb-2">Raport Wydajności Agenta</h2>
                <div class="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
                    ${createStatCard('Całkowity P/L', `${stats.total_p_l_percent.toFixed(2)}%`, 'percent')}
                    ${createStatCard('Win Rate', `${stats.win_rate_percent.toFixed(1)}%`, 'target')}
                    ${createStatCard('Profit Factor', stats.profit_factor.toFixed(2), 'scale')}
                    ${createStatCard('Transakcje', stats.total_trades, 'hash')}
                </div>
                <div class="overflow-x-auto bg-[#161B22] rounded-lg border border-gray-700 mb-8">
                    <table class="w-full text-sm text-left text-gray-300">
                        <thead class="text-xs text-gray-400 uppercase bg-[#0D1117]"><tr><th class="p-3">Strategia</th><th class="p-3 text-right">Ilość</th><th class="p-3 text-right">WR%</th><th class="p-3 text-right">P/L%</th></tr></thead>
                        <tbody>${setupRows}</tbody>
                    </table>
                </div>
                ${toolsPanel}
                <h3 class="text-xl font-bold text-gray-300 mt-8 mb-4">Historia Transakcji (Ostatnie)</h3>
                <div class="overflow-x-auto bg-[#161B22] rounded-lg border border-gray-700 max-h-[400px] overflow-y-auto">
                    <table class="w-full text-sm text-left text-gray-300">
                        <thead class="text-xs text-gray-400 uppercase bg-[#0D1117] sticky top-0"><tr><th class="p-2">Data</th><th class="p-2">Ticker</th><th class="p-2">Setup</th><th class="p-2 text-right">Status</th><th class="p-2 text-right">P/L</th><th class="p-2 text-right">AQM H3</th></tr></thead>
                        <tbody>${tradeRows}</tbody>
                    </table>
                </div>
                <!-- Pagination controls placeholder if needed -->
            </div>`;
        }
    };

    // --- LOGIKA BIZNESOWA I KONTROLERY ---

    function updateDashboardUI(status) {
        if (!document.getElementById('dashboard-view')) return;
        const elStatus = document.getElementById('dashboard-worker-status');
        const elPhase = document.getElementById('dashboard-current-phase');
        const elProgText = document.getElementById('progress-text');
        const elProgBar = document.getElementById('progress-bar');
        const elLog = document.getElementById('scan-log');
        const elActive = document.getElementById('dashboard-active-signals');
        const elDiscarded = document.getElementById('dashboard-discarded-signals');

        if (elStatus) {
            let cls = 'text-gray-500';
            if (status.status === 'RUNNING') cls = 'text-green-500';
            else if (status.status.includes('BUSY')) cls = 'text-purple-400';
            else if (status.status === 'ERROR') cls = 'text-red-500';
            
            elStatus.className = `text-4xl font-extrabold mt-2 ${cls}`;
            elStatus.textContent = status.status;
        }
        if (elPhase) elPhase.textContent = `Faza: ${status.phase}`;
        if (elProgText) elProgText.textContent = `${status.progress.processed} / ${status.progress.total}`;
        if (elProgBar) elProgBar.style.width = status.progress.total > 0 ? `${(status.progress.processed/status.progress.total)*100}%` : '0%';
        if (elLog && elLog.textContent !== status.log) elLog.textContent = status.log || '...';
        
        if (elActive) elActive.textContent = state.phase3.length;
        if (elDiscarded) elDiscarded.textContent = state.discardedSignalCount;

        // Aktualizacja paska statusu w sidebarze
        ui.workerStatusText.textContent = status.phase !== 'NONE' ? status.phase : status.status;
        ui.workerStatusText.className = `font-mono px-2 py-1 rounded-md text-xs transition-colors ${status.status === 'RUNNING' ? 'bg-green-900 text-green-400' : 'bg-gray-700 text-gray-300'}`;
    }

    async function pollWorkerStatus() {
        try {
            const status = await api.getWorkerStatus();
            state.workerStatus = status;
            updateDashboardUI(status);
            
            // Aktywacja przycisków
            const isBusy = status.status !== 'IDLE' && status.status !== 'ERROR';
            ui.startBtn.disabled = isBusy;
            ui.pauseBtn.disabled = status.status !== 'RUNNING';
            ui.resumeBtn.disabled = status.status !== 'PAUSED';

        } catch(e) {}
        setTimeout(pollWorkerStatus, 5000);
    }

    async function pollSystemAlerts() {
        try {
            const alert = await api.getSystemAlert();
            if (alert && alert.message !== 'NONE') {
                // Tu można dodać logikę wyświetlania toasta
                logger.info("System Alert:", alert.message);
                const div = document.createElement('div');
                div.className = 'alert-bar bg-red-600 text-white p-3 shadow-lg rounded-md animate-bounce mb-2 cursor-pointer';
                div.innerHTML = `<i data-lucide="alert-circle" class="inline w-5 h-5 mr-2"></i> ${alert.message}`;
                div.onclick = () => div.remove();
                ui.alertContainer.appendChild(div);
                lucide.createIcons();
                setTimeout(() => div.remove(), 10000);
            }
        } catch(e) {}
        setTimeout(pollSystemAlerts, ALERT_POLL_INTERVAL);
    }

    async function refreshData() {
        try {
            const [p1, p2, p3, discarded] = await Promise.all([
                api.getPhase1Candidates(), api.getPhase2Results(), api.getPhase3Signals(), api.getDiscardedCount()
            ]);
            state.phase1 = p1 || []; state.phase2 = p2 || []; state.phase3 = p3 || [];
            state.discardedSignalCount = discarded?.discarded_count_24h ?? 0;
            
            ui.phase1.list.innerHTML = renderers.phase1List(state.phase1); ui.phase1.count.textContent = state.phase1.length;
            ui.phase2.list.innerHTML = renderers.phase2List(state.phase2); ui.phase2.count.textContent = state.phase2.length;
            ui.phase3.list.innerHTML = renderers.phase3List(state.phase3); ui.phase3.count.textContent = state.phase3.length;
            
            updateDashboardUI(state.workerStatus); // Odśwież liczniki w dashboardzie
        } catch(e) {}
        setTimeout(refreshData, 15000);
    }

    // --- NAWIGACJA ---
    function stopAllPolling() {
        if (state.activePortfolioPolling) { clearTimeout(state.activePortfolioPolling); state.activePortfolioPolling = null; }
        if (state.activeH3DeepDivePolling) { clearTimeout(state.activeH3DeepDivePolling); state.activeH3DeepDivePolling = null; }
        if (state.activeAIOptimizerPolling) { clearTimeout(state.activeAIOptimizerPolling); state.activeAIOptimizerPolling = null; }
    }

    async function navigate(view) {
        stopAllPolling();
        document.querySelectorAll('.sidebar-item').forEach(el => el.classList.remove('sidebar-item-active'));
        
        if (view === 'dashboard') {
            ui.dashboardLink.classList.add('sidebar-item-active');
            ui.mainContent.innerHTML = renderers.dashboard();
            updateDashboardUI(state.workerStatus);
        } else if (view === 'portfolio') {
            ui.portfolioLink.classList.add('sidebar-item-active');
            ui.mainContent.innerHTML = renderers.loading("Ładowanie portfela...");
            try {
                state.portfolio = await api.getPortfolio();
                state.liveQuotes = {};
                ui.mainContent.innerHTML = renderers.portfolio(state.portfolio, {});
                pollPortfolioQuotes();
            } catch(e) { ui.mainContent.innerHTML = `<p class="text-red-500 p-4">Błąd: ${e.message}</p>`; }
        } else if (view === 'transactions') {
            ui.transactionsLink.classList.add('sidebar-item-active');
            ui.mainContent.innerHTML = renderers.loading("Ładowanie historii...");
            try {
                state.transactions = await api.getTransactionHistory();
                ui.mainContent.innerHTML = renderers.transactions(state.transactions);
            } catch(e) { ui.mainContent.innerHTML = `<p class="text-red-500 p-4">Błąd: ${e.message}</p>`; }
        } else if (view === 'agentReport') {
            ui.agentReportLink.classList.add('sidebar-item-active');
            loadAgentReport(1);
        }
        lucide.createIcons();
        if (window.innerWidth < 768) closeSidebar();
    }

    async function loadAgentReport(page) {
        ui.mainContent.innerHTML = renderers.loading(`Generowanie raportu...`);
        try {
            const report = await api.getVirtualAgentReport(page);
            ui.mainContent.innerHTML = renderers.agentReport(report);
            lucide.createIcons();
        } catch(e) { ui.mainContent.innerHTML = `<p class="text-red-500 p-4">Błąd raportu: ${e.message}</p>`; }
    }

    async function pollPortfolioQuotes() {
        if (!state.portfolio.length) return;
        try {
            const tickers = state.portfolio.map(h => h.ticker);
            const results = await Promise.all(tickers.map(t => api.getLiveQuote(t)));
            let updated = false;
            results.forEach((q, i) => { if (q) { state.liveQuotes[tickers[i]] = q; updated = true; } });
            if (updated && document.getElementById('portfolio-view')) {
                ui.mainContent.innerHTML = renderers.portfolio(state.portfolio, state.liveQuotes);
                lucide.createIcons();
            }
        } catch(e) {}
        state.activePortfolioPolling = setTimeout(pollPortfolioQuotes, PORTFOLIO_QUOTE_POLL_INTERVAL);
    }

    // --- MODALE I AKCJE ---

    // Kupno / Sprzedaż
    function showBuyModal(t) { ui.buyModal.tickerSpan.textContent = t; ui.buyModal.confirmBtn.dataset.ticker = t; ui.buyModal.backdrop.classList.remove('hidden'); }
    function hideBuyModal() { ui.buyModal.backdrop.classList.add('hidden'); }
    async function handleBuy() {
        const t = ui.buyModal.confirmBtn.dataset.ticker, q = parseInt(ui.buyModal.quantityInput.value), p = parseFloat(ui.buyModal.priceInput.value);
        if (!t || !q || !p) return alert("Błędne dane");
        try { await api.buyStock({ticker: t, quantity: q, price_per_share: p}); hideBuyModal(); navigate('portfolio'); } catch(e) { alert(e.message); }
    }

    function showSellModal(t, max) { ui.sellModal.tickerSpan.textContent = t; ui.sellModal.maxQuantitySpan.textContent = max; ui.sellModal.quantityInput.max = max; ui.sellModal.confirmBtn.dataset.ticker = t; ui.sellModal.backdrop.classList.remove('hidden'); }
    function hideSellModal() { ui.sellModal.backdrop.classList.add('hidden'); }
    async function handleSell() {
        const t = ui.sellModal.confirmBtn.dataset.ticker, q = parseInt(ui.sellModal.quantityInput.value), p = parseFloat(ui.sellModal.priceInput.value);
        if (!t || !q || !p) return alert("Błędne dane");
        try { await api.sellStock({ticker: t, quantity: q, price_per_share: p}); hideSellModal(); navigate('portfolio'); } catch(e) { alert(e.message); }
    }

    // Backtest Podstawowy
    async function handleRunBacktest() {
        const input = document.getElementById('backtest-year-input');
        const msg = document.getElementById('backtest-status-message');
        const year = input.value;
        if (!year || year.length !== 4) return msg.textContent = "Błędny rok";
        msg.textContent = "Zlecanie..."; msg.className = "text-yellow-400 text-sm mt-2";
        try {
            await api.requestBacktest(year, null);
            msg.textContent = "Zlecono. Worker przetwarza."; msg.className = "text-green-400 text-sm mt-2";
        } catch(e) { msg.textContent = e.message; msg.className = "text-red-400 text-sm mt-2"; }
    }

    // Pulpit Strategii H3 (Dynamiczne Parametry)
    function showH3StrategyModal() { ui.h3StrategyModal.backdrop.classList.remove('hidden'); ui.h3StrategyModal.statusMsg.textContent = ''; }
    function hideH3StrategyModal() { ui.h3StrategyModal.backdrop.classList.add('hidden'); }
    
    async function handleRunH3Strategy() {
        const year = ui.h3StrategyModal.yearInput.value;
        const params = {
            h3_percentile: parseFloat(ui.h3StrategyModal.percentileInput.value),
            h3_m_sq_threshold: parseFloat(ui.h3StrategyModal.mSqInput.value),
            h3_tp_multiplier: parseFloat(ui.h3StrategyModal.tpInput.value),
            h3_sl_multiplier: parseFloat(ui.h3StrategyModal.slInput.value),
            h3_max_hold: parseInt(ui.h3StrategyModal.holdInput.value),
            setup_name: `H3_CUSTOM_P${ui.h3StrategyModal.percentileInput.value}_M${ui.h3StrategyModal.mSqInput.value}`
        };

        if (!year || isNaN(params.h3_percentile)) {
            ui.h3StrategyModal.statusMsg.textContent = "Błędne dane formularza.";
            ui.h3StrategyModal.statusMsg.className = "text-red-400 text-sm mt-3";
            return;
        }

        ui.h3StrategyModal.runBtn.disabled = true;
        ui.h3StrategyModal.runBtn.textContent = "Wysyłanie...";
        
        try {
            await api.requestBacktest(year, params);
            ui.h3StrategyModal.statusMsg.textContent = "Zlecono test z parametrami.";
            ui.h3StrategyModal.statusMsg.className = "text-green-400 text-sm mt-3";
            setTimeout(hideH3StrategyModal, 2000);
        } catch(e) {
            ui.h3StrategyModal.statusMsg.textContent = e.message;
            ui.h3StrategyModal.statusMsg.className = "text-red-400 text-sm mt-3";
        } finally {
            ui.h3StrategyModal.runBtn.disabled = false;
            ui.h3StrategyModal.runBtn.textContent = "Uruchom Test z Parametrami";
        }
    }

    // AI Optimizer
    async function handleRunAIOptimizer() {
        const msg = document.getElementById('ai-optimizer-status-message');
        if(msg) msg.textContent = "Zlecanie...";
        try {
            await api.requestAIOptimizer();
            if(msg) msg.textContent = "Worker pracuje...";
            pollAIOptimizer();
        } catch(e) { if(msg) msg.textContent = e.message; }
    }
    async function pollAIOptimizer() {
        try {
            const res = await api.getAIOptimizerReport();
            const msg = document.getElementById('ai-optimizer-status-message');
            if (res.status === 'PROCESSING') {
                if(msg) msg.textContent = "Przetwarzanie... (AI myśli)";
                state.activeAIOptimizerPolling = setTimeout(pollAIOptimizer, AI_OPTIMIZER_POLL_INTERVAL);
            } else if (res.status === 'DONE') {
                if(msg) msg.textContent = "Gotowe.";
                ui.aiReportModal.content.innerHTML = `<pre class="text-xs whitespace-pre-wrap font-mono text-gray-300">${res.report_text}</pre>`;
                ui.aiReportModal.backdrop.classList.remove('hidden');
            }
        } catch(e) {}
    }

    // H3 Deep Dive
    function showH3DeepDiveModal() { ui.h3DeepDiveModal.backdrop.classList.remove('hidden'); ui.h3DeepDiveModal.statusMsg.textContent = ''; }
    function hideH3DeepDiveModal() { ui.h3DeepDiveModal.backdrop.classList.add('hidden'); state.activeH3DeepDivePolling && clearTimeout(state.activeH3DeepDivePolling); }
    
    async function handleRunH3DeepDive() {
        const year = ui.h3DeepDiveModal.yearInput.value;
        if (!year) return;
        ui.h3DeepDiveModal.statusMsg.textContent = "Zlecanie...";
        try {
            await api.requestH3DeepDive(year);
            pollH3DeepDive();
        } catch(e) { ui.h3DeepDiveModal.statusMsg.textContent = e.message; }
    }

    async function pollH3DeepDive() {
        try {
            const res = await api.getH3DeepDiveReport();
            if (res.status === 'PROCESSING') {
                ui.h3DeepDiveModal.statusMsg.textContent = "Analiza w toku...";
                state.activeH3DeepDivePolling = setTimeout(pollH3DeepDive, H3_DEEP_DIVE_POLL_INTERVAL);
            } else if (res.status === 'DONE') {
                ui.h3DeepDiveModal.statusMsg.textContent = "Zakończono.";
                ui.h3DeepDiveModal.content.innerHTML = `<pre class="text-xs whitespace-pre-wrap font-mono text-gray-300">${res.report_text}</pre>`;
            } else if (res.status === 'ERROR') {
                ui.h3DeepDiveModal.statusMsg.textContent = "Błąd analizy.";
                ui.h3DeepDiveModal.content.innerHTML = `<p class="text-red-400">${res.report_text}</p>`;
            }
        } catch(e) {}
    }

    // CSV Export
    async function handleCsvExport() {
        const msg = document.getElementById('csv-export-status-message');
        if(msg) { msg.textContent = "Generowanie..."; msg.className = "text-yellow-400 text-sm mt-2"; }
        try {
            const response = await fetch(`${API_BASE_URL}/api/v1/export/trades.csv`);
            if (!response.ok) throw new Error("Błąd pobierania");
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url; a.download = `apex_trades_${new Date().toISOString().slice(0,10)}.csv`;
            document.body.appendChild(a); a.click(); a.remove();
            if(msg) { msg.textContent = "Pobrano."; msg.className = "text-green-400 text-sm mt-2"; }
        } catch(e) { if(msg) { msg.textContent = e.message; msg.className = "text-red-400 text-sm mt-2"; } }
    }

    // --- EVENT LISTENERS ---
    ui.dashboardLink.onclick = (e) => { e.preventDefault(); navigate('dashboard'); };
    ui.portfolioLink.onclick = (e) => { e.preventDefault(); navigate('portfolio'); };
    ui.transactionsLink.onclick = (e) => { e.preventDefault(); navigate('transactions'); };
    ui.agentReportLink.onclick = (e) => { e.preventDefault(); navigate('agentReport'); };

    ui.startBtn.onclick = () => api.sendWorkerControl('start');
    ui.pauseBtn.onclick = () => api.sendWorkerControl('pause');
    ui.resumeBtn.onclick = () => api.sendWorkerControl('resume');

    ui.buyModal.cancelBtn.onclick = hideBuyModal;
    ui.buyModal.confirmBtn.onclick = handleBuy;
    ui.sellModal.cancelBtn.onclick = hideSellModal;
    ui.sellModal.confirmBtn.onclick = handleSell;
    ui.aiReportModal.closeBtn.onclick = () => ui.aiReportModal.backdrop.classList.add('hidden');

    // Listenery Modali Analitycznych
    ui.h3DeepDiveModal.closeBtn.onclick = hideH3DeepDiveModal;
    ui.h3DeepDiveModal.runBtn.onclick = handleRunH3DeepDive;
    
    ui.h3StrategyModal.closeBtn.onclick = hideH3StrategyModal;
    ui.h3StrategyModal.runBtn.onclick = handleRunH3Strategy;

    // Globalny Delegator Zdarzeń (dla dynamicznych elementów)
    document.body.addEventListener('click', (e) => {
        // Przyciski w tabeli portfela
        if (e.target.closest('.sell-stock-btn')) {
            const btn = e.target.closest('.sell-stock-btn');
            showSellModal(btn.dataset.ticker, parseInt(btn.dataset.quantity));
        }
        // Przyciski w raporcie Agenta
        if (e.target.id === 'run-backtest-year-btn') handleRunBacktest();
        if (e.target.id === 'run-ai-optimizer-btn') handleRunAIOptimizer();
        if (e.target.id === 'view-ai-report-btn') pollAIOptimizer(); // Ponowne sprawdzenie/pokazanie
        if (e.target.id === 'run-h3-deep-dive-modal-btn') showH3DeepDiveModal();
        if (e.target.id === 'run-csv-export-btn') handleCsvExport();
        if (e.target.id === 'open-h3-strategy-modal-btn') showH3StrategyModal();
    });

    // --- START APLIKACJI ---
    document.getElementById('login-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        ui.loginButton.textContent = "Łączenie..."; ui.loginButton.disabled = true;
        try {
            const status = await api.getApiRootStatus();
            if (status) {
                ui.loginScreen.classList.add('hidden');
                ui.dashboardScreen.classList.remove('hidden');
                navigate('dashboard');
                pollWorkerStatus();
                refreshData();
                pollSystemAlerts();
            }
        } catch(e) {
            ui.loginStatusText.textContent = "Błąd połączenia z API.";
            ui.loginButton.textContent = "Wejdź"; ui.loginButton.disabled = false;
        }
    });

    // Wstępne sprawdzenie API
    (async () => {
        try {
            await api.getApiRootStatus();
            ui.loginButton.disabled = false; ui.loginButton.textContent = "Wejdź do Systemu";
        } catch(e) { ui.loginStatusText.textContent = "API Offline"; }
    })();
});
