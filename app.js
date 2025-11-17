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
        // ==========================================================
        // === NOWY STAN (STRONICOWANIE) ===
        // ==========================================================
        currentReportPage: 1
    };

    // --- SELEKTORY UI (Definiowane od razu) ---
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
        aiReportModal: {
            backdrop: document.getElementById('ai-report-modal'),
            content: document.getElementById('ai-report-content'),
            closeBtn: document.getElementById('ai-report-close-btn')
        },
        sidebar: document.getElementById('app-sidebar'),
        sidebarBackdrop: document.getElementById('sidebar-backdrop'),
        mobileMenuBtn: document.getElementById('mobile-menu-btn'),
        mobileSidebarCloseBtn: document.getElementById('mobile-sidebar-close'),
        sidebarNav: document.querySelector('#app-sidebar nav'),
        sidebarPhasesContainer: document.getElementById('phases-container')
    };
    console.log("UI Selectors defined.");

    // --- KONFIGURACJA API ---
    const API_BASE_URL = "https://apex-predator-api-x0l8.onrender.com";
    
    const PORTFOLIO_QUOTE_POLL_INTERVAL = 30000; // 30 sekund
    const ALERT_POLL_INTERVAL = 7000; // 7 sekund
    const AI_OPTIMIZER_POLL_INTERVAL = 5000; // 5 sekund
    const PROFIT_ALERT_THRESHOLD = 1.02; // +2%
    // ==========================================================
    // === NOWA STAŁA (STRONICOWANIE) ===
    // ==========================================================
    const REPORT_PAGE_SIZE = 200; // Musi być zgodne z limitem w api/src/crud.py

    const logger = {
        error: (message, ...args) => console.error(message, ...args),
        info: (message, ...args) => console.log(message, ...args),
        warn: (message) => console.warn(message)
    };

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
                
                logger.error(`API Error ${response.status} for ${url}: ${errorText}`);
                if (response.status === 404) throw new Error(`404 - Nie znaleziono zasobu`);
                if (response.status === 409) throw new Error(`409 - Konflikt: Worker jest zajęty.`);
                if (response.status === 400) throw new Error(`400 - Błędne żądanie: ${errorText}`);
                if (response.status === 422) throw new Error(`422 - Błąd walidacji: ${errorText}`);
                
                throw new Error(`Błąd serwera: ${response.status} - ${errorText}`);
            }
            if (response.status === 204 || response.headers.get("Content-Length") === "0") return null;
            return await response.json();
        } catch (error) {
             logger.error(`Network or API Error for ${url}:`, error.message);
             if (ui.apiStatus) ui.apiStatus.innerHTML = '<span class="h-2 w-2 rounded-full bg-red-500 mr-2"></span>Offline';
             throw error;
        }
    };

    const api = {
        getWorkerStatus: () => apiRequest('api/v1/worker/status'),
        sendWorkerControl: (action) => apiRequest(`api/v1/worker/control/${action}`, { method: 'POST' }),
        getPhase1Candidates: () => apiRequest('api/v1/candidates/phase1'),
        getPhase2Results: () => apiRequest('api/v1/results/phase2'),
        getPhase3Signals: () => apiRequest('api/v1/signals/phase3'),
        getDiscardedCount: () => apiRequest('api/v1/signals/discarded-count-24h'),
        getLiveQuote: (ticker) => apiRequest(`api/v1/quote/${ticker}`),
        addToWatchlist: (ticker) => apiRequest(`api/v1/watchlist/${ticker}`, { method: 'POST' }),
        getSystemAlert: () => apiRequest('api/v1/system/alert'),
        getPortfolio: () => apiRequest('api/v1/portfolio'),
        buyStock: (data) => apiRequest('api/v1/portfolio/buy', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) }),
        sellStock: (data) => apiRequest('api/v1/portfolio/sell', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) }),
        getTransactionHistory: () => apiRequest('api/v1/transactions'),
        // ==========================================================
        // === AKTUALIZACJA (STRONICOWANIE) ===
        // ==========================================================
        getVirtualAgentReport: (page = 1, pageSize = REPORT_PAGE_SIZE) => apiRequest(`api/v1/virtual-agent/report?page=${page}&page_size=${pageSize}`),
        // ==========================================================
        requestBacktest: (year) => apiRequest('api/v1/backtest/request', { 
            method: 'POST', 
            headers: { 'Content-Type': 'application/json' }, 
            body: JSON.stringify({ year: year })
        }),
        getApiRootStatus: () => apiRequest(''),
        requestAIOptimizer: () => apiRequest('api/v1/ai-optimizer/request', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({})
        }),
        getAIOptimizerReport: () => apiRequest('api/v1/ai-optimizer/report'),
        // UWAGA: Endpoint eksportu CSV nie jest tutaj, ponieważ musi być wywoływany 
        // bezpośrednio przez 'fetch', aby obsłużyć 'blob', a nie 'json'.
    };
    console.log("Full API object defined.");

    // --- Sterowanie Mobilnym Sidebarem ---
    function openSidebar() {
        if (ui.sidebar) {
            ui.sidebar.classList.remove('-translate-x-full');
            ui.sidebar.classList.add('translate-x-0');
        }
        if (ui.sidebarBackdrop) {
            ui.sidebarBackdrop.classList.remove('hidden');
        }
    }

    function closeSidebar() {
        if (ui.sidebar) {
            ui.sidebar.classList.add('-translate-x-full');
            ui.sidebar.classList.remove('translate-x-0');
        }
        if (ui.sidebarBackdrop) {
            ui.sidebarBackdrop.classList.add('hidden');
        }
    }

    if (ui.mobileMenuBtn) ui.mobileMenuBtn.addEventListener('click', openSidebar);
    if (ui.mobileSidebarCloseBtn) ui.mobileSidebarCloseBtn.addEventListener('click', closeSidebar);
    if (ui.sidebarBackdrop) ui.sidebarBackdrop.addEventListener('click', closeSidebar);
    if (ui.sidebarNav) {
        ui.sidebarNav.addEventListener('click', (e) => {
            if (e.target.closest('a')) {
                closeSidebar();
            }
        });
    }
    console.log("Mobile sidebar logic initialized.");

    // --- Funkcje Minutnika Rynkowego ---
    function getNYTime() {
        try {
            const options = {
                timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit',
                hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false
            };
            const formatter = new Intl.DateTimeFormat('en-US', options);
            const parts = formatter.formatToParts(new Date());
            const find = (type) => parts.find(p => p.type === type)?.value;
            const year = find('year'), month = find('month'), day = find('day');
            const hour = find('hour') === '24' ? '00' : find('hour');
            const minute = find('minute'), second = find('second');
            return new Date(year, parseInt(month) - 1, day, hour, minute, second);
        } catch (e) {
            logger.error("Błąd pobierania czasu NY, używam czasu lokalnego.", e);
            return new Date();
        }
    }

    function formatCountdown(ms) {
        if (ms < 0) ms = 0;
        const totalSeconds = Math.floor(ms / 1000);
        const totalMinutes = Math.floor(totalSeconds / 60);
        const totalHours = Math.floor(totalMinutes / 60);
        const days = Math.floor(totalHours / 24);
        const hours = totalHours % 24;
        const minutes = totalMinutes % 60;
        const seconds = totalSeconds % 60;
        let str = '';
        if (days > 0) str += `${days}d `;
        str += `${String(hours).padStart(2, '0')}g ${String(minutes).padStart(2, '0')}m ${String(seconds).padStart(2, '0')}s`;
        return str;
    }

    function getMarketCountdown() {
        const now = getNYTime();
        const dayOfWeek = now.getDay();
        const isWeekend = (dayOfWeek === 0 || dayOfWeek === 6);
        const preMarketOpen = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 4, 0, 0);
        const marketOpen = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 9, 30, 0);
        const marketClose = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 16, 0, 0);
        let message = '', targetTime = null;

        if (isWeekend) {
            let daysToAdd = (dayOfWeek === 6) ? 2 : 1;
            targetTime = new Date(preMarketOpen.getTime() + daysToAdd * 24 * 60 * 60 * 1000);
            message = 'Do otwarcia Pre-Market: ';
        } else {
            if (now < preMarketOpen) {
                targetTime = preMarketOpen;
                message = 'Do otwarcia Pre-Market: ';
            } else if (now >= preMarketOpen && now < marketOpen) {
                targetTime = marketOpen;
                message = 'Do otwarcia Rynku: ';
            } else if (now >= marketOpen && now < marketClose) {
                targetTime = marketClose;
                message = 'Do zamknięcia Rynku: ';
            } else {
                let daysToAdd = (dayOfWeek === 5) ? 3 : 1;
                targetTime = new Date(preMarketOpen.getTime() + daysToAdd * 24 * 60 * 60 * 1000);
                message = 'Do otwarcia Pre-Market: ';
            }
        }
        const diff = targetTime.getTime() - now.getTime();
        return message + formatCountdown(diff);
    }

    function updateCountdownTimer() {
        const timerElement = document.getElementById('market-countdown-timer');
        if (timerElement) {
            timerElement.textContent = getMarketCountdown();
        }
    }

    function startMarketCountdown() {
        stopMarketCountdown();
        logger.info("Rozpoczynam odliczanie rynkowe (co 1s).");
        updateCountdownTimer();
        state.activeCountdownPolling = setInterval(updateCountdownTimer, 1000);
    }

    function stopMarketCountdown() {
        if (state.activeCountdownPolling) {
            logger.info("Zatrzymuję odliczanie rynkowe.");
            clearInterval(state.activeCountdownPolling);
            state.activeCountdownPolling = null;
        }
    }

    // --- Renderowanie i Widoki ---
    const renderQuoteBox = (quote, market) => {
         if (!quote || Object.keys(quote).length === 0) {
            return `<div class="bg-gray-800/20 border border-gray-700 p-4 rounded-lg text-sm text-gray-500 text-center">Brak danych cenowych.</div>`;
        }
         const safeMarket = market || { status: 'UNKNOWN', time_ny: 'N/A', date_ny: 'N/A' };
        try {
            const cleanedQuote = Object.fromEntries(
                Object.entries(quote).map(([key, value]) => [key.includes('. ') ? key.substring(key.indexOf('.') + 2) : key, value])
            );
            
            const price = parseFloat(cleanedQuote['price']);
            const change = parseFloat(cleanedQuote['change']);
            const changePercentStr = cleanedQuote['change percent'];
            const changePercent = parseFloat(changePercentStr ? changePercentStr.replace('%', '') : '0');
            const prevClose = parseFloat(cleanedQuote['previous close']);

            if (isNaN(price) || isNaN(change) || isNaN(changePercent) || isNaN(prevClose)) throw new Error(`Nieprawidłowe dane liczbowe w quote.`);
            
            const isPositive = change >= 0;
            const changeClass = isPositive ? 'text-green-500' : 'text-red-500';
            const changeIcon = isPositive ? 'trending-up' : 'trending-down';
            
            let statusText = 'Rynek Zamknięty';
            let statusClass = 'bg-gray-600';
            if (safeMarket.status === 'MARKET_OPEN') { statusText = 'Rynek Otwarty'; statusClass = 'bg-green-600'; }
            else if (safeMarket.status === 'PRE_MARKET') { statusText = 'Pre-Market'; statusClass = 'bg-yellow-600'; }
            else if (safeMarket.status === 'AFTER_MARKET') { statusText = 'After-Market'; statusClass = 'bg-blue-600'; }
            else if (safeMarket.status === 'UNKNOWN') { statusText = 'Status Nieznany'; statusClass = 'bg-purple-600'; }
            
            return `
                <div class="flex flex-wrap justify-between items-start gap-4">
                    <div>
                        <div class="flex items-end gap-3">
                            <span class="text-4xl font-bold text-white">${price.toFixed(2)}</span>
                            <div class="${changeClass} flex items-center gap-1 mb-1">
                                <i data-lucide="${changeIcon}" class="w-5 h-5"></i>
                                <span class="font-semibold">${change.toFixed(2)} (${changePercent.toFixed(2)}%)</span>
                            </div>
                        </div>
                        <p class="text-sm text-gray-500 mt-1">Poprz. zamkn.: ${prevClose.toFixed(2)}</p>
                    </div>
                    <div class="text-right">
                        <span class="flex items-center justify-end gap-2 text-sm font-semibold text-gray-300">
                            <span class="relative flex h-3 w-3">
                                <span class="animate-ping absolute inline-flex h-full w-full rounded-full ${statusClass} opacity-75"></span>
                                <span class="relative inline-flex rounded-full h-3 w-3 ${statusClass}"></span>
                            </span>
                            ${statusText}
                        </span>
                        <p class="text-xs text-gray-500 mt-1">Dane na ${safeMarket.date_ny} ${safeMarket.time_ny}</p>
                        <p class="text-xs text-yellow-400 font-mono mt-2" id="market-countdown-timer">Obliczanie...</p>
                    </div>
                </div>`;

        } catch (e) {
            logger.error("Błąd renderowania quoteBox:", e, { quote, market: safeMarket });
            return `<div class="bg-red-900/20 border border-red-500/30 p-4 rounded-lg text-sm text-red-400 text-center">Błąd parsowania danych cenowych: ${e.message}</div>`;
        }
    };

    const renderers = {
        loading: (text) => `<div class="text-center py-10"><div role="status" class="flex flex-col items-center"><svg aria-hidden="true" class="inline w-8 h-8 text-gray-600 animate-spin fill-sky-500" viewBox="0 0 100 101" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z" fill="currentColor"/><path d="M93.9676 39.0409C96.393 38.4038 97.8624 35.9116 97.0079 33.5539C95.2932 28.8227 92.871 24.3692 89.8167 20.348C85.8452 15.1192 80.8826 10.7238 75.2124 7.41289C69.5422 4.10194 63.2754 1.94025 56.7698 1.05124C51.7666 0.367541 46.6976 0.446843 41.7345 1.27873C39.2613 1.69328 37.813 4.19778 38.4501 6.62326C39.0873 9.04874 41.5694 10.4717 44.0505 10.1071C47.8511 9.54855 51.7191 9.52689 55.5402 10.0491C60.8642 10.7766 65.9928 12.5457 70.6331 15.2552C75.2735 17.9648 79.3347 21.5619 82.5849 25.841C84.9175 28.9121 86.7997 32.2913 88.1811 35.8758C89.083 38.2158 91.5421 39.6781 93.9676 39.0409Z" fill="currentFill"/></svg><p class="text-sky-400 mt-4">${text}</p></div></div>`,
        
        phase1List: (candidates) => candidates.map(c => `<div class="candidate-item flex justify-between items-center text-xs p-2 rounded-md cursor-default transition-colors phase-1-text"><span class="font-bold">${c.ticker}</span></div>`).join('') || `<p class="text-xs text-gray-500 p-2">Brak wyników.</p>`,
        phase2List: (results) => results.map(r => `<div class="candidate-item flex justify-between items-center text-xs p-2 rounded-md cursor-default transition-colors phase-2-text"><span class="font-bold">${r.ticker}</span><span>Score: ${r.total_score}/10</span></div>`).join('') || `<p class="text-xs text-gray-500 p-2">Brak wyników.</p>`,
        phase3List: (signals) => signals.map(s => {
            let statusClass, statusText, icon;
            if (s.status === 'ACTIVE') { statusClass = 'text-green-400'; statusText = 'AKTYWNY'; icon = 'zap';}
            else if (s.status === 'PENDING') { statusClass = 'text-yellow-400'; statusText = 'OCZEKUJĄCY'; icon = 'hourglass'; }
            else { statusClass = 'text-gray-500'; statusText = s.status.toUpperCase(); icon = 'help-circle'; }
            return `<div class="candidate-item flex items-center text-xs p-2 rounded-md cursor-default transition-colors ${statusClass}"><i data-lucide="${icon}" class="w-4 h-4 mr-2"></i><span class="font-bold">${s.ticker}</span><span class="ml-auto text-gray-500">${statusText}</span></div>`;
        }).join('') || `<p class="text-xs text-gray-500 p-2">Brak sygnałów.</p>`,
        
        dashboard: () => `<div id="dashboard-view" class="max-w-4xl mx-auto">
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
                                <h3 class="font-semibold text-gray-400 flex items-center">
                                    <i data-lucide="trending-up" class="w-4 h-4 mr-2 text-red-500"></i>Sygnały (Aktywne / Wyrzucone)
                                </h3>
                                <div class="flex items-baseline gap-x-4 gap-y-2 mt-2">
                                    <div>
                                        <p id="dashboard-active-signals" class="text-4xl font-extrabold text-red-400">0</p>
                                        <p class="text-sm text-gray-500 mt-1">Sygnały Aktywne</p>
                                    </div>
                                    <div class="border-l border-gray-700 pl-4">
                                        <p id="dashboard-discarded-signals" class="text-4xl font-extrabold text-gray-500">0</p>
                                        <p class="text-sm text-gray-500 mt-1">Wyrzucone (24h)</p>
                                    </div>
                                </div>
                            </div>
                            </div>
                        <h3 class="text-xl font-bold text-gray-300 mb-4 border-b border-gray-700 pb-1">Logi Silnika</h3>
                        <div id="scan-log-container" class="bg-[#161B22] p-4 rounded-lg shadow-inner h-96 overflow-y-scroll border border-gray-700">
                            <pre id="scan-log" class="text-xs text-gray-300 whitespace-pre-wrap font-mono">Czekam na rozpoczęcie skanowania...</pre>
                        </div>
                    </div>`,
        
        portfolio: (holdings, quotes) => {
            let totalPortfolioValue = 0;
            let totalProfitLoss = 0;
            const rows = holdings.map(h => {
                const quote = quotes[h.ticker];
                let currentPrice = null, dayChangePercent = null, profitLoss = null, currentValue = null;
                let priceClass = 'text-gray-400';
                
                if (quote && quote['05. price']) {
                    try {
                        currentPrice = parseFloat(quote['05. price']);
                        dayChangePercent = parseFloat(quote['change percent'] ? quote['change percent'].replace('%', '') : '0');
                        priceClass = dayChangePercent >= 0 ? 'text-green-500' : 'text-red-500';
                        currentValue = h.quantity * currentPrice;
                        const costBasis = h.quantity * h.average_buy_price;
                        profitLoss = currentValue - costBasis;
                        totalPortfolioValue += currentValue;
                        totalProfitLoss += profitLoss;
                    } catch (e) { console.error(`Błąd obliczeń dla ${h.ticker} w portfelu:`, e); }
                }
                const profitLossClass = profitLoss == null ? 'text-gray-500' : (profitLoss >= 0 ? 'text-green-500' : 'text-red-500');
                
                const takeProfitFormatted = h.take_profit ? h.take_profit.toFixed(2) : '---';
                
                return `<tr class="border-b border-gray-800 hover:bg-[#1f2937]">
                            <td class="p-3 font-bold text-sky-400">${h.ticker}</td>
                            <td class="p-3 text-right">${h.quantity}</td>
                            <td class="p-3 text-right">${h.average_buy_price.toFixed(4)}</td>
                            <td class="p-3 text-right ${priceClass}">${currentPrice ? currentPrice.toFixed(2) : '---'}</td>
                            <td class="p-3 text-right text-cyan-400 font-bold">${takeProfitFormatted}</td>
                            <td class="p-3 text-right ${profitLossClass}">${profitLoss != null ? profitLoss.toFixed(2) + ' USD' : '---'}</td>
                            <td class="p-3 text-right"><button data-ticker="${h.ticker}" data-quantity="${h.quantity}" class="sell-stock-btn text-xs bg-red-600/20 hover:bg-red-600/40 text-red-300 py-1 px-3 rounded">Sprzedaj</button></td>
                        </tr>`;
            }).join('');
            const totalProfitLossClass = totalProfitLoss >= 0 ? 'text-green-500' : 'text-red-500';
            
            const tableHeader = `<thead class="text-xs text-gray-400 uppercase bg-[#0D1117]">
                                    <tr>
                                        <th scope="col" class="p-3">Ticker</th>
                                        <th scope="col" class="p-3 text-right">Ilość</th>
                                        <th scope="col" class="p-3 text-right">Śr. Cena Zakupu (USD)</th>
                                        <th scope="col" class="p-3 text-right">Bieżąca Cena (USD)</th>
                                        <th scope="col" class="p-3 text-right">Cena Docelowa (USD)</th>
                                        <th scope="col" class="p-3 text-right">Zysk / Strata (USD)</th>
                                        <th scope="col" class="p-3 text-right">Akcja</th>
                                    </tr>
                                 </thead>`;
                                 
            return `<div id="portfolio-view" class="max-w-6xl mx-auto">
                        <h2 class="text-2xl font-bold text-sky-400 mb-6 border-b border-gray-700 pb-2 flex justify-between items-center">
                            Portfel Inwestycyjny
                            <span class="text-lg text-gray-400">Wartość: ${totalPortfolioValue.toFixed(2)} USD | Z/S: <span class="${totalProfitLossClass}">${totalProfitLoss.toFixed(2)} USD</span></span>
                        </h2>
                        ${holdings.length === 0 ? '<p class="text-center text-gray-500 py-10">Twój portfel jest pusty.</p>' : 
                        `<div class="overflow-x-auto bg-[#161B22] rounded-lg border border-gray-700">
                            <table class="w-full text-sm text-left text-gray-300">
                                ${tableHeader}
                                <tbody>${rows}</tbody>
                            </table>
                         </div>` }
                    </div>`;
        },
        
        transactions: (transactions) => {
             const rows = transactions.map(t => {
                const typeClass = t.transaction_type === 'BUY' ? 'text-green-400' : 'text-red-400';
                const profitLossClass = t.profit_loss_usd == null ? '' : (t.profit_loss_usd >= 0 ? 'text-green-500' : 'text-red-500');
                const transactionDate = new Date(t.transaction_date).toLocaleString('pl-PL');
                return `<tr class="border-b border-gray-800 hover:bg-[#1f2937]"><td class="p-3 text-gray-400 text-xs">${transactionDate}</td><td class="p-3 font-bold text-sky-400">${t.ticker}</td><td class="p-3 font-semibold ${typeClass}">${t.transaction_type}</td><td class="p-3 text-right">${t.quantity}</td><td class="p-3 text-right">${t.price_per_share.toFixed(4)}</td><td class="p-3 text-right ${profitLossClass}">${t.profit_loss_usd != null ? t.profit_loss_usd.toFixed(2) + ' USD' : '---'}</td></tr>`;
            }).join('');
            return `<div id="transactions-view" class="max-w-6xl mx-auto"><h2 class="text-2xl font-bold text-sky-400 mb-6 border-b border-gray-700 pb-2">Historia Transakcji</h2>${transactions.length === 0 ? '<p class="text-center text-gray-500 py-10">Brak historii transakcji.</p>' : `<div class="overflow-x-auto bg-[#161B22] rounded-lg border border-gray-700"><table class="w-full text-sm text-left text-gray-300"><thead class="text-xs text-gray-400 uppercase bg-[#0D1117]"><tr><th scope="col" class="p-3">Data</th><th scope="col" class="p-3">Ticker</th><th scope="col" class="p-3">Typ</th><th scope="col" class="p-3 text-right">Ilość</th><th scope="col" class="p-3 text-right">Cena (USD)</th><th scope="col" class="p-3 text-right">Zysk / Strata (USD)</th></tr></thead><tbody>${rows}</tbody></table></div>` }</div>`;
        },
        
        // ==========================================================
        // === AKTUALIZACJA (STRONICOWANIE I GŁĘBOKIE LOGOWANIE) ===
        // ==========================================================
        agentReport: (report) => {
            const stats = report.stats;
            const trades = report.trades;
            const total_trades_count = report.total_trades_count;
            
            // === Funkcje pomocnicze do formatowania ===
            const formatMetric = (val) => {
                if (typeof val !== 'number' || isNaN(val)) {
                    return `<span class="text-gray-600">---</span>`;
                }
                return val.toFixed(3);
            };
            const formatPercent = (val) => {
                if (typeof val !== 'number' || isNaN(val)) {
                    return `<span class="text-gray-500">---</span>`;
                }
                const color = val >= 0 ? 'text-green-500' : 'text-red-500';
                return `<span class="${color}">${val.toFixed(2)}%</span>`;
            };
            const formatProfitFactor = (val) => {
                 if (typeof val !== 'number' || isNaN(val)) {
                    return `<span class="text-gray-500">---</span>`;
                }
                 const color = val >= 1 ? 'text-green-500' : 'text-red-500';
                 return `<span class="${color}">${val.toFixed(2)}</span>`;
            };
            const formatNumber = (val) => {
                if (typeof val !== 'number' || isNaN(val)) {
                    return `<span class="text-gray-500">---</span>`;
                }
                return val.toFixed(2);
            };
            // === Koniec funkcji pomocniczych ===

            const createStatCard = (label, value, icon) => {
                return `<div class="bg-[#161B22] p-4 rounded-lg shadow-lg border border-gray-700">
                            <h3 class="font-semibold text-gray-400 flex items-center text-sm">
                                <i data-lucide="${icon}" class="w-4 h-4 mr-2 text-sky-400"></i>${label}
                            </h3>
                            <p class="text-3xl font-extrabold mt-2 text-white">${value}</p>
                        </div>`;
            };
            
            // --- Tabela statystyk per strategia (bez zmian) ---
            const setupRows = Object.entries(stats.by_setup).map(([setupName, setupStats]) => {
                return `<tr class="border-b border-gray-800 hover:bg-[#1f2937]">
                            <td class="p-3 font-semibold text-sky-400">${setupName}</td>
                            <td class="p-3 text-right">${setupStats.total_trades}</td>
                            <td class="p-3 text-right">${formatPercent(setupStats.win_rate_percent)}</td>
                            <td class="p-3 text-right">${formatPercent(setupStats.total_p_l_percent)}</td>
                            <td class="p-3 text-right">${formatProfitFactor(setupStats.profit_factor)}</td>
                        </tr>`;
            }).join('');
            
            const setupTable = setupRows.length > 0 ? 
                `<div class="overflow-x-auto bg-[#161B22] rounded-lg border border-gray-700">
                    <table class="w-full text-sm text-left text-gray-300">
                        <thead class="text-xs text-gray-400 uppercase bg-[#0D1117]">
                            <tr>
                                <th scope="col" class="p-3">Strategia</th>
                                <th scope="col" class="p-3 text-right">Ilość Transakcji</th>
                                <th scope="col" class="p-3 text-right">Win Rate (%)</th>
                                <th scope="col" class="p-3 text-right">Całkowity P/L (%)</th>
                                <th scope="col" class="p-3 text-right">Profit Factor</th>
                            </tr>
                        </thead>
                        <tbody>${setupRows}</tbody>
                    </table>
                 </div>` : `<p class="text-center text-gray-500 py-10">Brak danych per strategia.</p>`;

            // === NOWA, SZCZEGÓŁOWA TABELA HISTORII TRANSAKCJI ===
            
            // Definiujemy wszystkie nagłówki naszej nowej, szerokiej tabeli
            const tradeHeaders = [
                'Data Otwarcia', 'Ticker', 'Strategia', 'Status', 'Cena Wejścia', 'Cena Zamknięcia', 'P/L (%)',
                'ATR', 'T. Dil.', 'P. Grav.', 'TD %tile', 'PG %tile',
                'Inst. Sync', 'Retail Herd.',
                'AQM H3', 'AQM %tile', 'J (Norm)', '∇² (Norm)', 'm² (Norm)',
                'J (H4)', 'J Thresh.'
            ];
            
            // Definiujemy klasy CSS dla nagłówków (dla pozycjonowania i przyklejania)
            const headerClasses = [
                'sticky left-0', // Data Otwarcia
                'sticky left-[90px]', // Ticker
                'sticky left-[240px]', // Strategia (szersza)
                'text-right', // Status
                'text-right', // Cena Wejścia
                'text-right', // Cena Zamknięcia
                'text-right', // P/L (%)
                'text-right', // ATR
                'text-right', // T. Dil.
                'text-right', // P. Grav.
                'text-right', // TD %tile
                'text-right', // PG %tile
                'text-right', // Inst. Sync
                'text-right', // Retail Herd.
                'text-right', // AQM H3
                'text-right', // AQM %tile
                'text-right', // J (Norm)
                'text-right', // ∇² (Norm)
                'text-right', // m² (Norm)
                'text-right', // J (H4)
                'text-right'  // J Thresh.
            ];

            // Generujemy wiersze tabeli, teraz z nowymi danymi
            const tradeRows = trades.map(t => {
                const statusClass = t.status === 'CLOSED_TP' ? 'text-green-400' : (t.status === 'CLOSED_SL' ? 'text-red-400' : 'text-yellow-400');
                // Skracamy nazwę strategii dla czytelności
                const setupNameShort = (t.setup_type || 'UNKNOWN').replace('BACKTEST_', '').replace('_AQM_V3_', ' ').replace('QUANTUM_FIELD', 'H3').replace('INFO_THERMO', 'H4').replace('CONTRARIAN_ENTANGLEMENT', 'H2').replace('GRAVITY_MEAN_REVERSION', 'H1');
                
                return `<tr class="border-b border-gray-800 hover:bg-[#1f2937] text-xs font-mono">
                            <td class="p-2 whitespace-nowrap text-gray-400 sticky left-0 bg-[#161B22] hover:bg-[#1f2937]">${new Date(t.open_date).toLocaleDateString('pl-PL')}</td>
                            <td class="p-2 whitespace-nowrap font-bold text-sky-400 sticky left-[90px] bg-[#161B22] hover:bg-[#1f2937]">${t.ticker}</td>
                            <td class="p-2 whitespace-nowrap text-gray-300 sticky left-[160px] bg-[#161B22] hover:bg-[#1f2937]">${setupNameShort}</td>
                            
                            <td class="p-2 whitespace-nowrap text-right ${statusClass}">${t.status.replace('CLOSED_', '')}</td>
                            <td class="p-2 whitespace-nowrap text-right">${formatNumber(t.entry_price)}</td>
                            <td class="p-2 whitespace-nowrap text-right">${formatNumber(t.close_price)}</td>
                            <td class="p-2 whitespace-nowrap text-right font-bold">${formatPercent(t.final_profit_loss_percent)}</td>
                            
                            <td class="p-2 whitespace-nowrap text-right text-purple-300">${formatMetric(t.metric_atr_14)}</td>
                            
                            <td class="p-2 whitespace-nowrap text-right text-blue-300">${formatMetric(t.metric_time_dilation)}</td>
                            <td class="p-2 whitespace-nowrap text-right text-blue-300">${formatMetric(t.metric_price_gravity)}</td>
                            <td class="p-2 whitespace-nowrap text-right text-gray-500">${formatMetric(t.metric_td_percentile_90)}</td>
                            <td class="p-2 whitespace-nowrap text-right text-gray-500">${formatMetric(t.metric_pg_percentile_90)}</td>

                            <td class="p-2 whitespace-nowrap text-right text-green-300">${formatMetric(t.metric_inst_sync)}</td>
                            <td class="p-2 whitespace-nowrap text-right text-red-300">${formatMetric(t.metric_retail_herding)}</td>

                            <td class="p-2 whitespace-nowrap text-right text-yellow-300 font-bold">${formatMetric(t.metric_aqm_score_h3)}</td>
                            <td class="p-2 whitespace-nowrap text-right text-gray-500">${formatMetric(t.metric_aqm_percentile_95)}</td>
                            <td class="p-2 whitespace-nowrap text-right text-yellow-400">${formatMetric(t.metric_J_norm)}</td>
                            <td class="p-2 whitespace-nowrap text-right text-yellow-400">${formatMetric(t.metric_nabla_sq_norm)}</td>
                            <td class="p-2 whitespace-nowrap text-right text-yellow-400">${formatMetric(t.metric_m_sq_norm)}</td>

                            <td class="p-2 whitespace-nowrap text-right text-pink-300">${formatMetric(t.metric_J)}</td>
                            <td class="p-2 whitespace-nowrap text-right text-gray-500">${formatMetric(t.metric_J_threshold_2sigma)}</td>
                        </tr>`;
            }).join('');

            // Tworzymy finalną tabelę z kontenerem do przewijania
            const tradeTable = trades.length > 0 ?
                 `<div class="overflow-x-auto bg-[#161B22] rounded-lg border border-gray-700 max-h-[500px] overflow-y-auto">
                    <table class="w-full text-sm text-left text-gray-300 min-w-[2400px]">
                        <thead class="text-xs text-gray-400 uppercase bg-[#0D1117] sticky top-0 z-10">
                            <tr>
                                ${tradeHeaders.map((h, index) => `<th scope="col" class="p-2 whitespace-nowrap ${headerClasses[index]} ${index < 3 ? 'bg-[#0D1117]' : ''}">${h}</th>`).join('')}
                            </tr>
                        </thead>
                        <tbody>${tradeRows}</tbody>
                    </table>
                 </div>` : `<p class="text-center text-gray-500 py-10">Brak zamkniętych transakcji do wyświetlenia.</p>`;
            
            // --- Sekcje Backtestu i AI (bez zmian) ---
            const backtestSection = `
                <div class="bg-[#161B22] p-6 rounded-lg shadow-lg border border-gray-700">
                    <h4 class="text-lg font-semibold text-gray-300 mb-3">Uruchom Nowy Test Historyczny</h4>
                    <p class="text-sm text-gray-500 mb-4">Wpisz rok (np. 2010), aby przetestować strategie na historycznych danych dla tego roku.</p>
                    <div class="flex items-start gap-3">
                        <input type="number" id="backtest-year-input" class="modal-input w-32 !mb-0" placeholder="YYYY" min="2000" max="${new Date().getFullYear()}">
                        <button id="run-backtest-year-btn" class="modal-button modal-button-primary flex items-center flex-shrink-0">
                            <i data-lucide="play" class="w-4 h-4 mr-2"></i>
                            Uruchom Test Roczny
                        </button>
                    </div>
                    <div id="backtest-status-message" class="text-sm mt-3 h-4"></div>
                </div>
            `;
            
            const aiOptimizerSection = `
                <div class="bg-[#161B22] p-6 rounded-lg shadow-lg border border-gray-700">
                    <h4 class="text-lg font-semibold text-gray-300 mb-3">Analiza Mega Agenta AI</h4>
                    <p class="text-sm text-gray-500 mb-4">Uruchom Mega Agenta, aby przeanalizował wszystkie zebrane dane (powyżej) i zasugerował optymalizacje strategii.</p>
                    <div class="flex items-start gap-3">
                        <button id="run-ai-optimizer-btn" class="modal-button modal-button-primary flex items-center flex-shrink-0">
                            <i data-lucide="brain-circuit" class="w-4 h-4 mr-2"></i>
                            Uruchom Analizę AI
                        </button>
                        <button id="view-ai-report-btn" class="modal-button modal-button-secondary flex items-center flex-shrink-0">
                            <i data-lucide="eye" class="w-4 h-4 mr-2"></i>
                            Pokaż Ostatni Raport
                        </button>
                    </div>
                    <div id="ai-optimizer-status-message" class="text-sm mt-3 h-4"></div>
                </div>
            `;

            // ==========================================================
            // === NOWA SEKCJA: Przycisk Eksportu CSV ===
            // ==========================================================
            const exportSection = `
                <div class="bg-[#161B22] p-6 rounded-lg shadow-lg border border-gray-700">
                    <h4 class="text-lg font-semibold text-gray-300 mb-3">Eksport Danych</h4>
                    <p class="text-sm text-gray-500 mb-4">Pobierz *wszystkie* ${total_trades_count} transakcje z bazy danych jako plik CSV do własnej analizy.</p>
                    <div class="flex items-start gap-3">
                        <button id="run-csv-export-btn" class="modal-button modal-button-primary flex items-center flex-shrink-0">
                            <i data-lucide="download-cloud" class="w-4 h-4 mr-2"></i>
                            Eksportuj do CSV
                        </button>
                    </div>
                    <div id="csv-export-status-message" class="text-sm mt-3 h-4"></div>
                </div>
            `;
            // ==========================================================


            // === NOWA SEKCJA STRONICOWANIA ===
            const totalPages = Math.ceil(total_trades_count / REPORT_PAGE_SIZE);
            const startTrade = (state.currentReportPage - 1) * REPORT_PAGE_SIZE + 1;
            const endTrade = Math.min(state.currentReportPage * REPORT_PAGE_SIZE, total_trades_count);

            const paginationControls = totalPages > 1 ? `
                <div class="flex justify-between items-center mt-4">
                    <span class="text-sm text-gray-400">
                        Wyświetlanie ${startTrade}-${endTrade} z ${total_trades_count} transakcji
                    </span>
                    <div class="flex gap-2">
                        <button id="report-prev-btn" class="modal-button modal-button-secondary" ${state.currentReportPage === 1 ? 'disabled' : ''}>
                            <i data-lucide="arrow-left" class="w-4 h-4"></i>
                        </button>
                        <span class="text-sm text-gray-400 p-2">Strona ${state.currentReportPage} / ${totalPages}</span>
                        <button id="report-next-btn" class="modal-button modal-button-secondary" ${state.currentReportPage === totalPages ? 'disabled' : ''}>
                            <i data-lucide="arrow-right" class="w-4 h-4"></i>
                        </button>
                    </div>
                </div>
            ` : '';

            // --- OSTATECZNY HTML ---
            return `<div id="agent-report-view" class="max-w-6xl mx-auto">
                        <h2 class="text-2xl font-bold text-sky-400 mb-6 border-b border-gray-700 pb-2">Raport Wydajności Agenta</h2>
                        
                        <h3 class="text-xl font-bold text-gray-300 mb-4">Kluczowe Wskaźniki (Wszystkie ${stats.total_trades} Transakcji)</h3>
                        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
                            ${createStatCard('Całkowity P/L (%)', formatPercent(stats.total_p_l_percent), 'percent')}
                            ${createStatCard('Win Rate (%)', formatPercent(stats.win_rate_percent), 'target')}
                            ${createStatCard('Profit Factor', formatProfitFactor(stats.profit_factor), 'ratio')}
                            ${createStatCard('Ilość Transakcji', stats.total_trades, 'bar-chart-2')}
                        </div>
                        
                        <h3 class="text-xl font-bold text-gray-300 mb-4">Podsumowanie wg Strategii</h3>
                        ${setupTable}
                        
                        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mt-8">
                            <div>
                                <h3 class="text-xl font-bold text-gray-300 mb-4">Uruchom Backtesting</h3>
                                ${backtestSection}
                            </div>
                            <div>
                                <h3 class="text-xl font-bold text-gray-300 mb-4">Optymalizacja AI</h3>
                                ${aiOptimizerSection}
                            </div>
                            <div class="lg:col-span-1">
                                <h3 class="text-xl font-bold text-gray-300 mb-4">Pobierz Dane</h3>
                                ${exportSection}
                            </div>
                        </div>

                        <h3 class="text-xl font-bold text-gray-300 mt-8 mb-4">Historia Zamkniętych Transakcji (z Metrykami)</h3>
                        ${paginationControls}
                        ${tradeTable}
                        ${paginationControls} </div>`;
        }
        // ==========================================================
    };
    console.log("Renderers defined.");

    // --- GŁÓWNE FUNKCJE KONTROLI APLIKACJI ---
    
    function updateDashboardCounters() {
        const activeEl = document.getElementById('dashboard-active-signals');
        const discardedEl = document.getElementById('dashboard-discarded-signals');
        
        if (activeEl) {
            activeEl.textContent = state.phase3.length;
        }
        if (discardedEl) {
            discardedEl.textContent = state.discardedSignalCount;
        }
    }

    function updateDashboardUI(statusData) {
        if (!document.getElementById('dashboard-view')) return;
        
        const ui_dash = {
            status: document.getElementById('dashboard-worker-status'),
            phase: document.getElementById('dashboard-current-phase'),
            progressText: document.getElementById('progress-text'),
            progressBar: document.getElementById('progress-bar'),
            scanLog: document.getElementById('scan-log'),
        };
        if (!ui_dash.status || !ui_dash.scanLog) {
            logger.warn("Elementy UI dashboardu nie znalezione, pomijam aktualizację.");
            return;
        }
        ui_dash.status.textContent = statusData.status;
        ui_dash.phase.textContent = `Faza: ${statusData.phase || 'NONE'}`;
        const processed = statusData.progress.processed, total = statusData.progress.total;
        const percent = total > 0 ? Math.min((processed / total) * 100, 100) : 0;
        ui_dash.progressText.textContent = `${processed} / ${total}`;
        ui_dash.progressBar.style.width = `${percent.toFixed(0)}%`;
        
        if (ui_dash.scanLog.textContent !== statusData.log) {
            ui_dash.scanLog.textContent = statusData.log || 'Czekam na rozpoczęcie skanowania...';
            const logContainer = document.getElementById('scan-log-container');
            if(logContainer) logContainer.scrollTop = 0; 
        }
    }

    function displaySystemAlert(message) {
        if (!message || message === 'NONE') return;

        let alertKey = 'GENERAL';
        try {
            const parts = message.split(' ');
            const ticker = parts.find(p => p.length > 2 && p.length < 6 && p === p.toUpperCase());
            
            if (message.includes('ALERT ZYSKU')) alertKey = `PROFIT-${ticker || 'UNKNOWN'}`;
            else if (message.includes('ALARM CENOWY')) alertKey = `PRICE-${ticker || 'UNKNOWN'}`;
            else if (message.includes('PILNY ALERT')) alertKey = `NEWS-${ticker || 'UNKNOWN'}`;
            else if (message.includes('TAKE PROFIT')) alertKey = `TP-${ticker || 'UNKNOWN'}`;
            else if (message.includes('STOP LOSS')) alertKey = `SL-${ticker || 'UNKNOWN'}`;

        } catch(e) { 
            logger.warn("Nie udało się wygenerować klucza alertu", e);
        }

        if (state.snoozedAlerts[alertKey] && Date.now() < state.snoozedAlerts[alertKey]) {
            logger.info(`Alert ${alertKey} jest wyciszony. Pomijanie.`);
            return;
        }

        let alertClass = 'bg-sky-500';
        let alertIcon = 'bell-ring';

        if (message.includes('PILNY ALERT') && message.includes('NEGATYWNY')) {
            alertClass = 'bg-red-600';
            alertIcon = 'alert-octagon';
        } else if (message.includes('PILNY ALERT') && message.includes('POZYTYWNY')) {
            alertClass = 'bg-green-600';
            alertIcon = 'check-circle';
        } else if (message.includes('ALARM CENOWY') || message.includes('ALERT ZYSKU')) {
            alertClass = 'bg-yellow-500';
            alertIcon = 'dollar-sign';
        } else if (message.includes('TAKE PROFIT')) {
            alertClass = 'bg-green-600';
            alertIcon = 'trending-up';
        } else if (message.includes('STOP LOSS')) {
            alertClass = 'bg-red-600';
            alertIcon = 'trending-down';
        }


        const alertId = `alert-${Date.now()}`;
        const alertElement = document.createElement('div');
        alertElement.id = alertId;
        alertElement.className = `alert-bar flex items-center justify-between gap-4 ${alertClass} text-white p-3 shadow-lg rounded-md animate-pulse-once`;
        
        alertElement.innerHTML = `
            <div class="flex items-center gap-3">
                <i data-lucide="${alertIcon}" class="w-6 h-6"></i>
                <span class="font-semibold">${message}</span>
            </div>
            <button data-alert-id="${alertId}" data-alert-key="${alertKey}" class="close-alert-btn p-1 rounded-full hover:bg-black/20 transition-colors">
                <i data-lucide="x" class="w-5 h-5"></i>
            </button>
        `;
        ui.alertContainer.appendChild(alertElement);
        lucide.createIcons();

        const closeButton = alertElement.querySelector('.close-alert-btn');
        closeButton.addEventListener('click', () => {
            const keyToSnooze = closeButton.dataset.alertKey;
            if (keyToSnooze) {
                const snoozeDuration = 30 * 60 * 1000; // 30 minut
                state.snoozedAlerts[keyToSnooze] = Date.now() + snoozeDuration;
                logger.info(`Wyciszono alert '${keyToSnooze}' na 30 minut.`);
            }
            alertElement.remove();
        });

        setTimeout(() => {
            const elToRemove = document.getElementById(alertId);
            if (elToRemove) {
                elToRemove.remove();
            }
        }, 20000);
    }


    async function pollSystemAlerts() {
        try {
            const alertData = await api.getSystemAlert();
            if (alertData && alertData.message !== 'NONE') {
                displaySystemAlert(alertData.message);
            }
        } catch (e) {
        } finally {
            setTimeout(pollSystemAlerts, ALERT_POLL_INTERVAL);
        }
    }


    async function pollWorkerStatus() {
        try {
            const statusData = await api.getWorkerStatus();
            state.workerStatus = statusData;
            let statusClass = 'bg-gray-700 text-gray-200';
            if (statusData.status === 'RUNNING') statusClass = 'bg-green-600/20 text-green-400';
            else if (statusData.status === 'PAUSED') statusClass = 'bg-yellow-600/20 text-yellow-400';
            else if (statusData.status === 'ERROR') statusClass = 'bg-red-600/20 text-red-400';
            
            if (statusData.phase === 'BACKTESTING') {
                statusClass = 'bg-purple-600/20 text-purple-400';
            } else if (statusData.phase === 'AI_OPTIMIZING') {
                statusClass = 'bg-pink-600/20 text-pink-400';
            }

            ui.workerStatusText.className = `font-mono px-2 py-1 rounded-md text-xs ${statusClass} transition-colors`;
            ui.workerStatusText.textContent = statusData.phase === 'NONE' ? statusData.status : statusData.phase; // Pokaż fazę, jeśli jest aktywna

            if (statusData.last_heartbeat_utc) {
                const diffSeconds = (new Date() - new Date(statusData.last_heartbeat_utc)) / 1000;
                ui.heartbeatStatus.className = `text-xs ${diffSeconds > 30 ? 'text-red-500' : 'text-green-500'}`;
                ui.heartbeatStatus.textContent = diffSeconds > 30 ? 'PRZERWANY' : new Date(statusData.last_heartbeat_utc).toLocaleTimeString();
            }
            ui.startBtn.disabled = statusData.status !== 'IDLE' && statusData.status !== 'ERROR';
            ui.pauseBtn.disabled = statusData.status !== 'RUNNING';
            ui.resumeBtn.disabled = statusData.status !== 'PAUSED';
            updateDashboardUI(statusData);
        } catch (e) { /* Błędy logowane w apiRequest */ }
        setTimeout(pollWorkerStatus, 5000);
    }

    async function refreshSidebarData() {
        try {
            const [phase1, phase2, phase3, discardedCountData] = await Promise.all([
                api.getPhase1Candidates(), 
                api.getPhase2Results(), 
                api.getPhase3Signals(),
                api.getDiscardedCount()
            ]);
            
            state.phase1 = phase1 || [];
            state.phase2 = phase2 || [];
            state.phase3 = phase3 || [];
            state.discardedSignalCount = discardedCountData?.discarded_count_24h ?? 0;

            ui.phase1.list.innerHTML = renderers.phase1List(state.phase1);
            ui.phase1.count.textContent = state.phase1.length;
            ui.phase2.list.innerHTML = renderers.phase2List(state.phase2);
            ui.phase2.count.textContent = state.phase2.length;
            ui.phase3.list.innerHTML = renderers.phase3List(state.phase3);
            ui.phase3.count.textContent = state.phase3.length;
            
            updateDashboardCounters();

            lucide.createIcons();
        } catch (e) { /* Błędy logowane w apiRequest */ }
        setTimeout(refreshSidebarData, 15000); // Interwał bez zmian
    }

    function stopAllPolling() {
        logger.info("Zatrzymywanie wszystkich aktywnych timerów odpytywania.");
        if (state.activePortfolioPolling) { clearTimeout(state.activePortfolioPolling); state.activePortfolioPolling = null; }
        if (state.activeAIOptimizerPolling) { clearTimeout(state.activeAIOptimizerPolling); state.activeAIOptimizerPolling = null; }
        stopMarketCountdown(); 
    }

    function setActiveSidebar(linkElement) {
        document.querySelectorAll('.sidebar-item').forEach(el => el.classList.remove('sidebar-item-active'));
        if (linkElement) linkElement.classList.add('sidebar-item-active');
    }

    async function showDashboard() {
         stopAllPolling();
         setActiveSidebar(ui.dashboardLink);
         ui.mainContent.innerHTML = renderers.dashboard();
         updateDashboardUI(state.workerStatus);
         updateDashboardCounters();
         lucide.createIcons();
    }

    async function showPortfolio() {
        stopAllPolling();
        setActiveSidebar(ui.portfolioLink);
        ui.mainContent.innerHTML = renderers.loading("Ładowanie portfela...");
        try {
            const holdings = await api.getPortfolio();
            state.portfolio = holdings;
            state.liveQuotes = {}; // Wyczyść cache cen
            state.profitAlertsSent = {}; // Resetuj przy każdym załadowaniu portfela
            ui.mainContent.innerHTML = renderers.portfolio(state.portfolio, state.liveQuotes);
            lucide.createIcons();
            startPortfolioPolling(); // Rozpocznij odświeżanie cen
        } catch (e) {
             ui.mainContent.innerHTML = `<div class="bg-red-900/20 border border-red-500/30 text-red-300 p-6 rounded-lg text-center">Błąd ładowania portfela: ${e.message}</div>`;
        }
    }

    async function showTransactions() {
        stopAllPolling();
        setActiveSidebar(ui.transactionsLink);
        ui.mainContent.innerHTML = renderers.loading("Ładowanie historii transakcji...");
        try {
            const transactions = await api.getTransactionHistory();
            state.transactions = transactions;
            ui.mainContent.innerHTML = renderers.transactions(state.transactions);
            lucide.createIcons();
        } catch (e) {
             ui.mainContent.innerHTML = `<div class="bg-red-900/20 border border-red-500/30 text-red-300 p-6 rounded-lg text-center">Błąd ładowania transakcji: ${e.message}</div>`;
        }
    }
    
    // ==========================================================
    // === AKTUALIZACJA (STRONICOWANIE) ===
    // ==========================================================
    async function showAgentReport() {
        stopAllPolling();
        setActiveSidebar(ui.agentReportLink);
        // Resetuj na pierwszą stronę za każdym razem, gdy wchodzisz w ten widok
        await loadAgentReportPage(1);
    }
    
    async function loadAgentReportPage(page) {
        state.currentReportPage = page;
        ui.mainContent.innerHTML = renderers.loading(`Ładowanie raportu... (Strona ${page})`);
        try {
            // Pobierz konkretną stronę raportu
            const report = await api.getVirtualAgentReport(page);
            ui.mainContent.innerHTML = renderers.agentReport(report);
            lucide.createIcons();
        } catch (e) {
             ui.mainContent.innerHTML = `<div class="bg-red-900/20 border border-red-500/30 text-red-300 p-6 rounded-lg text-center">Błąd ładowania raportu agenta: ${e.message}</div>`;
        }
    }
    // ==========================================================
    
    async function pollPortfolioQuotes() {
         const portfolioTickers = state.portfolio.map(h => h.ticker);
         if (portfolioTickers.length === 0) { 
            state.activePortfolioPolling = null; 
            return; 
         }
         
         logger.info(`Odświeżanie cen dla portfela: ${portfolioTickers.join(', ')}`);
         let quotesUpdated = false;
         try {
             const quotePromises = portfolioTickers.map(ticker => api.getLiveQuote(ticker));
             const quoteResults = await Promise.all(quotePromises);
             
             const newQuotes = { ...state.liveQuotes };
             quoteResults.forEach((quoteData, index) => {
                 const ticker = portfolioTickers[index];
                 if (quoteData) { 
                     newQuotes[ticker] = quoteData; 
                     quotesUpdated = true; 
                 }
             });
             state.liveQuotes = newQuotes; 

             if (quotesUpdated && document.getElementById('portfolio-view')) {
                  ui.mainContent.innerHTML = renderers.portfolio(state.portfolio, state.liveQuotes);
                  lucide.createIcons();
             }

            checkPortfolioProfitAlerts();

         } catch (e) {
             logger.error("Błąd podczas odświeżania cen portfela:", e);
         } finally {
              state.activePortfolioPolling = setTimeout(pollPortfolioQuotes, PORTFOLIO_QUOTE_POLL_INTERVAL);
         }
    }
    
    function checkPortfolioProfitAlerts() {
        logger.info("Sprawdzanie alertów zysku w portfelu...");
        state.portfolio.forEach(holding => {
            const quote = state.liveQuotes[holding.ticker];
            if (quote && quote['05. price']) {
                try {
                    const currentPrice = parseFloat(quote['05. price']);
                    const avgBuyPrice = holding.average_buy_price;
                    
                    if (currentPrice >= (avgBuyPrice * PROFIT_ALERT_THRESHOLD)) {
                        if (!state.profitAlertsSent[holding.ticker]) {
                            const profitPercent = ((currentPrice / avgBuyPrice) - 1) * 100;
                            const alertMsg = `ALERT ZYSKU: ${holding.ticker} osiągnął +${profitPercent.toFixed(1)}% (Cena: ${currentPrice.toFixed(2)})`;
                            logger.warn(alertMsg);
                            displaySystemAlert(alertMsg);
                            state.profitAlertsSent[holding.ticker] = true; 
                        }
                    } else {
                        if (state.profitAlertsSent[holding.ticker]) {
                            logger.info(`Resetowanie alertu zysku dla ${holding.ticker}, cena spadła poniżej progu.`);
                            state.profitAlertsSent[holding.ticker] = false;
                        }
                    }
                } catch(e) {
                    logger.error(`Błąd podczas sprawdzania alertu zysku dla ${holding.ticker}:`, e);
                }
            }
        });
    }

    function startPortfolioPolling() {
         stopPortfolioPolling();
         if (state.portfolio.length > 0) {
             logger.info("Rozpoczynam odświeżanie cen portfela.");
             pollPortfolioQuotes();
         }
    }
    function stopPortfolioPolling() {
         if (state.activePortfolioPolling) {
             logger.info("Zatrzymuję odświeżanie cen portfela.");
             clearTimeout(state.activePortfolioPolling);
             state.activePortfolioPolling = null;
         }
    }

    // --- Funkcje Obsługi Modali ---
    function showBuyModal(ticker) {
        ui.buyModal.tickerSpan.textContent = ticker;
        ui.buyModal.quantityInput.value = '';
        ui.buyModal.priceInput.value = '';
        ui.buyModal.confirmBtn.dataset.ticker = ticker;
        ui.buyModal.backdrop.classList.remove('hidden');
        ui.buyModal.quantityInput.focus();
    }
    function hideBuyModal() { ui.buyModal.backdrop.classList.add('hidden'); }
    
    async function handleBuyConfirm() {
         const ticker = ui.buyModal.confirmBtn.dataset.ticker;
         const quantity = parseInt(ui.buyModal.quantityInput.value, 10);
         const price = parseFloat(ui.buyModal.priceInput.value);
         if (!ticker || isNaN(quantity) || quantity <= 0 || isNaN(price) || price <= 0) {
             displaySystemAlert("BŁĄD: Proszę wprowadzić poprawną ilość i cenę.");
             return; 
         }
         ui.buyModal.confirmBtn.disabled = true; ui.buyModal.confirmBtn.textContent = "Przetwarzanie...";
         try {
            await api.buyStock({ ticker, quantity, price_per_share: price });
            hideBuyModal();
            showPortfolio();
        } catch (e) { displaySystemAlert(`Błąd zakupu: ${e.message}`);
        } finally { ui.buyModal.confirmBtn.disabled = false; ui.buyModal.confirmBtn.textContent = "Inwestuj"; }
    }
    
    function showSellModal(ticker, maxQuantity) {
         ui.sellModal.tickerSpan.textContent = ticker;
         ui.sellModal.maxQuantitySpan.textContent = maxQuantity;
         ui.sellModal.quantityInput.value = '';
         ui.sellModal.quantityInput.max = maxQuantity;
         ui.sellModal.priceInput.value = '';
         ui.sellModal.confirmBtn.dataset.ticker = ticker;
         ui.sellModal.confirmBtn.dataset.maxQuantity = maxQuantity;
         ui.sellModal.backdrop.classList.remove('hidden');
         ui.sellModal.quantityInput.focus();
    }
    function hideSellModal() { ui.sellModal.backdrop.classList.add('hidden'); }
    
    async function handleSellConfirm() {
         const ticker = ui.sellModal.confirmBtn.dataset.ticker;
         const maxQuantity = parseInt(ui.sellModal.confirmBtn.dataset.maxQuantity, 10);
         const quantity = parseInt(ui.sellModal.quantityInput.value, 10);
         const price = parseFloat(ui.sellModal.priceInput.value);
         if (!ticker || isNaN(quantity) || quantity <= 0 || isNaN(price) || price <= 0) {
             displaySystemAlert("BŁĄD: Proszę wprowadzić poprawną ilość i cenę."); 
             return; 
         }
          if (quantity > maxQuantity) { 
              displaySystemAlert(`BŁĄD: Nie możesz sprzedać więcej akcji niż posiadasz (${maxQuantity}).`); 
              return; 
          }
         ui.sellModal.confirmBtn.disabled = true; ui.sellModal.confirmBtn.textContent = "Przetwarzanie...";
         try {
             await api.sellStock({ ticker, quantity, price_per_share: price });
             hideSellModal();
             await showPortfolio();
         } catch (e) { displaySystemAlert(`Błąd sprzedaży: ${e.message}`);
         } finally { ui.sellModal.confirmBtn.disabled = false; ui.sellModal.confirmBtn.textContent = "Realizuj"; }
    }
    
    function showAIReportModal() {
        if (ui.aiReportModal.backdrop) {
            ui.aiReportModal.backdrop.classList.remove('hidden');
            ui.aiReportModal.content.innerHTML = renderers.loading('Pobieranie raportu...');
            lucide.createIcons();
        }
    }
    function hideAIReportModal() {
        if (ui.aiReportModal.backdrop) {
            ui.aiReportModal.backdrop.classList.add('hidden');
            ui.aiReportModal.content.innerHTML = ''; // Wyczyść zawartość
        }
    }

    async function handleRunAIOptimizer() {
        const runBtn = document.getElementById('run-ai-optimizer-btn');
        const viewBtn = document.getElementById('view-ai-report-btn');
        const statusMsg = document.getElementById('ai-optimizer-status-message');
        if (!runBtn || !viewBtn || !statusMsg) return;

        runBtn.disabled = true;
        viewBtn.disabled = true;
        runBtn.innerHTML = `<i data-lucide="loader-2" class="w-4 h-4 mr-2 animate-spin"></i> Zlecanie...`;
        lucide.createIcons();
        statusMsg.className = 'text-sm mt-3 text-sky-400';
        statusMsg.textContent = 'Zlecanie analizy Mega Agentowi...';

        try {
            const response = await api.requestAIOptimizer();
            statusMsg.className = 'text-sm mt-3 text-green-400';
            statusMsg.textContent = response.message || 'Zlecono analizę. Worker rozpoczął pracę.';
            pollAIOptimizerReport();
        } catch (e) {
            statusMsg.className = 'text-sm mt-3 text-red-400';
            statusMsg.textContent = `Błąd zlecenia: ${e.message}`;
            runBtn.disabled = false;
            viewBtn.disabled = false;
            runBtn.innerHTML = `<i data-lucide="brain-circuit" class="w-4 h-4 mr-2"></i> Uruchom Analizę AI`;
            lucide.createIcons();
        }
    }
    
    async function pollAIOptimizerReport() {
        if (state.activeAIOptimizerPolling) {
            clearTimeout(state.activeAIOptimizerPolling);
        }
        
        const statusMsg = document.getElementById('ai-optimizer-status-message');
        
        try {
            const reportData = await api.getAIOptimizerReport();
            
            if (reportData.status === 'PROCESSING') {
                if(statusMsg) statusMsg.textContent = 'Worker przetwarza dane i konsultuje się z AI... (Sprawdzam ponownie za 5s)';
                state.activeAIOptimizerPolling = setTimeout(pollAIOptimizerReport, AI_OPTIMIZER_POLL_INTERVAL);
            } else if (reportData.status === 'DONE') {
                if(statusMsg) {
                    statusMsg.className = 'text-sm mt-3 text-green-400';
                    statusMsg.textContent = `Analiza AI zakończona (${new Date(reportData.last_updated).toLocaleString()}).`;
                }
                const runBtn = document.getElementById('run-ai-optimizer-btn');
                const viewBtn = document.getElementById('view-ai-report-btn');
                if (runBtn) {
                     runBtn.disabled = false;
                     runBtn.innerHTML = `<i data-lucide="brain-circuit" class="w-4 h-4 mr-2"></i> Uruchom Analizę AI`;
                     lucide.createIcons();
                }
                if (viewBtn) viewBtn.disabled = false;
                
                showAIReportModal();
                if (ui.aiReportModal.content) {
                    ui.aiReportModal.content.innerHTML = `<pre class="text-xs whitespace-pre-wrap font-mono">${reportData.report_text}</pre>`;
                }

            } else { // 'NONE' lub 'ERROR'
                if(statusMsg) {
                    statusMsg.className = 'text-sm mt-3 text-gray-400';
                    statusMsg.textContent = reportData.status === 'ERROR' ? reportData.report_text : 'Gotowy do analizy.';
                }
                const runBtn = document.getElementById('run-ai-optimizer-btn');
                const viewBtn = document.getElementById('view-ai-report-btn');
                if (runBtn) {
                     runBtn.disabled = false;
                     runBtn.innerHTML = `<i data-lucide="brain-circuit" class="w-4 h-4 mr-2"></i> Uruchom Analizę AI`;
                     lucide.createIcons();
                }
                if (viewBtn) viewBtn.disabled = false;
            }

        } catch (e) {
            logger.error('Błąd podczas odpytywania o raport AI', e);
            if(statusMsg) {
                statusMsg.className = 'text-sm mt-3 text-red-400';
                statusMsg.textContent = `Błąd odpytywania: ${e.message}`;
            }
        }
    }

    async function handleViewAIOptimizerReport() {
        showAIReportModal();
        try {
            const reportData = await api.getAIOptimizerReport();
            if (reportData.status === 'DONE' && reportData.report_text) {
                 if (ui.aiReportModal.content) {
                    ui.aiReportModal.content.innerHTML = `<pre class="text-xs whitespace-pre-wrap font-mono">${reportData.report_text}</pre>`;
                }
            } else if (reportData.status === 'PROCESSING') {
                if (ui.aiReportModal.content) {
                    ui.aiReportModal.content.innerHTML = renderers.loading('Analiza w toku... Worker nadal przetwarza dane.');
                    lucide.createIcons();
                }
            } else {
                 if (ui.aiReportModal.content) {
                    ui.aiReportModal.content.innerHTML = `<p class="text-gray-400">Brak dostępnego raportu. Uruchom najpierw analizę.</p>`;
                }
            }
        } catch (e) {
             if (ui.aiReportModal.content) {
                ui.aiReportModal.content.innerHTML = `<p class="text-red-400">Błąd pobierania raportu: ${e.message}</p>`;
            }
        }
    }
    
    async function handleYearBacktestRequest() {
        const yearInput = document.getElementById('backtest-year-input');
        const yearBtn = document.getElementById('run-backtest-year-btn');
        const statusMsg = document.getElementById('backtest-status-message');
        if (!yearInput || !yearBtn || !statusMsg) return;

        const year = yearInput.value.trim();
        const currentYear = new Date().getFullYear();

        if (!year || year.length !== 4 || !/^\d{4}$/.test(year) || parseInt(year) < 2000 || parseInt(year) > currentYear) {
            statusMsg.className = 'text-sm mt-3 text-red-400';
            statusMsg.textContent = `Błąd: Wprowadź poprawny rok (np. 2000 - ${currentYear}).`;
            return;
        }

        yearBtn.disabled = true;
        yearBtn.innerHTML = `<i data-lucide="loader-2" class="w-4 h-4 mr-2 animate-spin"></i> Zlecanie...`;
        lucide.createIcons();
        statusMsg.className = 'text-sm mt-3 text-sky-400';
        statusMsg.textContent = `Zlecanie testu dla roku ${year}...`;

        try {
            const response = await api.requestBacktest(year);
            statusMsg.className = 'text-sm mt-3 text-green-400';
            statusMsg.textContent = response.message || `Zlecono test dla ${year}. Worker rozpoczął pracę.`;
            setTimeout(() => {
                if (document.getElementById('dashboard-view')) {
                    pollWorkerStatus();
                }
            }, 2000);
        } catch (e) {
            statusMsg.className = 'text-sm mt-3 text-red-400';
            statusMsg.textContent = `Błąd zlecenia: ${e.message}`;
        } finally {
            yearBtn.disabled = false;
            yearBtn.innerHTML = `<i data-lucide="play" class="w-4 h-4 mr-2"></i> Uruchom Test Roczny`;
            lucide.createIcons();
        }
    }
    
    // ==========================================================
    // === NOWA FUNKCJA: Obsługa Eksportu CSV ===
    // ==========================================================
    async function handleCsvExport() {
        const exportBtn = document.getElementById('run-csv-export-btn');
        const statusMsg = document.getElementById('csv-export-status-message');
        if (!exportBtn || !statusMsg) return;

        exportBtn.disabled = true;
        exportBtn.innerHTML = `<i data-lucide="loader-2" class="w-4 h-4 mr-2 animate-spin"></i> Pobieranie...`;
        lucide.createIcons();
        statusMsg.className = 'text-sm mt-3 text-sky-400';
        statusMsg.textContent = 'Trwa pobieranie danych... to może potrwać chwilę.';

        try {
            // Musimy użyć 'fetch' bezpośrednio, aby obsłużyć blob, a nie 'apiRequest' (który oczekuje JSON)
            const response = await fetch(`${API_BASE_URL}/api/v1/export/trades.csv`);
            
            if (!response.ok) {
                throw new Error(`Błąd serwera: ${response.status} ${response.statusText}`);
            }

            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const link = document.createElement('a');
            link.href = url;
            
            // Pobranie nazwy pliku z nagłówka Content-Disposition
            const disposition = response.headers.get('content-disposition');
            let filename = 'apex_virtual_trades_export.csv';
            if (disposition && disposition.indexOf('attachment') !== -1) {
                const filenameRegex = /filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/;
                const matches = filenameRegex.exec(disposition);
                if (matches != null && matches[1]) {
                    filename = matches[1].replace(/['"]/g, '');
                }
            }

            link.setAttribute('download', filename);
            document.body.appendChild(link);
            link.click();
            
            // Sprzątanie
            link.parentNode.removeChild(link);
            window.URL.revokeObjectURL(url);

            statusMsg.className = 'text-sm mt-3 text-green-400';
            statusMsg.textContent = 'Eksport zakończony pomyślnie.';

        } catch (e) {
            logger.error("Błąd eksportu CSV:", e);
            statusMsg.className = 'text-sm mt-3 text-red-400';
            statusMsg.textContent = `Błąd eksportu: ${e.message}`;
        } finally {
            exportBtn.disabled = false;
            exportBtn.innerHTML = `<i data-lucide="download-cloud" class="w-4 h-4 mr-2"></i> Eksportuj do CSV`;
            lucide.createIcons();
        }
    }
    // ==========================================================
    
    // --- Handlery Zdarzeń (Dodawane od razu) ---
    
    ui.mainContent.addEventListener('click', async e => {
        const sellBtn = e.target.closest('.sell-stock-btn');
        const backtestYearBtn = e.target.closest('#run-backtest-year-btn');
        const runAIOptimizerBtn = e.target.closest('#run-ai-optimizer-btn');
        const viewAIReportBtn = e.target.closest('#view-ai-report-btn');
        // ==========================================================
        // === NOWY LISTENER (CSV) ===
        // ==========================================================
        const exportCsvBtn = e.target.closest('#run-csv-export-btn');
        // ==========================================================
        const prevBtn = e.target.closest('#report-prev-btn');
        const nextBtn = e.target.closest('#report-next-btn');
        // ==========================================================

        if (backtestYearBtn) {
            handleYearBacktestRequest();
        }
        else if (runAIOptimizerBtn) {
            handleRunAIOptimizer();
        }
        else if (viewAIReportBtn) {
            handleViewAIOptimizerReport();
        }
        // ==========================================================
        // === NOWY HANDLER (CSV) ===
        // ==========================================================
        else if (exportCsvBtn) {
            handleCsvExport();
        }
        // ==========================================================
        else if (sellBtn) {
             const ticker = sellBtn.dataset.ticker;
             const quantity = parseInt(sellBtn.dataset.quantity, 10);
             if (ticker && !isNaN(quantity)) showSellModal(ticker, quantity);
        }
        // ==========================================================
        // === NOWE HANDLERY (STRONICOWANIE) ===
        // ==========================================================
        else if (prevBtn && !prevBtn.disabled) {
            loadAgentReportPage(state.currentReportPage - 1);
        }
        else if (nextBtn && !nextBtn.disabled) {
            loadAgentReportPage(state.currentReportPage + 1);
        }
        // ==========================================================
    });

    ui.sidebarPhasesContainer.addEventListener('click', (e) => {
        const accordionToggle = e.target.closest('.accordion-toggle');
        
        if (accordionToggle) {
            const content = accordionToggle.nextElementSibling;
            const icon = accordionToggle.querySelector('.accordion-icon');
            if (content && icon) {
                 content.classList.toggle('hidden');
                 icon.classList.toggle('rotate-180');
            }
        }
    });

    ui.dashboardLink.addEventListener('click', (e) => { e.preventDefault(); showDashboard(); });
    ui.portfolioLink.addEventListener('click', (e) => { e.preventDefault(); showPortfolio(); });
    ui.transactionsLink.addEventListener('click', (e) => { e.preventDefault(); showTransactions(); });
    ui.agentReportLink.addEventListener('click', (e) => { e.preventDefault(); showAgentReport(); });

    ['start', 'pause', 'resume'].forEach(action => {
        document.getElementById(`${action}-btn`).addEventListener('click', async () => {
            try { await api.sendWorkerControl(action); await pollWorkerStatus(); }
            catch(e) { logger.error(`Błąd kontroli workera (${action}):`, e); }
        });
    });
    
    ui.buyModal.cancelBtn.addEventListener('click', hideBuyModal);
    ui.buyModal.confirmBtn.addEventListener('click', handleBuyConfirm);
    ui.sellModal.cancelBtn.addEventListener('click', hideSellModal);
    ui.sellModal.confirmBtn.addEventListener('click', handleSellConfirm);
    
    if(ui.aiReportModal.closeBtn) {
        ui.aiReportModal.closeBtn.addEventListener('click', hideAIReportModal);
    }
    
    console.log("Event listeners added.");

    // --- INICJALIZACJA ---
    function startApp() {
        logger.info("startApp called - Hiding login, showing dashboard.");
        ui.loginScreen.classList.add('hidden');
        ui.dashboardScreen.classList.remove('hidden');
        ui.apiStatus.innerHTML = '<span class="h-2 w-2 rounded-full bg-green-500 mr-2"></span>Online';
        showDashboard();
        pollWorkerStatus();
        refreshSidebarData();
        pollSystemAlerts(); 
        try { lucide.createIcons(); } catch(e) { logger.error("Lucide error:", e); }
    }
    console.log("startApp function defined.");

    document.getElementById('login-form').addEventListener('submit', (e) => {
        e.preventDefault();
        if (ui.loginButton && !ui.loginButton.disabled) {
            startApp();
        } else {
             logger.warn("Próba zalogowania, gdy przycisk zablokowany.");
        }
    });
    console.log("Login form listener added.");

    // --- BLOK STARTOWY (Tylko pętla sprawdzająca) ---
    logger.info("Starting initial API status check interval...");
    const intervalId = setInterval(async () => {
        logger.info("Sprawdzanie statusu API...");
        try {
            const statusData = await api.getApiRootStatus();
            
            if (statusData && statusData.status === "APEX Predator API is running") {
                logger.info("API OK, czyszczenie interwału i odblokowanie logowania.");
                clearInterval(intervalId);
                ui.loginStatusText.textContent = 'System gotowy.';
                ui.loginButton.disabled = false;
                ui.loginButton.textContent = 'Wejdź do Aplikacji';
            } else {
                 logger.warn("API zwróciło 200 OK, ale nieprawidłowe dane:", statusData);
                 ui.loginStatusText.textContent = 'Problem z odp. API...';
            }
        } catch (e) {
            logger.error("Błąd podczas sprawdzania statusu API:", e.message);
            ui.loginStatusText.textContent = 'Backend nie gotowy...';
        }
    }, 3000);
    console.log("Initial API status check interval started.");

}); // Koniec DOMContentLoaded
