/**
 * APEX PREDATOR AQM V3 - FRONTEND CONTROLLER
 * Wersja: Production-Ready (Full Implementation)
 * Autor: Apex Team
 * * Ten plik zawiera kompletną logikę obsługi interfejsu użytkownika, 
 * komunikacji z API (mikroserwisy Python), zarządzania stanem aplikacji
 * oraz obsługi nowych funkcji analitycznych (H3, Deep Dive, AI).
 */

document.addEventListener('DOMContentLoaded', () => {
    console.log(" [SYSTEM] DOM fully loaded. Initializing Apex Predator Frontend...");

    // ==========================================================================
    // 1. ZARZĄDZANIE STANEM APLIKACJI (GLOBAL STATE)
    // ==========================================================================
    const state = {
        // Dane Rynkowe i Skaner
        phase1: [], 
        phase2: [], 
        phase3: [],
        
        // Dane Portfela
        portfolio: [],
        transactions: [],
        liveQuotes: {}, // Cache dla cen live
        
        // Status Workera
        workerStatus: { 
            status: 'UNKNOWN', 
            phase: 'NONE', 
            progress: { processed: 0, total: 0 },
            last_heartbeat_utc: null,
            log: ''
        },
        discardedSignalCount: 0,
        
        // Timery i Interwały (dla czyszczenia przy zmianie widoku)
        activePortfolioPolling: null,
        activeWorkerPolling: null,
        activeAlertPolling: null,
        activeAIOptimizerPolling: null,
        activeH3DeepDivePolling: null,
        
        // Alerty
        profitAlertsSent: {}, 
        snoozedAlerts: {},
        
        // UI State
        currentReportPage: 1,
        isSidebarOpen: window.innerWidth >= 768, // Domyślnie otwarty na desktopie
        currentView: 'dashboard'
    };

    // ==========================================================================
    // 2. KONFIGURACJA I STAŁE
    // ==========================================================================
    const CONSTANTS = {
        API_BASE_URL: "https://apex-predator-api-x0l8.onrender.com",
        POLL_INTERVALS: {
            WORKER: 5000,      // 5 sekund
            PORTFOLIO: 30000,  // 30 sekund
            ALERTS: 10000,     // 10 sekund
            AI_REPORT: 5000,   // 5 sekund
            DEEP_DIVE: 5000    // 5 sekund
        },
        THRESHOLDS: {
            PROFIT_ALERT: 1.02, // +2%
            LOSS_ALERT: 0.95    // -5%
        },
        PAGINATION: {
            REPORT_PAGE_SIZE: 200
        }
    };

    // Logger systemowy z formatowaniem czasu
    const logger = {
        log: (msg, data = '') => console.log(`[${new Date().toLocaleTimeString()}] INFO: ${msg}`, data),
        error: (msg, err = '') => console.error(`[${new Date().toLocaleTimeString()}] ERROR: ${msg}`, err),
        warn: (msg, data = '') => console.warn(`[${new Date().toLocaleTimeString()}] WARN: ${msg}`, data)
    };

    // ==========================================================================
    // 3. SELEKTORY DOM (UI REFERENCES)
    // ==========================================================================
    const ui = {
        // Ekrany Główne
        screens: {
            login: document.getElementById('login-screen'),
            dashboard: document.getElementById('dashboard'),
            mainContent: document.getElementById('main-content')
        },
        
        // Logowanie
        login: {
            form: document.getElementById('login-form'),
            btn: document.getElementById('login-button'),
            status: document.getElementById('login-status-text')
        },

        // Pasek Boczny (Sidebar)
        sidebar: {
            container: document.getElementById('app-sidebar'),
            backdrop: document.getElementById('sidebar-backdrop'),
            mobileOpenBtn: document.getElementById('mobile-menu-btn'),
            mobileCloseBtn: document.getElementById('mobile-sidebar-close'),
            navLinks: document.querySelector('#app-sidebar nav'),
            phasesContainer: document.getElementById('phases-container'),
            statusIndicators: {
                api: document.getElementById('api-status'),
                worker: document.getElementById('worker-status-text'),
                heartbeat: document.getElementById('heartbeat-status')
            }
        },

        // Linki Nawigacyjne
        nav: {
            dashboard: document.getElementById('dashboard-link'),
            portfolio: document.getElementById('portfolio-link'),
            transactions: document.getElementById('transactions-link'),
            agentReport: document.getElementById('agent-report-link')
        },

        // Listy Skanera (Fazy)
        phases: {
            p1List: document.getElementById('phase-1-list'),
            p1Count: document.getElementById('phase-1-count'),
            p2List: document.getElementById('phase-2-list'),
            p2Count: document.getElementById('phase-2-count'),
            p3List: document.getElementById('phase-3-list'),
            p3Count: document.getElementById('phase-3-count')
        },

        // Kontrolki Workera
        controls: {
            start: document.getElementById('start-btn'),
            pause: document.getElementById('pause-btn'),
            resume: document.getElementById('resume-btn')
        },

        // Kontener Alertów
        alerts: document.getElementById('system-alert-container'),

        // --- MODALE ---
        modals: {
            buy: {
                el: document.getElementById('buy-modal'),
                ticker: document.getElementById('buy-modal-ticker'),
                qty: document.getElementById('buy-quantity'),
                price: document.getElementById('buy-price'),
                cancel: document.getElementById('buy-cancel-btn'),
                confirm: document.getElementById('buy-confirm-btn')
            },
            sell: {
                el: document.getElementById('sell-modal'),
                ticker: document.getElementById('sell-modal-ticker'),
                maxQty: document.getElementById('sell-max-quantity'),
                qty: document.getElementById('sell-quantity'),
                price: document.getElementById('sell-price'),
                cancel: document.getElementById('sell-cancel-btn'),
                confirm: document.getElementById('sell-confirm-btn')
            },
            aiReport: {
                el: document.getElementById('ai-report-modal'),
                content: document.getElementById('ai-report-content'),
                close: document.getElementById('ai-report-close-btn')
            },
            h3DeepDive: {
                el: document.getElementById('h3-deep-dive-modal'),
                yearInput: document.getElementById('h3-deep-dive-year-input'),
                runBtn: document.getElementById('run-h3-deep-dive-btn'),
                status: document.getElementById('h3-deep-dive-status-message'),
                content: document.getElementById('h3-deep-dive-report-content'),
                close: document.getElementById('h3-deep-dive-close-btn')
            },
            h3Strategy: {
                el: document.getElementById('h3-strategy-modal'),
                inputs: {
                    year: document.getElementById('h3-strategy-year'),
                    percentile: document.getElementById('h3-param-percentile'),
                    mSq: document.getElementById('h3-param-m-sq'),
                    tp: document.getElementById('h3-param-tp'),
                    sl: document.getElementById('h3-param-sl'),
                    hold: document.getElementById('h3-param-hold')
                },
                runBtn: document.getElementById('run-h3-strategy-btn'),
                status: document.getElementById('h3-strategy-status'),
                close: document.getElementById('h3-strategy-close-btn')
            }
        }
    };

    // ==========================================================================
    // 4. WARSTWA API (KOMUNIKACJA Z BACKENDEM)
    // ==========================================================================
    
    const api = {
        // Helper do zapytań
        async fetch(endpoint, options = {}) {
            const url = endpoint ? `${CONSTANTS.API_BASE_URL}/${endpoint}` : CONSTANTS.API_BASE_URL;
            try {
                const response = await fetch(url, options);
                
                // Aktualizacja wskaźnika statusu API
                if (ui.sidebar.statusIndicators.api) {
                    ui.sidebar.statusIndicators.api.innerHTML = '<span class="h-2 w-2 rounded-full bg-green-500 mr-2"></span>Online';
                }

                if (!response.ok) {
                    let errorDetail = response.statusText;
                    try {
                        const errorJson = await response.json();
                        errorDetail = errorJson.detail || errorDetail;
                    } catch (e) {} // Ignoruj jeśli nie ma JSONa
                    
                    if (response.status === 409) throw new Error("Zasób zablokowany (Worker zajęty).");
                    throw new Error(`API Error (${response.status}): ${errorDetail}`);
                }
                
                // Obsługa 204 No Content
                if (response.status === 204) return null;
                
                return await response.json();
            } catch (error) {
                logger.error(`Request failed: ${url}`, error);
                if (ui.sidebar.statusIndicators.api) {
                    ui.sidebar.statusIndicators.api.innerHTML = '<span class="h-2 w-2 rounded-full bg-red-500 mr-2"></span>Offline';
                }
                throw error;
            }
        },

        // Metody API
        checkHealth: () => api.fetch(''),
        
        // Worker
        getWorkerStatus: () => api.fetch('api/v1/worker/status'),
        sendControl: (action) => api.fetch(`api/v1/worker/control/${action}`, { method: 'POST' }),
        
        // Fazy
        getPhase1: () => api.fetch('api/v1/candidates/phase1'),
        getPhase2: () => api.fetch('api/v1/results/phase2'),
        getPhase3: () => api.fetch('api/v1/signals/phase3'),
        getDiscardedStats: () => api.fetch('api/v1/signals/discarded-count-24h'),
        
        // Portfel & Market
        getLiveQuote: (ticker) => api.fetch(`api/v1/quote/${ticker}`),
        getPortfolio: () => api.fetch('api/v1/portfolio'),
        buyStock: (data) => api.fetch('api/v1/portfolio/buy', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(data) }),
        sellStock: (data) => api.fetch('api/v1/portfolio/sell', { method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(data) }),
        getTransactions: () => api.fetch('api/v1/transactions'),
        
        // System
        getAlerts: () => api.fetch('api/v1/system/alert'),
        
        // NOWE FUNKCJE: Raporty i AI
        getVirtualAgentReport: (page = 1) => api.fetch(`api/v1/virtual-agent/report?page=${page}&page_size=${CONSTANTS.PAGINATION.REPORT_PAGE_SIZE}`),
        
        // Backtest & H3
        requestBacktest: (year, params = null) => api.fetch('api/v1/backtest/request', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ year, parameters: params })
        }),
        
        // AI Optimizer
        requestAIOptimizer: () => api.fetch('api/v1/ai-optimizer/request', {
            method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({})
        }),
        getAIOptimizerReport: () => api.fetch('api/v1/ai-optimizer/report'),
        
        // Deep Dive
        requestH3DeepDive: (year) => api.fetch('api/v1/analysis/h3-deep-dive', {
            method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({ year })
        }),
        getH3DeepDiveReport: () => api.fetch('api/v1/analysis/h3-deep-dive-report')
    };

    // ==========================================================================
    // 5. LOGIKA UI (UKŁAD, SIDEBAR, NAWIGACJA)
    // ==========================================================================

    // --- Naprawa Layoutu (Sidebar) ---
    function initLayout() {
        // Sprawdź rozmiar ekranu i ustaw początkowy stan sidebara
        if (window.innerWidth >= 768) {
            ui.sidebar.container.classList.remove('-translate-x-full');
            ui.sidebar.container.classList.add('translate-x-0');
            ui.sidebar.backdrop.classList.add('hidden');
        } else {
            ui.sidebar.container.classList.add('-translate-x-full');
            ui.sidebar.container.classList.remove('translate-x-0');
            ui.sidebar.backdrop.classList.add('hidden');
        }
        // Ikony
        lucide.createIcons();
    }

    // Obsługa Sidebara (Mobile)
    function toggleSidebar(show) {
        if (show) {
            ui.sidebar.container.classList.remove('-translate-x-full');
            ui.sidebar.container.classList.add('translate-x-0');
            ui.sidebar.backdrop.classList.remove('hidden');
        } else {
            ui.sidebar.container.classList.add('-translate-x-full');
            ui.sidebar.container.classList.remove('translate-x-0');
            ui.sidebar.backdrop.classList.add('hidden');
        }
    }

    // Listenery Sidebara
    if(ui.sidebar.mobileOpenBtn) ui.sidebar.mobileOpenBtn.addEventListener('click', () => toggleSidebar(true));
    if(ui.sidebar.mobileCloseBtn) ui.sidebar.mobileCloseBtn.addEventListener('click', () => toggleSidebar(false));
    if(ui.sidebar.backdrop) ui.sidebar.backdrop.addEventListener('click', () => toggleSidebar(false));

    // --- Nawigacja (Routing SPA) ---
    function clearAllIntervals() {
        if (state.activePortfolioPolling) clearInterval(state.activePortfolioPolling);
        if (state.activeAIOptimizerPolling) clearTimeout(state.activeAIOptimizerPolling);
        if (state.activeH3DeepDivePolling) clearTimeout(state.activeH3DeepDivePolling);
    }

    async function navigateTo(viewName) {
        logger.log(`Navigating to: ${viewName}`);
        clearAllIntervals();
        
        // Aktualizacja aktywnego linku
        document.querySelectorAll('.sidebar-item').forEach(el => el.classList.remove('sidebar-item-active'));
        if (ui.nav[viewName]) ui.nav[viewName].classList.add('sidebar-item-active');

        // Pokaż spinner ładowania
        ui.screens.mainContent.innerHTML = renderers.loadingSpinner("Ładowanie widoku...");

        try {
            switch (viewName) {
                case 'dashboard':
                    ui.screens.mainContent.innerHTML = renderers.dashboard();
                    updateDashboard(state.workerStatus); // Odśwież natychmiast
                    break;
                
                case 'portfolio':
                    const portfolioData = await api.getPortfolio();
                    state.portfolio = portfolioData;
                    state.liveQuotes = {}; // Reset cache cen
                    ui.screens.mainContent.innerHTML = renderers.portfolio(portfolioData);
                    startPortfolioPolling();
                    break;
                
                case 'transactions':
                    const txData = await api.getTransactions();
                    state.transactions = txData;
                    ui.screens.mainContent.innerHTML = renderers.transactions(txData);
                    break;
                
                case 'agentReport':
                    await loadAgentReport(1); // Załaduj stronę 1
                    break;
            }
            
            // Inicjalizacja ikon po wyrenderowaniu HTML
            lucide.createIcons();
            
            // Na mobile zamknij sidebar po kliknięciu
            if (window.innerWidth < 768) toggleSidebar(false);

        } catch (error) {
            ui.screens.mainContent.innerHTML = renderers.errorState(`Nie udało się załadować widoku: ${error.message}`);
        }
    }

    // ==========================================================================
    // 6. RENDERERY (HTML GENERATORS)
    // ==========================================================================
    const renderers = {
        loadingSpinner: (text) => `
            <div class="flex flex-col items-center justify-center h-full min-h-[400px]">
                <i data-lucide="loader-2" class="w-12 h-12 text-sky-500 animate-spin mb-4"></i>
                <p class="text-gray-400 text-lg font-medium">${text}</p>
            </div>`,

        errorState: (msg) => `
            <div class="p-6 bg-red-900/20 border border-red-500/30 rounded-xl text-center m-4">
                <i data-lucide="alert-triangle" class="w-12 h-12 text-red-500 mx-auto mb-3"></i>
                <h3 class="text-xl font-bold text-red-400 mb-2">Wystąpił błąd</h3>
                <p class="text-gray-300">${msg}</p>
            </div>`,

        // --- DASHBOARD (Pełny) ---
        dashboard: () => `
            <div class="max-w-7xl mx-auto space-y-6 animate-fade-in">
                <div class="flex items-center justify-between border-b border-gray-800 pb-4">
                    <h2 class="text-3xl font-bold text-sky-400">Centrum Dowodzenia</h2>
                    <span class="text-xs text-gray-500 font-mono">AQM V3 ENGINE</span>
                </div>

                <!-- Karty Statusu -->
                <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
                    <!-- Karta 1: Silnik -->
                    <div class="bg-[#161B22] p-6 rounded-xl shadow-lg border border-gray-700 relative overflow-hidden group">
                        <div class="absolute top-0 right-0 p-4 opacity-10 group-hover:opacity-20 transition-opacity">
                            <i data-lucide="cpu" class="w-24 h-24 text-sky-500"></i>
                        </div>
                        <h3 class="font-semibold text-gray-400 flex items-center mb-4">
                            <i data-lucide="activity" class="w-5 h-5 mr-2 text-sky-400"></i>Status Silnika
                        </h3>
                        <p id="dashboard-worker-status" class="text-4xl font-extrabold text-white mt-auto tracking-tight">ŁADOWANIE...</p>
                        <div class="mt-2 flex items-center">
                            <div class="h-2 w-2 rounded-full bg-gray-500 mr-2" id="status-dot"></div>
                            <p id="dashboard-current-phase" class="text-sm text-gray-400 font-mono">Inicjalizacja...</p>
                        </div>
                    </div>

                    <!-- Karta 2: Postęp -->
                    <div class="bg-[#161B22] p-6 rounded-xl shadow-lg border border-gray-700 relative overflow-hidden">
                        <h3 class="font-semibold text-gray-400 flex items-center mb-4">
                            <i data-lucide="scan-line" class="w-5 h-5 mr-2 text-yellow-400"></i>Postęp Skanowania
                        </h3>
                        <div class="mt-auto">
                            <div class="flex justify-between items-end mb-2">
                                <span id="progress-text" class="text-3xl font-bold text-white">0 / 0</span>
                                <span class="text-xs text-gray-500 uppercase">Tickers</span>
                            </div>
                            <div class="w-full bg-gray-800 rounded-full h-3 overflow-hidden">
                                <div id="progress-bar" class="bg-gradient-to-r from-yellow-600 to-yellow-400 h-full rounded-full transition-all duration-500 ease-out" style="width: 0%"></div>
                            </div>
                        </div>
                    </div>

                    <!-- Karta 3: Sygnały -->
                    <div class="bg-[#161B22] p-6 rounded-xl shadow-lg border border-gray-700 relative overflow-hidden">
                        <h3 class="font-semibold text-gray-400 flex items-center mb-4">
                            <i data-lucide="zap" class="w-5 h-5 mr-2 text-red-500"></i>Aktywne Sygnały
                        </h3>
                        <div class="flex items-baseline gap-8 mt-auto">
                            <div>
                                <p id="dashboard-active-signals" class="text-4xl font-extrabold text-red-400">0</p>
                                <p class="text-xs text-gray-500 uppercase mt-1 font-semibold">W Grze</p>
                            </div>
                            <div class="border-l border-gray-700 pl-8">
                                <p id="dashboard-discarded-signals" class="text-4xl font-extrabold text-gray-500">0</p>
                                <p class="text-xs text-gray-500 uppercase mt-1 font-semibold">Odrzucone (24h)</p>
                            </div>
                        </div>
                    </div>
                </div>

                <!-- Placeholder Wykresów (Dla zachowania układu) -->
                <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    <div class="bg-[#161B22] rounded-xl border border-gray-700 p-6 h-64 flex flex-col items-center justify-center text-center group hover:border-sky-500/30 transition-colors">
                        <div class="bg-gray-800/50 p-4 rounded-full mb-4 group-hover:bg-sky-500/10 transition-colors">
                            <i data-lucide="bar-chart" class="w-8 h-8 text-gray-500 group-hover:text-sky-400"></i>
                        </div>
                        <h4 class="text-lg font-medium text-gray-300">Analiza Wolumenu</h4>
                        <p class="text-sm text-gray-500 max-w-xs mt-2">Wizualizacja wolumenu skumulowanego będzie dostępna po zakończeniu Fazy 2.</p>
                    </div>
                    <div class="bg-[#161B22] rounded-xl border border-gray-700 p-6 h-64 flex flex-col items-center justify-center text-center group hover:border-purple-500/30 transition-colors">
                        <div class="bg-gray-800/50 p-4 rounded-full mb-4 group-hover:bg-purple-500/10 transition-colors">
                            <i data-lucide="pie-chart" class="w-8 h-8 text-gray-500 group-hover:text-purple-400"></i>
                        </div>
                        <h4 class="text-lg font-medium text-gray-300">Dystrybucja Sektorowa</h4>
                        <p class="text-sm text-gray-500 max-w-xs mt-2">Mapa cieplna sektorów zostanie wygenerowana przez Agenta Makro.</p>
                    </div>
                </div>

                <!-- Terminal Logów -->
                <div class="bg-[#161B22] rounded-xl shadow-lg border border-gray-700 overflow-hidden flex flex-col h-[400px]">
                    <div class="bg-[#0d1117] px-4 py-3 border-b border-gray-700 flex justify-between items-center shrink-0">
                        <h3 class="font-semibold text-gray-300 flex items-center text-sm">
                            <i data-lucide="terminal" class="w-4 h-4 mr-2 text-green-400"></i>Logi Systemowe
                        </h3>
                        <div class="flex items-center gap-2">
                            <span class="flex h-2 w-2 rounded-full bg-green-500 animate-pulse"></span>
                            <span class="text-xs text-gray-500 font-mono">LIVE STREAM</span>
                        </div>
                    </div>
                    <div id="scan-log-container" class="flex-1 overflow-y-auto p-4 bg-[#0d1117] font-mono text-xs leading-relaxed scrollbar-thin scrollbar-thumb-gray-700 scrollbar-track-transparent">
                        <pre id="scan-log" class="text-green-400/90 whitespace-pre-wrap">Inicjalizacja połączenia z serwerem...</pre>
                    </div>
                </div>
            </div>`,

        // --- PORTFOLIO ---
        portfolio: (holdings, quotes = {}) => {
            let totalVal = 0, totalPL = 0;
            
            const rows = holdings.map(h => {
                const quote = quotes[h.ticker];
                let currentPrice = quote && quote['05. price'] ? parseFloat(quote['05. price']) : 0;
                let val = h.quantity * currentPrice;
                let cost = h.quantity * h.average_buy_price;
                let pl = currentPrice ? (val - cost) : 0;
                let plPercent = cost ? (pl / cost * 100) : 0;
                
                if (currentPrice) {
                    totalVal += val;
                    totalPL += pl;
                }

                const plColor = pl >= 0 ? 'text-green-400' : 'text-red-400';
                const plBg = pl >= 0 ? 'bg-green-400/10' : 'bg-red-400/10';

                return `
                <tr class="border-b border-gray-800 hover:bg-[#1f2937] transition-colors group">
                    <td class="p-4">
                        <div class="font-bold text-sky-400 text-lg">${h.ticker}</div>
                        <div class="text-xs text-gray-500">${new Date(h.first_purchase_date).toLocaleDateString()}</div>
                    </td>
                    <td class="p-4 text-right font-mono text-gray-300">${h.quantity}</td>
                    <td class="p-4 text-right font-mono text-gray-400">${h.average_buy_price.toFixed(2)}</td>
                    <td class="p-4 text-right font-mono font-bold text-white">${currentPrice ? currentPrice.toFixed(2) : '<span class="animate-pulse">...</span>'}</td>
                    <td class="p-4 text-right font-mono text-cyan-400">${h.take_profit ? h.take_profit.toFixed(2) : '-'}</td>
                    <td class="p-4 text-right">
                        <div class="${plColor} font-mono font-bold">${pl ? (pl > 0 ? '+' : '') + pl.toFixed(2) : '-'} $</div>
                        <div class="text-xs ${plColor}">${plPercent.toFixed(2)}%</div>
                    </td>
                    <td class="p-4 text-right">
                        <button data-ticker="${h.ticker}" data-quantity="${h.quantity}" class="sell-stock-btn opacity-0 group-hover:opacity-100 bg-red-500/10 hover:bg-red-500/20 text-red-400 border border-red-500/30 px-3 py-1 rounded text-xs transition-all">
                            Zamknij
                        </button>
                    </td>
                </tr>`;
            }).join('');

            return `
            <div id="portfolio-view" class="max-w-6xl mx-auto animate-fade-in">
                <!-- Nagłówek Portfela -->
                <div class="bg-[#161B22] rounded-xl p-6 border border-gray-700 mb-8 shadow-lg flex flex-col md:flex-row justify-between items-center gap-6">
                    <div>
                        <h2 class="text-3xl font-bold text-white mb-1">Twój Portfel</h2>
                        <p class="text-sm text-gray-400">Podsumowanie aktywów i wyników</p>
                    </div>
                    <div class="flex gap-6">
                        <div class="text-right">
                            <p class="text-xs text-gray-500 uppercase font-semibold">Wartość Całkowita</p>
                            <p class="text-3xl font-extrabold text-white tracking-tight">${totalVal.toFixed(2)} <span class="text-lg text-gray-500">USD</span></p>
                        </div>
                        <div class="h-12 w-px bg-gray-700"></div>
                        <div class="text-right">
                            <p class="text-xs text-gray-500 uppercase font-semibold">Zysk / Strata</p>
                            <p class="text-3xl font-extrabold ${totalPL >= 0 ? 'text-green-500' : 'text-red-500'} tracking-tight">
                                ${totalPL > 0 ? '+' : ''}${totalPL.toFixed(2)} <span class="text-lg text-gray-500">USD</span>
                            </p>
                        </div>
                    </div>
                </div>

                <!-- Tabela -->
                ${holdings.length === 0 ? 
                    `<div class="text-center py-20 bg-[#161B22] rounded-xl border border-gray-700 border-dashed">
                        <div class="bg-gray-800/50 w-20 h-20 rounded-full flex items-center justify-center mx-auto mb-4">
                            <i data-lucide="wallet" class="w-10 h-10 text-gray-600"></i>
                        </div>
                        <h3 class="text-xl font-bold text-gray-300">Portfel jest pusty</h3>
                        <p class="text-gray-500 mt-2">Czekaj na sygnały lub dodaj pozycję ręcznie.</p>
                    </div>` : 
                    `<div class="bg-[#161B22] rounded-xl border border-gray-700 shadow-xl overflow-hidden">
                        <div class="overflow-x-auto">
                            <table class="w-full text-sm text-left">
                                <thead class="text-xs text-gray-400 uppercase bg-[#0D1117] border-b border-gray-700">
                                    <tr>
                                        <th class="p-4 font-semibold">Instrument</th>
                                        <th class="p-4 text-right font-semibold">Ilość</th>
                                        <th class="p-4 text-right font-semibold">Śr. Cena</th>
                                        <th class="p-4 text-right font-semibold">Kurs Live</th>
                                        <th class="p-4 text-right font-semibold">Target</th>
                                        <th class="p-4 text-right font-semibold">P/L</th>
                                        <th class="p-4 text-right font-semibold">Akcja</th>
                                    </tr>
                                </thead>
                                <tbody class="divide-y divide-gray-800 text-gray-300">
                                    ${rows}
                                </tbody>
                            </table>
                        </div>
                    </div>`
                }
            </div>`;
        },

        // --- TRANSAKCJE ---
        transactions: (txs) => {
            const rows = txs.map(t => `
                <tr class="border-b border-gray-800 hover:bg-[#1f2937] transition-colors">
                    <td class="p-4 text-gray-400 font-mono text-xs">${new Date(t.transaction_date).toLocaleString()}</td>
                    <td class="p-4 font-bold text-sky-400">${t.ticker}</td>
                    <td class="p-4">
                        <span class="px-2 py-1 rounded text-xs font-bold ${t.transaction_type === 'BUY' ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400'}">
                            ${t.transaction_type}
                        </span>
                    </td>
                    <td class="p-4 text-right font-mono">${t.quantity}</td>
                    <td class="p-4 text-right font-mono text-gray-300">${t.price_per_share.toFixed(2)}</td>
                    <td class="p-4 text-right font-mono font-bold ${t.profit_loss_usd >= 0 ? 'text-green-400' : 'text-red-400'}">
                        ${t.profit_loss_usd !== null ? (t.profit_loss_usd > 0 ? '+' : '') + t.profit_loss_usd.toFixed(2) : '-'}
                    </td>
                </tr>
            `).join('');

            return `
            <div class="max-w-6xl mx-auto animate-fade-in">
                <h2 class="text-3xl font-bold text-sky-400 mb-6 pb-2 border-b border-gray-800">Historia Operacji</h2>
                <div class="bg-[#161B22] rounded-xl border border-gray-700 shadow-xl overflow-hidden">
                    <div class="overflow-x-auto">
                        <table class="w-full text-sm text-left">
                            <thead class="text-xs text-gray-400 uppercase bg-[#0D1117] border-b border-gray-700">
                                <tr>
                                    <th class="p-4">Data</th>
                                    <th class="p-4">Ticker</th>
                                    <th class="p-4">Typ</th>
                                    <th class="p-4 text-right">Ilość</th>
                                    <th class="p-4 text-right">Cena Exec</th>
                                    <th class="p-4 text-right">Wynik (P/L)</th>
                                </tr>
                            </thead>
                            <tbody class="divide-y divide-gray-800 text-gray-300">${rows}</tbody>
                        </table>
                        ${txs.length === 0 ? '<div class="p-8 text-center text-gray-500">Brak historii transakcji</div>' : ''}
                    </div>
                </div>
            </div>`;
        },

        // --- RAPORT AGENTA ---
        agentReport: (report) => {
            const stats = report.stats;
            const setupRows = Object.entries(stats.by_setup).map(([k, v]) => `
                <tr class="border-b border-gray-800 hover:bg-[#1f2937]">
                    <td class="p-3 text-sky-400 font-semibold">${k}</td>
                    <td class="p-3 text-right font-mono">${v.total_trades}</td>
                    <td class="p-3 text-right font-mono ${v.win_rate_percent >= 50 ? 'text-green-400' : 'text-red-400'}">${v.win_rate_percent.toFixed(1)}%</td>
                    <td class="p-3 text-right font-mono font-bold ${v.total_p_l_percent >= 0 ? 'text-green-400' : 'text-red-400'}">${v.total_p_l_percent.toFixed(2)}%</td>
                </tr>`).join('');

            const tradeRows = report.trades.map(t => `
                <tr class="border-b border-gray-800 text-xs font-mono hover:bg-[#1f2937] transition-colors">
                    <td class="p-3 text-gray-400">${new Date(t.open_date).toLocaleDateString()}</td>
                    <td class="p-3 font-bold text-sky-400">${t.ticker}</td>
                    <td class="p-3 text-gray-300">${t.setup_type.replace('BACKTEST_', '').substring(0, 20)}...</td>
                    <td class="p-3 text-right"><span class="px-2 py-0.5 rounded ${t.status.includes('TP') ? 'bg-green-900/30 text-green-400' : 'bg-red-900/30 text-red-400'}">${t.status}</span></td>
                    <td class="p-3 text-right font-bold ${t.final_profit_loss_percent >= 0 ? 'text-green-400' : 'text-red-400'}">${t.final_profit_loss_percent ? t.final_profit_loss_percent.toFixed(2) + '%' : '-'}</td>
                    <td class="p-3 text-right text-yellow-300 font-bold">${t.metric_aqm_score_h3 ? t.metric_aqm_score_h3.toFixed(3) : '-'}</td>
                </tr>
            `).join('');

            // Panel Narzędzi (Backtest, AI, Deep Dive)
            const toolsHtml = `
                <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 mt-8">
                    <!-- Backtest -->
                    <div class="bg-[#161B22] p-6 rounded-xl border border-gray-700 shadow-md hover:border-gray-600 transition-colors">
                        <div class="flex items-center gap-3 mb-4">
                            <div class="p-2 bg-purple-500/10 rounded-lg"><i data-lucide="history" class="w-6 h-6 text-purple-400"></i></div>
                            <div><h4 class="text-lg font-bold text-white">Backtest Historyczny</h4><p class="text-xs text-gray-500">Symulacja Slice-First na danych EOD</p></div>
                        </div>
                        <div class="flex gap-2 mb-3">
                            <input type="number" id="backtest-year-input" class="modal-input bg-gray-900 border-gray-600 text-white flex-1 rounded px-3 py-2" placeholder="Rok (np. 2022)" min="2000" max="2099">
                            <button id="run-backtest-year-btn" class="bg-purple-600 hover:bg-purple-700 text-white px-4 py-2 rounded font-medium flex items-center transition-colors"><i data-lucide="play" class="w-4 h-4 mr-2"></i>Start</button>
                        </div>
                        <button id="open-h3-strategy-modal-btn" class="w-full text-xs text-purple-300 hover:text-white py-2 border border-dashed border-purple-500/30 hover:border-purple-500 rounded transition-colors flex justify-center items-center gap-2">
                            <i data-lucide="settings-2" class="w-3 h-3"></i> Konfiguruj Parametry H3
                        </button>
                        <div id="backtest-status-message" class="text-sm mt-2 h-4 font-mono text-gray-400"></div>
                    </div>

                    <!-- AI Tools -->
                    <div class="bg-[#161B22] p-6 rounded-xl border border-gray-700 shadow-md hover:border-gray-600 transition-colors flex flex-col gap-4">
                        <div>
                            <div class="flex items-center gap-3 mb-3">
                                <div class="p-2 bg-pink-500/10 rounded-lg"><i data-lucide="brain-circuit" class="w-6 h-6 text-pink-400"></i></div>
                                <div><h4 class="text-lg font-bold text-white">Mega Agent AI</h4><p class="text-xs text-gray-500">Analiza wyników przez LLM Gemini</p></div>
                            </div>
                            <div class="flex gap-2">
                                <button id="run-ai-optimizer-btn" class="flex-1 bg-pink-600 hover:bg-pink-700 text-white px-4 py-2 rounded font-medium flex items-center justify-center transition-colors"><i data-lucide="sparkles" class="w-4 h-4 mr-2"></i>Optymalizuj</button>
                                <button id="view-ai-report-btn" class="bg-gray-700 hover:bg-gray-600 text-white px-4 py-2 rounded transition-colors"><i data-lucide="file-text" class="w-4 h-4"></i></button>
                            </div>
                            <div id="ai-optimizer-status-message" class="text-sm mt-1 h-4 font-mono text-gray-400"></div>
                        </div>
                        <div class="border-t border-gray-700 pt-4 flex gap-2">
                             <button id="run-h3-deep-dive-modal-btn" class="flex-1 flex items-center justify-center text-sm bg-sky-500/10 hover:bg-sky-500/20 text-sky-400 border border-sky-500/30 px-3 py-2 rounded transition-all"><i data-lucide="microscope" class="w-4 h-4 mr-2"></i>Deep Dive</button>
                             <button id="run-csv-export-btn" class="flex-1 flex items-center justify-center text-sm bg-green-500/10 hover:bg-green-500/20 text-green-400 border border-green-500/30 px-3 py-2 rounded transition-all"><i data-lucide="download" class="w-4 h-4 mr-2"></i>CSV</button>
                        </div>
                         <div id="csv-export-status-message" class="text-sm h-4 text-center font-mono text-gray-500"></div>
                    </div>
                </div>`;

            return `
            <div id="agent-report-view" class="max-w-6xl mx-auto pb-12 animate-fade-in">
                <div class="flex justify-between items-end mb-6 border-b border-gray-800 pb-4">
                    <h2 class="text-3xl font-bold text-sky-400">Centrum Analityczne</h2>
                    <span class="text-sm text-gray-500">Wyniki symulacji i AI</span>
                </div>
                
                <!-- KPI Cards -->
                <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
                    <div class="bg-[#161B22] p-5 rounded-xl border border-gray-700 shadow-lg">
                        <span class="text-gray-400 text-xs uppercase font-bold">Całkowity P/L</span>
                        <p class="text-3xl font-extrabold ${stats.total_p_l_percent >= 0 ? 'text-green-400' : 'text-red-400'} mt-1">${stats.total_p_l_percent.toFixed(2)}%</p>
                    </div>
                    <div class="bg-[#161B22] p-5 rounded-xl border border-gray-700 shadow-lg">
                        <span class="text-gray-400 text-xs uppercase font-bold">Win Rate</span>
                        <p class="text-3xl font-extrabold text-white mt-1">${stats.win_rate_percent.toFixed(1)}%</p>
                    </div>
                    <div class="bg-[#161B22] p-5 rounded-xl border border-gray-700 shadow-lg">
                        <span class="text-gray-400 text-xs uppercase font-bold">Profit Factor</span>
                        <p class="text-3xl font-extrabold text-sky-400 mt-1">${stats.profit_factor.toFixed(2)}</p>
                    </div>
                    <div class="bg-[#161B22] p-5 rounded-xl border border-gray-700 shadow-lg">
                        <span class="text-gray-400 text-xs uppercase font-bold">Liczba Transakcji</span>
                        <p class="text-3xl font-extrabold text-white mt-1">${stats.total_trades}</p>
                    </div>
                </div>

                <!-- Tabela Strategii -->
                <div class="bg-[#161B22] rounded-xl border border-gray-700 shadow-xl mb-8 overflow-hidden">
                    <div class="px-6 py-3 bg-[#0D1117] border-b border-gray-700"><h3 class="text-sm font-bold text-gray-300 uppercase">Wyniki wg Strategii</h3></div>
                    <table class="w-full text-sm text-left text-gray-300">
                        <thead class="text-xs text-gray-500 bg-[#0D1117]"><tr><th class="p-4">Nazwa</th><th class="p-4 text-right">Ilość</th><th class="p-4 text-right">Win Rate</th><th class="p-4 text-right">Wynik</th></tr></thead>
                        <tbody>${setupRows}</tbody>
                    </table>
                </div>

                ${toolsHtml}

                <h3 class="text-xl font-bold text-gray-300 mt-10 mb-4 flex items-center"><i data-lucide="list" class="w-5 h-5 mr-2"></i>Dziennik Transakcji</h3>
                <div class="bg-[#161B22] rounded-xl border border-gray-700 shadow-xl max-h-[500px] overflow-y-auto custom-scrollbar">
                    <table class="w-full text-sm text-left text-gray-300">
                        <thead class="text-xs text-gray-400 uppercase bg-[#0D1117] sticky top-0 z-10 shadow-md"><tr><th class="p-3">Data</th><th class="p-3">Ticker</th><th class="p-3">Setup</th><th class="p-3 text-right">Status</th><th class="p-3 text-right">P/L</th><th class="p-3 text-right">AQM H3</th></tr></thead>
                        <tbody>${tradeRows}</tbody>
                    </table>
                </div>
            </div>`;
        }
    };

    // ==========================================================================
    // 7. LOGIKA BIZNESOWA (CONTROLLER)
    // ==========================================================================

    // --- Aktualizacja Dashboardu ---
    function updateDashboard(status) {
        const els = {
            status: document.getElementById('dashboard-worker-status'),
            phase: document.getElementById('dashboard-current-phase'),
            dot: document.getElementById('status-dot'),
            progText: document.getElementById('progress-text'),
            progBar: document.getElementById('progress-bar'),
            log: document.getElementById('scan-log'),
            active: document.getElementById('dashboard-active-signals'),
            discarded: document.getElementById('dashboard-discarded-signals')
        };

        // Aktualizacja Liczników Fazy w Sidebarze
        if(ui.phases.p1Count) ui.phases.p1Count.textContent = state.phase1.length;
        if(ui.phases.p2Count) ui.phases.p2Count.textContent = state.phase2.length;
        if(ui.phases.p3Count) ui.phases.p3Count.textContent = state.phase3.length;

        // Jeśli nie jesteśmy na widoku Dashboard, przerywamy aktualizację DOM dashboardu
        if (!els.status) return;

        // Status Workera
        if (status.status === 'RUNNING') {
            els.status.textContent = 'AKTYWNY';
            els.status.className = 'text-4xl font-extrabold text-green-400 mt-auto tracking-tight animate-pulse';
            if(els.dot) els.dot.className = 'h-2 w-2 rounded-full bg-green-500 mr-2 animate-ping';
        } else if (status.status.includes('BUSY')) {
            els.status.textContent = 'ZAJĘTY';
            els.status.className = 'text-4xl font-extrabold text-purple-400 mt-auto tracking-tight';
            if(els.dot) els.dot.className = 'h-2 w-2 rounded-full bg-purple-500 mr-2 animate-pulse';
        } else if (status.status === 'ERROR') {
            els.status.textContent = 'BŁĄD';
            els.status.className = 'text-4xl font-extrabold text-red-500 mt-auto tracking-tight';
            if(els.dot) els.dot.className = 'h-2 w-2 rounded-full bg-red-500 mr-2';
        } else {
            els.status.textContent = 'IDLE';
            els.status.className = 'text-4xl font-extrabold text-gray-500 mt-auto tracking-tight';
            if(els.dot) els.dot.className = 'h-2 w-2 rounded-full bg-gray-500 mr-2';
        }

        if(els.phase) els.phase.textContent = `Faza: ${status.phase}`;
        
        // Pasek Postępu
        if(els.progText) els.progText.textContent = `${status.progress.processed} / ${status.progress.total}`;
        if(els.progBar) els.progBar.style.width = status.progress.total > 0 ? `${(status.progress.processed/status.progress.total)*100}%` : '0%';

        // Logi (Auto-scroll)
        if (els.log && els.log.textContent !== status.log) {
            els.log.textContent = status.log || 'Oczekiwanie na logi...';
            const container = document.getElementById('scan-log-container');
            if (container) container.scrollTop = container.scrollHeight;
        }

        // Liczniki
        if(els.active) els.active.textContent = state.phase3.length;
        if(els.discarded) els.discarded.textContent = state.discardedSignalCount;
    }

    // --- Polling Danych w Tle ---
    async function pollWorkerLoop() {
        try {
            const status = await api.getWorkerStatus();
            state.workerStatus = status;
            
            // Aktualizacja małego statusu w sidebarze
            if (ui.sidebar.statusIndicators.worker) {
                ui.sidebar.statusIndicators.worker.textContent = status.phase !== 'NONE' ? status.phase : status.status;
                ui.sidebar.statusIndicators.worker.className = `font-mono px-2 py-1 rounded-md text-xs transition-colors ${status.status === 'RUNNING' ? 'bg-green-900/50 text-green-400 border border-green-800' : 'bg-gray-800 text-gray-400 border border-gray-700'}`;
            }
            if (ui.sidebar.statusIndicators.heartbeat && status.last_heartbeat_utc) {
                const lastBeat = new Date(status.last_heartbeat_utc);
                const diff = (new Date() - lastBeat) / 1000;
                ui.sidebar.statusIndicators.heartbeat.textContent = diff > 60 ? 'ZATRZYMANY' : lastBeat.toLocaleTimeString();
                ui.sidebar.statusIndicators.heartbeat.className = diff > 60 ? 'text-xs text-red-500' : 'text-xs text-green-500';
            }

            // Aktywacja Przycisków
            const isBusy = status.status !== 'IDLE' && status.status !== 'ERROR' && status.status !== 'PAUSED';
            if(ui.controls.start) ui.controls.start.disabled = isBusy;
            if(ui.controls.pause) ui.controls.pause.disabled = status.status !== 'RUNNING';
            if(ui.controls.resume) ui.controls.resume.disabled = status.status !== 'PAUSED';

            // Odśwież dashboard jeśli jest aktywny
            if (state.currentView === 'dashboard') updateDashboard(status);

        } catch (e) {}
        state.activeWorkerPolling = setTimeout(pollWorkerLoop, CONSTANTS.POLL_INTERVALS.WORKER);
    }

    async function refreshDataLoop() {
        try {
            const [p1, p2, p3, discarded] = await Promise.all([
                api.getPhase1(), api.getPhase2(), api.getPhase3(), api.getDiscardedStats()
            ]);
            state.phase1 = p1 || [];
            state.phase2 = p2 || [];
            state.phase3 = p3 || [];
            state.discardedSignalCount = discarded?.discarded_count_24h ?? 0;
            
            // Renderowanie list w sidebarze
            if(ui.phases.p1List) ui.phases.p1List.innerHTML = renderers.phase1List(state.phase1);
            if(ui.phases.p2List) ui.phases.p2List.innerHTML = renderers.phase2List(state.phase2);
            if(ui.phases.p3List) ui.phases.p3List.innerHTML = renderers.phase3List(state.phase3);
            
        } catch(e) {}
        // Mniej częste odświeżanie list bocznych
        setTimeout(refreshDataLoop, 15000);
    }

    async function pollPortfolioLoop() {
        if (!state.portfolio.length || state.currentView !== 'portfolio') return;
        try {
            const tickers = state.portfolio.map(h => h.ticker);
            const results = await Promise.all(tickers.map(t => api.getLiveQuote(t)));
            let updated = false;
            results.forEach((q, i) => { 
                if (q) { 
                    state.liveQuotes[tickers[i]] = q; 
                    updated = true; 
                } 
            });
            if (updated) {
                ui.screens.mainContent.innerHTML = renderers.portfolio(state.portfolio, state.liveQuotes);
                lucide.createIcons();
            }
        } catch(e) {}
        state.activePortfolioPolling = setTimeout(pollPortfolioLoop, CONSTANTS.POLL_INTERVALS.PORTFOLIO);
    }

    // --- ACTIONS & HANDLERS ---
    
    async function loadAgentReport(page) {
        try {
            const report = await api.getVirtualAgentReport(page);
            ui.screens.mainContent.innerHTML = renderers.agentReport(report);
            lucide.createIcons();
        } catch(e) {
            ui.screens.mainContent.innerHTML = renderers.errorState("Błąd ładowania raportu agenta: " + e.message);
        }
    }

    // Obsługa Backtestu
    async function handleBacktest() {
        const input = document.getElementById('backtest-year-input');
        const msg = document.getElementById('backtest-status-message');
        const year = input.value;
        
        if(!year || year.length !== 4) {
            if(msg) msg.textContent = "Podaj poprawny rok (YYYY)";
            return;
        }
        
        if(msg) msg.textContent = "Wysyłanie zlecenia...";
        try {
            await api.requestBacktest(year);
            if(msg) {
                msg.textContent = "Zlecono pomyślnie. Worker rozpoczyna pracę.";
                msg.className = "text-sm mt-2 h-4 font-mono text-green-400";
            }
        } catch(e) {
            if(msg) {
                msg.textContent = "Błąd: " + e.message;
                msg.className = "text-sm mt-2 h-4 font-mono text-red-400";
            }
        }
    }

    // Obsługa AI
    async function handleAIOptimizer() {
        const msg = document.getElementById('ai-optimizer-status-message');
        if(msg) msg.textContent = "Zlecanie analizy...";
        try {
            await api.requestAIOptimizer();
            pollAIReport();
        } catch(e) {
            if(msg) msg.textContent = "Błąd: " + e.message;
        }
    }
    
    async function pollAIReport() {
        try {
            const res = await api.getAIOptimizerReport();
            const msg = document.getElementById('ai-optimizer-status-message');
            
            if (res.status === 'PROCESSING') {
                if(msg) msg.textContent = "AI analizuje dane... proszę czekać.";
                state.activeAIOptimizerPolling = setTimeout(pollAIReport, CONSTANTS.POLL_INTERVALS.AI_REPORT);
            } else if (res.status === 'DONE') {
                if(msg) msg.textContent = "Analiza zakończona.";
                ui.modals.aiReport.content.innerHTML = `<pre class="text-xs text-gray-300 font-mono whitespace-pre-wrap">${res.report_text}</pre>`;
                ui.modals.aiReport.el.classList.remove('hidden');
            }
        } catch(e) {}
    }

    // Obsługa H3 Deep Dive
    async function handleDeepDive() {
        // Pobieramy rok z modala Deep Dive (musi być otwarty)
        const year = ui.modals.h3DeepDive.yearInput.value;
        if(!year) return;
        
        ui.modals.h3DeepDive.status.textContent = "Zlecanie analizy...";
        try {
            await api.requestH3DeepDive(year);
            pollDeepDive();
        } catch(e) {
            ui.modals.h3DeepDive.status.textContent = "Błąd: " + e.message;
        }
    }
    
    async function pollDeepDive() {
        try {
            const res = await api.getH3DeepDiveReport();
            if (res.status === 'PROCESSING') {
                ui.modals.h3DeepDive.status.textContent = "Analiza w toku...";
                state.activeH3DeepDivePolling = setTimeout(pollDeepDive, CONSTANTS.POLL_INTERVALS.DEEP_DIVE);
            } else if (res.status === 'DONE') {
                ui.modals.h3DeepDive.status.textContent = "Zakończono.";
                ui.modals.h3DeepDive.content.innerHTML = `<pre class="text-xs text-gray-300 font-mono whitespace-pre-wrap">${res.report_text}</pre>`;
            } else if (res.status === 'ERROR') {
                ui.modals.h3DeepDive.status.textContent = "Błąd analizy.";
                ui.modals.h3DeepDive.content.innerHTML = `<p class="text-red-400">${res.report_text}</p>`;
            }
        } catch(e) {}
    }

    // Obsługa CSV
    async function handleExportCSV() {
        const msg = document.getElementById('csv-export-status-message');
        if(msg) msg.textContent = "Generowanie...";
        try {
            const response = await fetch(`${CONSTANTS.API_BASE_URL}/api/v1/export/trades.csv`);
            if (!response.ok) throw new Error("Network response was not ok");
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `apex_trades_${new Date().toISOString().slice(0,10)}.csv`;
            document.body.appendChild(a);
            a.click();
            a.remove();
            if(msg) msg.textContent = "Pobrano pomyślnie.";
        } catch(e) {
            if(msg) msg.textContent = "Błąd pobierania.";
        }
    }

    // ==========================================================================
    // 8. EVENT LISTENERS & INIT
    // ==========================================================================

    // Globalny Delegator Zdarzeń (Obsługuje dynamicznie dodawane elementy)
    document.body.addEventListener('click', (e) => {
        // Modale Kupna/Sprzedaży
        if (e.target.closest('.sell-stock-btn')) {
            const btn = e.target.closest('.sell-stock-btn');
            ui.modals.sell.ticker.textContent = btn.dataset.ticker;
            ui.modals.sell.maxQty.textContent = btn.dataset.quantity;
            ui.modals.sell.qty.max = btn.dataset.quantity;
            ui.modals.sell.confirm.dataset.ticker = btn.dataset.ticker;
            ui.modals.sell.el.classList.remove('hidden');
        }
        
        // Przyciski w raporcie Agenta
        if (e.target.id === 'run-backtest-year-btn') handleBacktest();
        if (e.target.id === 'run-ai-optimizer-btn') handleAIOptimizer();
        if (e.target.id === 'view-ai-report-btn') pollAIReport();
        if (e.target.id === 'run-h3-deep-dive-modal-btn') ui.modals.h3DeepDive.el.classList.remove('hidden');
        if (e.target.id === 'run-csv-export-btn') handleExportCSV();
        if (e.target.id === 'open-h3-strategy-modal-btn') ui.modals.h3Strategy.el.classList.remove('hidden');
    });

    // Nawigacja Sidebar
    ui.nav.dashboard.onclick = (e) => { e.preventDefault(); state.currentView='dashboard'; navigateTo('dashboard'); };
    ui.nav.portfolio.onclick = (e) => { e.preventDefault(); state.currentView='portfolio'; navigateTo('portfolio'); };
    ui.nav.transactions.onclick = (e) => { e.preventDefault(); state.currentView='transactions'; navigateTo('transactions'); };
    ui.nav.agentReport.onclick = (e) => { e.preventDefault(); state.currentView='agentReport'; navigateTo('agentReport'); };

    // Kontrolki Workera
    ui.controls.start.onclick = () => api.sendControl('start');
    ui.controls.pause.onclick = () => api.sendControl('pause');
    ui.controls.resume.onclick = () => api.sendControl('resume');

    // Zamykanie Modali
    const closeModals = () => document.querySelectorAll('.modal-backdrop').forEach(m => m.classList.add('hidden'));
    document.querySelectorAll('.modal-button-secondary').forEach(btn => btn.addEventListener('click', closeModals));
    // Specjalne przyciski zamykania (X)
    if(ui.modals.aiReport.close) ui.modals.aiReport.close.onclick = () => ui.modals.aiReport.el.classList.add('hidden');
    if(ui.modals.h3DeepDive.close) ui.modals.h3DeepDive.close.onclick = () => ui.modals.h3DeepDive.el.classList.add('hidden');
    if(ui.modals.h3Strategy.close) ui.modals.h3Strategy.close.onclick = () => ui.modals.h3Strategy.el.classList.add('hidden');

    // Logika przycisków w modalach
    ui.modals.h3DeepDive.runBtn.onclick = handleDeepDive;
    ui.modals.h3Strategy.runBtn.onclick = async () => {
        const inputs = ui.modals.h3Strategy.inputs;
        const params = {
            h3_percentile: parseFloat(inputs.percentile.value),
            h3_m_sq_threshold: parseFloat(inputs.mSq.value),
            h3_tp_multiplier: parseFloat(inputs.tp.value),
            h3_sl_multiplier: parseFloat(inputs.sl.value),
            h3_max_hold: parseInt(inputs.hold.value),
            setup_name: `H3_CUSTOM_${new Date().getTime()}`
        };
        ui.modals.h3Strategy.status.textContent = "Wysyłanie...";
        try {
            await api.requestBacktest(inputs.year.value, params);
            ui.modals.h3Strategy.status.textContent = "Zlecono pomyślnie.";
            setTimeout(() => ui.modals.h3Strategy.el.classList.add('hidden'), 1500);
        } catch(e) {
            ui.modals.h3Strategy.status.textContent = "Błąd: " + e.message;
        }
    };

    // Logowanie
    ui.login.form.addEventListener('submit', async (e) => {
        e.preventDefault();
        ui.login.btn.textContent = "Łączenie...";
        ui.login.btn.disabled = true;
        
        try {
            const status = await api.checkHealth();
            if (status) {
                ui.screens.login.classList.add('hidden');
                ui.screens.dashboard.classList.remove('hidden');
                initLayout(); // Ustaw Sidebar
                
                // Uruchom pętle
                navigateTo('dashboard');
                pollWorkerLoop();
                refreshDataLoop();
                // pollAlertsLoop();
            }
        } catch (error) {
            ui.login.status.textContent = "Nie można połączyć z serwerem API.";
            ui.login.btn.textContent = "Wejdź do Aplikacji";
            ui.login.btn.disabled = false;
        }
    });

    // Auto-Check na starcie (dla dev)
    (async () => {
        try {
            await api.checkHealth();
            ui.login.btn.disabled = false;
            console.log(" [SYSTEM] API Online. Ready to login.");
        } catch (e) {
            console.log(" [SYSTEM] API Offline.");
            ui.login.status.textContent = "API Offline. Sprawdź Render.com";
        }
    })();
});
