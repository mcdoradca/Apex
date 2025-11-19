import { logger, state, REPORT_PAGE_SIZE } from './state.js';

export const ui = {
    init: () => {
        const get = (id) => document.getElementById(id);
        return {
            loginScreen: get('login-screen'),
            dashboardScreen: get('dashboard'),
            loginButton: get('login-button'),
            loginStatusText: get('login-status-text'),
            mainContent: get('main-content'),
            startBtn: get('start-btn'),
            pauseBtn: get('pause-btn'),
            resumeBtn: get('resume-btn'),
            apiStatus: get('api-status'),
            workerStatusText: get('worker-status-text'),
            dashboardLink: get('dashboard-link'),
            portfolioLink: get('portfolio-link'),
            transactionsLink: get('transactions-link'),
            agentReportLink: get('agent-report-link'),
            heartbeatStatus: get('heartbeat-status'),
            alertContainer: get('system-alert-container'),
            phase1: { list: get('phase-1-list'), count: get('phase-1-count') },
            phase2: { list: get('phase-2-list'), count: get('phase-2-count') },
            phase3: { list: get('phase-3-list'), count: get('phase-3-count') },
            
            // Nowe przyciski sidebara
            btnPhase1: get('btn-phase-1'),
            btnPhase3: get('btn-phase-3'),
            
            // NOWY MODAL H3 Live
            h3LiveModal: {
                backdrop: get('h3-live-modal'),
                percentile: get('h3-live-percentile'),
                mass: get('h3-live-mass'),
                tp: get('h3-live-tp'),
                sl: get('h3-live-sl'),
                cancelBtn: get('h3-live-cancel-btn'),
                startBtn: get('h3-live-start-btn')
            },

            buyModal: { 
                backdrop: get('buy-modal'), tickerSpan: get('buy-modal-ticker'), 
                quantityInput: get('buy-quantity'), priceInput: get('buy-price'),
                cancelBtn: get('buy-cancel-btn'), confirmBtn: get('buy-confirm-btn')
            },
            sellModal: { 
                backdrop: get('sell-modal'), tickerSpan: get('sell-modal-ticker'), 
                maxQuantitySpan: get('sell-max-quantity'), quantityInput: get('sell-quantity'), 
                priceInput: get('sell-price'), cancelBtn: get('sell-cancel-btn'), confirmBtn: get('sell-confirm-btn')
            },
            aiReportModal: {
                backdrop: get('ai-report-modal'), content: get('ai-report-content'), closeBtn: get('ai-report-close-btn')
            },
            h3DeepDiveModal: {
                backdrop: get('h3-deep-dive-modal'), yearInput: get('h3-deep-dive-year-input'),
                runBtn: get('run-h3-deep-dive-btn'), statusMsg: get('h3-deep-dive-status-message'),
                content: get('h3-deep-dive-report-content'), closeBtn: get('h3-deep-dive-close-btn')
            },
            sidebar: get('app-sidebar'),
            sidebarBackdrop: get('sidebar-backdrop'),
            mobileMenuBtn: get('mobile-menu-btn'),
            mobileSidebarCloseBtn: get('mobile-sidebar-close'),
            sidebarNav: document.querySelector('#app-sidebar nav'),
            sidebarPhasesContainer: get('phases-container')
        };
    }
};

export const renderers = {
    // ... (cała zawartość renderers bez zmian, skopiuj z poprzedniego pliku)
    // Aby nie zajmować miejsca, wklejam skrót - użyj poprzedniej wersji renderers
    loading: (text) => `<div class="text-center py-10"><p class="text-sky-400 mt-4">${text}</p></div>`,
    phase1List: (c) => c.map(i => `<div class="candidate-item flex justify-between items-center text-xs p-2 rounded-md cursor-default transition-colors phase-1-text"><span class="font-bold">${i.ticker}</span></div>`).join('') || `<p class="text-xs text-gray-500 p-2">Brak wyników.</p>`,
    phase2List: () => "",
    phase3List: (s) => s.map(x => {
        let sc = ""; if(x.notes && x.notes.includes("Score:")) { try{sc=`<span class="ml-2 text-xs text-blue-300 bg-blue-900/30 px-1 rounded">AQM: ${parseFloat(x.notes.split("Score:")[1].trim().split(" ")[0]).toFixed(2)}</span>`}catch(e){} }
        return `<div class="candidate-item flex items-center text-xs p-2 rounded-md cursor-default transition-colors ${x.status==='ACTIVE'?'text-green-400':'text-yellow-400'}"><i data-lucide="${x.status==='ACTIVE'?'zap':'hourglass'}" class="w-4 h-4 mr-2"></i><span class="font-bold">${x.ticker}</span>${sc}<span class="ml-auto text-gray-500">${x.status}</span></div>`
    }).join('') || `<p class="text-xs text-gray-500 p-2">Brak sygnałów.</p>`,
    dashboard: () => `<div id="dashboard-view" class="max-w-4xl mx-auto"><h2 class="text-2xl font-bold text-sky-400 mb-6 border-b border-gray-700 pb-2">Panel Kontrolny Systemu</h2><div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8"><div class="bg-[#161B22] p-4 rounded-lg shadow-lg border border-gray-700"><h3 class="font-semibold text-gray-400 flex items-center"><i data-lucide="cpu" class="w-4 h-4 mr-2 text-sky-400"></i>Status Silnika</h3><p id="dashboard-worker-status" class="text-4xl font-extrabold mt-2 text-green-500">IDLE</p><p id="dashboard-current-phase" class="text-sm text-gray-500 mt-1">Faza: NONE</p></div><div class="bg-[#161B22] p-4 rounded-lg shadow-lg border border-gray-700"><h3 class="font-semibold text-gray-400 flex items-center"><i data-lucide="bar-chart-2" class="w-4 h-4 mr-2 text-yellow-400"></i>Postęp Skanera (F1)</h3><div class="mt-2"><span id="progress-text" class="text-2xl font-extrabold">0 / 0</span><span class="text-gray-500 text-sm"> tickery</span></div><div class="w-full bg-gray-700 rounded-full h-2.5 mt-2"><div id="progress-bar" class="bg-sky-600 h-2.5 rounded-full transition-all duration-500" style="width: 0%"></div></div></div><div class="bg-[#161B22] p-4 rounded-lg shadow-lg border border-gray-700"><h3 class="font-semibold text-gray-400 flex items-center"><i data-lucide="target" class="w-4 h-4 mr-2 text-red-500"></i>Sygnały H3</h3><div class="mt-2"><p id="dashboard-active-signals" class="text-4xl font-extrabold text-red-400">0</p><p class="text-sm text-gray-500 mt-1">Aktywne / Oczekujące</p></div></div></div><h3 class="text-xl font-bold text-gray-300 mb-4 border-b border-gray-700 pb-1">Logi Silnika</h3><div id="scan-log-container" class="bg-[#161B22] p-4 rounded-lg shadow-inner h-96 overflow-y-scroll border border-gray-700"><pre id="scan-log" class="text-xs text-gray-300 whitespace-pre-wrap font-mono">Czekam na rozpoczęcie skanowania...</pre></div></div>`,
    // ... (pozostałe: portfolio, transactions, agentReport - użyj poprzednich wersji)
};
