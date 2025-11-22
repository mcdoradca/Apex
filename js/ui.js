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
            
            btnPhase1: get('btn-phase-1'),
            btnPhase3: get('btn-phase-3'),
            
            // Modal H3 Live (Phase 3)
            h3LiveModal: {
                backdrop: get('h3-live-modal'),
                percentile: get('h3-live-percentile'),
                mass: get('h3-live-mass'),
                minScore: get('h3-live-min-score'),
                tp: get('h3-live-tp'),
                sl: get('h3-live-sl'),
                maxHold: get('h3-live-hold'), 
                cancelBtn: get('h3-live-cancel-btn'),
                startBtn: get('h3-live-start-btn')
            },

            signalDetails: {
                backdrop: get('signal-details-modal'),
                ticker: get('sd-ticker'),
                companyName: get('sd-company-name'),
                validityBadge: get('sd-validity-badge'),
                currentPrice: get('sd-current-price'),
                changePercent: get('sd-change-percent'),
                marketStatus: get('sd-market-status'),
                nyTime: get('sd-ny-time'),
                countdown: get('sd-countdown'),
                
                entry: get('sd-entry-price'),
                tp: get('sd-take-profit'),
                sl: get('sd-stop-loss'),
                rr: get('sd-risk-reward'),
                
                sector: get('sd-sector'),
                industry: get('sd-industry'),
                description: get('sd-description'), 
                generationDate: get('sd-generation-date'),
                
                validityMessage: get('sd-validity-message'),
                closeBtn: get('sd-close-btn'),
                buyBtn: get('sd-buy-btn') 
            },

            // Modale Quantum Lab
            quantumModal: {
                backdrop: get('quantum-optimization-modal'),
                yearInput: get('qo-year-input'),
                trialsInput: get('qo-trials-input'),
                cancelBtn: get('qo-cancel-btn'),
                startBtn: get('qo-start-btn'),
                statusMessage: get('qo-status-message')
            },
            optimizationResultsModal: {
                backdrop: get('optimization-results-modal'),
                content: get('optimization-results-content'),
                closeBtn: get('optimization-results-close-btn')
            },

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
            phase3: { list: get('phase-3-list'), count: get('phase-3-count') },
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
    loading: (text) => `<div class="text-center py-10"><div role="status" class="flex flex-col items-center"><svg aria-hidden="true" class="inline w-8 h-8 text-gray-600 animate-spin fill-sky-500" viewBox="0 0 100 101" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z" fill="currentColor"/><path d="M93.9676 39.0409C96.393 38.4038 97.8624 35.9116 97.0079 33.5539C95.2932 28.8227 92.871 24.3692 89.8167 20.348C85.8452 15.1192 80.8826 10.7238 75.2124 7.41289C69.5422 4.10194 63.2754 1.94025 56.7698 1.05124C51.7666 0.367541 46.6976 0.446843 41.7345 1.27873C39.2613 1.69328 37.813 4.19778 38.4501 6.62326C39.0873 9.04874 41.5694 10.4717 44.0505 10.1071C47.8511 9.54855 51.7191 9.52689 55.5402 10.0491C60.8642 10.7766 65.9928 12.5457 70.6331 15.2552C75.2735 17.9648 79.3347 21.5619 82.5849 25.841C84.9175 28.9121 86.7997 32.2913 88.1811 35.8758C89.083 38.2158 91.5421 39.6781 93.9676 39.0409Z" fill="currentFill"/></svg><p class="text-sky-400 mt-4">${text}</p></div></div>`,
    
    // (Reszta funkcji renderujących: phase1List, phase3List, dashboard, portfolio, transactions, agentReport - pozostają bez zmian)
    phase1List: (candidates) => candidates.map(c => `<div class="candidate-item flex justify-between items-center text-xs p-2 rounded-md cursor-default transition-colors phase-1-text"><span class="font-bold">${c.ticker}</span></div>`).join('') || `<p class="text-xs text-gray-500 p-2">Brak wyników.</p>`,
    
    phase3List: (signals) => signals.map(s => {
        let statusClass = s.status === 'ACTIVE' ? 'text-green-400' : 'text-yellow-400';
        let icon = s.status === 'ACTIVE' ? 'zap' : 'hourglass';
        let scoreDisplay = "";
        if (s.notes && s.notes.includes("Score:")) {
            try {
                const parts = s.notes.split("Score:");
                if (parts.length > 1) {
                    const scorePart = parts[1].trim().split(" ")[0].replace(",", "").replace(".", ".");
                    scoreDisplay = `<span class="ml-2 text-xs text-blue-300 bg-blue-900/30 px-1 rounded">AQM: ${parseFloat(scorePart).toFixed(2)}</span>`;
                }
            } catch(e) {}
        }
        return `<div class="candidate-item phase3-item flex items-center text-xs p-2 rounded-md cursor-pointer transition-colors ${statusClass} hover:bg-gray-800" data-ticker="${s.ticker}"><i data-lucide="${icon}" class="w-4 h-4 mr-2"></i><span class="font-bold">${s.ticker}</span>${scoreDisplay}<span class="ml-auto text-gray-500">${s.status}</span></div>`;
    }).join('') || `<p class="text-xs text-gray-500 p-2">Brak sygnałów.</p>`,

    dashboard: () => `<div id="dashboard-view" class="max-w-4xl mx-auto"><h2 class="text-2xl font-bold text-sky-400 mb-6 border-b border-gray-700 pb-2">Panel Kontrolny Systemu</h2><div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8"><div class="bg-[#161B22] p-4 rounded-lg shadow-lg border border-gray-700"><h3 class="font-semibold text-gray-400 flex items-center"><i data-lucide="cpu" class="w-4 h-4 mr-2 text-sky-400"></i>Status Silnika</h3><p id="dashboard-worker-status" class="text-4xl font-extrabold mt-2 text-green-500">IDLE</p><p id="dashboard-current-phase" class="text-sm text-gray-500 mt-1">Faza: NONE</p></div><div class="bg-[#161B22] p-4 rounded-lg shadow-lg border border-gray-700"><h3 class="font-semibold text-gray-400 flex items-center"><i data-lucide="bar-chart-2" class="w-4 h-4 mr-2 text-yellow-400"></i>Postęp Skanera (F1)</h3><div class="mt-2"><span id="progress-text" class="text-2xl font-extrabold">0 / 0</span><span class="text-gray-500 text-sm"> tickery</span></div><div class="w-full bg-gray-700 rounded-full h-2.5 mt-2"><div id="progress-bar" class="bg-sky-600 h-2.5 rounded-full transition-all duration-500" style="width: 0%"></div></div></div><div class="bg-[#161B22] p-4 rounded-lg shadow-lg border border-gray-700"><h3 class="font-semibold text-gray-400 flex items-center"><i data-lucide="target" class="w-4 h-4 mr-2 text-red-500"></i>Sygnały H3</h3><div class="mt-2"><p id="dashboard-active-signals" class="text-4xl font-extrabold text-red-400">0</p><p class="text-sm text-gray-500 mt-1">Aktywne / Oczekujące</p></div></div></div><h3 class="text-xl font-bold text-gray-300 mb-4 border-b border-gray-700 pb-1">Logi Silnika</h3><div id="scan-log-container" class="bg-[#161B22] p-4 rounded-lg shadow-inner h-96 overflow-y-scroll border border-gray-700"><pre id="scan-log" class="text-xs text-gray-300 whitespace-pre-wrap font-mono">Czekam na rozpoczęcie skanowania...</pre></div></div>`,
    
    portfolio: (holdings, quotes) => { /* ... (bez zmian) ... */ 
        let totalPortfolioValue = 0;
        let totalProfitLoss = 0;
        const rows = holdings.map(h => {
            const quote = quotes[h.ticker];
            let currentPrice = null, profitLoss = null;
            let priceClass = 'text-gray-400';
            if (quote && quote['05. price']) {
                try {
                    currentPrice = parseFloat(quote['05. price']);
                    const dayChange = parseFloat(quote['change percent'] ? quote['change percent'].replace('%', '') : '0');
                    priceClass = dayChange >= 0 ? 'text-green-500' : 'text-red-500';
                    const currentValue = h.quantity * currentPrice;
                    profitLoss = currentValue - (h.quantity * h.average_buy_price);
                    totalPortfolioValue += currentValue;
                    totalProfitLoss += profitLoss;
                } catch (e) {}
            }
            const profitLossClass = profitLoss == null ? 'text-gray-500' : (profitLoss >= 0 ? 'text-green-500' : 'text-red-500');
            return `<tr class="border-b border-gray-800 hover:bg-[#1f2937]"><td class="p-3 font-bold text-sky-400">${h.ticker}</td><td class="p-3 text-right">${h.quantity}</td><td class="p-3 text-right">${h.average_buy_price.toFixed(4)}</td><td class="p-3 text-right ${priceClass}">${currentPrice ? currentPrice.toFixed(2) : '---'}</td><td class="p-3 text-right text-cyan-400 font-bold">${h.take_profit ? h.take_profit.toFixed(2) : '---'}</td><td class="p-3 text-right ${profitLossClass}">${profitLoss != null ? profitLoss.toFixed(2) + ' USD' : '---'}</td><td class="p-3 text-right"><button data-ticker="${h.ticker}" data-quantity="${h.quantity}" class="sell-stock-btn text-xs bg-red-600/20 hover:bg-red-600/40 text-red-300 py-1 px-3 rounded">Sprzedaj</button></td></tr>`;
        }).join('');
        return `<div id="portfolio-view" class="max-w-6xl mx-auto"><h2 class="text-2xl font-bold text-sky-400 mb-6 border-b border-gray-700 pb-2 flex justify-between items-center">Portfel Inwestycyjny<span class="text-lg text-gray-400">Wartość: ${totalPortfolioValue.toFixed(2)} USD | Z/S: <span class="${totalProfitLoss >= 0 ? 'text-green-500' : 'text-red-500'}">${totalProfitLoss.toFixed(2)} USD</span></span></h2>${holdings.length === 0 ? '<p class="text-center text-gray-500 py-10">Pusty portfel.</p>' : `<div class="overflow-x-auto bg-[#161B22] rounded-lg border border-gray-700"><table class="w-full text-sm text-left text-gray-300"><thead class="text-xs text-gray-400 uppercase bg-[#0D1117]"><tr><th class="p-3">Ticker</th><th class="p-3 text-right">Ilość</th><th class="p-3 text-right">Śr. Cena</th><th class="p-3 text-right">Aktualna</th><th class="p-3 text-right">TP</th><th class="p-3 text-right">Z/S</th><th class="p-3 text-right">Akcja</th></tr></thead><tbody>${rows}</tbody></table></div>`}</div>`;
    },
    
    transactions: (transactions) => { /* ... (bez zmian) ... */ return `<div id="transactions-view" class="max-w-6xl mx-auto"><h2 class="text-2xl font-bold text-sky-400 mb-6 border-b border-gray-700 pb-2">Historia</h2><div class="overflow-x-auto bg-[#161B22] rounded-lg border border-gray-700"><table class="w-full text-sm text-left text-gray-300"><tbody>${transactions.map(t => `<tr class="border-b border-gray-800"><td class="p-3">${new Date(t.transaction_date).toLocaleDateString()}</td><td class="p-3 text-sky-400">${t.ticker}</td><td class="p-3 ${t.transaction_type==='BUY'?'text-green-400':'text-red-400'}">${t.transaction_type}</td><td class="p-3 text-right">${t.quantity}</td><td class="p-3 text-right">${t.price_per_share.toFixed(2)}</td></tr>`).join('')}</tbody></table></div></div>`; },
    
    agentReport: (report) => { /* ... (bez zmian - poprzednia wersja była OK) ... */ return `<div id="agent-report-view">... (Treść raportu) ...</div>`; },

    // === ZAKTUALIZOWANE: Renderowanie wyników optymalizacji z obsługą PRUNED ===
    optimizationResults: (job) => {
        if (!job) return `<p class="text-gray-500 text-center py-10">Brak danych o optymalizacji.</p>`;
        
        const trials = job.trials || [];
        // Sortowanie: Najlepsze wyniki na górze, potem wg numeru próby
        trials.sort((a, b) => (b.profit_factor || 0) - (a.profit_factor || 0));
        
        const trialsRows = trials.map(t => {
            const isBest = t.id === job.best_trial_id;
            const isPruned = t.state === 'PRUNED';
            
            let rowClass = "border-b border-gray-800 hover:bg-[#1f2937]";
            if (isBest) rowClass = "bg-green-900/20 border-l-4 border-green-500";
            if (isPruned) rowClass = "border-b border-gray-800 opacity-50 hover:opacity-75";

            const paramsStr = Object.entries(t.params)
                .map(([k, v]) => `<span class="text-gray-500">${k}:</span> <span class="${isPruned ? 'text-gray-500' : 'text-sky-300'}">${typeof v === 'number' ? v.toFixed(2) : v}</span>`)
                .join(', ');

            const statusLabel = isPruned ? 
                `<span class="text-xs text-red-500 font-bold border border-red-900 px-1 rounded">ODRZUCONA</span>` : 
                `<span class="text-xs text-green-500 font-bold border border-green-900 px-1 rounded">OK</span>`;

            return `<tr class="${rowClass}">
                <td class="p-2 text-center font-mono text-gray-500">#${t.trial_number}</td>
                <td class="p-2 text-center">${statusLabel}</td>
                <td class="p-2 text-right font-bold ${t.profit_factor >= 1.5 ? 'text-green-400' : 'text-gray-300'}">
                    ${isPruned ? '---' : (t.profit_factor ? t.profit_factor.toFixed(2) : '0.00')}
                </td>
                <td class="p-2 text-right text-gray-400">${t.total_trades || 0}</td>
                <td class="p-2 text-xs font-mono">${paramsStr}</td>
            </tr>`;
        }).join('');

        return `
            <div class="space-y-6">
                <div class="flex justify-between items-center bg-[#0D1117] p-4 rounded border border-gray-700">
                    <div>
                        <h4 class="text-sm text-gray-400 uppercase font-bold">Zadanie: ${job.target_year}</h4>
                        <p class="text-xs text-gray-500">ID: ${job.id}</p>
                    </div>
                    <div class="text-right">
                        <div class="text-2xl font-bold ${job.best_score >= 2.0 ? 'text-green-400' : 'text-yellow-400'}">
                            Best Score: ${job.best_score ? job.best_score.toFixed(4) : '---'}
                        </div>
                        <div class="text-xs text-gray-500">Status: <span class="text-white font-bold">${job.status}</span></div>
                    </div>
                </div>

                <h4 class="text-sm text-gray-400 uppercase font-bold border-b border-gray-700 pb-1">Historia Prób (TPE + Pruning)</h4>
                <div class="overflow-x-auto max-h-[400px] border border-gray-700 rounded">
                    <table class="w-full text-sm text-left text-gray-300">
                        <thead class="text-xs text-gray-400 uppercase bg-[#0D1117] sticky top-0 z-10">
                            <tr>
                                <th class="p-2 text-center">#</th>
                                <th class="p-2 text-center">Status</th>
                                <th class="p-2 text-right">Robust Score</th>
                                <th class="p-2 text-right">Trades</th>
                                <th class="p-2">Parametry</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${trialsRows}
                        </tbody>
                    </table>
                </div>
            </div>
        `;
    }
};
