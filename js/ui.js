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
            
            // Modal H3 Live (Phase 3) - referencje do inputów
            h3LiveModal: {
                backdrop: get('h3-live-modal'),
                percentile: get('h3-live-percentile'),
                mass: get('h3-live-mass'),
                minScore: get('h3-live-min-score'),
                tp: get('h3-live-tp'),
                sl: get('h3-live-sl'),
                maxHold: get('h3-live-hold'), // NOWE V4
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
                // ZMIANA: Zwiększono limit w UI do 5000
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

        return `<div class="candidate-item phase3-item flex items-center text-xs p-2 rounded-md cursor-pointer transition-colors ${statusClass} hover:bg-gray-800" data-ticker="${s.ticker}">
                    <i data-lucide="${icon}" class="w-4 h-4 mr-2"></i>
                    <span class="font-bold">${s.ticker}</span>
                    ${scoreDisplay}
                    <span class="ml-auto text-gray-500">${s.status}</span>
                </div>`;
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
                                <h3 class="font-semibold text-gray-400 flex items-center"><i data-lucide="bar-chart-2" class="w-4 h-4 mr-2 text-yellow-400"></i>Postęp Skanera (F1)</h3>
                                <div class="mt-2"><span id="progress-text" class="text-2xl font-extrabold">0 / 0</span><span class="text-gray-500 text-sm"> tickery</span></div>
                                <div class="w-full bg-gray-700 rounded-full h-2.5 mt-2"><div id="progress-bar" class="bg-sky-600 h-2.5 rounded-full transition-all duration-500" style="width: 0%"></div></div>
                            </div>
                            <div class="bg-[#161B22] p-4 rounded-lg shadow-lg border border-gray-700">
                                <h3 class="font-semibold text-gray-400 flex items-center"><i data-lucide="target" class="w-4 h-4 mr-2 text-red-500"></i>Sygnały H3</h3>
                                <div class="mt-2">
                                    <p id="dashboard-active-signals" class="text-4xl font-extrabold text-red-400">0</p>
                                    <p class="text-sm text-gray-500 mt-1">Aktywne / Oczekujące</p>
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
    
    agentReport: (report) => {
        const stats = report.stats;
        const trades = report.trades;
        const total_trades_count = report.total_trades_count;
        
        const formatMetric = (val) => (typeof val !== 'number' || isNaN(val)) ? `<span class="text-gray-600">---</span>` : val.toFixed(3);
        const formatPercent = (val) => {
            if (typeof val !== 'number' || isNaN(val)) return `<span class="text-gray-500">---</span>`;
            const color = val >= 0 ? 'text-green-500' : 'text-red-500';
            return `<span class="${color}">${val.toFixed(2)}%</span>`;
        };
        const formatProfitFactor = (val) => {
             if (typeof val !== 'number' || isNaN(val)) return `<span class="text-gray-500">---</span>`;
             const color = val >= 1 ? 'text-green-500' : 'text-red-500';
             return `<span class="${color}">${val.toFixed(2)}</span>`;
        };
        const formatNumber = (val) => (typeof val !== 'number' || isNaN(val)) ? `<span class="text-gray-500">---</span>` : val.toFixed(2);

        const createStatCard = (label, value, icon) => {
            return `<div class="bg-[#161B22] p-4 rounded-lg shadow-lg border border-gray-700">
                        <h3 class="font-semibold text-gray-400 flex items-center text-sm">
                            <i data-lucide="${icon}" class="w-4 h-4 mr-2 text-sky-400"></i>${label}
                        </h3>
                        <p class="text-3xl font-extrabold mt-2 text-white">${value}</p>
                    </div>`;
        };
        
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

        const tradeHeaders = [
            'Data Otwarcia', 'Ticker', 'Strategia', 'Status', 'Cena Wejścia', 'Cena Zamknięcia', 'P/L (%)',
            'ATR', 'T. Dil.', 'P. Grav.', 'TD %tile', 'PG %tile',
            'Inst. Sync', 'Retail Herd.',
            'AQM H3', 'AQM %tile', 'J (Norm)', '∇² (Norm)', 'm² (Norm)',
            'J (H4)', 'J Thresh.'
        ];
        
        const headerClasses = [
            'sticky left-0', 'sticky left-[90px]', 'sticky left-[160px]', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right'
        ];

        const tradeRows = trades.map(t => {
            const statusClass = t.status === 'CLOSED_TP' ? 'text-green-400' : (t.status === 'CLOSED_SL' ? 'text-red-400' : 'text-yellow-400');
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
        
        // =========================================================================
        // PRZYWRÓCONY I ROZSZERZONY MODUŁ BACKTESTU (V3 + V4 Params)
        // =========================================================================
        const backtestSection = `
            <div class="bg-[#161B22] p-6 rounded-lg shadow-lg border border-gray-700">
                <h4 class="text-lg font-semibold text-gray-300 mb-3">Uruchom Nowy Test Historyczny</h4>
                <p class="text-sm text-gray-500 mb-4">Wpisz rok (np. 2010), aby przetestować strategie na historycznych danych dla tego roku.</p>
                <div class="flex items-start gap-3 mb-4">
                    <input type="number" id="backtest-year-input" class="modal-input w-32 !mb-0" placeholder="YYYY" min="2000" max="${new Date().getFullYear()}">
                    <button id="run-backtest-year-btn" class="modal-button modal-button-primary flex items-center flex-shrink-0 bg-sky-600 hover:bg-sky-700">
                        <i data-lucide="play" class="w-4 h-4 mr-2"></i>
                        Uruchom Test
                    </button>
                </div>

                <button id="toggle-h3-params" class="text-xs text-gray-400 hover:text-white flex items-center focus:outline-none border border-gray-700 px-3 py-1 rounded bg-[#0D1117]">
                    <span class="font-bold text-sky-500 mr-2">Zaawansowana Konfiguracja H3 (Symulator)</span>
                    <i data-lucide="chevron-down" id="h3-params-icon" class="w-4 h-4 transition-transform"></i>
                </button>

                <div id="h3-params-container" class="mt-3 p-4 bg-[#0D1117] border border-gray-700 rounded hidden grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div>
                        <label class="block text-xs font-bold text-gray-500 mb-1 uppercase">Percentyl AQM</label>
                        <input type="number" id="h3-param-percentile" class="modal-input !mb-0 text-xs" placeholder="0.95" step="0.01" value="0.95">
                        <p class="text-[10px] text-gray-600 mt-1">Domyślny: 0.95</p>
                    </div>
                    <div>
                        <label class="block text-xs font-bold text-gray-500 mb-1 uppercase">Próg Masy m²</label>
                        <input type="number" id="h3-param-mass" class="modal-input !mb-0 text-xs" placeholder="-0.5" step="0.1" value="-0.5">
                        <p class="text-[10px] text-gray-600 mt-1">Domyślny: -0.5</p>
                    </div>
                    <div>
                        <label class="block text-xs font-bold text-gray-500 mb-1 uppercase">Min. AQM Score</label>
                        <input type="number" id="h3-param-min-score" class="modal-input !mb-0 text-xs" placeholder="0.0" step="0.1" value="0.0">
                        <p class="text-[10px] text-gray-600 mt-1">Hard Floor (V4)</p>
                    </div>
                    
                    <div>
                        <label class="block text-xs font-bold text-gray-500 mb-1 uppercase">Mnożnik TP (ATR)</label>
                        <input type="number" id="h3-param-tp" class="modal-input !mb-0 text-xs" placeholder="5.0" step="0.5" value="5.0">
                        <p class="text-[10px] text-gray-600 mt-1">Domyślny: 5.0</p>
                    </div>
                    <div>
                        <label class="block text-xs font-bold text-gray-500 mb-1 uppercase">Mnożnik SL (ATR)</label>
                        <input type="number" id="h3-param-sl" class="modal-input !mb-0 text-xs" placeholder="2.0" step="0.5" value="2.0">
                        <p class="text-[10px] text-gray-600 mt-1">Domyślny: 2.0</p>
                    </div>
                    <div>
                        <label class="block text-xs font-bold text-gray-500 mb-1 uppercase">Max Hold (Dni)</label>
                        <input type="number" id="h3-param-hold" class="modal-input !mb-0 text-xs" placeholder="5" step="1" value="5">
                        <p class="text-[10px] text-gray-600 mt-1">Nowe w V4</p>
                    </div>

                    <div class="md:col-span-3 border-t border-gray-800 pt-3 mt-1">
                        <label class="block text-xs font-bold text-gray-500 mb-1 uppercase">Nazwa Setupu (Suffix)</label>
                        <input type="text" id="h3-param-name" class="modal-input !mb-0 text-xs" placeholder="CUSTOM_TEST_1">
                        <p class="text-[10px] text-gray-600 mt-1">Oznaczenie w raportach</p>
                    </div>
                </div>

                <div id="backtest-status-message" class="text-sm mt-3 h-4"></div>
            </div>
        `;
        // =========================================================================
        
        const quantumLabSection = `
            <div class="bg-[#161B22] p-6 rounded-lg shadow-lg border border-gray-700 relative overflow-hidden">
                <div class="absolute top-0 right-0 p-2 opacity-5 pointer-events-none">
                    <i data-lucide="atom" class="w-32 h-32 text-purple-500"></i>
                </div>
                <h4 class="text-lg font-semibold text-purple-400 mb-3 flex items-center">
                    <i data-lucide="flask-conical" class="w-5 h-5 mr-2"></i> Quantum Lab (Apex V4)
                </h4>
                <p class="text-sm text-gray-500 mb-4">Uruchom optymalizację bayesowską (Optuna), aby znaleźć idealne parametry H3 dla wybranego roku.</p>
                
                <div class="flex flex-wrap gap-3">
                    <button id="open-quantum-modal-btn" class="modal-button modal-button-primary bg-purple-600 hover:bg-purple-700 flex items-center flex-shrink-0">
                        <i data-lucide="cpu" class="w-4 h-4 mr-2"></i>
                        Konfiguruj Optymalizację
                    </button>
                    <button id="view-optimization-results-btn" class="modal-button modal-button-secondary flex items-center flex-shrink-0">
                        <i data-lucide="list" class="w-4 h-4 mr-2"></i>
                        Wyniki
                    </button>
                </div>
                <div id="quantum-lab-status" class="text-sm mt-3 h-4"></div>
            </div>
        `;

        const aiOptimizerSection = `
            <div class="bg-[#161B22] p-6 rounded-lg shadow-lg border border-gray-700">
                <h4 class="text-lg font-semibold text-gray-300 mb-3">Analiza Mega Agenta AI</h4>
                <p class="text-sm text-gray-500 mb-4">Uruchom Mega Agenta, aby przeanalizował wszystkie zebrane dane i zasugerował optymalizacje strategii.</p>
                <div class="flex items-start gap-3">
                    <button id="run-ai-optimizer-btn" class="modal-button modal-button-primary flex items-center flex-shrink-0">
                        <i data-lucide="brain-circuit" class="w-4 h-4 mr-2"></i>
                        Analiza AI
                    </button>
                    <button id="view-ai-report-btn" class="modal-button modal-button-secondary flex items-center flex-shrink-0">
                        <i data-lucide="eye" class="w-4 h-4 mr-2"></i>
                        Raport
                    </button>
                </div>
                <div id="ai-optimizer-status-message" class="text-sm mt-3 h-4"></div>
            </div>
        `;

        const exportSection = `
            <div class="bg-[#161B22] p-6 rounded-lg shadow-lg border border-gray-700">
                <h4 class="text-lg font-semibold text-gray-300 mb-3">Eksport Danych</h4>
                <p class="text-sm text-gray-500 mb-4">Pobierz *wszystkie* ${total_trades_count} transakcje jako CSV.</p>
                <div class="flex items-start gap-3">
                    <button id="run-csv-export-btn" class="modal-button modal-button-primary flex items-center flex-shrink-0">
                        <i data-lucide="download-cloud" class="w-4 h-4 mr-2"></i>
                        Eksport CSV
                    </button>
                </div>
                <div id="csv-export-status-message" class="text-sm mt-3 h-4"></div>
            </div>
        `;
        
        const h3DeepDiveSection = `
            <div class="bg-[#161B22] p-6 rounded-lg shadow-lg border border-gray-700">
                <h4 class="text-lg font-semibold text-gray-300 mb-3">Analiza Porażek H3</h4>
                <p class="text-sm text-gray-500 mb-4">Analiza "słabego roku" (Deep Dive).</p>
                <div class="flex items-start gap-3">
                    <button id="run-h3-deep-dive-modal-btn" class="modal-button modal-button-primary flex items-center flex-shrink-0">
                        <i data-lucide="search-check" class="w-4 h-4 mr-2"></i>
                        Analiza Deep Dive
                    </button>
                </div>
                <div id="h3-deep-dive-main-status" class="text-sm mt-3 h-4"></div>
            </div>
        `;

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
                    
                    <h3 class="text-xl font-bold text-gray-300 mt-8 mb-4">Narzędzia Analityczne</h3>
                    <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mt-6">
                        ${backtestSection}
                        ${quantumLabSection}
                        ${aiOptimizerSection}
                        ${h3DeepDiveSection}
                        ${exportSection}
                    </div>

                    <h3 class="text-xl font-bold text-gray-300 mt-8 mb-4">Historia Zamkniętych Transakcji</h3>
                    ${paginationControls}
                    ${tradeTable}
                    ${paginationControls} </div>`;
    },

    // === NOWOŚĆ: Renderowanie wyników optymalizacji ===
    optimizationResults: (job) => {
        if (!job) return `<p class="text-gray-500">Brak danych o optymalizacji.</p>`;
        
        const trials = job.trials || [];
        // Sortowanie: Najlepsze wyniki na górze (Profit Factor)
        trials.sort((a, b) => (b.profit_factor || 0) - (a.profit_factor || 0));
        
        const bestTrial = trials[0];
        
        const trialsRows = trials.map(t => {
            const isBest = t.id === job.best_trial_id;
            const rowClass = isBest ? "bg-green-900/20 border-l-4 border-green-500" : "border-b border-gray-800 hover:bg-[#1f2937]";
            
            // Formatowanie parametrów do czytelnego stringa
            const paramsStr = Object.entries(t.params)
                .map(([k, v]) => `<span class="text-gray-400">${k}:</span> <span class="text-sky-300">${typeof v === 'number' ? v.toFixed(2) : v}</span>`)
                .join(', ');

            return `<tr class="${rowClass}">
                <td class="p-2 text-center font-mono text-gray-500">#${t.trial_number}</td>
                <td class="p-2 text-right font-bold ${t.profit_factor >= 1.5 ? 'text-green-400' : 'text-gray-300'}">${t.profit_factor ? t.profit_factor.toFixed(2) : '0.00'}</td>
                <td class="p-2 text-right">${t.win_rate ? t.win_rate.toFixed(1) : '0.0'}%</td>
                <td class="p-2 text-right">${t.total_trades || 0}</td>
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
                        <div class="text-xs text-gray-500">Status: ${job.status}</div>
                    </div>
                </div>

                <h4 class="text-sm text-gray-400 uppercase font-bold border-b border-gray-700 pb-1">Ranking Prób (Top Wyniki)</h4>
                <div class="overflow-x-auto max-h-64 border border-gray-700 rounded">
                    <table class="w-full text-sm text-left text-gray-300">
                        <thead class="text-xs text-gray-400 uppercase bg-[#0D1117] sticky top-0">
                            <tr>
                                <th class="p-2 text-center">#</th>
                                <th class="p-2 text-right">PF</th>
                                <th class="p-2 text-right">Win Rate</th>
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
