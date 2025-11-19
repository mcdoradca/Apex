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
            
            h3LiveModal: {
                backdrop: get('h3-live-modal'),
                percentile: get('h3-live-percentile'),
                mass: get('h3-live-mass'),
                // NOWY SELEKTOR
                minScore: get('h3-live-min-score'),
                // ============
                tp: get('h3-live-tp'),
                sl: get('h3-live-sl'),
                cancelBtn: get('h3-live-cancel-btn'),
                startBtn: get('h3-live-start-btn')
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
            phase2: { list: get('phase-2-list'), count: get('phase-2-count') },
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
    
    phase2List: (results) => "", 
    
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

        return `<div class="candidate-item flex items-center text-xs p-2 rounded-md cursor-default transition-colors ${statusClass}">
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
    
    // ==========================================================
    // === AKTUALIZACJA (STRONICOWANIE I GŁĘBOKIE LOGOWANIE) ===
    // ==========================================================
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
        
        // --- Tabela statystyk per strategia ---
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

        // --- Tabela historii transakcji ---
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
        
        // --- Sekcje Backtestu (z nowym polem w UI) ---
        const backtestSection = `
            <div class="bg-[#161B22] p-6 rounded-lg shadow-lg border border-gray-700">
                <h4 class="text-lg font-semibold text-gray-300 mb-3">Uruchom Nowy Test Historyczny</h4>
                <p class="text-sm text-gray-500 mb-4">Wpisz rok (np. 2010), aby przetestować strategie na historycznych danych dla tego roku.</p>
                
                <div class="flex items-start gap-3 mb-4">
                    <input type="number" id="backtest-year-input" class="modal-input w-32 !mb-0" placeholder="YYYY" min="2000" max="${new Date().getFullYear()}">
                    <button id="run-backtest-year-btn" class="modal-button modal-button-primary flex items-center flex-shrink-0">
                        <i data-lucide="play" class="w-4 h-4 mr-2"></i>
                        Uruchom Test
                    </button>
                </div>

                <div class="border-t border-gray-700 pt-3">
                    <button id="toggle-h3-params" class="text-xs text-sky-400 flex items-center hover:text-sky-300 transition-colors mb-3">
                        <i data-lucide="settings-2" class="w-3 h-3 mr-1"></i>
                        Zaawansowana Konfiguracja H3 (Symulator)
                        <i data-lucide="chevron-down" id="h3-params-icon" class="w-3 h-3 ml-1 transition-transform"></i>
                    </button>
                    
                    <div id="h3-params-container" class="hidden grid grid-cols-1 md:grid-cols-3 gap-3 bg-gray-800/30 p-3 rounded-md border border-gray-700/50">
                        
                        <div>
                            <label class="text-[10px] text-gray-400 uppercase font-bold">Percentyl AQM (Domyślny: 0.95)</label>
                            <input type="number" id="h3-param-percentile" class="modal-input text-xs !py-1 !h-8" placeholder="0.95" step="0.01" value="0.95">
                        </div>
                        
                        <div>
                            <label class="text-[10px] text-gray-400 uppercase font-bold">Próg Masy m² (Domyślny: -0.5)</label>
                            <input type="number" id="h3-param-mass" class="modal-input text-xs !py-1 !h-8" placeholder="-0.5" step="0.1" value="-0.5">
                        </div>
                        
                        <div>
                            <label class="text-[10px] text-gray-400 uppercase font-bold">Min. AQM Score (Hard Floor)</label>
                            <input type="number" id="h3-param-min-score" class="modal-input text-xs !py-1 !h-8" placeholder="0.0" step="0.1" value="0.0">
                        </div>
                        <div>
                            <label class="text-[10px] text-gray-400 uppercase font-bold">Mnożnik TP (ATR) (Domyślny: 5.0)</label>
                            <input type="number" id="h3-param-tp" class="modal-input text-xs !py-1 !h-8" placeholder="5.0" step="0.5" value="5.0">
                        </div>

                        <div>
                            <label class="text-[10px] text-gray-400 uppercase font-bold">Mnożnik SL (ATR) (Domyślny: 2.0)</label>
                            <input type="number" id="h3-param-sl" class="modal-input text-xs !py-1 !h-8" placeholder="2.0" step="0.5" value="2.0">
                        </div>

                         <div>
                            <label class="text-[10px] text-gray-400 uppercase font-bold">Max Hold (Dni) (Domyślny: 5)</label>
                            <input type="number" id="h3-param-hold" class="modal-input text-xs !py-1 !h-8" placeholder="5" step="1" value="5">
                        </div>

                        <div class="md:col-span-3">
                            <label class="text-[10px] text-gray-400 uppercase font-bold">Nazwa Setupu (Suffix)</label>
                            <input type="text" id="h3-param-name" class="modal-input text-xs !py-1 !h-8" placeholder="CUSTOM_TEST_1">
                        </div>
                    </div>
                </div>

                <div id="backtest-status-message" class="text-sm mt-3 h-4"></div>
            </div>
        `;
        
        const aiOptimizerSection = `<div class="bg-[#161B22] p-6 rounded-lg shadow-lg border border-gray-700"><h4 class="text-lg font-semibold text-gray-300 mb-3">Analiza Mega Agenta AI</h4><p class="text-sm text-gray-500 mb-4">Uruchom Mega Agenta, aby przeanalizował wszystkie zebrane dane (powyżej) i zasugerował optymalizacje strategii.</p><div class="flex items-start gap-3"><button id="run-ai-optimizer-btn" class="modal-button modal-button-primary flex items-center flex-shrink-0"><i data-lucide="brain-circuit" class="w-4 h-4 mr-2"></i> Uruchom Analizę AI</button><button id="view-ai-report-btn" class="modal-button modal-button-secondary flex items-center flex-shrink-0"><i data-lucide="eye" class="w-4 h-4 mr-2"></i> Pokaż Ostatni Raport</button></div><div id="ai-optimizer-status-message" class="text-sm mt-3 h-4"></div></div>`;
        const exportSection = `<div class="bg-[#161B22] p-6 rounded-lg shadow-lg border border-gray-700"><h4 class="text-lg font-semibold text-gray-300 mb-3">Eksport Danych</h4><p class="text-sm text-gray-500 mb-4">Pobierz *wszystkie* ${total_trades_count} transakcje z bazy danych jako plik CSV do własnej analizy.</p><div class="flex items-start gap-3"><button id="run-csv-export-btn" class="modal-button modal-button-primary flex items-center flex-shrink-0"><i data-lucide="download-cloud" class="w-4 h-4 mr-2"></i> Eksportuj do CSV</button></div><div id="csv-export-status-message" class="text-sm mt-3 h-4"></div></div>`;
        const h3DeepDiveSection = `<div class="bg-[#161B22] p-6 rounded-lg shadow-lg border border-gray-700"><h4 class="text-lg font-semibold text-gray-300 mb-3">Analiza Porażek H3 (Deep Dive)</h4><p class="text-sm text-gray-500 mb-4">Uruchom analizę "słabego roku", aby dowiedzieć się, dlaczego transakcje H3 zawiodły w tym okresie.</p><div class="flex items-start gap-3"><button id="run-h3-deep-dive-modal-btn" class="modal-button modal-button-primary flex items-center flex-shrink-0"><i data-lucide="search-check" class="w-4 h-4 mr-2"></i> Uruchom Analizę Porażek</button></div><div id="h3-deep-dive-main-status" class="text-sm mt-3 h-4"></div></div>`;
        
        const totalPages = Math.ceil(total_trades_count / REPORT_PAGE_SIZE);
        const startTrade = (state.currentReportPage - 1) * REPORT_PAGE_SIZE + 1;
        const endTrade = Math.min(state.currentReportPage * REPORT_PAGE_SIZE, total_trades_count);

        const paginationControls = totalPages > 1 ? `<div class="flex justify-between items-center mt-4"><span class="text-sm text-gray-400">Wyświetlanie ${startTrade}-${endTrade} z ${total_trades_count} transakcji</span><div class="flex gap-2"><button id="report-prev-btn" class="modal-button modal-button-secondary" ${state.currentReportPage === 1 ? 'disabled' : ''}><i data-lucide="arrow-left" class="w-4 h-4"></i></button><span class="text-sm text-gray-400 p-2">Strona ${state.currentReportPage} / ${totalPages}</span><button id="report-next-btn" class="modal-button modal-button-secondary" ${state.currentReportPage === totalPages ? 'disabled' : ''}><i data-lucide="arrow-right" class="w-4 h-4"></i></button></div></div>` : '';

        return `<div id="agent-report-view" class="max-w-6xl mx-auto"><h2 class="text-2xl font-bold text-sky-400 mb-6 border-b border-gray-700 pb-2">Raport Wydajności Agenta</h2><h3 class="text-xl font-bold text-gray-300 mb-4">Kluczowe Wskaźniki (Wszystkie ${stats.total_trades} Transakcji)</h3><div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">${createStatCard('Całkowity P/L (%)', formatPercent(stats.total_p_l_percent), 'percent')}${createStatCard('Win Rate (%)', formatPercent(stats.win_rate_percent), 'target')}${createStatCard('Profit Factor', formatProfitFactor(stats.profit_factor), 'ratio')}${createStatCard('Ilość Transakcji', stats.total_trades, 'bar-chart-2')}</div><h3 class="text-xl font-bold text-gray-300 mb-4">Podsumowanie wg Strategii</h3>${setupTable}<h3 class="text-xl font-bold text-gray-300 mt-8 mb-4">Narzędzia Analityczne</h3><div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-2 gap-6 mt-6">${backtestSection}${aiOptimizerSection}${h3DeepDiveSection}${exportSection}</div><h3 class="text-xl font-bold text-gray-300 mt-8 mb-4">Historia Zamkniętych Transakcji (z Metrykami)</h3>${paginationControls}${tradeTable}${paginationControls}</div>`;
    }
};

}

{
type: uploaded file
fileName: mcdoradca/apex/Apex-29b5b053cb2f273eaada70b8027fbcdb343a4c1c/js/logic.js
fullContent:
import { state, logger, ALERT_POLL_INTERVAL, PROFIT_ALERT_THRESHOLD, PORTFOLIO_QUOTE_POLL_INTERVAL, AI_OPTIMIZER_POLL_INTERVAL, H3_DEEP_DIVE_POLL_INTERVAL, REPORT_PAGE_SIZE } from './state.js';
import { api } from './api.js';
import { renderers } from './ui.js';

let ui = null;
export const setUI = (uiObj) => { ui = uiObj; };

export function getNYTime() {
    try {
        const options = { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false };
        const formatter = new Intl.DateTimeFormat('en-US', options);
        const parts = formatter.formatToParts(new Date());
        const find = (type) => parts.find(p => p.type === type)?.value;
        const year = find('year'), month = find('month'), day = find('day');
        const hour = find('hour') === '24' ? '00' : find('hour');
        const minute = find('minute'), second = find('second');
        return new Date(year, parseInt(month) - 1, day, hour, minute, second);
    } catch (e) { return new Date(); }
}

export function formatCountdown(ms) {
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

export function getMarketCountdown() {
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
        if (now < preMarketOpen) { targetTime = preMarketOpen; message = 'Do otwarcia Pre-Market: '; }
        else if (now >= preMarketOpen && now < marketOpen) { targetTime = marketOpen; message = 'Do otwarcia Rynku: '; }
        else if (now >= marketOpen && now < marketClose) { targetTime = marketClose; message = 'Do zamknięcia Rynku: '; }
        else {
            let daysToAdd = (dayOfWeek === 5) ? 3 : 1;
            targetTime = new Date(preMarketOpen.getTime() + daysToAdd * 24 * 60 * 60 * 1000);
            message = 'Do otwarcia Pre-Market: ';
        }
    }
    const diff = targetTime.getTime() - now.getTime();
    return message + formatCountdown(diff);
}

export function updateCountdownTimer() {
    const timerElement = document.getElementById('market-countdown-timer');
    if (timerElement) { timerElement.textContent = getMarketCountdown(); }
}

export function startMarketCountdown() {
    stopMarketCountdown();
    updateCountdownTimer();
    state.activeCountdownPolling = setInterval(updateCountdownTimer, 1000);
}
export function stopMarketCountdown() {
    if (state.activeCountdownPolling) { clearInterval(state.activeCountdownPolling); state.activeCountdownPolling = null; }
}

export function setActiveSidebar(linkElement) {
    document.querySelectorAll('.sidebar-item').forEach(el => el.classList.remove('sidebar-item-active'));
    if (linkElement) linkElement.classList.add('sidebar-item-active');
}

export function stopAllPolling() {
    if (state.activePortfolioPolling) clearTimeout(state.activePortfolioPolling);
    if (state.activeAIOptimizerPolling) clearTimeout(state.activeAIOptimizerPolling);
    if (state.activeH3DeepDivePolling) clearTimeout(state.activeH3DeepDivePolling);
    stopMarketCountdown();
}

export function updateDashboardUI(statusData) {
    if (!document.getElementById('dashboard-view')) return;
    const elStatus = document.getElementById('dashboard-worker-status');
    const elPhase = document.getElementById('dashboard-current-phase');
    const elProgText = document.getElementById('progress-text');
    const elProgBar = document.getElementById('progress-bar');
    const elLog = document.getElementById('scan-log');
    
    const elSignals = document.getElementById('dashboard-active-signals');
    if (elSignals) elSignals.textContent = (state.phase3 || []).length;
    
    if (!elStatus || !elLog) return;
    
    elStatus.textContent = statusData.status;
    elPhase.textContent = `Faza: ${statusData.phase || 'NONE'}`;
    const processed = statusData.progress.processed, total = statusData.progress.total;
    const percent = total > 0 ? Math.min((processed / total) * 100, 100) : 0;
    elProgText.textContent = `${processed} / ${total}`;
    elProgBar.style.width = `${percent.toFixed(0)}%`;
    
    if (elLog.textContent !== statusData.log) {
        elLog.textContent = statusData.log || 'Czekam na rozpoczęcie skanowania...';
        const logContainer = document.getElementById('scan-log-container');
        if(logContainer) logContainer.scrollTop = 0; 
    }
}

export function updateDashboardCounters() {
    const activeEl = document.getElementById('dashboard-active-signals');
    if (activeEl) activeEl.textContent = (state.phase3 || []).length;
}

export function displaySystemAlert(message) {
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
    } catch(e) {}

    if (state.snoozedAlerts[alertKey] && Date.now() < state.snoozedAlerts[alertKey]) return;

    let alertClass = 'bg-sky-500';
    let alertIcon = 'bell-ring';

    if (message.includes('PILNY ALERT') && message.includes('NEGATYWNY')) { alertClass = 'bg-red-600'; alertIcon = 'alert-octagon'; } 
    else if (message.includes('PILNY ALERT') && message.includes('POZYTYWNY')) { alertClass = 'bg-green-600'; alertIcon = 'check-circle'; } 
    else if (message.includes('ALARM CENOWY') || message.includes('ALERT ZYSKU')) { alertClass = 'bg-yellow-500'; alertIcon = 'dollar-sign'; } 
    else if (message.includes('TAKE PROFIT')) { alertClass = 'bg-green-600'; alertIcon = 'trending-up'; } 
    else if (message.includes('STOP LOSS')) { alertClass = 'bg-red-600'; alertIcon = 'trending-down'; }

    const alertId = `alert-${Date.now()}`;
    const alertElement = document.createElement('div');
    alertElement.id = alertId;
    alertElement.className = `alert-bar flex items-center justify-between gap-4 ${alertClass} text-white p-3 shadow-lg rounded-md animate-pulse-once`;
    alertElement.innerHTML = `
        <div class="flex items-center gap-3"><i data-lucide="${alertIcon}" class="w-6 h-6"></i><span class="font-semibold">${message}</span></div>
        <button data-alert-id="${alertId}" data-alert-key="${alertKey}" class="close-alert-btn p-1 rounded-full hover:bg-black/20 transition-colors"><i data-lucide="x" class="w-5 h-5"></i></button>
    `;
    ui.alertContainer.appendChild(alertElement);
    lucide.createIcons();

    const closeButton = alertElement.querySelector('.close-alert-btn');
    closeButton.addEventListener('click', () => {
        const keyToSnooze = closeButton.dataset.alertKey;
        if (keyToSnooze) state.snoozedAlerts[keyToSnooze] = Date.now() + 30 * 60 * 1000;
        alertElement.remove();
    });
    setTimeout(() => { const el = document.getElementById(alertId); if (el) el.remove(); }, 20000);
}

export async function pollSystemAlerts() {
    try {
        const alertData = await api.getSystemAlert();
        if (alertData && alertData.message !== 'NONE') displaySystemAlert(alertData.message);
    } catch (e) {} finally { setTimeout(pollSystemAlerts, ALERT_POLL_INTERVAL); }
}

export async function pollWorkerStatus() {
    try {
        const statusData = await api.getWorkerStatus();
        state.workerStatus = statusData;
        let statusClass = 'bg-gray-700 text-gray-200';
        if (statusData.status === 'RUNNING') statusClass = 'bg-green-600/20 text-green-400';
        else if (statusData.status === 'PAUSED') statusClass = 'bg-yellow-600/20 text-yellow-400';
        else if (statusData.status === 'ERROR') statusClass = 'bg-red-600/20 text-red-400';
        if (statusData.phase === 'BACKTESTING') statusClass = 'bg-purple-600/20 text-purple-400';
        else if (statusData.phase === 'AI_OPTIMIZING') statusClass = 'bg-pink-600/20 text-pink-400';
        else if (statusData.phase === 'DEEP_DIVE_H3') statusClass = 'bg-cyan-600/20 text-cyan-400';

        if(ui.workerStatusText) {
            ui.workerStatusText.className = `font-mono px-2 py-1 rounded-md text-xs ${statusClass} transition-colors`;
            ui.workerStatusText.textContent = statusData.phase === 'NONE' ? statusData.status : statusData.phase;
        }
        if(ui.heartbeatStatus && statusData.last_heartbeat_utc) {
            const diffSeconds = (new Date() - new Date(statusData.last_heartbeat_utc)) / 1000;
            ui.heartbeatStatus.className = `text-xs ${diffSeconds > 30 ? 'text-red-500' : 'text-green-500'}`;
            ui.heartbeatStatus.textContent = diffSeconds > 30 ? 'PRZERWANY' : new Date(statusData.last_heartbeat_utc).toLocaleTimeString();
        }
        
        const isBusy = statusData.status !== 'IDLE' && statusData.status !== 'ERROR';
        if (ui.btnPhase1) ui.btnPhase1.disabled = isBusy;
        if (ui.btnPhase3) ui.btnPhase3.disabled = isBusy;

        updateDashboardUI(statusData);
    } catch (e) {}
    setTimeout(pollWorkerStatus, 5000);
}

export async function refreshSidebarData() {
    try {
        const [phase1, phase3] = await Promise.all([
            api.getPhase1Candidates(),
            api.getPhase3Signals()
        ]);
        state.phase1 = phase1 || [];
        state.phase3 = phase3 || [];

        if(ui.phase1.list) ui.phase1.list.innerHTML = renderers.phase1List(state.phase1);
        if(ui.phase1.count) ui.phase1.count.textContent = state.phase1.length;
        if(ui.phase3.list) ui.phase3.list.innerHTML = renderers.phase3List(state.phase3);
        if(ui.phase3.count) ui.phase3.count.textContent = state.phase3.length;
        
        updateDashboardCounters();
        lucide.createIcons();
    } catch (e) {}
    setTimeout(refreshSidebarData, 15000);
}

export async function showDashboard() {
    stopAllPolling();
    setActiveSidebar(ui.dashboardLink);
    ui.mainContent.innerHTML = renderers.dashboard();
    updateDashboardUI(state.workerStatus);
    updateDashboardCounters();
    lucide.createIcons();
    startMarketCountdown();
}

function checkPortfolioProfitAlerts() {
    state.portfolio.forEach(holding => {
        const quote = state.liveQuotes[holding.ticker];
        if (quote && quote['05. price']) {
            try {
                const currentPrice = parseFloat(quote['05. price']);
                const avgBuyPrice = holding.average_buy_price;
                if (currentPrice >= (avgBuyPrice * PROFIT_ALERT_THRESHOLD)) {
                    if (!state.profitAlertsSent[holding.ticker]) {
                        const profitPercent = ((currentPrice / avgBuyPrice) - 1) * 100;
                        displaySystemAlert(`ALERT ZYSKU: ${holding.ticker} osiągnął +${profitPercent.toFixed(1)}% (Cena: ${currentPrice.toFixed(2)})`);
                        state.profitAlertsSent[holding.ticker] = true; 
                    }
                } else if (state.profitAlertsSent[holding.ticker]) state.profitAlertsSent[holding.ticker] = false;
            } catch(e) {}
        }
    });
}

async function pollPortfolioQuotes() {
    const portfolioTickers = state.portfolio.map(h => h.ticker);
    if (portfolioTickers.length === 0) { state.activePortfolioPolling = null; return; }
    let quotesUpdated = false;
    try {
        const quoteResults = await Promise.all(portfolioTickers.map(ticker => api.getLiveQuote(ticker)));
        const newQuotes = { ...state.liveQuotes };
        quoteResults.forEach((quoteData, index) => {
            const ticker = portfolioTickers[index];
            if (quoteData) { newQuotes[ticker] = quoteData; quotesUpdated = true; }
        });
        state.liveQuotes = newQuotes; 
        if (quotesUpdated && document.getElementById('portfolio-view')) {
            ui.mainContent.innerHTML = renderers.portfolio(state.portfolio, state.liveQuotes);
            lucide.createIcons();
        }
        checkPortfolioProfitAlerts();
    } catch (e) {} finally {
        state.activePortfolioPolling = setTimeout(pollPortfolioQuotes, PORTFOLIO_QUOTE_POLL_INTERVAL);
    }
}

function startPortfolioPolling() {
    stopPortfolioPolling();
    if (state.portfolio.length > 0) pollPortfolioQuotes();
}
function stopPortfolioPolling() {
    if (state.activePortfolioPolling) { clearTimeout(state.activePortfolioPolling); state.activePortfolioPolling = null; }
}

export async function showPortfolio() {
    stopAllPolling();
    setActiveSidebar(ui.portfolioLink);
    ui.mainContent.innerHTML = renderers.loading("Ładowanie portfela...");
    try {
        const holdings = await api.getPortfolio();
        state.portfolio = holdings;
        state.liveQuotes = {}; 
        state.profitAlertsSent = {}; 
        ui.mainContent.innerHTML = renderers.portfolio(state.portfolio, state.liveQuotes);
        lucide.createIcons();
        startPortfolioPolling();
    } catch (e) {
         ui.mainContent.innerHTML = `<div class="bg-red-900/20 border border-red-500/30 text-red-300 p-6 rounded-lg text-center">Błąd ładowania portfela: ${e.message}</div>`;
    }
}

export async function showTransactions() {
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

export async function loadAgentReportPage(page) {
    state.currentReportPage = page;
    ui.mainContent.innerHTML = renderers.loading(`Ładowanie raportu... (Strona ${page})`);
    try {
        const report = await api.getVirtualAgentReport(page);
        ui.mainContent.innerHTML = renderers.agentReport(report);
        lucide.createIcons();
    } catch (e) {
         ui.mainContent.innerHTML = `<div class="bg-red-900/20 border border-red-500/30 text-red-300 p-6 rounded-lg text-center">Błąd ładowania raportu agenta: ${e.message}</div>`;
    }
}

export async function showAgentReport() {
    stopAllPolling();
    setActiveSidebar(ui.agentReportLink);
    await loadAgentReportPage(1);
}

export function showBuyModal(ticker) {
    ui.buyModal.tickerSpan.textContent = ticker;
    ui.buyModal.quantityInput.value = '';
    ui.buyModal.priceInput.value = '';
    ui.buyModal.confirmBtn.dataset.ticker = ticker;
    ui.buyModal.backdrop.classList.remove('hidden');
    ui.buyModal.quantityInput.focus();
}
export function hideBuyModal() { ui.buyModal.backdrop.classList.add('hidden'); }

export async function handleBuyConfirm() {
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

export function showSellModal(ticker, maxQuantity) {
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
export function hideSellModal() { ui.sellModal.backdrop.classList.add('hidden'); }

export async function handleSellConfirm() {
     const ticker = ui.sellModal.confirmBtn.dataset.ticker;
     const maxQuantity = parseInt(ui.sellModal.confirmBtn.dataset.maxQuantity, 10);
     const quantity = parseInt(ui.sellModal.quantityInput.value, 10);
     const price = parseFloat(ui.sellModal.priceInput.value);
     if (!ticker || isNaN(quantity) || quantity <= 0 || isNaN(price) || price <= 0) { displaySystemAlert("BŁĄD: Proszę wprowadzić poprawną ilość i cenę."); return; }
     if (quantity > maxQuantity) { displaySystemAlert(`BŁĄD: Nie możesz sprzedać więcej akcji niż posiadasz (${maxQuantity}).`); return; }
     ui.sellModal.confirmBtn.disabled = true; ui.sellModal.confirmBtn.textContent = "Przetwarzanie...";
     try {
         await api.sellStock({ ticker, quantity, price_per_share: price });
         hideSellModal();
         await showPortfolio();
     } catch (e) { displaySystemAlert(`Błąd sprzedaży: ${e.message}`);
     } finally { ui.sellModal.confirmBtn.disabled = false; ui.sellModal.confirmBtn.textContent = "Realizuj"; }
}

export function showAIReportModal() {
    if (ui.aiReportModal.backdrop) {
        ui.aiReportModal.backdrop.classList.remove('hidden');
        ui.aiReportModal.content.innerHTML = renderers.loading('Pobieranie raportu...');
        lucide.createIcons();
    }
}
export function hideAIReportModal() {
    if (ui.aiReportModal.backdrop) {
        ui.aiReportModal.backdrop.classList.add('hidden');
        ui.aiReportModal.content.innerHTML = ''; 
    }
}

export async function pollAIOptimizerReport() {
    if (state.activeAIOptimizerPolling) clearTimeout(state.activeAIOptimizerPolling);
    const statusMsg = document.getElementById('ai-optimizer-status-message');
    try {
        const reportData = await api.getAIOptimizerReport();
        if (reportData.status === 'PROCESSING') {
            if(statusMsg) statusMsg.textContent = 'Worker przetwarza dane...';
            state.activeAIOptimizerPolling = setTimeout(pollAIOptimizerReport, AI_OPTIMIZER_POLL_INTERVAL);
        } else if (reportData.status === 'DONE') {
            if(statusMsg) { statusMsg.className = 'text-sm mt-3 text-green-400'; statusMsg.textContent = 'Analiza zakończona.'; }
            const runBtn = document.getElementById('run-ai-optimizer-btn');
            const viewBtn = document.getElementById('view-ai-report-btn');
            if (runBtn) { runBtn.disabled = false; runBtn.innerHTML = `<i data-lucide="brain-circuit" class="w-4 h-4 mr-2"></i> Uruchom Analizę AI`; lucide.createIcons(); }
            if (viewBtn) viewBtn.disabled = false;
            showAIReportModal();
            if (ui.aiReportModal.content) ui.aiReportModal.content.innerHTML = `<pre class="text-xs whitespace-pre-wrap font-mono">${reportData.report_text}</pre>`;
        }
    } catch (e) {}
}

export async function handleRunAIOptimizer() {
    const runBtn = document.getElementById('run-ai-optimizer-btn');
    const viewBtn = document.getElementById('view-ai-report-btn');
    const statusMsg = document.getElementById('ai-optimizer-status-message');
    if (!runBtn || !viewBtn || !statusMsg) return;
    runBtn.disabled = true; viewBtn.disabled = true;
    runBtn.innerHTML = `<i data-lucide="loader-2" class="w-4 h-4 mr-2 animate-spin"></i> Zlecanie...`; lucide.createIcons();
    statusMsg.className = 'text-sm mt-3 text-sky-400'; statusMsg.textContent = 'Zlecanie analizy...';
    try {
        await api.requestAIOptimizer();
        statusMsg.className = 'text-sm mt-3 text-green-400'; statusMsg.textContent = 'Zlecono analizę.';
        pollAIOptimizerReport();
    } catch (e) {
        statusMsg.className = 'text-sm mt-3 text-red-400'; statusMsg.textContent = `Błąd: ${e.message}`;
        runBtn.disabled = false; viewBtn.disabled = false;
        runBtn.innerHTML = `<i data-lucide="brain-circuit" class="w-4 h-4 mr-2"></i> Uruchom Analizę AI`; lucide.createIcons();
    }
}

export async function handleViewAIOptimizerReport() {
    showAIReportModal();
    try {
        const reportData = await api.getAIOptimizerReport();
        if (reportData.status === 'DONE' && reportData.report_text) ui.aiReportModal.content.innerHTML = `<pre class="text-xs whitespace-pre-wrap font-mono">${reportData.report_text}</pre>`;
        else if (reportData.status === 'PROCESSING') { ui.aiReportModal.content.innerHTML = renderers.loading('Analiza w toku...'); lucide.createIcons(); }
        else ui.aiReportModal.content.innerHTML = `<p class="text-gray-400">Brak raportu.</p>`;
    } catch (e) { ui.aiReportModal.content.innerHTML = `<p class="text-red-400">Błąd: ${e.message}</p>`; }
}

export async function handleYearBacktestRequest() {
    const yearInput = document.getElementById('backtest-year-input');
    const yearBtn = document.getElementById('run-backtest-year-btn');
    const statusMsg = document.getElementById('backtest-status-message');
    
    const paramPercentile = document.getElementById('h3-param-percentile');
    const paramMass = document.getElementById('h3-param-mass');
    const paramTp = document.getElementById('h3-param-tp');
    const paramSl = document.getElementById('h3-param-sl');
    const paramHold = document.getElementById('h3-param-hold');
    const paramName = document.getElementById('h3-param-name');
    const paramMinScore = document.getElementById('h3-param-min-score'); // <-- NOWE POLE

    if (!yearInput || !yearBtn || !statusMsg) return;
    const year = yearInput.value.trim();
    if (!year || year.length !== 4) { statusMsg.textContent = 'Błędny rok.'; return; }

    const h3Params = {};
    if (paramPercentile && paramPercentile.value) h3Params.h3_percentile = parseFloat(paramPercentile.value);
    if (paramMass && paramMass.value) h3Params.h3_m_sq_threshold = parseFloat(paramMass.value);
    if (paramTp && paramTp.value) h3Params.h3_tp_multiplier = parseFloat(paramTp.value);
    if (paramSl && paramSl.value) h3Params.h3_sl_multiplier = parseFloat(paramSl.value);
    if (paramHold && paramHold.value) h3Params.h3_max_hold = parseInt(paramHold.value);
    if (paramName && paramName.value) h3Params.setup_name = paramName.value.trim();
    if (paramMinScore && paramMinScore.value) h3Params.h3_min_score = parseFloat(paramMinScore.value); // <-- OBSŁUGA MIN SCORE

    yearBtn.disabled = true; yearBtn.innerHTML = `<i data-lucide="loader-2" class="w-4 h-4 mr-2 animate-spin"></i> Zlecanie...`; lucide.createIcons();
    statusMsg.className = 'text-sm mt-3 text-sky-400'; statusMsg.textContent = `Zlecanie testu dla ${year}...`;

    try {
        const response = await api.requestBacktest(year, Object.keys(h3Params).length ? h3Params : null);
        statusMsg.className = 'text-sm mt-3 text-green-400'; statusMsg.textContent = response.message;
        setTimeout(() => pollWorkerStatus(), 2000);
    } catch (e) {
        statusMsg.className = 'text-sm mt-3 text-red-400'; statusMsg.textContent = `Błąd: ${e.message}`;
    } finally {
        yearBtn.disabled = false; yearBtn.innerHTML = `<i data-lucide="play" class="w-4 h-4 mr-2"></i> Uruchom Test`; lucide.createIcons();
    }
}

export async function handleCsvExport() {
    const exportBtn = document.getElementById('run-csv-export-btn');
    const statusMsg = document.getElementById('csv-export-status-message');
    if (!exportBtn || !statusMsg) return;

    exportBtn.disabled = true; exportBtn.innerHTML = `<i data-lucide="loader-2" class="w-4 h-4 mr-2 animate-spin"></i> Pobieranie...`; lucide.createIcons();
    statusMsg.className = 'text-sm mt-3 text-sky-400'; statusMsg.textContent = 'Pobieranie...';

    try {
        const response = await fetch(api.getExportCsvUrl());
        if (!response.ok) throw new Error(response.statusText);
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.setAttribute('download', 'apex_export.csv');
        document.body.appendChild(link);
        link.click();
        link.parentNode.removeChild(link);
        statusMsg.className = 'text-sm mt-3 text-green-400'; statusMsg.textContent = 'Gotowe.';
    } catch (e) {
        statusMsg.className = 'text-sm mt-3 text-red-400'; statusMsg.textContent = `Błąd: ${e.message}`;
    } finally {
        exportBtn.disabled = false; exportBtn.innerHTML = `<i data-lucide="download-cloud" class="w-4 h-4 mr-2"></i> Eksportuj do CSV`; lucide.createIcons();
    }
}

export function showH3DeepDiveModal() {
    if (ui.h3DeepDiveModal.backdrop) {
        ui.h3DeepDiveModal.backdrop.classList.remove('hidden');
        ui.h3DeepDiveModal.yearInput.value = '';
        ui.h3DeepDiveModal.statusMsg.textContent = '';
        ui.h3DeepDiveModal.runBtn.disabled = false;
        ui.h3DeepDiveModal.runBtn.innerHTML = `<i data-lucide="search-check" class="w-4 h-4 mr-2"></i> Analizuj Rok`;
        lucide.createIcons();
        handleViewH3DeepDiveReport();
    }
}
export function hideH3DeepDiveModal() {
    if (ui.h3DeepDiveModal.backdrop) {
        ui.h3DeepDiveModal.backdrop.classList.add('hidden');
        ui.h3DeepDiveModal.content.innerHTML = '';
    }
}

export async function pollH3DeepDiveReport() {
    if (state.activeH3DeepDivePolling) clearTimeout(state.activeH3DeepDivePolling);
    if (!ui.h3DeepDiveModal.backdrop || ui.h3DeepDiveModal.backdrop.classList.contains('hidden')) return;
    
    try {
        const reportData = await api.getH3DeepDiveReport();
        if (reportData.status === 'PROCESSING') {
            ui.h3DeepDiveModal.statusMsg.textContent = 'Przetwarzanie...';
            state.activeH3DeepDivePolling = setTimeout(pollH3DeepDiveReport, H3_DEEP_DIVE_POLL_INTERVAL);
        } else if (reportData.status === 'DONE') {
            ui.h3DeepDiveModal.statusMsg.textContent = 'Zakończono.';
            ui.h3DeepDiveModal.runBtn.disabled = false;
            ui.h3DeepDiveModal.content.innerHTML = `<pre class="text-xs whitespace-pre-wrap font-mono">${reportData.report_text}</pre>`;
        }
    } catch(e) {}
}

export async function handleRunH3DeepDive() {
    const yearInput = ui.h3DeepDiveModal.yearInput;
    const runBtn = ui.h3DeepDiveModal.runBtn;
    const statusMsg = ui.h3DeepDiveModal.statusMsg;
    if (!yearInput || !runBtn || !statusMsg) return;

    const year = parseInt(yearInput.value.trim(), 10);
    if (isNaN(year)) { statusMsg.textContent = 'Błędny rok.'; return; }

    runBtn.disabled = true;
    runBtn.innerHTML = `<i data-lucide="loader-2" class="w-4 h-4 mr-2 animate-spin"></i> Zlecanie...`; lucide.createIcons();
    statusMsg.textContent = `Zlecanie dla ${year}...`;

    try {
        await api.requestH3DeepDive(year);
        pollH3DeepDiveReport();
    } catch (e) {
        statusMsg.textContent = `Błąd: ${e.message}`;
        runBtn.disabled = false;
    }
}

export async function handleViewH3DeepDiveReport() {
    ui.h3DeepDiveModal.content.innerHTML = renderers.loading('Pobieranie...'); lucide.createIcons();
    try {
        const reportData = await api.getH3DeepDiveReport();
        if (reportData.status === 'DONE') ui.h3DeepDiveModal.content.innerHTML = `<pre class="text-xs whitespace-pre-wrap font-mono">${reportData.report_text}</pre>`;
        else if (reportData.status === 'PROCESSING') pollH3DeepDiveReport();
        else ui.h3DeepDiveModal.content.innerHTML = '<p class="text-gray-500">Brak raportu.</p>';
    } catch (e) { ui.h3DeepDiveModal.content.innerHTML = `<p class="text-red-400">${e.message}</p>`; }
}

// === NOWA LOGIKA H3 LIVE ===

export function showH3LiveParamsModal() {
    if (ui.h3LiveModal.backdrop) {
        ui.h3LiveModal.backdrop.classList.remove('hidden');
        ui.h3LiveModal.percentile.value = "0.95";
        ui.h3LiveModal.mass.value = "-0.5";
        ui.h3LiveModal.minScore.value = "0.0"; // <-- USTAW WARTOŚĆ 0.0
        ui.h3LiveModal.tp.value = "5.0";
        ui.h3LiveModal.sl.value = "2.0";
    }
}

export function hideH3LiveParamsModal() {
    if (ui.h3LiveModal.backdrop) {
        ui.h3LiveModal.backdrop.classList.add('hidden');
    }
}

export async function handleRunH3LiveScan() {
    const params = {
        h3_percentile: parseFloat(ui.h3LiveModal.percentile.value) || 0.95,
        h3_m_sq_threshold: parseFloat(ui.h3LiveModal.mass.value) || -0.5,
        h3_min_score: parseFloat(ui.h3LiveModal.minScore.value) || 0.0, // <-- PRZEKAŻ WARTOŚĆ
        h3_tp_multiplier: parseFloat(ui.h3LiveModal.tp.value) || 5.0,
        h3_sl_multiplier: parseFloat(ui.h3LiveModal.sl.value) || 2.0,
        setup_name: 'AQM_H3_LIVE' 
    };

    hideH3LiveParamsModal();
    
    try {
        await api.sendWorkerControl('start_phase3', params);
        logger.info("Wysłano komendę Start Fazy 3 z parametrami:", params);
    } catch(e) {
        logger.error("Błąd uruchamiania Fazy 3:", e);
        displaySystemAlert("Błąd uruchamiania: " + e.message);
    }
}

}

{
type: uploaded file
fileName: mcdoradca/apex/Apex-29b5b053cb2f273eaada70b8027fbcdb343a4c1c/worker/src/analysis/aqm_v3_h3_simulator.py
fullContent:
import logging
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from typing import Dict, Any

# Importujemy modele i funkcje pomocnicze
from .. import models
# ZMIANA: Importujemy _resolve_trade z utils.py, nie z H1 simulator
from .utils import calculate_atr, _resolve_trade

logger = logging.getLogger(__name__)

def _simulate_trades_h3(
    session: Session, 
    ticker: str, 
    historical_data: Dict[str, pd.DataFrame], 
    year: str,
    parameters: Dict[str, Any] = None
) -> int:
    """
    Symulator Hipotezy H3 (Simplified Field Model).
    W pełni niezależny od innych symulatorów.
    """
    trades_found = 0
    daily_df = historical_data.get("daily")

    if daily_df is None or daily_df.empty:
        return 0

    # === KONFIGURACJA PARAMETRÓW ===
    params = parameters or {}
    DEFAULT_PERCENTILE = 0.95
    DEFAULT_M_SQ_THRESHOLD = -0.5
    DEFAULT_TP_MULT = 5.0
    DEFAULT_SL_MULT = 2.0
    DEFAULT_MAX_HOLD = 5
    DEFAULT_SETUP_NAME = 'AQM_V3_H3_DYNAMIC'
    DEFAULT_MIN_SCORE = 0.0 # <-- NOWY DOMYŚLNY PARAMETR

    try:
        param_percentile = float(params.get('h3_percentile')) if params.get('h3_percentile') is not None else DEFAULT_PERCENTILE
        param_m_sq_threshold = float(params.get('h3_m_sq_threshold')) if params.get('h3_m_sq_threshold') is not None else DEFAULT_M_SQ_THRESHOLD
        param_tp_mult = float(params.get('h3_tp_multiplier')) if params.get('h3_tp_multiplier') is not None else DEFAULT_TP_MULT
        param_sl_mult = float(params.get('h3_sl_multiplier')) if params.get('h3_sl_multiplier') is not None else DEFAULT_SL_MULT
        param_max_hold = int(params.get('h3_max_hold')) if params.get('h3_max_hold') is not None else DEFAULT_MAX_HOLD
        param_name = str(params.get('setup_name')) if params.get('setup_name') and str(params.get('setup_name')).strip() else DEFAULT_SETUP_NAME
        setup_name_suffix = param_name
        
        # === NOWY PARAMETR: MIN AQM SCORE ===
        param_min_score = float(params.get('h3_min_score')) if params.get('h3_min_score') is not None else DEFAULT_MIN_SCORE

    except (ValueError, TypeError) as e:
        logger.error(f"Błąd parsowania parametrów H3 dla {ticker}: {e}. Używam domyślnych.")
        param_percentile = DEFAULT_PERCENTILE
        param_m_sq_threshold = DEFAULT_M_SQ_THRESHOLD
        param_tp_mult = DEFAULT_TP_MULT
        param_sl_mult = DEFAULT_SL_MULT
        param_max_hold = DEFAULT_MAX_HOLD
        param_min_score = DEFAULT_MIN_SCORE
        setup_name_suffix = 'AQM_V3_H3_PARSING_ERROR'

    history_buffer = 201 
    percentile_window = 100 
    
    if len(daily_df) < history_buffer + 1:
        return 0

    # === OBLICZENIA METRYK H3 ===
    
    j_mean = daily_df['J'].rolling(window=percentile_window).mean()
    j_norm = (daily_df['J'] - j_mean) / daily_df['J'].rolling(window=percentile_window).std(ddof=1)
    
    nabla_mean = daily_df['nabla_sq'].rolling(window=percentile_window).mean()
    nabla_norm = (daily_df['nabla_sq'] - nabla_mean) / daily_df['nabla_sq'].rolling(window=percentile_window).std(ddof=1)
    
    m_mean = daily_df['m_sq'].rolling(window=percentile_window).mean()
    m_norm = (daily_df['m_sq'] - m_mean) / daily_df['m_sq'].rolling(window=percentile_window).std(ddof=1)

    j_norm = j_norm.replace([np.inf, -np.inf], np.nan).fillna(0)
    nabla_norm = nabla_norm.replace([np.inf, -np.inf], np.nan).fillna(0)
    m_norm = m_norm.replace([np.inf, -np.inf], np.nan).fillna(0)

    # Główna Formuła Pola (AQM V3 Score)
    aqm_score_series = (1.0 * j_norm) - (1.0 * nabla_norm) - (1.0 * m_norm)
    
    # Dynamiczny próg (percentyl)
    percentile_threshold_series = aqm_score_series.rolling(window=percentile_window).quantile(param_percentile)

    # === PĘTLA SYMULACYJNA ===
    for i in range(history_buffer, len(daily_df) - 1): 
        candle_D = daily_df.iloc[i] 

        current_aqm_score = aqm_score_series.iloc[i]
        current_threshold = percentile_threshold_series.iloc[i]
        current_m_norm = m_norm.iloc[i]

        if pd.isna(current_aqm_score) or pd.isna(current_threshold):
            continue
        
        # ============================================================
        # === WARUNEK WEJŚCIA H3 + HARD FLOOR (DYNAMICZNY) ===
        # ============================================================
        # Zmieniono sztywne > 0 na dynamiczny parametr param_min_score
        
        if (current_aqm_score > current_threshold) and \
           (current_m_norm < param_m_sq_threshold) and \
           (current_aqm_score > param_min_score):  # <--- UŻYCIE PARAMETRU
            
            try:
                candle_D_plus_1 = daily_df.iloc[i + 1]
                entry_price = candle_D_plus_1['open']
                atr_value = candle_D['atr_14']
                
                if pd.isna(entry_price) or pd.isna(atr_value) or atr_value == 0:
                    continue
                
                take_profit = entry_price + (param_tp_mult * atr_value)
                stop_loss = entry_price - (param_sl_mult * atr_value)
                
                setup_h3 = {
                    "ticker": ticker,
                    "setup_type": setup_name_suffix, 
                    "entry_price": float(entry_price),
                    "stop_loss": float(stop_loss),
                    "take_profit": float(take_profit),
                    
                    # Logowanie Metryk H3
                    "metric_atr_14": float(atr_value),
                    "metric_aqm_score_h3": float(current_aqm_score),
                    "metric_aqm_percentile_95": float(current_threshold), 
                    "metric_J_norm": float(j_norm.iloc[i]),
                    "metric_nabla_sq_norm": float(nabla_norm.iloc[i]),
                    "metric_m_sq_norm": float(current_m_norm),
                    
                    # Logowanie Komponentów Składowych
                    "metric_J": float(candle_D['J']),
                    "metric_inst_sync": float(candle_D['institutional_sync']),
                    "metric_retail_herding": float(candle_D['retail_herding']),
                    "metric_time_dilation": float(candle_D['time_dilation']),
                    "metric_price_gravity": float(candle_D['price_gravity']),
                }

                trade = _resolve_trade(
                    daily_df, 
                    i + 1, 
                    setup_h3, 
                    param_max_hold, 
                    year, 
                    direction='LONG'
                )
                if trade:
                    session.add(trade)
                    trades_found += 1
                    
            except IndexError:
                continue
            except Exception as e:
                logger.error(f"[Backtest H3] Error (Day {daily_df.index[i].date()}): {e}", exc_info=True)
                session.rollback()

    if trades_found > 0:
        try:
            session.commit()
            logger.debug(f"[Backtest H3] Saved {trades_found} trades for {ticker}.")
        except Exception as e:
            logger.error(f"Error committing H3 trades: {e}")
            session.rollback()
        
    return trades_found

}

{
type: uploaded file
fileName: mcdoradca/apex/Apex-063c7fbd60bb8995ec2cdbe5297dc953d459ca7b/worker/src/analysis/phase3_sniper.py
fullContent:
import logging
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import text
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any

from ..data_ingestion.alpha_vantage_client import AlphaVantageClient
from .. import models

from .utils import (
    get_raw_data_with_cache,
    standardize_df_columns,
    calculate_atr,
    append_scan_log,
    update_scan_progress,
    send_telegram_alert
)
from . import aqm_v3_metrics
from . import aqm_v3_h2_loader
from .aqm_v3_h3_loader import _parse_bbands

logger = logging.getLogger(__name__)

# ============================================================================
# === STAŁE KONFIGURACYJNE H3 (LIVE - Defaults) ===
# ============================================================================
DEFAULT_PERCENTILE = 0.95
DEFAULT_M_SQ_THRESHOLD = -0.5
DEFAULT_MIN_SCORE = 0.0 # <-- NOWY DOMYŚLNY
DEFAULT_TP_MULT = 5.0
DEFAULT_SL_MULT = 2.0
H3_WINDOW = 100 
H3_HISTORY_BUFFER = 201 

def _pre_calculate_metrics_live(daily_df: pd.DataFrame, insider_df: pd.DataFrame, news_df: pd.DataFrame) -> pd.DataFrame:
    df = daily_df.copy()
    if insider_df.index.tz is not None: insider_df = insider_df.tz_convert(None)
    if news_df.index.tz is not None: news_df = news_df.tz_convert(None)
    try: df['institutional_sync'] = df.apply(lambda row: aqm_v3_metrics.calculate_institutional_sync_from_data(insider_df, row.name), axis=1)
    except: df['institutional_sync'] = 0.0
    try: df['retail_herding'] = df.apply(lambda row: aqm_v3_metrics.calculate_retail_herding_from_data(news_df, row.name), axis=1)
    except: df['retail_herding'] = 0.0
    df['daily_returns'] = df['close'].pct_change()
    df['market_temperature'] = df['daily_returns'].rolling(window=30).std()
    df['nabla_sq'] = df.get('price_gravity', 0.0) 
    df['avg_volume_10d'] = df['volume'].rolling(window=10).mean()
    df['vol_mean_200d'] = df['avg_volume_10d'].rolling(window=200).mean()
    df['vol_std_200d'] = df['avg_volume_10d'].rolling(window=200).std()
    df['normalized_volume'] = ((df['avg_volume_10d'] - df['vol_mean_200d']) / df['vol_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)
    if not news_df.empty:
        news_counts_daily = news_df.groupby(news_df.index.date).size()
        news_counts_daily.index = pd.to_datetime(news_counts_daily.index)
        news_counts_daily = news_counts_daily.reindex(df.index, fill_value=0)
        df['information_entropy'] = news_counts_daily.rolling(window=10).sum()
        df['news_mean_200d'] = df['information_entropy'].rolling(window=200).mean()
        df['news_std_200d'] = df['information_entropy'].rolling(window=200).std()
        df['normalized_news'] = ((df['information_entropy'] - df['news_mean_200d']) / df['news_std_200d']).replace([np.inf, -np.inf], 0).fillna(0)
    else:
        df['information_entropy'] = 0.0
        df['normalized_news'] = 0.0
    df['m_sq'] = df['normalized_volume'] + df['normalized_news']
    S = df['information_entropy']
    Q = df['retail_herding']
    T = df['market_temperature']
    mu = df['institutional_sync']
    J = S - (Q / T.replace(0, np.nan)) + (mu * 1.0)
    df['J'] = J.fillna(S + (mu * 1.0))
    return df

def run_h3_live_scan(session: Session, candidates: List[str], api_client: AlphaVantageClient, parameters: Dict[str, Any] = None):
    logger.info("Uruchamianie Fazy 3: H3 LIVE ENGINE...")
    append_scan_log(session, "Faza 3 (H3): Rozpoczynanie analizy kwantowej kandydatów...")
    
    # Konfiguracja parametrów z obsługą domyślnych
    params = parameters or {}
    
    # Pobieramy parametry, używając stałych DEFAULT jako fallback
    h3_percentile = float(params.get('h3_percentile', DEFAULT_PERCENTILE))
    h3_m_sq_threshold = float(params.get('h3_m_sq_threshold', DEFAULT_M_SQ_THRESHOLD))
    h3_min_score = float(params.get('h3_min_score', DEFAULT_MIN_SCORE)) # <-- NOWY PARAMETR
    h3_tp_mult = float(params.get('h3_tp_multiplier', DEFAULT_TP_MULT))
    h3_sl_mult = float(params.get('h3_sl_multiplier', DEFAULT_SL_MULT))
    
    signals_generated = 0
    total_candidates = len(candidates)

    for i, ticker in enumerate(candidates):
        if i % 10 == 0: update_scan_progress(session, i, total_candidates)
        try:
            daily_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_OHLCV', 'get_time_series_daily', expiry_hours=12, outputsize='full')
            if not daily_raw: continue
            daily_adj_raw = get_raw_data_with_cache(session, api_client, ticker, 'DAILY_ADJUSTED', 'get_daily_adjusted', expiry_hours=12, outputsize='full')
            if not daily_adj_raw: continue
            get_raw_data_with_cache(session, api_client, ticker, 'BBANDS', 'get_bollinger_bands', expiry_hours=24, interval='daily', time_period=20)
            h2_data = aqm_v3_h2_loader.load_h2_data_into_cache(ticker, api_client, session)
            
            daily_ohlcv = standardize_df_columns(pd.DataFrame.from_dict(daily_raw.get('Time Series (Daily)', {}), orient='index'))
            daily_ohlcv.index = pd.to_datetime(daily_ohlcv.index)
            daily_adj = standardize_df_columns(pd.DataFrame.from_dict(daily_adj_raw.get('Time Series (Daily)', {}), orient='index'))
            daily_adj.index = pd.to_datetime(daily_adj.index)
            
            if len(daily_adj) < H3_HISTORY_BUFFER + 1: continue

            daily_ohlcv['vwap_proxy'] = (daily_ohlcv['high'] + daily_ohlcv['low'] + daily_ohlcv['close']) / 3.0
            df = daily_adj.join(daily_ohlcv[['open', 'high', 'low', 'vwap_proxy']], rsuffix='_ohlcv')
            close_col = 'close_ohlcv' if 'close_ohlcv' in df.columns else 'close'
            df['price_gravity'] = (df['vwap_proxy'] - df[close_col]) / df[close_col]
            df['atr_14'] = calculate_atr(df, period=14).ffill().fillna(0)
            df['time_dilation'] = df['close'].pct_change().rolling(20).std()
            df.replace([np.inf, -np.inf], np.nan, inplace=True)
            df.fillna(0, inplace=True)

            df = _pre_calculate_metrics_live(df, h2_data['insider_df'], h2_data['news_df'])
            
            j_mean = df['J'].rolling(window=H3_WINDOW).mean()
            j_std = df['J'].rolling(window=H3_WINDOW).std(ddof=1)
            j_norm = ((df['J'] - j_mean) / j_std).fillna(0)
            nabla_mean = df['nabla_sq'].rolling(window=H3_WINDOW).mean()
            nabla_std = df['nabla_sq'].rolling(window=H3_WINDOW).std(ddof=1)
            nabla_norm = ((df['nabla_sq'] - nabla_mean) / nabla_std).fillna(0)
            m_mean = df['m_sq'].rolling(window=H3_WINDOW).mean()
            m_std = df['m_sq'].rolling(window=H3_WINDOW).std(ddof=1)
            m_norm = ((df['m_sq'] - m_mean) / m_std).fillna(0)
            
            aqm_score_series = (1.0 * j_norm) - (1.0 * nabla_norm) - (1.0 * m_norm)
            threshold_series = aqm_score_series.rolling(window=H3_WINDOW).quantile(h3_percentile)

            last_idx = -1
            current_aqm = aqm_score_series.iloc[last_idx]
            current_thresh = threshold_series.iloc[last_idx]
            current_m = m_norm.iloc[last_idx]
            
            # ============================================================
            # === WARUNEK WEJŚCIA H3 + HARD FLOOR (DYNAMICZNY) ===
            # ============================================================
            if (current_aqm > current_thresh) and \
               (current_m < h3_m_sq_threshold) and \
               (current_aqm > h3_min_score):  # <--- UŻYCIE PARAMETRU
               
                atr = df['atr_14'].iloc[last_idx]
                ref_price = df['close'].iloc[last_idx] 
                take_profit = ref_price + (h3_tp_mult * atr)
                stop_loss = ref_price - (h3_sl_mult * atr)
                
                logger.info(f"H3 SIGNAL FOUND: {ticker} (AQM: {current_aqm:.2f} > {current_thresh:.2f}).")
                existing = session.query(models.TradingSignal).filter(
                    models.TradingSignal.ticker == ticker,
                    models.TradingSignal.status.in_(['ACTIVE', 'PENDING']),
                    models.TradingSignal.generation_date >= datetime.now(timezone.utc) - timedelta(hours=20)
                ).first()
                
                if not existing:
                    new_signal = models.TradingSignal(
                        ticker=ticker, status='PENDING', generation_date=datetime.now(timezone.utc),
                        entry_price=float(ref_price), stop_loss=float(stop_loss), take_profit=float(take_profit),
                        risk_reward_ratio=float(h3_tp_mult/h3_sl_mult),
                        entry_zone_top=float(ref_price + (0.5 * atr)), entry_zone_bottom=float(ref_price - (0.5 * atr)),
                        notes=f"AQM H3 Live Setup. Score: {current_aqm:.2f}. J:{df['J'].iloc[-1]:.2f}, N:{df['nabla_sq'].iloc[-1]:.2f}, M:{df['m_sq'].iloc[-1]:.2f}"
                    )
                    session.add(new_signal)
                    session.commit()
                    signals_generated += 1
                    send_telegram_alert(f"⚛️ H3 QUANTUM SIGNAL: {ticker}\nAQM Score: {current_aqm:.2f}\nTP: {take_profit:.2f} | SL: {stop_loss:.2f}")

        except Exception as e:
            logger.error(f"Błąd H3 Live dla {ticker}: {e}", exc_info=True)
            continue

    append_scan_log(session, f"Faza 3 (H3 Live) zakończona. Wygenerowano {signals_generated} sygnałów.")
    logger.info(f"Faza 3 zakończona. Sygnałów: {signals_generated}")

}
