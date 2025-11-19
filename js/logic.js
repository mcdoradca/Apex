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

export function setActiveSidebar(linkElement) {
    document.querySelectorAll('.sidebar-item').forEach(el => el.classList.remove('sidebar-item-active'));
    if (linkElement) linkElement.classList.add('sidebar-item-active');
}

export function stopAllPolling() {
    if (state.activePortfolioPolling) { clearTimeout(state.activePortfolioPolling); state.activePortfolioPolling = null; }
    if (state.activeAIOptimizerPolling) { clearTimeout(state.activeAIOptimizerPolling); state.activeAIOptimizerPolling = null; }
    if (state.activeH3DeepDivePolling) { clearTimeout(state.activeH3DeepDivePolling); state.activeH3DeepDivePolling = null; }
}

export function updateDashboardUI(statusData) {
    if (!document.getElementById('dashboard-view')) return;
    const elStatus = document.getElementById('dashboard-worker-status');
    const elPhase = document.getElementById('dashboard-current-phase');
    const elProgText = document.getElementById('progress-text');
    const elProgBar = document.getElementById('progress-bar');
    const elLog = document.getElementById('scan-log');
    
    // Dashboard signals counter
    const elSignals = document.getElementById('dashboard-active-signals');
    if (elSignals) elSignals.textContent = state.phase3.length;
    
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

        updateDashboardUI(statusData);
    } catch (e) {}
    setTimeout(pollWorkerStatus, 5000);
}

export async function refreshSidebarData() {
    try {
        // ZMIANA: Przywrócono getPhase3Signals
        const [phase1, phase3] = await Promise.all([
            api.getPhase1Candidates(),
            api.getPhase3Signals()
        ]);
        state.phase1 = phase1 || [];
        state.phase3 = phase3 || []; // Zapisujemy sygnały H3

        if(ui.phase1.list) ui.phase1.list.innerHTML = renderers.phase1List(state.phase1);
        if(ui.phase1.count) ui.phase1.count.textContent = state.phase1.length;
        
        // Renderujemy listę H3
        if(ui.phase3.list) ui.phase3.list.innerHTML = renderers.phase3List(state.phase3);
        if(ui.phase3.count) ui.phase3.count.textContent = state.phase3.length;
        
        lucide.createIcons();
    } catch (e) {}
    setTimeout(refreshSidebarData, 15000);
}

export async function showDashboard() {
    stopAllPolling();
    setActiveSidebar(ui.dashboardLink);
    ui.mainContent.innerHTML = renderers.dashboard();
    updateDashboardUI(state.workerStatus);
    lucide.createIcons();
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

// Modal Handlers
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
        else ui.h3DeepDiveModal.content.innerHTML = '<p class="text-gray-400">Brak raportu.</p>';
    } catch (e) { ui.h3DeepDiveModal.content.innerHTML = `<p class="text-red-400">${e.message}</p>`; }
}
