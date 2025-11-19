import { state, logger, ALERT_POLL_INTERVAL, PROFIT_ALERT_THRESHOLD, PORTFOLIO_QUOTE_POLL_INTERVAL, AI_OPTIMIZER_POLL_INTERVAL, H3_DEEP_DIVE_POLL_INTERVAL, REPORT_PAGE_SIZE } from './state.js';
import { api } from './api.js';
import { renderers } from './ui.js';

let ui = null;

export const setUI = (uiObj) => { ui = uiObj; };

// ... (funkcje czasu: getNYTime, formatCountdown, getMarketCountdown, updateCountdownTimer, start/stopCountdown - BEZ ZMIAN) ...
// (Wklej je tutaj z poprzedniej wersji pliku)
export function getNYTime() { try { const options = { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }; const formatter = new Intl.DateTimeFormat('en-US', options); const parts = formatter.formatToParts(new Date()); const find = (type) => parts.find(p => p.type === type)?.value; return new Date(find('year'), parseInt(find('month')) - 1, find('day'), find('hour') === '24' ? '00' : find('hour'), find('minute'), find('second')); } catch (e) { return new Date(); } }
export function formatCountdown(ms) { if (ms < 0) ms = 0; const totalSeconds = Math.floor(ms / 1000); const totalMinutes = Math.floor(totalSeconds / 60); const totalHours = Math.floor(totalMinutes / 60); const days = Math.floor(totalHours / 24); const hours = totalHours % 24; const minutes = totalMinutes % 60; const seconds = totalSeconds % 60; let str = ''; if (days > 0) str += `${days}d `; str += `${String(hours).padStart(2, '0')}g ${String(minutes).padStart(2, '0')}m ${String(seconds).padStart(2, '0')}s`; return str; }
export function getMarketCountdown() { const now = getNYTime(); const dayOfWeek = now.getDay(); const isWeekend = (dayOfWeek === 0 || dayOfWeek === 6); const preMarketOpen = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 4, 0, 0); const marketOpen = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 9, 30, 0); const marketClose = new Date(now.getFullYear(), now.getMonth(), now.getDate(), 16, 0, 0); let message = '', targetTime = null; if (isWeekend) { let daysToAdd = (dayOfWeek === 6) ? 2 : 1; targetTime = new Date(preMarketOpen.getTime() + daysToAdd * 24 * 60 * 60 * 1000); message = 'Do otwarcia Pre-Market: '; } else { if (now < preMarketOpen) { targetTime = preMarketOpen; message = 'Do otwarcia Pre-Market: '; } else if (now >= preMarketOpen && now < marketOpen) { targetTime = marketOpen; message = 'Do otwarcia Rynku: '; } else if (now >= marketOpen && now < marketClose) { targetTime = marketClose; message = 'Do zamknięcia Rynku: '; } else { let daysToAdd = (dayOfWeek === 5) ? 3 : 1; targetTime = new Date(preMarketOpen.getTime() + daysToAdd * 24 * 60 * 60 * 1000); message = 'Do otwarcia Pre-Market: '; } } const diff = targetTime.getTime() - now.getTime(); return message + formatCountdown(diff); }
export function updateCountdownTimer() { const timerElement = document.getElementById('market-countdown-timer'); if (timerElement) { timerElement.textContent = getMarketCountdown(); } }
export function startMarketCountdown() { stopMarketCountdown(); updateCountdownTimer(); state.activeCountdownPolling = setInterval(updateCountdownTimer, 1000); }
export function stopMarketCountdown() { if (state.activeCountdownPolling) { clearInterval(state.activeCountdownPolling); state.activeCountdownPolling = null; } }

// ... (funkcje UI: setActiveSidebar, stopAllPolling, updateDashboardUI, updateDashboardCounters, displaySystemAlert, pollSystemAlerts, pollWorkerStatus, refreshSidebarData, showDashboard - BEZ ZMIAN) ...
// (Wklej je tutaj)
export function setActiveSidebar(linkElement) { document.querySelectorAll('.sidebar-item').forEach(el => el.classList.remove('sidebar-item-active')); if (linkElement) linkElement.classList.add('sidebar-item-active'); }
export function stopAllPolling() { if (state.activePortfolioPolling) clearTimeout(state.activePortfolioPolling); if (state.activeAIOptimizerPolling) clearTimeout(state.activeAIOptimizerPolling); if (state.activeH3DeepDivePolling) clearTimeout(state.activeH3DeepDivePolling); stopMarketCountdown(); }
export function updateDashboardUI(statusData) { if (!document.getElementById('dashboard-view')) return; const elStatus = document.getElementById('dashboard-worker-status'); const elPhase = document.getElementById('dashboard-current-phase'); const elProgText = document.getElementById('progress-text'); const elProgBar = document.getElementById('progress-bar'); const elLog = document.getElementById('scan-log'); const elSignals = document.getElementById('dashboard-active-signals'); if (elSignals) elSignals.textContent = (state.phase3 || []).length; if (!elStatus || !elLog) return; elStatus.textContent = statusData.status; elPhase.textContent = `Faza: ${statusData.phase || 'NONE'}`; const processed = statusData.progress.processed, total = statusData.progress.total; const percent = total > 0 ? Math.min((processed / total) * 100, 100) : 0; elProgText.textContent = `${processed} / ${total}`; elProgBar.style.width = `${percent.toFixed(0)}%`; if (elLog.textContent !== statusData.log) { elLog.textContent = statusData.log || 'Czekam na rozpoczęcie skanowania...'; const logContainer = document.getElementById('scan-log-container'); if(logContainer) logContainer.scrollTop = 0; } }
export function updateDashboardCounters() { const activeEl = document.getElementById('dashboard-active-signals'); if (activeEl) activeEl.textContent = (state.phase3 || []).length; }
export function displaySystemAlert(message) { if (!message || message === 'NONE') return; let alertKey = 'GENERAL'; try { const parts = message.split(' '); const ticker = parts.find(p => p.length > 2 && p.length < 6 && p === p.toUpperCase()); if (message.includes('ALERT ZYSKU')) alertKey = `PROFIT-${ticker || 'UNKNOWN'}`; else if (message.includes('ALARM CENOWY')) alertKey = `PRICE-${ticker || 'UNKNOWN'}`; else if (message.includes('PILNY ALERT')) alertKey = `NEWS-${ticker || 'UNKNOWN'}`; else if (message.includes('TAKE PROFIT')) alertKey = `TP-${ticker || 'UNKNOWN'}`; else if (message.includes('STOP LOSS')) alertKey = `SL-${ticker || 'UNKNOWN'}`; } catch(e) {} if (state.snoozedAlerts[alertKey] && Date.now() < state.snoozedAlerts[alertKey]) return; let alertClass = 'bg-sky-500'; let alertIcon = 'bell-ring'; if (message.includes('PILNY ALERT') && message.includes('NEGATYWNY')) { alertClass = 'bg-red-600'; alertIcon = 'alert-octagon'; } else if (message.includes('PILNY ALERT') && message.includes('POZYTYWNY')) { alertClass = 'bg-green-600'; alertIcon = 'check-circle'; } else if (message.includes('ALARM CENOWY') || message.includes('ALERT ZYSKU')) { alertClass = 'bg-yellow-500'; alertIcon = 'dollar-sign'; } else if (message.includes('TAKE PROFIT')) { alertClass = 'bg-green-600'; alertIcon = 'trending-up'; } else if (message.includes('STOP LOSS')) { alertClass = 'bg-red-600'; alertIcon = 'trending-down'; } const alertId = `alert-${Date.now()}`; const alertElement = document.createElement('div'); alertElement.id = alertId; alertElement.className = `alert-bar flex items-center justify-between gap-4 ${alertClass} text-white p-3 shadow-lg rounded-md animate-pulse-once`; alertElement.innerHTML = `<div class="flex items-center gap-3"><i data-lucide="${alertIcon}" class="w-6 h-6"></i><span class="font-semibold">${message}</span></div><button data-alert-id="${alertId}" data-alert-key="${alertKey}" class="close-alert-btn p-1 rounded-full hover:bg-black/20 transition-colors"><i data-lucide="x" class="w-5 h-5"></i></button>`; ui.alertContainer.appendChild(alertElement); lucide.createIcons(); const closeButton = alertElement.querySelector('.close-alert-btn'); closeButton.addEventListener('click', () => { const keyToSnooze = closeButton.dataset.alertKey; if (keyToSnooze) state.snoozedAlerts[keyToSnooze] = Date.now() + 30 * 60 * 1000; alertElement.remove(); }); setTimeout(() => { const el = document.getElementById(alertId); if (el) el.remove(); }, 20000); }
export async function pollSystemAlerts() { try { const alertData = await api.getSystemAlert(); if (alertData && alertData.message !== 'NONE') displaySystemAlert(alertData.message); } catch (e) {} finally { setTimeout(pollSystemAlerts, ALERT_POLL_INTERVAL); } }
export async function pollWorkerStatus() { try { const statusData = await api.getWorkerStatus(); state.workerStatus = statusData; let statusClass = 'bg-gray-700 text-gray-200'; if (statusData.status === 'RUNNING') statusClass = 'bg-green-600/20 text-green-400'; else if (statusData.status === 'PAUSED') statusClass = 'bg-yellow-600/20 text-yellow-400'; else if (statusData.status === 'ERROR') statusClass = 'bg-red-600/20 text-red-400'; if (statusData.phase === 'BACKTESTING') statusClass = 'bg-purple-600/20 text-purple-400'; else if (statusData.phase === 'AI_OPTIMIZING') statusClass = 'bg-pink-600/20 text-pink-400'; else if (statusData.phase === 'DEEP_DIVE_H3') statusClass = 'bg-cyan-600/20 text-cyan-400'; if(ui.workerStatusText) { ui.workerStatusText.className = `font-mono px-2 py-1 rounded-md text-xs ${statusClass} transition-colors`; ui.workerStatusText.textContent = statusData.phase === 'NONE' ? statusData.status : statusData.phase; } if(ui.heartbeatStatus && statusData.last_heartbeat_utc) { const diffSeconds = (new Date() - new Date(statusData.last_heartbeat_utc)) / 1000; ui.heartbeatStatus.className = `text-xs ${diffSeconds > 30 ? 'text-red-500' : 'text-green-500'}`; ui.heartbeatStatus.textContent = diffSeconds > 30 ? 'PRZERWANY' : new Date(statusData.last_heartbeat_utc).toLocaleTimeString(); } const isBusy = statusData.status !== 'IDLE' && statusData.status !== 'ERROR'; if (ui.btnPhase1) ui.btnPhase1.disabled = isBusy; if (ui.btnPhase3) ui.btnPhase3.disabled = isBusy; updateDashboardUI(statusData); } catch (e) {} setTimeout(pollWorkerStatus, 5000); }
export async function refreshSidebarData() { try { const [phase1, phase3] = await Promise.all([api.getPhase1Candidates(), api.getPhase3Signals()]); state.phase1 = phase1 || []; state.phase3 = phase3 || []; if(ui.phase1.list) ui.phase1.list.innerHTML = renderers.phase1List(state.phase1); if(ui.phase1.count) ui.phase1.count.textContent = state.phase1.length; if(ui.phase3.list) ui.phase3.list.innerHTML = renderers.phase3List(state.phase3); if(ui.phase3.count) ui.phase3.count.textContent = state.phase3.length; updateDashboardCounters(); lucide.createIcons(); } catch (e) {} setTimeout(refreshSidebarData, 15000); }
export async function showDashboard() { stopAllPolling(); setActiveSidebar(ui.dashboardLink); ui.mainContent.innerHTML = renderers.dashboard(); updateDashboardUI(state.workerStatus); updateDashboardCounters(); lucide.createIcons(); startMarketCountdown(); }

// ... (pozostałe funkcje bez zmian: checkPortfolioProfitAlerts, pollPortfolioQuotes, start/stopPortfolioPolling, showPortfolio, showTransactions, loadAgentReportPage, showAgentReport, modal handlers) ...
// (Proszę wkleić całą resztę z poprzedniej wersji pliku, skupimy się na nowej logice)

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

// ... (reszta handlerów backtestu itp.)
    
    async function handleYearBacktestRequest() {
        const yearInput = document.getElementById('backtest-year-input');
        const yearBtn = document.getElementById('run-backtest-year-btn');
        const statusMsg = document.getElementById('backtest-status-message');
        
        // Pobieranie parametrów H3
        const paramPercentile = document.getElementById('h3-param-percentile');
        const paramMass = document.getElementById('h3-param-mass');
        const paramTp = document.getElementById('h3-param-tp');
        const paramSl = document.getElementById('h3-param-sl');
        const paramHold = document.getElementById('h3-param-hold');
        const paramName = document.getElementById('h3-param-name');
        const paramMinScore = document.getElementById('h3-param-min-score'); // <-- NOWE POLE

        if (!yearInput || !yearBtn || !statusMsg) return;

        const year = yearInput.value.trim();
        const currentYear = new Date().getFullYear();

        if (!year || year.length !== 4 || !/^\d{4}$/.test(year) || parseInt(year) < 2000 || parseInt(year) > currentYear) {
            statusMsg.className = 'text-sm mt-3 text-red-400';
            statusMsg.textContent = `Błąd: Wprowadź poprawny rok (np. 2000 - ${currentYear}).`;
            return;
        }

        // Budowanie obiektu parametrów (tylko wypełnione pola)
        const h3Params = {};
        if (paramPercentile && paramPercentile.value) h3Params.h3_percentile = parseFloat(paramPercentile.value);
        if (paramMass && paramMass.value) h3Params.h3_m_sq_threshold = parseFloat(paramMass.value);
        if (paramTp && paramTp.value) h3Params.h3_tp_multiplier = parseFloat(paramTp.value);
        if (paramSl && paramSl.value) h3Params.h3_sl_multiplier = parseFloat(paramSl.value);
        if (paramHold && paramHold.value) h3Params.h3_max_hold = parseInt(paramHold.value);
        if (paramName && paramName.value) h3Params.setup_name = paramName.value.trim();
        if (paramMinScore && paramMinScore.value) h3Params.h3_min_score = parseFloat(paramMinScore.value); // <-- OBSŁUGA MIN SCORE

        yearBtn.disabled = true;
        yearBtn.innerHTML = `<i data-lucide="loader-2" class="w-4 h-4 mr-2 animate-spin"></i> Zlecanie...`;
        lucide.createIcons();
        statusMsg.className = 'text-sm mt-3 text-sky-400';
        
        const hasParams = Object.keys(h3Params).length > 0;
        statusMsg.textContent = `Zlecanie testu dla roku ${year} ${hasParams ? '(z niestandardowymi parametrami)' : ''}...`;

        try {
            // Przekazujemy h3Params (może być pusty, wtedy backend użyje domyślnych)
            const response = await api.requestBacktest(year, Object.keys(h3Params).length ? h3Params : null);
            statusMsg.className = 'text-sm mt-3 text-green-400';
            statusMsg.textContent = response.message || `Zlecono test dla ${year}. Worker rozpoczął pracę.`;
            setTimeout(() => {
                // Nie przechodź do dashboardu, ale zaktualizuj status workera
                // (ponieważ użytkownik jest już na stronie raportu)
                pollWorkerStatus();
            }, 2000);
        } catch (e) {
            statusMsg.className = 'text-sm mt-3 text-red-400';
            statusMsg.textContent = `Błąd zlecenia: ${e.message}`;
        } finally {
            yearBtn.disabled = false;
            yearBtn.innerHTML = `<i data-lucide="play" class="w-4 h-4 mr-2"></i> Uruchom Test`; // Skrócona nazwa
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
    
    // ==========================================================
    // === NOWE FUNKCJE (Krok 4B - H3 Deep Dive) ===
    // ==========================================================
    function showH3DeepDiveModal() {
        if (ui.h3DeepDiveModal.backdrop) {
            ui.h3DeepDiveModal.backdrop.classList.remove('hidden');
            ui.h3DeepDiveModal.yearInput.value = ''; // Wyczyść input
            ui.h3DeepDiveModal.statusMsg.textContent = ''; // Wyczyść status
            ui.h3DeepDiveModal.runBtn.disabled = false;
            ui.h3DeepDiveModal.runBtn.innerHTML = `<i data-lucide="search-check" class="w-4 h-4 mr-2"></i> Analizuj Rok`;
            lucide.createIcons();
            // Automatycznie spróbuj załadować ostatni raport
            handleViewH3DeepDiveReport();
        }
    }

    function hideH3DeepDiveModal() {
        if (ui.h3DeepDiveModal.backdrop) {
            ui.h3DeepDiveModal.backdrop.classList.add('hidden');
            ui.h3DeepDiveModal.content.innerHTML = ''; // Wyczyść zawartość
        }
    }

    async function handleRunH3DeepDive() {
        const yearInput = ui.h3DeepDiveModal.yearInput;
        const runBtn = ui.h3DeepDiveModal.runBtn;
        const statusMsg = ui.h3DeepDiveModal.statusMsg;
        if (!yearInput || !runBtn || !statusMsg) return;

        const yearStr = yearInput.value.trim();
        const year = parseInt(yearStr, 10);
        const currentYear = new Date().getFullYear();

        if (!yearStr || isNaN(year) || year < 2000 || year > currentYear) {
            statusMsg.className = 'text-sm mt-3 text-red-400';
            statusMsg.textContent = `Błąd: Wprowadź poprawny rok (np. 2000 - ${currentYear}).`;
            return;
        }

        runBtn.disabled = true;
        runBtn.innerHTML = `<i data-lucide="loader-2" class="w-4 h-4 mr-2 animate-spin"></i> Zlecanie...`;
        lucide.createIcons();
        statusMsg.className = 'text-sm mt-3 text-sky-400';
        statusMsg.textContent = `Zlecanie analizy H3 Deep Dive dla roku ${year}...`;

        try {
            const response = await api.requestH3DeepDive(year);
            statusMsg.className = 'text-sm mt-3 text-green-400';
            statusMsg.textContent = response.message || `Zlecono analizę dla ${year}. Worker rozpoczął pracę.`;
            pollH3DeepDiveReport(); // Zacznij odpytywać o wyniki
        } catch (e) {
            statusMsg.className = 'text-sm mt-3 text-red-400';
            statusMsg.textContent = `Błąd zlecenia: ${e.message}`;
            runBtn.disabled = false;
            runBtn.innerHTML = `<i data-lucide="search-check" class="w-4 h-4 mr-2"></i> Analizuj Rok`;
            lucide.createIcons();
        }
    }

    async function pollH3DeepDiveReport() {
        if (state.activeH3DeepDivePolling) {
            clearTimeout(state.activeH3DeepDivePolling);
        }
        
        // Sprawdź, czy modal jest nadal otwarty
        if (!ui.h3DeepDiveModal.backdrop || ui.h3DeepDiveModal.backdrop.classList.contains('hidden')) {
            logger.info("Modal H3 Deep Dive zamknięty, zatrzymuję odpytywanie.");
            return; // Zatrzymaj odpytywanie, jeśli modal jest zamknięty
        }

        const statusMsg = ui.h3DeepDiveModal.statusMsg;
        const contentEl = ui.h3DeepDiveModal.content;
        const runBtn = ui.h3DeepDiveModal.runBtn;

        try {
            const reportData = await api.getH3DeepDiveReport();
            
            if (reportData.status === 'PROCESSING') {
                if(statusMsg) statusMsg.textContent = 'Worker przetwarza dane... (Sprawdzam ponownie za 5s)';
                if(contentEl) contentEl.innerHTML = renderers.loading('Przetwarzanie danych...');
                lucide.createIcons();
                state.activeH3DeepDivePolling = setTimeout(pollH3DeepDiveReport, H3_DEEP_DIVE_POLL_INTERVAL);
            
            } else if (reportData.status === 'DONE') {
                if(statusMsg) {
                    statusMsg.className = 'text-sm mt-3 text-green-400';
                    statusMsg.textContent = `Analiza H3 zakończona (${new Date(reportData.last_updated).toLocaleString()}).`;
                }
                if (runBtn) {
                     runBtn.disabled = false;
                     runBtn.innerHTML = `<i data-lucide="search-check" class="w-4 h-4 mr-2"></i> Analizuj Rok`;
                     lucide.createIcons();
                }
                if (contentEl) {
                    contentEl.innerHTML = `<pre class="text-xs whitespace-pre-wrap font-mono">${reportData.report_text}</pre>`;
                }

            } else { // 'NONE' lub 'ERROR'
                if(statusMsg) {
                    statusMsg.className = 'text-sm mt-3 text-gray-400';
                    statusMsg.textContent = reportData.status === 'ERROR' ? reportData.report_text : 'Gotowy do analizy.';
                }
                if (contentEl) {
                    contentEl.innerHTML = `<p class="text-gray-500">${reportData.status === 'ERROR' ? reportData.report_text : 'Zleć analizę roku, aby zobaczyć raport.'}</p>`;
                }
                if (runBtn) {
                     runBtn.disabled = false;
                     runBtn.innerHTML = `<i data-lucide="search-check" class="w-4 h-4 mr-2"></i> Analizuj Rok`;
                     lucide.createIcons();
                }
            }
        } catch (e) {
            logger.error('Błąd podczas odpytywania o raport H3 Deep Dive', e);
            if(statusMsg) {
                statusMsg.className = 'text-sm mt-3 text-red-400';
                statusMsg.textContent = `Błąd odpytywania: ${e.message}`;
            }
        }
    }

    async function handleViewH3DeepDiveReport() {
        // Ta funkcja jest teraz wywoływana przy otwarciu modala
        const contentEl = ui.h3DeepDiveModal.content;
        const statusMsg = ui.h3DeepDiveModal.statusMsg;
        if (!contentEl || !statusMsg) return;

        contentEl.innerHTML = renderers.loading('Pobieranie ostatniego raportu...');
        lucide.createIcons();
        statusMsg.textContent = '';

        try {
            const reportData = await api.getH3DeepDiveReport();
            
            if (reportData.status === 'DONE' && reportData.report_text) {
                 contentEl.innerHTML = `<pre class="text-xs whitespace-pre-wrap font-mono">${reportData.report_text}</pre>`;
                 statusMsg.textContent = `Załadowano ostatni raport z ${new Date(reportData.last_updated).toLocaleString()}.`;
                 statusMsg.className = 'text-sm mt-3 text-gray-400';
            } else if (reportData.status === 'PROCESSING') {
                contentEl.innerHTML = renderers.loading('Analiza w toku... Worker nadal przetwarza dane.');
                lucide.createIcons();
                pollH3DeepDiveReport(); // Rozpocznij odpytywanie
            } else {
                 contentEl.innerHTML = `<p class="text-gray-500">Brak dostępnego raportu. Uruchom najpierw analizę.</p>`;
                 statusMsg.textContent = 'Gotowy do analizy.';
                 statusMsg.className = 'text-sm mt-3 text-gray-400';
            }
        } catch (e) {
             contentEl.innerHTML = `<p class="text-red-400">Błąd pobierania raportu: ${e.message}</p>`;
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
        // === NOWY LISTENER (Krok 4B - H3 Deep Dive) ===
        // ==========================================================
        const h3DeepDiveModalBtn = e.target.closest('#run-h3-deep-dive-modal-btn');
        // ==========================================================
        const prevBtn = e.target.closest('#report-prev-btn');
        const nextBtn = e.target.closest('#report-next-btn');
        // ==========================================================
        // === OBSŁUGA UI ZAAWANSOWANEGO BACKTESTU ===
        const toggleH3ParamsBtn = e.target.closest('#toggle-h3-params');
        // ==========================================================

        if (backtestYearBtn) {
            handleYearBacktestRequest();
        }
        // ==========================================================
        // === TOGGLE DLA PARAMETRÓW H3 ===
        else if (toggleH3ParamsBtn) {
            const container = document.getElementById('h3-params-container');
            const icon = document.getElementById('h3-params-icon');
            if (container && icon) {
                container.classList.toggle('hidden');
                icon.classList.toggle('rotate-180');
            }
        }
        // ==========================================================
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
        // === NOWY HANDLER (Krok 4B - H3 Deep Dive) ===
        // ==========================================================
        else if (h3DeepDiveModalBtn) {
            showH3DeepDiveModal();
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
