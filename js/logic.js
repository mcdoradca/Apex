import { state, logger, ALERT_POLL_INTERVAL, PROFIT_ALERT_THRESHOLD, PORTFOLIO_QUOTE_POLL_INTERVAL, AI_OPTIMIZER_POLL_INTERVAL, H3_DEEP_DIVE_POLL_INTERVAL } from './state.js';
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
        // Reset to defaults or keep last used? Defaults for now.
        ui.h3LiveModal.percentile.value = "0.95";
        ui.h3LiveModal.mass.value = "-0.5";
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
    // Zbieranie danych z formularza
    const params = {
        h3_percentile: parseFloat(ui.h3LiveModal.percentile.value) || 0.95,
        h3_m_sq_threshold: parseFloat(ui.h3LiveModal.mass.value) || -0.5,
        h3_tp_multiplier: parseFloat(ui.h3LiveModal.tp.value) || 5.0,
        h3_sl_multiplier: parseFloat(ui.h3LiveModal.sl.value) || 2.0,
        setup_name: 'AQM_H3_LIVE' // Domyślna nazwa
    };

    hideH3LiveParamsModal();
    
    try {
        // Wysyłamy request z parametrami
        // UWAGA: api.sendWorkerControl musi być zaktualizowane (plik js/api.js)
        await api.sendWorkerControl('start_phase3', params);
        logger.info("Wysłano komendę Start Fazy 3 z parametrami:", params);
    } catch(e) {
        logger.error("Błąd uruchamiania Fazy 3:", e);
        displaySystemAlert("Błąd uruchamiania: " + e.message);
    }
}

// ... (reszta handlerów backtestu itp.)
