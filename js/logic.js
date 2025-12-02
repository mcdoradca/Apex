import { api } from './api.js';
import { state, logger, PORTFOLIO_QUOTE_POLL_INTERVAL, ALERT_POLL_INTERVAL, REPORT_PAGE_SIZE, AI_OPTIMIZER_POLL_INTERVAL, H3_DEEP_DIVE_POLL_INTERVAL } from './state.js';
import { renderers } from './ui.js';

let UI = null;
let signalDetailsInterval = null;
let signalDetailsClockInterval = null;

// === KONFIGURACJA ODŚWIEŻANIA (Frontend Heartbeat) ===
const VIEW_POLL_INTERVAL_MS = 3500; // 3.5 sekundy dla wszystkich widoków LIVE

export const setUI = (uiInstance) => {
    UI = uiInstance;
};

// === GLOBALNY ZARZĄDCA ODŚWIEŻANIA ===
// Zatrzymuje wszelkie aktywne pętle odświeżania widoków
const stopViewPolling = () => {
    if (state.activeViewPolling) {
        clearInterval(state.activeViewPolling);
        state.activeViewPolling = null;
    }
};

const updateElement = (el, content, isHtml = false) => {
    if (!el) return;
    if (isHtml) el.innerHTML = content;
    else el.textContent = content;
};

const showLoading = () => {
    if (UI && UI.mainContent) UI.mainContent.innerHTML = renderers.loading("Ładowanie danych...");
};

const updateMarketTimeDisplay = () => {
    if (!UI || !UI.signalDetails.nyTime) return;

    const now = new Date();
    const nyTimeOptions = { timeZone: 'America/New_York', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false };
    const nyTimeStr = now.toLocaleTimeString('en-US', nyTimeOptions);
    UI.signalDetails.nyTime.textContent = nyTimeStr;

    const openHour = 15;
    const openMinute = 30;
    
    const target = new Date(now);
    target.setHours(openHour, openMinute, 0, 0);
    
    if (now > target) {
        const closeTime = new Date(now);
        closeTime.setHours(22, 0, 0, 0);
        if (now < closeTime) {
            UI.signalDetails.countdown.textContent = "RYNEK OTWARTY";
            UI.signalDetails.countdown.className = "text-green-400 font-mono font-bold";
        } else {
             UI.signalDetails.countdown.textContent = "RYNEK ZAMKNIĘTY";
             UI.signalDetails.countdown.className = "text-gray-500 font-mono";
        }
    } else {
        const diff = target - now;
        const hours = Math.floor(diff / (1000 * 60 * 60));
        const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
        const seconds = Math.floor((diff % (1000 * 60)) / 1000);
        UI.signalDetails.countdown.textContent = `Otwarcie za: ${hours}h ${minutes}m ${seconds}s`;
        UI.signalDetails.countdown.className = "text-sky-400 font-mono font-bold";
    }
};

export const showDashboard = async () => {
    stopViewPolling(); 
    if (!UI) return;
    UI.mainContent.innerHTML = renderers.dashboard();
    
    // === PUNKT 1: AKTYWACJA PASKA WYSZUKIWANIA ===
    const searchInput = document.querySelector('input[placeholder*="Wpisz ticker"]');
    if (searchInput) {
        searchInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter') {
                const ticker = e.target.value.trim().toUpperCase();
                if (ticker) {
                    showSignalDetails(ticker);
                    e.target.value = '';
                }
            }
        });
    }

    try {
        const countData = await api.getDiscardedCount();
        if (countData && countData.discarded_count_24h !== undefined) {
            state.discardedSignalCount = countData.discarded_count_24h;
            const discardedEl = document.getElementById('dashboard-discarded-signals');
            if (discardedEl) discardedEl.textContent = state.discardedSignalCount;
        }
    } catch (e) {
        logger.error("Błąd dashboardu:", e);
    }
    refreshSidebarData();
};

export const showPortfolio = async (silent = false) => {
    stopViewPolling();
    
    if (!silent) showLoading();

    const runCycle = async () => {
        try {
            const holdings = await api.getPortfolio();
            state.portfolio = holdings;
            
            const tickers = holdings.map(h => h.ticker);
            const quotes = {};
            
            if (tickers.length > 0) {
                try {
                    const bulkData = await api.getBulkQuotes(tickers);
                    if (bulkData && Array.isArray(bulkData)) {
                        bulkData.forEach(q => {
                            if (q['01. symbol']) {
                                quotes[q['01. symbol']] = q;
                            }
                        });
                    }
                } catch(e) {
                    logger.warn("Błąd pobierania cen Bulk dla portfela:", e);
                }
            }
            
            UI.mainContent.innerHTML = renderers.portfolio(holdings, quotes);
            
        } catch (error) {
            if (!silent) UI.mainContent.innerHTML = `<p class="text-red-500 p-4">Błąd ładowania portfela: ${error.message}</p>`;
        }
    };

    await runCycle();
    state.activeViewPolling = setInterval(runCycle, VIEW_POLL_INTERVAL_MS);
};

const _extractScore = (notes) => {
    if (!notes) return 0;
    const match = notes.match(/SCORE:\s*(\d+)/);
    return match ? parseInt(match[1]) : 0;
};

const _renderH3ViewInternal = () => {
    const sortedSignals = [...state.phase3].sort((a, b) => {
        let valA, valB;
        
        switch (state.h3SortBy) {
            case 'score': 
                valA = _extractScore(a.notes);
                valB = _extractScore(b.notes);
                return valB - valA; 
            case 'rr': 
                valA = parseFloat(a.risk_reward_ratio || 0);
                valB = parseFloat(b.risk_reward_ratio || 0);
                return valB - valA;
            case 'time': 
                valA = a.expiration_date ? new Date(a.expiration_date).getTime() : Number.MAX_SAFE_INTEGER;
                valB = b.expiration_date ? new Date(b.expiration_date).getTime() : Number.MAX_SAFE_INTEGER;
                return valA - valB;
            case 'ticker': 
                return a.ticker.localeCompare(b.ticker);
            default:
                return 0;
        }
    });

    UI.mainContent.innerHTML = renderers.h3SignalsPanel(sortedSignals, state.liveQuotes);

    const sortSelect = document.getElementById('h3-sort-select');
    const refreshBtn = document.getElementById('h3-refresh-btn');
    const cards = document.querySelectorAll('.phase3-item'); 

    if (sortSelect) {
        sortSelect.value = state.h3SortBy;
        sortSelect.addEventListener('change', (e) => {
            state.h3SortBy = e.target.value;
            _renderH3ViewInternal(); 
        });
    }

    if (refreshBtn) {
        refreshBtn.addEventListener('click', () => showH3Signals(false));
    }

    cards.forEach(card => {
        card.addEventListener('click', () => {
            const ticker = card.dataset.ticker;
            if (ticker) showSignalDetails(ticker);
        });
    });

    if (window.lucide) window.lucide.createIcons();
};

export const showH3Signals = async (silent = false) => {
    stopViewPolling();
    
    if (!silent && UI) showLoading();

    const runCycle = async () => {
        try {
            const signals = await api.getPhase3Signals();
            state.phase3 = signals || [];
            
            const tickers = signals.map(s => s.ticker);
            if (tickers.length > 0) {
                try {
                    const bulkData = await api.getBulkQuotes(tickers);
                    const newQuotes = {};
                    if (bulkData && Array.isArray(bulkData)) {
                        bulkData.forEach(q => {
                            if (q['01. symbol']) {
                                newQuotes[q['01. symbol']] = q;
                            }
                        });
                    }
                    state.liveQuotes = { ...state.liveQuotes, ...newQuotes };
                } catch(e) {
                    logger.warn("Błąd pobierania cen H3 Bulk:", e);
                }
            }

            _renderH3ViewInternal();
            
            if (UI && UI.phase3 && UI.phase3.count) {
                updateElement(UI.phase3.count, state.phase3.length);
            }
        } catch (e) {
            if (!silent && UI) UI.mainContent.innerHTML = `<p class="text-red-500 p-4">Błąd pobierania sygnałów: ${e.message}</p>`;
        }
    };

    await runCycle();
    state.activeViewPolling = setInterval(runCycle, VIEW_POLL_INTERVAL_MS);
};

// === NOWOŚĆ: WIDOK FAZY X (BioX) ===
export const showPhaseX = async () => {
    stopViewPolling(); // Tu nie ma live cen na liście (na razie), więc polling niepotrzebny
    showLoading();
    
    try {
        const candidates = await api.getPhaseXCandidates();
        UI.mainContent.innerHTML = renderers.phaseXView(candidates);
        
        // Obsługa przycisku skanowania wewnątrz widoku (jeśli dodamy go w ui.js)
        const runBtn = document.getElementById('run-phasex-scan-btn');
        if (runBtn) {
            runBtn.addEventListener('click', handleRunPhaseXScan);
        }
    } catch (e) {
        UI.mainContent.innerHTML = `<p class="text-red-500 p-4">Błąd pobierania Fazy X: ${e.message}</p>`;
    }
};

export const handleRunPhaseXScan = async () => {
    const btn = document.getElementById('run-phasex-scan-btn');
    if (btn) {
        btn.disabled = true;
        btn.textContent = "Skanowanie w tle...";
    }
    try {
        await api.sendWorkerControl('start_phasex');
        // showSystemAlert("Rozpoczęto skanowanie BioX (Faza X)."); // Funkcja alertu musi być dostępna
        alert("Rozpoczęto skanowanie BioX (Faza X). Sprawdź status workera.");
    } catch (e) {
        alert("Błąd startu Fazy X: " + e.message);
        if (btn) {
            btn.disabled = false;
            btn.textContent = "Skanuj BioX";
        }
    }
};

export const showTransactions = async (silent = false) => {
    stopViewPolling();
    if (!silent) showLoading();
    
    const runCycle = async () => {
        try {
            const history = await api.getTransactionHistory();
            UI.mainContent.innerHTML = renderers.transactions(history);
        } catch (error) {
            if (!silent) UI.mainContent.innerHTML = `<p class="text-red-500 p-4">Błąd ładowania historii: ${error.message}</p>`;
        }
    };

    await runCycle();
    state.activeViewPolling = setInterval(runCycle, VIEW_POLL_INTERVAL_MS);
};

export const showAgentReport = async () => {
    stopViewPolling();
    loadAgentReportPage(1);
};

export const loadAgentReportPage = async (page) => {
    showLoading();
    try {
        state.currentReportPage = page;
        const reportData = await api.getVirtualAgentReport(page, REPORT_PAGE_SIZE);
        UI.mainContent.innerHTML = renderers.agentReport(reportData);
    } catch (error) {
        UI.mainContent.innerHTML = `<p class="text-red-500 p-4">Błąd raportu agenta: ${error.message}</p>`;
    }
};

export const refreshSidebarData = async () => {
    try {
        const phase1Data = await api.getPhase1Candidates();
        state.phase1 = phase1Data || [];
        updateElement(UI.phase1.count, state.phase1.length);
        updateElement(UI.phase1.list, renderers.phase1List(state.phase1), true);

        const phase3Data = await api.getPhase3Signals();
        state.phase3 = phase3Data || [];
        updateElement(UI.phase3.count, state.phase3.length);
        updateElement(UI.phase3.list, renderers.phase3List(state.phase3), true);

    } catch (e) {
        logger.error("Błąd odświeżania sidebaru:", e);
    }
};

export const pollWorkerStatus = () => {
    const check = async () => {
        try {
            const status = await api.getWorkerStatus();
            state.workerStatus = status;
            
            if (UI.workerStatusText) {
                UI.workerStatusText.textContent = status.status;
                UI.workerStatusText.className = `font-mono px-2 py-1 rounded-md text-xs ${
                    status.status.includes('RUNNING') || status.status.includes('BUSY') ? 'bg-green-900 text-green-300 animate-pulse' : 'bg-gray-700 text-gray-200'
                }`;
            }

            if (UI.heartbeatStatus) {
                const hb = new Date(status.last_heartbeat_utc);
                UI.heartbeatStatus.textContent = hb.toLocaleTimeString();
            }
            
            const progressBar = document.getElementById('progress-bar');
            const progressText = document.getElementById('progress-text');
            const scanLog = document.getElementById('scan-log');
            const currentPhaseTxt = document.getElementById('dashboard-current-phase');
            const dashboardWorkerStatus = document.getElementById('dashboard-worker-status');

            if (dashboardWorkerStatus) {
                dashboardWorkerStatus.textContent = status.status;
                let statusClass = "text-5xl font-black tracking-tighter drop-shadow-lg ";
                if (status.status.includes('RUNNING') || status.status.includes('BUSY')) {
                    statusClass += "text-yellow-400 animate-pulse";
                } else if (status.status === 'PAUSED') {
                    statusClass += "text-red-500";
                } else {
                    statusClass += "text-green-500";
                }
                dashboardWorkerStatus.className = statusClass;
            }

            if (currentPhaseTxt) {
                currentPhaseTxt.textContent = `Faza: ${status.phase}`;
            }

            if (progressBar && status.progress.total > 0) {
                const pct = Math.round((status.progress.processed / status.progress.total) * 100);
                progressBar.style.width = `${pct}%`;
                progressText.textContent = `${status.progress.processed} / ${status.progress.total}`;
            }
            
            if (scanLog && scanLog.textContent !== status.log) {
                const container = document.getElementById('scan-log-container');
                const isAtTop = container ? container.scrollTop < 50 : true;
                scanLog.textContent = status.log;
                if (container && isAtTop) container.scrollTop = 0;
            }
            
            const dashboardSignals = document.getElementById('dashboard-active-signals');
            if (dashboardSignals) {
                const activeCount = state.phase3.filter(s => s.status === 'ACTIVE').length;
                const pendingCount = state.phase3.filter(s => s.status === 'PENDING').length;
                dashboardSignals.textContent = activeCount + pendingCount;
            }
            
            const discardedSignals = document.getElementById('dashboard-discarded-signals');
            if (discardedSignals && state.discardedSignalCount !== undefined) {
                discardedSignals.textContent = state.discardedSignalCount;
            }

        } catch (e) {}
    };
    
    check();
    setInterval(check, 2000);
};

export const pollSystemAlerts = () => {
    setInterval(async () => {
        try {
            const alert = await api.getSystemAlert();
            if (alert && alert.message !== 'NONE') {
                showSystemAlert(alert.message);
            }
        } catch(e) {}
    }, ALERT_POLL_INTERVAL);
};

const showSystemAlert = (msg) => {
    if (!UI.alertContainer) return;
    const div = document.createElement('div');
    div.className = 'alert-bar bg-red-600 text-white px-4 py-3 rounded shadow-lg flex justify-between items-center mb-2 animate-bounce';
    div.innerHTML = `<span>${msg}</span><button onclick="this.parentElement.remove()" class="ml-4 font-bold">X</button>`;
    UI.alertContainer.appendChild(div);
    setTimeout(() => div.remove(), 10000);
};

export const showBuyModal = (ticker) => {
    UI.buyModal.tickerSpan.textContent = ticker;
    UI.buyModal.quantityInput.value = "";
    UI.buyModal.priceInput.value = "";
    api.getLiveQuote(ticker).then(q => {
        if (q && q['05. price']) {
            UI.buyModal.priceInput.value = parseFloat(q['05. price']).toFixed(2);
        }
    });
    UI.buyModal.backdrop.classList.remove('hidden');
};

export const hideBuyModal = () => {
    UI.buyModal.backdrop.classList.add('hidden');
};

export const handleBuyConfirm = async () => {
    const ticker = UI.buyModal.tickerSpan.textContent;
    const qty = parseInt(UI.buyModal.quantityInput.value);
    const price = parseFloat(UI.buyModal.priceInput.value);
    if (!qty || qty <= 0 || !price || price <= 0) {
        alert("Podaj poprawną ilość i cenę."); return;
    }
    try {
        UI.buyModal.confirmBtn.disabled = true;
        UI.buyModal.confirmBtn.textContent = "Przetwarzanie...";
        await api.buyStock({ ticker, quantity: qty, price_per_share: price });
        hideBuyModal();
        showPortfolio();
        showSystemAlert(`Kupiono ${qty} akcji ${ticker}.`);
    } catch (e) {
        alert(e.message);
    } finally {
        UI.buyModal.confirmBtn.disabled = false;
        UI.buyModal.confirmBtn.textContent = "Inwestuj";
    }
};

export const handleGhostBuy = async (ticker) => {
    alert("Funkcja Ghost Mode została wyłączona.");
};

export const showSellModal = (ticker, maxQty) => {
    UI.sellModal.tickerSpan.textContent = ticker;
    UI.sellModal.maxQuantitySpan.textContent = maxQty;
    UI.sellModal.quantityInput.value = maxQty; 
    UI.sellModal.quantityInput.max = maxQty;
    UI.sellModal.priceInput.value = "";
    api.getLiveQuote(ticker).then(q => {
        if (q && q['05. price']) UI.sellModal.priceInput.value = parseFloat(q['05. price']).toFixed(2);
    });
    UI.sellModal.backdrop.classList.remove('hidden');
};

export const hideSellModal = () => {
    UI.sellModal.backdrop.classList.add('hidden');
};

export const handleSellConfirm = async () => {
    const ticker = UI.sellModal.tickerSpan.textContent;
    const qty = parseInt(UI.sellModal.quantityInput.value);
    const price = parseFloat(UI.sellModal.priceInput.value);
    if (!qty || qty <= 0 || !price || price <= 0) {
        alert("Błędne dane."); return;
    }
    try {
        UI.sellModal.confirmBtn.disabled = true;
        await api.sellStock({ ticker, quantity: qty, price_per_share: price });
        hideSellModal();
        showPortfolio();
        showSystemAlert(`Sprzedano ${qty} akcji ${ticker}.`);
    } catch (e) {
        alert(e.message);
    } finally {
        UI.sellModal.confirmBtn.disabled = false;
    }
};

export const handleYearBacktestRequest = async () => {
    const input = document.getElementById('backtest-year-input');
    const status = document.getElementById('backtest-status-message');
    const strategySelect = document.getElementById('backtest-strategy-select');
    const strategyMode = strategySelect ? strategySelect.value : 'H3';

    if (!input || !input.value) return;
    
    const params = {
        strategy_mode: strategyMode,
        h3_percentile: document.getElementById('h3-param-percentile')?.value || 0.95,
        h3_m_sq_threshold: document.getElementById('h3-param-mass')?.value || -0.5,
        h3_min_score: document.getElementById('h3-param-min-score')?.value || 0.0,
        h3_tp_multiplier: document.getElementById('h3-param-tp')?.value || 5.0,
        h3_sl_multiplier: document.getElementById('h3-param-sl')?.value || 2.0,
        h3_max_hold: document.getElementById('h3-param-hold')?.value || 5,
        setup_name: document.getElementById('h3-param-name')?.value || ""
    };
    try {
        status.textContent = `Wysyłanie zlecenia (${strategyMode})...`;
        status.className = "text-yellow-400 text-sm mt-3 h-4";
        await api.requestBacktest(input.value, params);
        status.textContent = "Zlecenie przyjęte. Sprawdź status Workera.";
        status.className = "text-green-400 text-sm mt-3 h-4";
    } catch (e) {
        status.textContent = "Błąd: " + e.message;
        status.className = "text-red-400 text-sm mt-3 h-4";
    }
};

export const handleCsvExport = async () => {
    const status = document.getElementById('csv-export-status-message');
    try {
        if(status) status.textContent = "Generowanie CSV...";
        window.location.href = api.getExportCsvUrl();
        setTimeout(() => { if(status) status.textContent = "Pobieranie rozpoczęte."; }, 2000);
    } catch(e) {
        if(status) status.textContent = "Błąd pobierania.";
    }
};

export const showH3DeepDiveModal = () => {
    UI.h3DeepDiveModal.backdrop.classList.remove('hidden');
    UI.h3DeepDiveModal.statusMsg.textContent = "";
    UI.h3DeepDiveModal.content.innerHTML = '<p class="text-gray-500">Oczekiwanie na dane...</p>';
    api.getH3DeepDiveReport().then(r => {
        if (r.report_text) UI.h3DeepDiveModal.content.innerHTML = `<pre class="whitespace-pre-wrap text-xs font-mono text-green-300">${r.report_text}</pre>`;
    });
};

export const hideH3DeepDiveModal = () => {
    UI.h3DeepDiveModal.backdrop.classList.add('hidden');
    if (state.activeH3DeepDivePolling) clearInterval(state.activeH3DeepDivePolling);
};

export const handleRunH3DeepDive = async () => {
    const year = UI.h3DeepDiveModal.yearInput.value;
    if (!year) return;
    try {
        UI.h3DeepDiveModal.runBtn.disabled = true;
        UI.h3DeepDiveModal.statusMsg.textContent = "Wysyłanie...";
        await api.requestH3DeepDive(parseInt(year));
        UI.h3DeepDiveModal.statusMsg.textContent = "Przetwarzanie... Proszę czekać.";
        state.activeH3DeepDivePolling = setInterval(async () => {
            const rep = await api.getH3DeepDiveReport();
            if (rep.status === 'DONE') {
                UI.h3DeepDiveModal.content.innerHTML = `<pre class="whitespace-pre-wrap text-xs font-mono text-green-300">${rep.report_text}</pre>`;
                UI.h3DeepDiveModal.statusMsg.textContent = "Zakończono.";
                UI.h3DeepDiveModal.runBtn.disabled = false;
                clearInterval(state.activeH3DeepDivePolling);
            } else if (rep.status === 'ERROR') {
                UI.h3DeepDiveModal.content.textContent = "Błąd: " + rep.report_text;
                UI.h3DeepDiveModal.runBtn.disabled = false;
                clearInterval(state.activeH3DeepDivePolling);
            }
        }, H3_DEEP_DIVE_POLL_INTERVAL);
    } catch (e) {
        UI.h3DeepDiveModal.statusMsg.textContent = "Błąd API: " + e.message;
        UI.h3DeepDiveModal.runBtn.disabled = false;
    }
};

export const handleRunAIOptimizer = async () => {
    const status = document.getElementById('ai-optimizer-status-message');
    try {
        if(status) status.textContent = "Zlecanie analizy AI...";
        await api.requestAIOptimizer();
        if(status) status.textContent = "Mega Agent pracuje. To potrwa ok. 1-2 minuty.";
    } catch (e) {
        if(status) status.textContent = "Błąd: " + e.message;
    }
};

export const handleViewAIOptimizerReport = async () => {
    UI.aiReportModal.backdrop.classList.remove('hidden');
    UI.aiReportModal.content.innerHTML = "Ładowanie raportu...";
    try {
        const report = await api.getAIOptimizerReport();
        if (report.status === 'DONE') {
            UI.aiReportModal.content.innerHTML = `<pre class="whitespace-pre-wrap font-mono text-xs text-green-300">${report.report_text}</pre>`;
        } else if (report.status === 'PROCESSING') {
            UI.aiReportModal.content.innerHTML = "<p class='text-yellow-400'>Raport jest w trakcie generowania...</p>";
        } else {
            UI.aiReportModal.content.innerHTML = "<p class='text-gray-500'>Brak raportu.</p>";
        }
    } catch(e) {
        UI.aiReportModal.content.innerHTML = "<p class='text-red-500'>Błąd pobierania raportu.</p>";
    }
};

export const hideAIReportModal = () => {
    UI.aiReportModal.backdrop.classList.add('hidden');
};

export const showH3LiveParamsModal = () => { UI.h3LiveModal.backdrop.classList.remove('hidden'); };
export const hideH3LiveParamsModal = () => { UI.h3LiveModal.backdrop.classList.add('hidden'); };

export const handleRunH3LiveScan = async () => {
    const params = {
        h3_percentile: UI.h3LiveModal.percentile.value,
        h3_m_sq_threshold: UI.h3LiveModal.mass.value,
        h3_min_score: UI.h3LiveModal.minScore.value,
        h3_tp_multiplier: UI.h3LiveModal.tp.value,
        h3_sl_multiplier: UI.h3LiveModal.sl.value,
        h3_max_hold: UI.h3LiveModal.maxHold.value,
        aqm_component_min: document.getElementById('h3-live-aqm-min')?.value || 0.5 
    };
    
    try {
        UI.h3LiveModal.startBtn.disabled = true;
        await api.sendWorkerControl('start_phase3', params);
        hideH3LiveParamsModal();
        showSystemAlert("Rozpoczęto Skanowanie H3 Live.");
    } catch (e) {
        alert("Błąd startu H3: " + e.message);
    } finally {
        UI.h3LiveModal.startBtn.disabled = false;
    }
};

export const showSignalDetails = async (ticker) => {
    UI.signalDetails.backdrop.classList.remove('hidden');
    UI.signalDetails.ticker.textContent = ticker;
    UI.signalDetails.companyName.textContent = "Ładowanie...";
    UI.signalDetails.currentPrice.textContent = "---";
    
    const existingRanking = document.getElementById('dynamic-ranking-card');
    if (existingRanking) existingRanking.remove();

    const priceLabel = UI.signalDetails.currentPrice.previousElementSibling;
    if (priceLabel) {
        priceLabel.textContent = "Cena Aktualna";
        priceLabel.className = "text-gray-400 text-sm";
    }

    UI.signalDetails.validityBadge.textContent = "Checking...";
    UI.signalDetails.validityBadge.className = "text-sm px-2 py-1 rounded bg-gray-700 text-gray-400 font-mono";
    UI.signalDetails.validityMessage.classList.add('hidden');
    UI.signalDetails.sector.textContent = "---";
    UI.signalDetails.industry.textContent = "---";
    UI.signalDetails.description.textContent = "Ładowanie opisu...";
    
    const newsContainer = document.getElementById('sd-news-container');
    if (newsContainer) newsContainer.classList.add('hidden');

    if (UI.signalDetails.buyBtn) {
        UI.signalDetails.buyBtn.textContent = "Inwestuj (Kup)";
        UI.signalDetails.buyBtn.onclick = () => {
            hideSignalDetails();
            showBuyModal(ticker);
        };
    }

    updateMarketTimeDisplay();
    if (signalDetailsClockInterval) clearInterval(signalDetailsClockInterval);
    signalDetailsClockInterval = setInterval(updateMarketTimeDisplay, 1000);

    const fetchData = async () => {
        try {
            const data = await api.getSignalDetails(ticker);
            
            if (data.status === 'INVALIDATED' && !data.company) {
                 UI.signalDetails.validityBadge.textContent = "INVALID";
                 UI.signalDetails.validityBadge.className = "text-sm px-2 py-1 rounded bg-red-900 text-red-200 font-mono";
                 UI.signalDetails.validityMessage.textContent = data.reason;
                 UI.signalDetails.validityMessage.classList.remove('hidden');
                 return;
            }
            
            if (data.company) {
                UI.signalDetails.companyName.textContent = data.company.name;
                UI.signalDetails.sector.textContent = data.company.sector || "N/A";
                UI.signalDetails.industry.textContent = data.company.industry || "N/A";
                UI.signalDetails.description.textContent = data.company.description || "Brak opisu spółki w bazie danych.";
            }
            
            if (data.market_data) {
                const price = parseFloat(data.market_data.current_price);
                UI.signalDetails.currentPrice.textContent = price > 0 ? price.toFixed(2) : "---";
                
                const priceLabel = UI.signalDetails.currentPrice.previousElementSibling;
                const source = data.market_data.price_source;
                let statusText = data.market_data.market_status;

                if (priceLabel) {
                    if (source === 'extended_hours') {
                        priceLabel.textContent = "Cena (Pre/Post Market)";
                        priceLabel.className = "text-purple-400 text-sm font-bold animate-pulse";
                        if (statusText.toLowerCase() === 'closed') statusText = "Extended Hours";
                    } else if (source === 'previous_close') {
                        priceLabel.textContent = "Cena Zamknięcia (Wczoraj)";
                        priceLabel.className = "text-yellow-500 text-sm font-semibold";
                    } else {
                         const isClosed = statusText.toLowerCase().includes('closed');
                         priceLabel.textContent = isClosed ? "Cena Zamknięcia" : "Cena Aktualna";
                         priceLabel.className = "text-gray-400 text-sm";
                    }
                }

                UI.signalDetails.changePercent.textContent = data.market_data.change_percent;
                const changeVal = parseFloat(data.market_data.change_percent.replace('%', ''));
                UI.signalDetails.changePercent.className = `font-mono text-lg font-bold ${changeVal >= 0 ? 'text-green-400' : 'text-red-400'}`;
                UI.signalDetails.marketStatus.textContent = statusText;
            }
            
            if (data.setup) {
                UI.signalDetails.entry.textContent = data.setup.entry_price ? data.setup.entry_price.toFixed(2) : "---";
                UI.signalDetails.tp.textContent = data.setup.take_profit ? data.setup.take_profit.toFixed(2) : "---";
                UI.signalDetails.sl.textContent = data.setup.stop_loss ? data.setup.stop_loss.toFixed(2) : "---";
                UI.signalDetails.rr.textContent = data.setup.risk_reward ? data.setup.risk_reward.toFixed(2) : "---";
                UI.signalDetails.generationDate.textContent = new Date(data.setup.generation_date).toLocaleString('pl-PL');

                if (data.setup.notes && data.setup.notes.includes("RANKING:")) {
                    _injectRankingCard(data.setup.notes);
                }
            }

            if (data.validity) {
                 const isValid = data.validity.is_valid;
                 UI.signalDetails.validityBadge.textContent = isValid ? "VALID" : "INVALID";
                 UI.signalDetails.validityBadge.className = `text-sm px-2 py-1 rounded font-mono ${isValid ? 'bg-green-900 text-green-200' : 'bg-red-900 text-red-200'}`;
                 
                 if (!isValid) {
                     UI.signalDetails.validityMessage.textContent = data.validity.message;
                     UI.signalDetails.validityMessage.classList.remove('hidden');
                 } else {
                     UI.signalDetails.validityMessage.classList.add('hidden');
                 }
            }

            if (data.news_context && newsContainer) {
                newsContainer.classList.remove('hidden');
            }
            
        } catch (e) {
            console.error("Błąd pobierania danych sygnału:", e);
            UI.signalDetails.companyName.textContent = "Błąd połączenia...";
        }
    };

    fetchData();
    if (signalDetailsInterval) clearInterval(signalDetailsInterval);
    signalDetailsInterval = setInterval(fetchData, 3000);
};

const _injectRankingCard = (notes) => {
    if (document.getElementById('dynamic-ranking-card')) return; 

    const evMatch = notes.match(/EV:\s*([+\-]?\d+\.?\d*)%/i);
    const scoreMatch = notes.match(/SCORE:\s*(\d+)\/100/i);
    const recMatch = notes.match(/REKOMENDACJA:\s*(.*?)(?:\n|$)/i);
    const detailsMatch = notes.match(/Tech:(\d+)\s*Mkt:(\d+)\s*RS:(\d+)\s*Ctx:(\d+)/i);

    if (!evMatch || !scoreMatch) return;

    const evVal = parseFloat(evMatch[1]);
    const scoreVal = parseInt(scoreMatch[1]);
    const recText = recMatch ? recMatch[1].trim() : "---";
    
    const det = detailsMatch ? {
        tech: parseInt(detailsMatch[1]),
        mkt: parseInt(detailsMatch[2]),
        rs: parseInt(detailsMatch[3]),
        ctx: parseInt(detailsMatch[4])
    } : { tech: 0, mkt: 0, rs: 0, ctx: 0 };

    const scoreColor = scoreVal >= 80 ? "text-purple-400" : (scoreVal >= 60 ? "text-green-400" : "text-yellow-400");
    const evColor = evVal > 2.0 ? "text-green-400" : (evVal > 0 ? "text-blue-300" : "text-gray-400");

    const html = `
        <div id="dynamic-ranking-card" class="bg-gradient-to-br from-gray-800 to-gray-900 p-5 rounded-lg border border-purple-500/30 shadow-lg shadow-purple-900/20 mb-6 relative overflow-hidden">
            <div class="absolute top-0 right-0 p-2 opacity-10 pointer-events-none">
                <i data-lucide="crown" class="w-24 h-24 text-white"></i>
            </div>
            
            <h4 class="text-xs font-bold text-purple-400 uppercase mb-4 flex items-center tracking-wider">
                <i data-lucide="award" class="w-4 h-4 mr-2"></i> Apex Quantum Rank
            </h4>

            <div class="flex justify-between items-end mb-4 border-b border-gray-700 pb-4">
                <div>
                    <span class="block text-xs text-gray-500 uppercase font-bold mb-1">Jakość Setupu</span>
                    <span class="text-4xl font-black ${scoreColor}">${scoreVal}<span class="text-lg text-gray-600">/100</span></span>
                </div>
                <div class="text-right">
                    <span class="block text-xs text-gray-500 uppercase font-bold mb-1">EV (Potencjał)</span>
                    <span class="text-2xl font-mono font-bold ${evColor}">${evVal > 0 ? '+' : ''}${evVal}%</span>
                </div>
            </div>

            <div class="flex items-center justify-between mb-4">
                <span class="text-sm text-gray-300 font-medium">Rekomendacja AI:</span>
                <span class="px-3 py-1 rounded bg-gray-700 border border-gray-600 text-white text-xs font-bold tracking-wide uppercase shadow-sm">${recText}</span>
            </div>

            <div class="space-y-3 text-xs">
                <div>
                    <div class="flex justify-between mb-1"><span class="text-gray-400">Siła Techniczna (AQM)</span><span class="text-gray-300">${det.tech}/40</span></div>
                    <div class="w-full bg-gray-800 rounded-full h-1.5"><div class="bg-blue-500 h-1.5 rounded-full" style="width: ${(det.tech/40)*100}%"></div></div>
                </div>
                <div>
                    <div class="flex justify-between mb-1"><span class="text-gray-400">Kontekst Rynkowy</span><span class="text-gray-300">${det.mkt}/30</span></div>
                    <div class="w-full bg-gray-800 rounded-full h-1.5"><div class="bg-purple-500 h-1.5 rounded-full" style="width: ${(det.mkt/30)*100}%"></div></div>
                </div>
                <div>
                    <div class="flex justify-between mb-1"><span class="text-gray-400">Siła Relatywna (RS)</span><span class="text-gray-300">${det.rs}/20</span></div>
                    <div class="w-full bg-gray-800 rounded-full h-1.5"><div class="bg-green-500 h-1.5 rounded-full" style="width: ${(det.rs/20)*100}%"></div></div>
                </div>
            </div>
        </div>
    `;

    const entryEl = document.getElementById('sd-entry-price');
    if (entryEl) {
        const rightCol = entryEl.closest('.space-y-6');
        if (rightCol) {
            rightCol.insertAdjacentHTML('afterbegin', html);
            if (window.lucide) window.lucide.createIcons();
        }
    }
};

export const hideSignalDetails = () => {
    UI.signalDetails.backdrop.classList.add('hidden');
    if (signalDetailsInterval) clearInterval(signalDetailsInterval);
    if (signalDetailsClockInterval) clearInterval(signalDetailsClockInterval);
    signalDetailsInterval = null;
    signalDetailsClockInterval = null;
};

export const showQuantumModal = () => {
    UI.quantumModal.backdrop.classList.remove('hidden');
    UI.quantumModal.statusMessage.textContent = "";
};

export const hideQuantumModal = () => {
    UI.quantumModal.backdrop.classList.add('hidden');
};

export const handleStartQuantumOptimization = async () => {
    const year = parseInt(UI.quantumModal.yearInput.value);
    const trials = parseInt(UI.quantumModal.trialsInput.value);
    const strategy = UI.quantumModal.strategySelect.value;
    
    if (!year || !trials || trials < 10) {
        UI.quantumModal.statusMessage.textContent = "Podaj poprawny rok i min. 10 prób.";
        UI.quantumModal.statusMessage.className = "text-red-400 text-sm mt-3 h-4 text-center";
        return;
    }

    try {
        UI.quantumModal.startBtn.disabled = true;
        UI.quantumModal.statusMessage.textContent = `Uruchamianie silnika (${strategy})...`;
        UI.quantumModal.statusMessage.className = "text-yellow-400 text-sm mt-3 h-4 text-center";
        
        await api.startOptimization({ 
            target_year: year, 
            n_trials: trials,
            parameter_space: { strategy: strategy } 
        });
        
        UI.quantumModal.statusMessage.textContent = "Zlecenie przyjęte! Sprawdź wyniki.";
        UI.quantumModal.statusMessage.className = "text-green-400 text-sm mt-3 h-4 text-center";
        
        setTimeout(() => {
            hideQuantumModal();
            UI.quantumModal.startBtn.disabled = false;
        }, 2000);
        
    } catch (e) {
        UI.quantumModal.statusMessage.textContent = "Błąd: " + e.message;
        UI.quantumModal.statusMessage.className = "text-red-400 text-sm mt-3 h-4 text-center";
        UI.quantumModal.startBtn.disabled = false;
    }
};

export const showOptimizationResults = async () => {
    UI.optimizationResultsModal.backdrop.classList.remove('hidden');
    UI.optimizationResultsModal.content.innerHTML = renderers.loading("Pobieranie wyników Optuny...");
    
    try {
        const results = await api.getOptimizationResults();
        UI.optimizationResultsModal.content.innerHTML = renderers.optimizationResults(results);
        
        const useButtons = UI.optimizationResultsModal.content.querySelectorAll('.use-params-btn');
        useButtons.forEach(btn => {
            btn.addEventListener('click', (e) => {
                const paramsData = e.currentTarget.dataset.params;
                try {
                    const params = JSON.parse(paramsData);
                    hideOptimizationResults();
                    showH3LiveParamsModal();
                    
                    const _fmt = (val, prec=2) => {
                        if (typeof val === 'number') return parseFloat(val.toFixed(prec));
                        return val;
                    };

                    setTimeout(() => {
                        if (UI.h3LiveModal.percentile && params.h3_percentile) UI.h3LiveModal.percentile.value = _fmt(params.h3_percentile, 2);
                        if (UI.h3LiveModal.mass && params.h3_m_sq_threshold) UI.h3LiveModal.mass.value = _fmt(params.h3_m_sq_threshold, 2);
                        if (UI.h3LiveModal.minScore && params.h3_min_score) UI.h3LiveModal.minScore.value = _fmt(params.h3_min_score, 4);
                        if (UI.h3LiveModal.minScore && params.aqm_min_score) {
                            UI.h3LiveModal.minScore.value = _fmt(params.aqm_min_score, 4);
                        }
                        if (UI.h3LiveModal.tp && params.h3_tp_multiplier) UI.h3LiveModal.tp.value = _fmt(params.h3_tp_multiplier, 2);
                        if (UI.h3LiveModal.sl && params.h3_sl_multiplier) UI.h3LiveModal.sl.value = _fmt(params.h3_sl_multiplier, 2);
                        if (UI.h3LiveModal.maxHold && params.h3_max_hold) UI.h3LiveModal.maxHold.value = parseInt(params.h3_max_hold);
                    }, 100);
                    
                } catch(err) {
                    console.error("Błąd parsowania parametrów:", err);
                }
            });
        });

    } catch (e) {
        UI.optimizationResultsModal.content.innerHTML = `<p class="text-red-500 p-4">Błąd: ${e.message}</p>`;
    }
};

export const hideOptimizationResults = () => {
    UI.optimizationResultsModal.backdrop.classList.add('hidden');
};
