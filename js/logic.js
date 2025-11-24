import { api } from './api.js';
import { state, logger, PORTFOLIO_QUOTE_POLL_INTERVAL, ALERT_POLL_INTERVAL, REPORT_PAGE_SIZE, AI_OPTIMIZER_POLL_INTERVAL, H3_DEEP_DIVE_POLL_INTERVAL } from './state.js';
import { renderers } from './ui.js';

let UI = null;
let signalDetailsInterval = null;
let signalDetailsClockInterval = null;

export const setUI = (uiInstance) => {
    UI = uiInstance;
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
    if (!UI) return;
    UI.mainContent.innerHTML = renderers.dashboard();
    try {
        const countData = await api.getDiscardedCount();
    } catch (e) {
        logger.error("Błąd dashboardu:", e);
    }
    refreshSidebarData();
};

export const showPortfolio = async () => {
    showLoading();
    try {
        const holdings = await api.getPortfolio();
        state.portfolio = holdings;
        const tickers = holdings.map(h => h.ticker);
        const quotes = {};
        
        if (tickers.length > 0) {
            for (const t of tickers) {
                try {
                    const q = await api.getLiveQuote(t);
                    if (q) quotes[t] = q;
                } catch(e) {}
            }
        }
        UI.mainContent.innerHTML = renderers.portfolio(holdings, quotes);
    } catch (error) {
        UI.mainContent.innerHTML = `<p class="text-red-500 p-4">Błąd ładowania portfela: ${error.message}</p>`;
    }
};

export const showTransactions = async () => {
    showLoading();
    try {
        const history = await api.getTransactionHistory();
        UI.mainContent.innerHTML = renderers.transactions(history);
    } catch (error) {
        UI.mainContent.innerHTML = `<p class="text-red-500 p-4">Błąd ładowania historii: ${error.message}</p>`;
    }
};

export const showAgentReport = async () => {
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

            if (progressBar && status.progress.total > 0) {
                const pct = Math.round((status.progress.processed / status.progress.total) * 100);
                progressBar.style.width = `${pct}%`;
                progressText.textContent = `${status.progress.processed} / ${status.progress.total}`;
            }
            
            if (scanLog && scanLog.textContent !== status.log) {
                const container = document.getElementById('scan-log-container');
                const isAtTop = container ? container.scrollTop < 50 : true;
                
                scanLog.textContent = status.log;
                
                if (container && isAtTop) {
                    container.scrollTop = 0;
                }
            }

            if (currentPhaseTxt) {
                currentPhaseTxt.textContent = `Faza: ${status.phase}`;
            }
            
            const dashboardSignals = document.getElementById('dashboard-active-signals');
            if (dashboardSignals) dashboardSignals.textContent = state.phase3.length;

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
    if (!input || !input.value) return;
    const params = {
        h3_percentile: document.getElementById('h3-param-percentile')?.value || 0.95,
        h3_m_sq_threshold: document.getElementById('h3-param-mass')?.value || -0.5,
        h3_min_score: document.getElementById('h3-param-min-score')?.value || 0.0,
        h3_tp_multiplier: document.getElementById('h3-param-tp')?.value || 5.0,
        h3_sl_multiplier: document.getElementById('h3-param-sl')?.value || 2.0,
        h3_max_hold: document.getElementById('h3-param-hold')?.value || 5,
        setup_name: document.getElementById('h3-param-name')?.value || ""
    };
    try {
        status.textContent = "Wysyłanie zlecenia...";
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
        h3_max_hold: UI.h3LiveModal.maxHold.value // V4 Parameter
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
                
                const sentimentEl = document.getElementById('sd-news-sentiment');
                const headlineEl = document.getElementById('sd-news-headline');
                const timeEl = document.getElementById('sd-news-time');
                const linkEl = document.getElementById('sd-news-link');

                if (sentimentEl) {
                    sentimentEl.textContent = data.news_context.sentiment;
                    let bgClass = 'bg-gray-700 text-gray-300';
                    if (data.news_context.sentiment === 'CRITICAL_POSITIVE') bgClass = 'bg-green-600 text-white';
                    if (data.news_context.sentiment === 'CRITICAL_NEGATIVE') bgClass = 'bg-red-600 text-white';
                    sentimentEl.className = `text-xs px-2 py-0.5 rounded font-bold ${bgClass}`;
                }
                if (headlineEl) headlineEl.textContent = data.news_context.headline;
                if (timeEl) {
                    const newsDate = new Date(data.news_context.processed_at);
                    timeEl.textContent = newsDate.toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'});
                }
                if (linkEl && data.news_context.url) {
                    linkEl.href = data.news_context.url;
                    linkEl.classList.remove('hidden');
                }
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

export const hideSignalDetails = () => {
    UI.signalDetails.backdrop.classList.add('hidden');
    if (signalDetailsInterval) clearInterval(signalDetailsInterval);
    if (signalDetailsClockInterval) clearInterval(signalDetailsClockInterval);
    signalDetailsInterval = null;
    signalDetailsClockInterval = null;
};

// === NOWOŚĆ: Logika Quantum Lab (V4) ===

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
    
    if (!year || !trials || trials < 10) {
        UI.quantumModal.statusMessage.textContent = "Podaj poprawny rok i min. 10 prób.";
        UI.quantumModal.statusMessage.className = "text-red-400 text-sm mt-3 h-4 text-center";
        return;
    }

    try {
        UI.quantumModal.startBtn.disabled = true;
        UI.quantumModal.statusMessage.textContent = "Uruchamianie silnika...";
        UI.quantumModal.statusMessage.className = "text-yellow-400 text-sm mt-3 h-4 text-center";
        
        await api.startOptimization({ target_year: year, n_trials: trials });
        
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
        
        // === OBSŁUGA KLIKNIĘCIA W 'UŻYJ' ===
        // Dodajemy listenery do nowo wygenerowanych przycisków
        const useButtons = UI.optimizationResultsModal.content.querySelectorAll('.use-params-btn');
        useButtons.forEach(btn => {
            btn.addEventListener('click', (e) => {
                const paramsData = e.currentTarget.dataset.params;
                try {
                    const params = JSON.parse(paramsData);
                    // 1. Zamknij okno wyników
                    hideOptimizationResults();
                    
                    // 2. Otwórz okno H3 Live (jeśli nie otwarte)
                    // Lub jeśli chcemy tylko wypełnić formularz przed otwarciem...
                    // Najlepszy flow: Otwórz modal H3 Live i wypełnij go.
                    showH3LiveParamsModal();
                    
                    // 3. Wypełnij pola (z małym opóźnieniem dla pewności renderowania)
                    setTimeout(() => {
                        if (UI.h3LiveModal.percentile) UI.h3LiveModal.percentile.value = params.h3_percentile;
                        if (UI.h3LiveModal.mass) UI.h3LiveModal.mass.value = params.h3_m_sq_threshold;
                        if (UI.h3LiveModal.minScore) UI.h3LiveModal.minScore.value = params.h3_min_score;
                        if (UI.h3LiveModal.tp) UI.h3LiveModal.tp.value = params.h3_tp_multiplier;
                        if (UI.h3LiveModal.sl) UI.h3LiveModal.sl.value = params.h3_sl_multiplier;
                        if (UI.h3LiveModal.maxHold) UI.h3LiveModal.maxHold.value = params.h3_max_hold;
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
