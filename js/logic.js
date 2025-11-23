import { api } from './api.js';
import { state, logger, PORTFOLIO_QUOTE_POLL_INTERVAL, ALERT_POLL_INTERVAL, REPORT_PAGE_SIZE, H3_DEEP_DIVE_POLL_INTERVAL, WORKER_POLL_INTERVAL } from './state.js';
import { renderers } from './ui.js';

let UI = null;
// Interwały
let workerPollInterval = null;
let systemAlertPollInterval = null;
let signalDetailsInterval = null;
let signalDetailsClockInterval = null;
let optimizationPollingInterval = null; 
let h3DeepDivePollingInterval = null;

export const setUI = (uiInstance) => {
    UI = uiInstance;
};

const updateElement = (el, content, isHtml = false) => {
    if (!el) return;
    if (isHtml) el.innerHTML = content;
    else el.textContent = content;
};

const showLoading = (message = "Ładowanie danych...") => {
    if (UI && UI.mainContent) UI.mainContent.innerHTML = renderers.loading(message);
};

const showError = (message, retryFunction = null) => {
    if (UI && UI.mainContent) {
        UI.mainContent.innerHTML = renderers.error(message);
        // Jeśli przekazano funkcję ponawiania, można by dodać przycisk (opcjonalne rozszerzenie)
    }
};

// --- ZEGAR RYNKOWY (Dla Modala) ---
const updateMarketTimeDisplay = () => {
    if (!UI || !UI.signalDetails || !UI.signalDetails.nyTime) return;

    const now = new Date();
    const nyTimeOptions = { timeZone: 'America/New_York', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false };
    const nyTimeStr = now.toLocaleTimeString('en-US', nyTimeOptions);
    UI.signalDetails.nyTime.textContent = nyTimeStr;

    const openHour = 15; // 9:30 AM NY to 15:30 PL (zima) / 14:30 (lato) - uproszczenie na sztywno
    // Bardziej precyzyjne byłoby użycie biblioteki moment-timezone, ale tu JS vanilla
    // Uproszczona logika odliczania
    UI.signalDetails.countdown.textContent = "---";
};

// =================================================================
// GŁÓWNE WIDOKI
// =================================================================

export const showDashboard = async () => {
    if (!UI) return;
    
    // 1. Renderuj szkielet (natychmiast)
    UI.mainContent.innerHTML = renderers.dashboard();
    
    // 2. Wypełnij danymi (asynchronicznie)
    await refreshSidebarData();
    // Status workera zaktualizuje się sam przez polling
};

export const showPortfolio = async () => {
    showLoading("Pobieranie Twojego portfela...");
    try {
        const holdings = await api.getPortfolio();
        state.portfolio = Array.isArray(holdings) ? holdings : [];
        
        const quotes = {};
        // Pobierz wyceny live dla posiadanych akcji
        if (state.portfolio.length > 0) {
            const promises = state.portfolio.map(async (h) => {
                try {
                    const q = await api.getLiveQuote(h.ticker);
                    if (q) quotes[h.ticker] = q;
                } catch(e) { /* ignoruj błędy pojedynczych tickerów */ }
            });
            await Promise.all(promises);
        }
        UI.mainContent.innerHTML = renderers.portfolio(state.portfolio, quotes);
    } catch (error) {
        showError(`Nie udało się pobrać portfela. <br>Szczegóły: ${error.message}`);
    }
};

export const showTransactions = async () => {
    showLoading("Pobieranie historii transakcji...");
    try {
        const history = await api.getTransactionHistory();
        const safeHistory = Array.isArray(history) ? history : [];
        UI.mainContent.innerHTML = renderers.transactions(safeHistory);
    } catch (error) {
        showError(`Nie udało się pobrać historii. <br>Szczegóły: ${error.message}`);
    }
};

export const showAgentReport = async () => {
    loadAgentReportPage(1);
};

export const loadAgentReportPage = async (page) => {
    showLoading(`Pobieranie raportu Agenta (Strona ${page})...`);
    try {
        state.currentReportPage = page;
        const reportData = await api.getVirtualAgentReport(page, REPORT_PAGE_SIZE);
        
        if (!reportData) throw new Error("Otrzymano puste dane z API");
        
        UI.mainContent.innerHTML = renderers.agentReport(reportData);
    } catch (error) {
        logger.error("Agent Report Error:", error);
        // Przycisk odświeżania w przypadku błędu
        UI.mainContent.innerHTML = `
            <div class="flex flex-col items-center justify-center h-64 text-center">
                <div class="text-red-500 text-xl mb-4 font-bold">Błąd pobierania raportu</div>
                <p class="text-gray-400 mb-6">${error.message}</p>
                <button id="retry-report-btn" class="bg-sky-600 hover:bg-sky-700 text-white font-bold py-2 px-4 rounded transition">
                    Spróbuj ponownie
                </button>
            </div>
        `;
        // Dodaj listener do przycisku retry (musi być po wstawieniu do DOM)
        setTimeout(() => {
            const btn = document.getElementById('retry-report-btn');
            if(btn) btn.onclick = () => loadAgentReportPage(page);
        }, 100);
    }
};

// =================================================================
// POLLING I DANE TŁA
// =================================================================

export const refreshSidebarData = async () => {
    if (!UI) return;

    // Faza 1
    api.getPhase1Candidates()
        .then(data => {
            state.phase1 = Array.isArray(data) ? data : [];
            updateElement(UI.phase1.count, state.phase1.length);
            updateElement(UI.phase1.list, renderers.phase1List(state.phase1), true);
        })
        .catch(() => {
            updateElement(UI.phase1.count, "Err");
            updateElement(UI.phase1.list, '<p class="text-xs text-red-500 p-2">Błąd połączenia</p>', true);
        });

    // Faza 3
    api.getPhase3Signals()
        .then(data => {
            state.phase3 = Array.isArray(data) ? data : [];
            updateElement(UI.phase3.count, state.phase3.length);
            updateElement(UI.phase3.list, renderers.phase3List(state.phase3), true);
            // Aktualizuj licznik na dashboardzie jeśli jest widoczny
            const dashboardSignals = document.getElementById('dashboard-active-signals');
            if (dashboardSignals) dashboardSignals.textContent = state.phase3.length;
        })
        .catch(() => {
            updateElement(UI.phase3.count, "Err");
        });
};

export const pollWorkerStatus = () => {
    if (workerPollInterval) clearInterval(workerPollInterval);

    const check = async () => {
        try {
            const status = await api.getWorkerStatus();
            
            // Jeśli API nie odpowiada (status null), nie robimy nic, żeby nie psuć UI
            if (!status || !status.progress) return;
            
            state.workerStatus = status;
            
            // Aktualizacja tekstu statusu w Sidebarze
            if (UI.workerStatusText) {
                UI.workerStatusText.textContent = status.status;
                // Stylizacja w zależności od stanu
                let bgClass = 'bg-gray-700 text-gray-200';
                if (status.status.includes('RUNNING') || status.status.includes('BUSY')) {
                    bgClass = 'bg-green-900 text-green-300 animate-pulse';
                } else if (status.status.includes('ERROR')) {
                    bgClass = 'bg-red-900 text-red-300';
                }
                UI.workerStatusText.className = `font-mono px-2 py-1 rounded-md text-xs ${bgClass}`;
            }

            if (UI.heartbeatStatus && status.last_heartbeat_utc) {
                const hb = new Date(status.last_heartbeat_utc);
                UI.heartbeatStatus.textContent = hb.toLocaleTimeString();
            }
            
            // Aktualizacja Dashboardu (jeśli jest widoczny)
            const progressBar = document.getElementById('progress-bar');
            const progressText = document.getElementById('progress-text');
            const scanLog = document.getElementById('scan-log');
            const currentPhaseTxt = document.getElementById('dashboard-current-phase');
            const workerStatusBig = document.getElementById('dashboard-worker-status');

            if (workerStatusBig) workerStatusBig.textContent = status.status;

            if (progressBar && progressText && status.progress.total > 0) {
                const pct = Math.round((status.progress.processed / status.progress.total) * 100);
                progressBar.style.width = `${pct}%`;
                progressText.textContent = `${status.progress.processed} / ${status.progress.total}`;
            }
            
            if (scanLog && scanLog.textContent !== status.log) {
                const container = document.getElementById('scan-log-container');
                // Sprawdź czy scroll jest na górze przed aktualizacją
                const isAtTop = container ? container.scrollTop < 50 : true;
                
                scanLog.textContent = status.log || "Brak logów.";
                
                // Auto-scroll do góry (najnowsze logi)
                if (container && isAtTop) {
                    container.scrollTop = 0;
                }
            }

            if (currentPhaseTxt) {
                currentPhaseTxt.textContent = `Faza: ${status.phase}`;
            }

        } catch (e) {
            // Ignorujemy błędy w pętli pollingu, żeby nie spamować konsoli
            // api.js już obsłużył status offline
        }
    };
    
    check(); // Pierwsze wywołanie
    workerPollInterval = setInterval(check, WORKER_POLL_INTERVAL);
};

export const pollSystemAlerts = () => {
    if (systemAlertPollInterval) clearInterval(systemAlertPollInterval);
    
    systemAlertPollInterval = setInterval(async () => {
        try {
            const alert = await api.getSystemAlert();
            if (alert && alert.message && alert.message !== 'NONE') {
                showSystemAlert(alert.message);
            }
        } catch(e) {}
    }, ALERT_POLL_INTERVAL);
};

const showSystemAlert = (msg) => {
    if (!UI.alertContainer) return;
    const div = document.createElement('div');
    div.className = 'alert-bar bg-red-600 text-white px-4 py-3 rounded shadow-lg flex justify-between items-center mb-2 animate-bounce border border-red-400';
    div.innerHTML = `
        <div class="flex items-center">
            <i data-lucide="alert-triangle" class="w-5 h-5 mr-2"></i>
            <span class="font-bold text-sm">${msg}</span>
        </div>
        <button class="ml-4 font-bold hover:text-gray-200 focus:outline-none">X</button>
    `;
    
    const closeBtn = div.querySelector('button');
    closeBtn.onclick = () => div.remove();

    UI.alertContainer.appendChild(div);
    // Ikony dla dynamicznie dodanego elementu
    if (window.lucide) window.lucide.createIcons();
    
    // Auto-close po 15s
    setTimeout(() => { if(div.parentNode) div.remove(); }, 15000);
};

// =================================================================
// MODALE I AKCJE
// =================================================================

// --- BUY MODAL ---
export const showBuyModal = (ticker) => {
    UI.buyModal.tickerSpan.textContent = ticker;
    UI.buyModal.quantityInput.value = "";
    UI.buyModal.priceInput.value = "";
    // Próba pobrania ceny live
    api.getLiveQuote(ticker).then(q => {
        if (q && q['05. price']) {
            UI.buyModal.priceInput.value = parseFloat(q['05. price']).toFixed(2);
        }
    }).catch(() => {}); 
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
        showPortfolio(); // Odśwież widok
        showSystemAlert(`Kupiono ${qty} akcji ${ticker}.`);
    } catch (e) {
        alert(e.message);
    } finally {
        UI.buyModal.confirmBtn.disabled = false;
        UI.buyModal.confirmBtn.textContent = "Inwestuj";
    }
};

// --- SELL MODAL ---
export const showSellModal = (ticker, maxQty) => {
    UI.sellModal.tickerSpan.textContent = ticker;
    UI.sellModal.maxQuantitySpan.textContent = maxQty;
    UI.sellModal.quantityInput.value = maxQty; 
    UI.sellModal.quantityInput.max = maxQty;
    UI.sellModal.priceInput.value = "";
    
    api.getLiveQuote(ticker).then(q => {
        if (q && q['05. price']) UI.sellModal.priceInput.value = parseFloat(q['05. price']).toFixed(2);
    }).catch(() => {});
    
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
        UI.sellModal.confirmBtn.textContent = "Przetwarzanie...";
        
        await api.sellStock({ ticker, quantity: qty, price_per_share: price });
        
        hideSellModal();
        showPortfolio();
        showSystemAlert(`Sprzedano ${qty} akcji ${ticker}.`);
    } catch (e) {
        alert(e.message);
    } finally {
        UI.sellModal.confirmBtn.disabled = false;
        UI.sellModal.confirmBtn.textContent = "Realizuj";
    }
};

// --- BACKTEST REQUEST ---
export const handleYearBacktestRequest = async () => {
    const input = document.getElementById('backtest-year-input');
    const status = document.getElementById('backtest-status-message');
    
    if (!input || !input.value) {
        if(status) status.textContent = "Podaj rok.";
        return;
    }

    const getVal = (id, def) => {
        const el = document.getElementById(id);
        return (el && el.value !== "") ? el.value : def;
    };

    const params = {
        h3_percentile: getVal('h3-param-percentile', 0.95),
        h3_m_sq_threshold: getVal('h3-param-mass', -0.5),
        h3_min_score: getVal('h3-param-min-score', 1.0),
        h3_tp_multiplier: getVal('h3-param-tp', 5.0),
        h3_sl_multiplier: getVal('h3-param-sl', 2.0),
        h3_max_hold: getVal('h3-param-hold', 5),
        setup_name: getVal('h3-param-name', "")
    };
    
    logger.info(`Wysyłanie zlecenia Backtestu (${input.value}). Params:`, params);

    try {
        if(status) {
            status.textContent = "Wysyłanie zlecenia...";
            status.className = "text-yellow-400 text-sm mt-3 h-4";
        }
        
        await api.requestBacktest(input.value, params);
        
        if(status) {
            status.textContent = "Zlecenie przyjęte. Sprawdź status Workera.";
            status.className = "text-green-400 text-sm mt-3 h-4";
        }
    } catch (e) {
        if(status) {
            status.textContent = "Błąd: " + e.message;
            status.className = "text-red-400 text-sm mt-3 h-4";
        }
    }
};

export const handleCsvExport = async () => {
    const status = document.getElementById('csv-export-status-message');
    try {
        if(status) status.textContent = "Generowanie CSV...";
        // Bezpośrednie przekierowanie wywołuje pobieranie pliku
        window.location.href = api.getExportCsvUrl();
        setTimeout(() => { if(status) status.textContent = "Pobieranie rozpoczęte."; }, 2000);
    } catch(e) {
        if(status) status.textContent = "Błąd pobierania.";
    }
};

// --- H3 DEEP DIVE ---
export const showH3DeepDiveModal = () => {
    UI.h3DeepDiveModal.backdrop.classList.remove('hidden');
    UI.h3DeepDiveModal.statusMsg.textContent = "";
    UI.h3DeepDiveModal.content.innerHTML = '<p class="text-gray-500">Sprawdzanie ostatnich raportów...</p>';
    
    api.getH3DeepDiveReport().then(r => {
        if (r && r.report_text) {
            UI.h3DeepDiveModal.content.innerHTML = `<pre class="whitespace-pre-wrap text-xs font-mono text-green-300">${r.report_text}</pre>`;
        } else {
            UI.h3DeepDiveModal.content.innerHTML = '<p class="text-gray-500">Brak ostatniego raportu. Uruchom analizę.</p>';
        }
    }).catch(e => {
        UI.h3DeepDiveModal.content.innerHTML = `<p class="text-red-500 text-sm">Błąd połączenia: ${e.message}</p>`;
    });
};

export const hideH3DeepDiveModal = () => {
    UI.h3DeepDiveModal.backdrop.classList.add('hidden');
    if (h3DeepDivePollingInterval) {
        clearInterval(h3DeepDivePollingInterval);
        h3DeepDivePollingInterval = null;
    }
};

export const handleRunH3DeepDive = async () => {
    const year = UI.h3DeepDiveModal.yearInput.value;
    if (!year) return;
    
    try {
        UI.h3DeepDiveModal.runBtn.disabled = true;
        UI.h3DeepDiveModal.statusMsg.textContent = "Wysyłanie zlecenia...";
        
        await api.requestH3DeepDive(parseInt(year));
        
        UI.h3DeepDiveModal.statusMsg.textContent = "Przetwarzanie... Proszę czekać (ok. 30s).";
        
        // Polling wyniku
        h3DeepDivePollingInterval = setInterval(async () => {
            try {
                const rep = await api.getH3DeepDiveReport();
                if (rep.status === 'DONE') {
                    UI.h3DeepDiveModal.content.innerHTML = `<pre class="whitespace-pre-wrap text-xs font-mono text-green-300">${rep.report_text}</pre>`;
                    UI.h3DeepDiveModal.statusMsg.textContent = "Analiza zakończona.";
                    UI.h3DeepDiveModal.runBtn.disabled = false;
                    clearInterval(h3DeepDivePollingInterval);
                } else if (rep.status === 'ERROR') {
                    UI.h3DeepDiveModal.content.textContent = "Błąd Workera: " + rep.report_text;
                    UI.h3DeepDiveModal.runBtn.disabled = false;
                    clearInterval(h3DeepDivePollingInterval);
                }
            } catch(e) {}
        }, H3_DEEP_DIVE_POLL_INTERVAL);
        
    } catch (e) {
        UI.h3DeepDiveModal.statusMsg.textContent = "Błąd API: " + e.message;
        UI.h3DeepDiveModal.runBtn.disabled = false;
    }
};

// --- AI OPTIMIZER ---
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
    UI.aiReportModal.content.innerHTML = renderers.loading("Pobieranie raportu AI...");
    
    try {
        const report = await api.getAIOptimizerReport();
        if (report.status === 'DONE') {
            UI.aiReportModal.content.innerHTML = `<pre class="whitespace-pre-wrap font-mono text-xs text-green-300">${report.report_text}</pre>`;
        } else if (report.status === 'PROCESSING') {
            UI.aiReportModal.content.innerHTML = "<p class='text-yellow-400 p-4 text-center'>Raport jest w trakcie generowania przez Agenta AI...</p>";
        } else {
            UI.aiReportModal.content.innerHTML = "<p class='text-gray-500 p-4 text-center'>Brak raportu. Uruchom 'Analiza AI'.</p>";
        }
    } catch(e) {
        UI.aiReportModal.content.innerHTML = `<p class="text-red-500 p-4 text-center">Błąd pobierania raportu: ${e.message}</p>`;
    }
};

export const hideAIReportModal = () => {
    UI.aiReportModal.backdrop.classList.add('hidden');
};

// --- H3 LIVE MODAL ---
export const showH3LiveParamsModal = () => { UI.h3LiveModal.backdrop.classList.remove('hidden'); };
export const hideH3LiveParamsModal = () => { UI.h3LiveModal.backdrop.classList.add('hidden'); };

export const handleRunH3LiveScan = async () => {
    const getVal = (el, def) => (el && el.value !== "") ? el.value : def;

    const params = {
        h3_percentile: getVal(UI.h3LiveModal.percentile, 0.95),
        h3_m_sq_threshold: getVal(UI.h3LiveModal.mass, -0.5),
        h3_min_score: getVal(UI.h3LiveModal.minScore, 1.0),
        h3_tp_multiplier: getVal(UI.h3LiveModal.tp, 5.0),
        h3_sl_multiplier: getVal(UI.h3LiveModal.sl, 2.0),
        h3_max_hold: getVal(UI.h3LiveModal.maxHold, 5)
    };
    
    try {
        UI.h3LiveModal.startBtn.disabled = true;
        UI.h3LiveModal.startBtn.textContent = "Uruchamianie...";
        
        await api.sendWorkerControl('start_phase3', params);
        
        hideH3LiveParamsModal();
        showSystemAlert("Rozpoczęto Skanowanie H3 Live.");
        // Automatycznie odśwież sidebar po chwili
        setTimeout(refreshSidebarData, 2000);
    } catch (e) {
        alert("Błąd startu H3: " + e.message);
    } finally {
        UI.h3LiveModal.startBtn.disabled = false;
        UI.h3LiveModal.startBtn.textContent = "Start Skanowania";
    }
};

// --- SIGNAL DETAILS ---
export const showSignalDetails = async (ticker) => {
    UI.signalDetails.backdrop.classList.remove('hidden');
    // Reset widoku
    UI.signalDetails.ticker.textContent = ticker;
    UI.signalDetails.companyName.textContent = "Pobieranie danych...";
    UI.signalDetails.currentPrice.textContent = "---";
    UI.signalDetails.validityBadge.textContent = "Checking...";
    UI.signalDetails.validityBadge.className = "text-sm px-2 py-1 rounded bg-gray-700 text-gray-400 font-mono";
    UI.signalDetails.validityMessage.classList.add('hidden');
    
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
            if (!data) return; 

            if (data.status === 'INVALIDATED' && !data.company) {
                 UI.signalDetails.validityBadge.textContent = "INVALID";
                 UI.signalDetails.validityBadge.className = "text-sm px-2 py-1 rounded bg-red-900 text-red-200 font-mono";
                 UI.signalDetails.validityMessage.textContent = data.reason;
                 UI.signalDetails.validityMessage.classList.remove('hidden');
                 return;
            }
            
            // Wypełnianie danych UI... (bez zmian w logice mapowania, tylko zabezpieczenie przed nullami)
            if (data.company) {
                UI.signalDetails.companyName.textContent = data.company.name || "N/A";
                UI.signalDetails.sector.textContent = data.company.sector || "N/A";
                UI.signalDetails.industry.textContent = data.company.industry || "N/A";
                UI.signalDetails.description.textContent = data.company.description || "Brak opisu.";
            }
            
            if (data.market_data) {
                const price = parseFloat(data.market_data.current_price);
                UI.signalDetails.currentPrice.textContent = price > 0 ? price.toFixed(2) : "---";
                
                const priceLabel = UI.signalDetails.currentPrice.previousElementSibling;
                const source = data.market_data.price_source;
                
                if (priceLabel) {
                    if (source === 'extended_hours') {
                        priceLabel.textContent = "Cena (Extended)";
                        priceLabel.className = "text-purple-400 text-sm font-bold animate-pulse";
                    } else {
                         priceLabel.textContent = "Cena Aktualna";
                         priceLabel.className = "text-gray-400 text-sm";
                    }
                }

                UI.signalDetails.changePercent.textContent = data.market_data.change_percent || "0%";
                const changeVal = parseFloat((data.market_data.change_percent || "0").replace('%', ''));
                UI.signalDetails.changePercent.className = `font-mono text-lg font-bold ${changeVal >= 0 ? 'text-green-400' : 'text-red-400'}`;
                UI.signalDetails.marketStatus.textContent = data.market_data.market_status || "Unknown";
            }
            
            if (data.setup) {
                const fmt = (v) => v ? v.toFixed(2) : "---";
                UI.signalDetails.entry.textContent = fmt(data.setup.entry_price);
                UI.signalDetails.tp.textContent = fmt(data.setup.take_profit);
                UI.signalDetails.sl.textContent = fmt(data.setup.stop_loss);
                UI.signalDetails.rr.textContent = fmt(data.setup.risk_reward);
                
                if(data.setup.generation_date) {
                    UI.signalDetails.generationDate.textContent = new Date(data.setup.generation_date).toLocaleString('pl-PL');
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

            // Obsługa newsów (jeśli są w response)
            if (data.news_context && newsContainer) {
                newsContainer.classList.remove('hidden');
                const sentimentEl = document.getElementById('sd-news-sentiment');
                const headlineEl = document.getElementById('sd-news-headline');
                const linkEl = document.getElementById('sd-news-link');

                if (sentimentEl) sentimentEl.textContent = data.news_context.sentiment;
                if (headlineEl) headlineEl.textContent = data.news_context.headline;
                if (linkEl && data.news_context.url) linkEl.href = data.news_context.url;
            }
            
        } catch (e) {
            // Błąd pobierania szczegółów (np. ticker usunięty)
            UI.signalDetails.companyName.textContent = "Błąd danych";
        }
    };

    fetchData(); // Pierwsze pobranie
    if (signalDetailsInterval) clearInterval(signalDetailsInterval);
    signalDetailsInterval = setInterval(fetchData, 3000); // Odświeżanie co 3s
};

export const hideSignalDetails = () => {
    UI.signalDetails.backdrop.classList.add('hidden');
    if (signalDetailsInterval) clearInterval(signalDetailsInterval);
    if (signalDetailsClockInterval) clearInterval(signalDetailsClockInterval);
    signalDetailsInterval = null;
    signalDetailsClockInterval = null;
};

// --- QUANTUM LAB V4 ---
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
            showOptimizationResults(); 
        }, 1500);
        
    } catch (e) {
        UI.quantumModal.statusMessage.textContent = "Błąd: " + e.message;
        UI.quantumModal.statusMessage.className = "text-red-400 text-sm mt-3 h-4 text-center";
        UI.quantumModal.startBtn.disabled = false;
    }
};

export const showOptimizationResults = async () => {
    UI.optimizationResultsModal.backdrop.classList.remove('hidden');
    UI.optimizationResultsModal.content.innerHTML = renderers.loading("Pobieranie wyników Optuny...");
    
    const fetchResults = async () => {
        try {
            const results = await api.getOptimizationResults();
            if (!results) return;

            UI.optimizationResultsModal.content.innerHTML = renderers.optimizationResults(results);
            
            // Jeśli zakończono, zatrzymaj polling
            if (results.status === 'COMPLETED' || results.status === 'FAILED') {
                if (optimizationPollingInterval) {
                    clearInterval(optimizationPollingInterval);
                    optimizationPollingInterval = null;
                }
            }
        } catch (e) {
            UI.optimizationResultsModal.content.innerHTML = renderers.error(e.message);
            if (optimizationPollingInterval) clearInterval(optimizationPollingInterval);
        }
    };

    await fetchResults();
    if (optimizationPollingInterval) clearInterval(optimizationPollingInterval);
    optimizationPollingInterval = setInterval(fetchResults, 2000);
};

export const hideOptimizationResults = () => {
    UI.optimizationResultsModal.backdrop.classList.add('hidden');
    if (optimizationPollingInterval) {
        clearInterval(optimizationPollingInterval);
        optimizationPollingInterval = null;
    }
};
