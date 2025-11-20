import { api } from './api.js';
import { state, logger, PORTFOLIO_QUOTE_POLL_INTERVAL, ALERT_POLL_INTERVAL, REPORT_PAGE_SIZE, AI_OPTIMIZER_POLL_INTERVAL, H3_DEEP_DIVE_POLL_INTERVAL } from './state.js';
import { renderers } from './ui.js';

let UI = null;

export const setUI = (uiInstance) => {
    UI = uiInstance;
};

// === Funkcje Pomocnicze ===
const updateElement = (el, content, isHtml = false) => {
    if (!el) return;
    if (isHtml) el.innerHTML = content;
    else el.textContent = content;
};

const showLoading = () => {
    if (UI && UI.mainContent) UI.mainContent.innerHTML = renderers.loading("Ładowanie danych...");
};

// === Główne Widoki ===

export const showDashboard = async () => {
    if (!UI) return;
    UI.mainContent.innerHTML = renderers.dashboard();
    
    // Odśwież liczniki na dashboardzie
    try {
        const countData = await api.getDiscardedCount();
        // Tutaj można dodać logikę wyświetlania odrzuconych sygnałów, jeśli UI to przewiduje
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
        
        // Pobierz aktualne ceny dla wszystkich tickerów w portfelu
        const tickers = holdings.map(h => h.ticker);
        const quotes = {};
        
        if (tickers.length > 0) {
            // Tutaj w przyszłości można zoptymalizować na jedno zapytanie bulk,
            // na razie pobieramy pojedynczo lub korzystamy z cache w state
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

// === Obsługa Sidebar (Fazy) ===

export const refreshSidebarData = async () => {
    try {
        // Faza 1
        const phase1Data = await api.getPhase1Candidates();
        state.phase1 = phase1Data || [];
        updateElement(UI.phase1.count, state.phase1.length);
        updateElement(UI.phase1.list, renderers.phase1List(state.phase1), true);

        // Faza 3 (Sygnały)
        const phase3Data = await api.getPhase3Signals();
        state.phase3 = phase3Data || [];
        updateElement(UI.phase3.count, state.phase3.length);
        updateElement(UI.phase3.list, renderers.phase3List(state.phase3), true);

    } catch (e) {
        logger.error("Błąd odświeżania sidebaru:", e);
    }
};

// === Polling Statusu Workera ===

export const pollWorkerStatus = () => {
    const check = async () => {
        try {
            const status = await api.getWorkerStatus();
            state.workerStatus = status;
            
            // Aktualizacja tekstu statusu
            if (UI.workerStatusText) {
                UI.workerStatusText.textContent = status.status;
                UI.workerStatusText.className = `font-mono px-2 py-1 rounded-md text-xs ${
                    status.status.includes('RUNNING') || status.status.includes('BUSY') ? 'bg-green-900 text-green-300 animate-pulse' : 'bg-gray-700 text-gray-200'
                }`;
            }

            // Aktualizacja Heartbeat
            if (UI.heartbeatStatus) {
                const hb = new Date(status.last_heartbeat_utc);
                UI.heartbeatStatus.textContent = hb.toLocaleTimeString();
            }
            
            // Aktualizacja Progress Baru na Dashboardzie
            const progressBar = document.getElementById('progress-bar');
            const progressText = document.getElementById('progress-text');
            const scanLog = document.getElementById('scan-log');
            const currentPhaseTxt = document.getElementById('dashboard-current-phase');

            if (progressBar && status.progress.total > 0) {
                const pct = Math.round((status.progress.processed / status.progress.total) * 100);
                progressBar.style.width = `${pct}%`;
                progressText.textContent = `${status.progress.processed} / ${status.progress.total}`;
            }
            
            if (scanLog) {
                // Proste unikanie nadpisywania jeśli log jest ten sam, aby nie skakało
                if (scanLog.textContent !== status.log) scanLog.textContent = status.log;
                // Auto-scroll na dół
                const container = document.getElementById('scan-log-container');
                if (container) container.scrollTop = container.scrollHeight;
            }

            if (currentPhaseTxt) {
                currentPhaseTxt.textContent = `Faza: ${status.phase}`;
            }
            
            // Aktywne sygnały na dashboardzie
            const dashboardSignals = document.getElementById('dashboard-active-signals');
            if (dashboardSignals) {
                dashboardSignals.textContent = state.phase3.length;
            }

        } catch (e) {
            // logger.error("Błąd pollera:", e);
        }
    };
    
    check(); // Pierwsze wywołanie natychmiast
    setInterval(check, 2000); // Co 2 sekundy
};

// === Obsługa Alertów Systemowych ===
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


// === Obsługa Modali Transakcyjnych ===

// -- KUPNO --
export const showBuyModal = (ticker) => { /* Logika wywoływana z poziomu UI (jeśli dodamy przyciski kupna na liście) */ };
// (Obecnie brak przycisków kupna na liście, ale struktura jest gotowa)

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

// -- SPRZEDAŻ --
export const showSellModal = (ticker, maxQty) => {
    UI.sellModal.tickerSpan.textContent = ticker;
    UI.sellModal.maxQuantitySpan.textContent = maxQty;
    UI.sellModal.quantityInput.value = maxQty; // Domyślnie max
    UI.sellModal.quantityInput.max = maxQty;
    UI.sellModal.priceInput.value = "";
    
    // Pobierz aktualną cenę dla wygody
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

// === Obsługa Backtestu i AI ===

export const handleYearBacktestRequest = async () => {
    const input = document.getElementById('backtest-year-input');
    const status = document.getElementById('backtest-status-message');
    if (!input || !input.value) return;
    
    // Pobieranie parametrów H3 z UI
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

// === H3 Deep Dive Modal ===
export const showH3DeepDiveModal = () => {
    UI.h3DeepDiveModal.backdrop.classList.remove('hidden');
    UI.h3DeepDiveModal.statusMsg.textContent = "";
    UI.h3DeepDiveModal.content.innerHTML = '<p class="text-gray-500">Oczekiwanie na dane...</p>';
    // Spróbuj pobrać ostatni raport
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
        
        // Polluj o wynik
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

// === AI Optimizer ===
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
            // Renderowanie Markdown jako prosty tekst (można dodać parser MD później)
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

// === H3 Live Params Modal ===
export const showH3LiveParamsModal = () => {
    UI.h3LiveModal.backdrop.classList.remove('hidden');
};

export const hideH3LiveParamsModal = () => {
    UI.h3LiveModal.backdrop.classList.add('hidden');
};

export const handleRunH3LiveScan = async () => {
    // Pobierz parametry
    const params = {
        h3_percentile: UI.h3LiveModal.percentile.value,
        h3_m_sq_threshold: UI.h3LiveModal.mass.value,
        h3_min_score: UI.h3LiveModal.minScore.value,
        h3_tp_multiplier: UI.h3LiveModal.tp.value,
        h3_sl_multiplier: UI.h3LiveModal.sl.value
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

// === Signal Details Modal ===

export const showSignalDetails = async (ticker) => {
    UI.signalDetails.backdrop.classList.remove('hidden');
    // Reset widoku
    UI.signalDetails.ticker.textContent = ticker;
    UI.signalDetails.companyName.textContent = "Ładowanie...";
    UI.signalDetails.currentPrice.textContent = "---";
    UI.signalDetails.validityBadge.textContent = "Checking...";
    UI.signalDetails.validityBadge.className = "text-xs px-2 py-1 rounded bg-gray-700 text-gray-400";
    UI.signalDetails.validityMessage.classList.add('hidden');
    
    // Ukryj sekcję newsów na starcie
    const newsContainer = document.getElementById('sd-news-container');
    if (newsContainer) newsContainer.classList.add('hidden');

    try {
        const data = await api.getSignalDetails(ticker);
        
        if (data.status === 'INVALIDATED' && !data.company) {
             // Sygnał usunięty/nieważny
             UI.signalDetails.validityBadge.textContent = "INVALID";
             UI.signalDetails.validityBadge.className = "text-xs px-2 py-1 rounded bg-red-900 text-red-200";
             UI.signalDetails.validityMessage.textContent = data.reason;
             UI.signalDetails.validityMessage.classList.remove('hidden');
             return;
        }
        
        // Wypełnij dane firmy
        if (data.company) {
            UI.signalDetails.companyName.textContent = data.company.name;
            UI.signalDetails.sector.textContent = data.company.sector;
            UI.signalDetails.industry.textContent = data.company.industry;
        }
        
        // Wypełnij dane rynkowe
        if (data.market_data) {
            const price = parseFloat(data.market_data.current_price);
            UI.signalDetails.currentPrice.textContent = price > 0 ? price.toFixed(2) : "---";
            UI.signalDetails.changePercent.textContent = data.market_data.change_percent;
            
            // Kolor zmiany
            const changeVal = parseFloat(data.market_data.change_percent.replace('%', ''));
            UI.signalDetails.changePercent.className = `font-mono text-sm font-bold ${changeVal >= 0 ? 'text-green-400' : 'text-red-400'}`;
            
            UI.signalDetails.marketStatus.textContent = data.market_data.market_status;
        }
        
        // Wypełnij setup
        if (data.setup) {
            UI.signalDetails.entry.textContent = data.setup.entry_price ? data.setup.entry_price.toFixed(2) : "---";
            UI.signalDetails.tp.textContent = data.setup.take_profit ? data.setup.take_profit.toFixed(2) : "---";
            UI.signalDetails.sl.textContent = data.setup.stop_loss ? data.setup.stop_loss.toFixed(2) : "---";
            UI.signalDetails.rr.textContent = data.setup.risk_reward ? data.setup.risk_reward.toFixed(2) : "---";
            UI.signalDetails.generationDate.textContent = new Date(data.setup.generation_date).toLocaleString('pl-PL');
        }

        // Wypełnij status walidacji
        if (data.validity) {
             const isValid = data.validity.is_valid;
             UI.signalDetails.validityBadge.textContent = isValid ? "VALID" : "INVALID";
             UI.signalDetails.validityBadge.className = `text-xs px-2 py-1 rounded ${isValid ? 'bg-green-900 text-green-200' : 'bg-red-900 text-red-200'}`;
             
             if (!isValid) {
                 UI.signalDetails.validityMessage.textContent = data.validity.message;
                 UI.signalDetails.validityMessage.classList.remove('hidden');
             }
        }

        // === NOWOŚĆ: Obsługa News Sentiment Context ===
        if (data.news_context && newsContainer) {
            newsContainer.classList.remove('hidden');
            
            const sentimentEl = document.getElementById('sd-news-sentiment');
            const headlineEl = document.getElementById('sd-news-headline');
            const timeEl = document.getElementById('sd-news-time');
            const linkEl = document.getElementById('sd-news-link');

            if (sentimentEl) {
                sentimentEl.textContent = data.news_context.sentiment;
                // Kolorowanie badge'a sentymentu
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
            } else if (linkEl) {
                linkEl.classList.add('hidden');
            }
        }
        
    } catch (e) {
        UI.signalDetails.companyName.textContent = "Błąd pobierania danych";
        console.error(e);
    }
};

export const hideSignalDetails = () => {
    UI.signalDetails.backdrop.classList.add('hidden');
};
