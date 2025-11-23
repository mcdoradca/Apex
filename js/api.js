import { API_BASE_URL, logger, REPORT_PAGE_SIZE, state } from './state.js';

// Funkcja aktualizująca kropkę statusu w UI
const updateApiStatus = (status) => {
    const el = document.getElementById('api-status');
    if (el) {
        if (status === 'online') {
            el.innerHTML = '<span class="h-2 w-2 rounded-full bg-green-500 mr-2"></span>Online';
            state.isApiOnline = true;
        } else if (status === 'loading') {
            el.innerHTML = '<span class="h-2 w-2 rounded-full bg-yellow-500 mr-2 animate-pulse"></span>Łączenie...';
        } else {
            el.innerHTML = '<span class="h-2 w-2 rounded-full bg-red-500 mr-2"></span>Offline';
            state.isApiOnline = false;
        }
    }
};

// Główna funkcja fetchująca z obsługą błędów i timeoutów
const apiRequest = async (endpoint, options = {}, retries = 1) => {
    const url = endpoint ? `${API_BASE_URL}/${endpoint}` : API_BASE_URL;
    
    // Domyślne nagłówki
    const defaultOptions = {
        ...options,
        headers: {
            'Accept': 'application/json',
            ...options.headers
        }
    };

    for (let i = 0; i <= retries; i++) {
        try {
            // AbortController do obsługi timeoutu (np. 15 sekund)
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 15000);
            
            const response = await fetch(url, { 
                ...defaultOptions, 
                signal: controller.signal 
            });
            
            clearTimeout(timeoutId);
            updateApiStatus('online');
            
            // Obsługa HTTP Errors
            if (!response.ok) {
                // Próba odczytania szczegółów błędu z JSONa
                let errorDetail = response.statusText;
                try {
                    const errorJson = await response.json();
                    if (errorJson.detail) errorDetail = errorJson.detail;
                } catch (e) {
                    // Jeśli nie ma JSONa, używamy text() lub statusText
                }

                // Specyficzne błędy
                if (response.status === 404) throw new Error(`Nie znaleziono zasobu (404)`);
                if (response.status === 422) throw new Error(`Błąd walidacji danych (422): ${errorDetail}`);
                if (response.status === 500) throw new Error(`Błąd wewnętrzny serwera (500)`);
                if (response.status === 503) throw new Error(`Serwer niedostępny (503) - Budzenie maszyny...`);

                throw new Error(`API Błąd (${response.status}): ${errorDetail}`);
            }

            // Obsługa pustych odpowiedzi (204 No Content)
            if (response.status === 204) return null;
            
            // Próba parsowania JSON
            try {
                return await response.json();
            } catch (parseError) {
                // Jeśli odpowiedź jest tekstem (np. CSV)
                const text = await response.text();
                if (text) return text; // Zwracamy jako tekst (dla CSV)
                return null;
            }

        } catch (error) {
            const isLastAttempt = i === retries;
            
            if (error.name === 'AbortError') {
                logger.warn(`Timeout zapytania do ${url}`);
            } else {
                logger.warn(`Próba ${i + 1}/${retries + 1} nieudana dla ${url}: ${error.message}`);
            }

            if (isLastAttempt) {
                updateApiStatus('offline');
                logger.error(`Krytyczny błąd API dla ${url}:`, error);
                throw error; // Rzucamy dalej do UI
            }
            
            // Czekamy chwilę przed retry (backoff)
            await new Promise(resolve => setTimeout(resolve, 1000 * (i + 1)));
        }
    }
};

export const api = {
    // --- Worker & System ---
    getWorkerStatus: () => apiRequest('api/v1/worker/status', {}, 0), // Bez retry dla statusu (polling)
    
    sendWorkerControl: (action, params = null) => apiRequest(`api/v1/worker/control/${action}`, { 
        method: 'POST',
        headers: params ? { 'Content-Type': 'application/json' } : {},
        body: params ? JSON.stringify(params) : null
    }),
    
    getSystemAlert: () => apiRequest('api/v1/system/alert', {}, 0),
    getApiRootStatus: () => apiRequest('', {}, 2), // Retry przy starcie

    // --- Fazy Analizy ---
    getPhase1Candidates: () => apiRequest('api/v1/candidates/phase1'),
    getPhase2Results: () => apiRequest('api/v1/results/phase2'),
    getPhase3Signals: () => apiRequest('api/v1/signals/phase3'),
    getSignalDetails: (ticker) => apiRequest(`api/v1/signal/${ticker}/details`),
    
    // --- Portfel i Rynek ---
    getLiveQuote: (ticker) => apiRequest(`api/v1/quote/${ticker}`),
    addToWatchlist: (ticker) => apiRequest(`api/v1/watchlist/${ticker}`, { method: 'POST' }),
    getPortfolio: () => apiRequest('api/v1/portfolio'),
    buyStock: (data) => apiRequest('api/v1/portfolio/buy', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) }),
    sellStock: (data) => apiRequest('api/v1/portfolio/sell', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) }),
    getTransactionHistory: () => apiRequest('api/v1/transactions'),
    
    // --- Raporty i Narzędzia ---
    getVirtualAgentReport: (page = 1, pageSize = REPORT_PAGE_SIZE) => apiRequest(`api/v1/virtual-agent/report?page=${page}&page_size=${pageSize}`, {}, 2),
    
    requestBacktest: (year, params = null) => apiRequest('api/v1/backtest/request', { 
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ year: year, parameters: params })
    }),
    
    requestAIOptimizer: () => apiRequest('api/v1/ai-optimizer/request', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({})
    }),
    getAIOptimizerReport: () => apiRequest('api/v1/ai-optimizer/report'),
    
    requestH3DeepDive: (year) => apiRequest('api/v1/analysis/h3-deep-dive', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ year: year })
    }),
    getH3DeepDiveReport: () => apiRequest('api/v1/analysis/h3-deep-dive-report'),
    
    getExportCsvUrl: () => `${API_BASE_URL}/api/v1/export/trades.csv`,

    // --- QUANTUM LAB V4 ---
    startOptimization: (requestData) => apiRequest('api/v1/optimization/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestData)
    }),
    
    getOptimizationResults: () => apiRequest('api/v1/optimization/results')
};
