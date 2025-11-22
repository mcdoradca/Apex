import { API_BASE_URL, logger, REPORT_PAGE_SIZE } from './state.js';

const updateApiStatus = (status) => {
    const el = document.getElementById('api-status');
    if (el) {
        if (status === 'online') el.innerHTML = '<span class="h-2 w-2 rounded-full bg-green-500 mr-2"></span>Online';
        else el.innerHTML = '<span class="h-2 w-2 rounded-full bg-red-500 mr-2"></span>Offline';
    }
};

const apiRequest = async (endpoint, options = {}) => {
    const url = endpoint ? `${API_BASE_URL}/${endpoint}` : API_BASE_URL;
    try {
        const response = await fetch(url, options);
        updateApiStatus('online');
        
        if (!response.ok) {
            let errorText = response.statusText;
            try {
                const errorJson = await response.json();
                errorText = errorJson.detail || errorText;
            } catch (e) {
                errorText = await response.text() || errorText;
            }
            
            logger.error(`API Error ${response.status} for ${url}: ${errorText}`);
            // Obsługa specyficznych błędów
            if (response.status === 404) throw new Error(`404 - Zasób nie istnieje`);
            if (response.status === 409) throw new Error(`409 - Konflikt: Zasób zajęty`);
            
            throw new Error(`Błąd serwera (${response.status}): ${errorText}`);
        }
        // Pusta odpowiedź 204
        if (response.status === 204 || response.headers.get("Content-Length") === "0") return null;
        
        return await response.json();
    } catch (error) {
         logger.error(`Network/API Error for ${url}:`, error.message);
         updateApiStatus('offline');
         throw error;
    }
};

export const api = {
    // Worker & System
    getWorkerStatus: () => apiRequest('api/v1/worker/status'),
    sendWorkerControl: (action, params = null) => apiRequest(`api/v1/worker/control/${action}`, { 
        method: 'POST',
        headers: params ? { 'Content-Type': 'application/json' } : {},
        body: params ? JSON.stringify(params) : null
    }),
    getSystemAlert: () => apiRequest('api/v1/system/alert'),
    getApiRootStatus: () => apiRequest(''),

    // Fazy Analizy
    getPhase1Candidates: () => apiRequest('api/v1/candidates/phase1'),
    getPhase2Results: () => apiRequest('api/v1/results/phase2'),
    getPhase3Signals: () => apiRequest('api/v1/signals/phase3'),
    getSignalDetails: (ticker) => apiRequest(`api/v1/signal/${ticker}/details`),
    getDiscardedCount: () => apiRequest('api/v1/signals/discarded-count-24h'),
    
    // Portfel i Rynek
    getLiveQuote: (ticker) => apiRequest(`api/v1/quote/${ticker}`),
    addToWatchlist: (ticker) => apiRequest(`api/v1/watchlist/${ticker}`, { method: 'POST' }),
    getPortfolio: () => apiRequest('api/v1/portfolio'),
    buyStock: (data) => apiRequest('api/v1/portfolio/buy', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) }),
    sellStock: (data) => apiRequest('api/v1/portfolio/sell', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) }),
    getTransactionHistory: () => apiRequest('api/v1/transactions'),
    
    // Raporty i Narzędzia
    getVirtualAgentReport: (page = 1, pageSize = REPORT_PAGE_SIZE) => apiRequest(`api/v1/virtual-agent/report?page=${page}&page_size=${pageSize}`),
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

    // === QUANTUM LAB V4 (Fix) ===
    startOptimization: (requestData) => apiRequest('api/v1/optimization/start', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestData)
    }),
    
    getOptimizationResults: () => apiRequest('api/v1/optimization/results')
};
