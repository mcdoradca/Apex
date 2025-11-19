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
            if (response.status === 404) throw new Error(`404 - Nie znaleziono zasobu`);
            if (response.status === 409) throw new Error(`409 - Konflikt: Worker jest zajęty.`);
            if (response.status === 400) throw new Error(`400 - Błędne żądanie: ${errorText}`);
            if (response.status === 422) throw new Error(`422 - Błąd walidacji: ${errorText}`);
            
            throw new Error(`Błąd serwera: ${response.status} - ${errorText}`);
        }
        if (response.status === 204 || response.headers.get("Content-Length") === "0") return null;
        return await response.json();
    } catch (error) {
         logger.error(`Network or API Error for ${url}:`, error.message);
         updateApiStatus('offline');
         throw error;
    }
};

export const api = {
    getWorkerStatus: () => apiRequest('api/v1/worker/status'),
    getPhase1Candidates: () => apiRequest('api/v1/candidates/phase1'),
    getDiscardedCount: () => apiRequest('api/v1/signals/discarded-count-24h'),
    getLiveQuote: (ticker) => apiRequest(`api/v1/quote/${ticker}`),
    addToWatchlist: (ticker) => apiRequest(`api/v1/watchlist/${ticker}`, { method: 'POST' }),
    getSystemAlert: () => apiRequest('api/v1/system/alert'),
    getPortfolio: () => apiRequest('api/v1/portfolio'),
    buyStock: (data) => apiRequest('api/v1/portfolio/buy', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) }),
    sellStock: (data) => apiRequest('api/v1/portfolio/sell', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) }),
    getTransactionHistory: () => apiRequest('api/v1/transactions'),
    getVirtualAgentReport: (page = 1, pageSize = REPORT_PAGE_SIZE) => apiRequest(`api/v1/virtual-agent/report?page=${page}&page_size=${pageSize}`),
    requestBacktest: (year, params = null) => apiRequest('api/v1/backtest/request', { 
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ year: year, parameters: params })
    }),
    getApiRootStatus: () => apiRequest(''),
    requestAIOptimizer: () => apiRequest('api/v1/ai-optimizer/request', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({})
    }),
    getAIOptimizerReport: () => apiRequest('api/v1/ai-optimizer/report'),
    requestH3DeepDive: (year) => apiRequest('api/v1/analysis/h3-deep-dive', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ year: year })
    }),
    getH3DeepDiveReport: () => apiRequest('api/v1/analysis/h3-deep-dive-report'),
    getExportCsvUrl: () => `${API_BASE_URL}/api/v1/export/trades.csv`
};
