export const API_BASE_URL = "https://apex-predator-api-x0l8.onrender.com";
export const PORTFOLIO_QUOTE_POLL_INTERVAL = 30000;
export const ALERT_POLL_INTERVAL = 7000;
export const AI_OPTIMIZER_POLL_INTERVAL = 5000;
export const H3_DEEP_DIVE_POLL_INTERVAL = 5000;
export const PROFIT_ALERT_THRESHOLD = 1.02;
export const REPORT_PAGE_SIZE = 200;

export const state = {
    phase1: [],
    phase2: [], 
    phase3: [], 
    portfolio: [],
    transactions: [],
    liveQuotes: {},
    workerStatus: { status: 'IDLE', phase: 'NONE', progress: { processed: 0, total: 0 } },
    discardedSignalCount: 0,
    activePortfolioPolling: null,
    activeCountdownPolling: null, 
    profitAlertsSent: {}, 
    snoozedAlerts: {},
    activeAIOptimizerPolling: null,
    currentReportPage: 1,
    activeH3DeepDivePolling: null,

    // === NOWOŚĆ: Stan sortowania dla widoku Sygnały H3 ===
    h3SortBy: 'score',      // Opcje: 'score', 'rr', 'time', 'ticker', 'price'
    h3SortDirection: 'desc' // Opcje: 'asc', 'desc'
};

export const logger = {
    error: (message, ...args) => console.error(message, ...args),
    info: (message, ...args) => console.log(message, ...args),
    warn: (message) => console.warn(message)
};
