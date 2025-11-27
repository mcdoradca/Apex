import { ui, renderers } from './ui.js';
import { api } from './api.js';
import { logger, state } from './state.js';
import { 
    setUI, 
    showDashboard, showPortfolio, showTransactions, showAgentReport,
    pollWorkerStatus, refreshSidebarData, pollSystemAlerts,
    loadAgentReportPage,
    showBuyModal, hideBuyModal, handleBuyConfirm,
    showSellModal, hideSellModal, handleSellConfirm,
    handleYearBacktestRequest, handleCsvExport,
    showH3DeepDiveModal, hideH3DeepDiveModal, handleRunH3DeepDive,
    handleRunAIOptimizer, handleViewAIOptimizerReport, hideAIReportModal,
    showH3LiveParamsModal, hideH3LiveParamsModal, handleRunH3LiveScan,
    showSignalDetails, hideSignalDetails,
    showQuantumModal, hideQuantumModal, handleStartQuantumOptimization,
    showOptimizationResults, hideOptimizationResults,
    // === NOWOŚĆ: Import kontrolera widoku H3 ===
    showH3Signals
} from './logic.js';

// Tworzymy lokalny obiekt Logic dla kompatybilności z resztą kodu
const Logic = {
    setUI, 
    showDashboard, showPortfolio, showTransactions, showAgentReport,
    pollWorkerStatus, refreshSidebarData, pollSystemAlerts,
    loadAgentReportPage,
    showBuyModal, hideBuyModal, handleBuyConfirm,
    showSellModal, hideSellModal, handleSellConfirm,
    handleYearBacktestRequest, handleCsvExport,
    showH3DeepDiveModal, hideH3DeepDiveModal, handleRunH3DeepDive,
    handleRunAIOptimizer, handleViewAIOptimizerReport, hideAIReportModal,
    showH3LiveParamsModal, hideH3LiveParamsModal, handleRunH3LiveScan,
    showSignalDetails, hideSignalDetails,
    showQuantumModal, hideQuantumModal, handleStartQuantumOptimization,
    showOptimizationResults, hideOptimizationResults,
    // Dodajemy nową funkcję do obiektu Logic
    showH3Signals
};

document.addEventListener('DOMContentLoaded', () => {
    try {
        logger.info("DOM loaded. Initializing APEX Predator...");

        const UI = ui.init(); 
        
        Logic.setUI(UI);      

        function startApp() {
            logger.info("Starting App...");
            UI.loginScreen.classList.add('hidden');
            UI.dashboardScreen.classList.remove('hidden');
            
            if (UI.apiStatus) {
                UI.apiStatus.innerHTML = '<span class="h-2 w-2 rounded-full bg-green-500 mr-2"></span>Online';
            }
            
            Logic.showDashboard();
            Logic.pollWorkerStatus();
            Logic.refreshSidebarData(); 
            Logic.pollSystemAlerts();   
            
            try { if (window.lucide) window.lucide.createIcons(); } catch(e) { console.warn("Lucide icons not loaded"); }
        }

        if (document.getElementById('login-form')) {
            document.getElementById('login-form').addEventListener('submit', (e) => {
                e.preventDefault();
                if (!UI.loginButton.disabled) startApp();
            });
        }

        if (UI.mainContent) {
            UI.mainContent.addEventListener('click', async e => {
                const target = e.target;
                const sellBtn = target.closest('.sell-stock-btn');
                const prevBtn = target.closest('#report-prev-btn');
                const nextBtn = target.closest('#report-next-btn');
                
                // === OBSŁUGA PRZYCISKÓW (Delegacja Zdarzeń) ===
                
                // 1. Backtest i Konfiguracja
                if (target.closest('#run-backtest-year-btn')) Logic.handleYearBacktestRequest();
                else if (target.closest('#toggle-h3-params')) {
                     const container = document.getElementById('h3-params-container');
                     const icon = document.getElementById('h3-params-icon');
                     if (container) { container.classList.toggle('hidden'); icon.classList.toggle('rotate-180'); }
                }

                // 2. Quantum Lab (Apex V4)
                else if (target.closest('#open-quantum-modal-btn')) Logic.showQuantumModal();
                else if (target.closest('#view-optimization-results-btn')) Logic.showOptimizationResults();
                
                // 3. Inne narzędzia
                else if (target.closest('#run-h3-deep-dive-modal-btn')) Logic.showH3DeepDiveModal();
                else if (target.closest('#run-csv-export-btn')) Logic.handleCsvExport();
                else if (target.closest('#run-ai-optimizer-btn')) Logic.handleRunAIOptimizer();
                else if (target.closest('#view-ai-report-btn')) Logic.handleViewAIOptimizerReport();
                
                // 4. Akcje Portfelowe i Raporty
                else if (sellBtn) {
                     const ticker = sellBtn.dataset.ticker;
                     const quantity = parseInt(sellBtn.dataset.quantity, 10);
                     if (ticker) Logic.showSellModal(ticker, quantity);
                }
                else if (prevBtn && !prevBtn.disabled) Logic.loadAgentReportPage(state.currentReportPage - 1);
                else if (nextBtn && !nextBtn.disabled) Logic.loadAgentReportPage(state.currentReportPage + 1);
            });
        }

        // Obsługa Sidebaru (Accordion)
        if (UI.sidebarPhasesContainer) {
            UI.sidebarPhasesContainer.addEventListener('click', (e) => {
                const toggle = e.target.closest('.accordion-toggle');
                const signalItem = e.target.closest('.phase3-item'); // Obsługa kliknięć w małą listę w sidebarze
        
                if (toggle) {
                    const content = toggle.nextElementSibling;
                    const icon = toggle.querySelector('.accordion-icon');
                    if (content) { content.classList.toggle('hidden'); icon.classList.toggle('rotate-180'); }
                }
                else if (signalItem) {
                    const ticker = signalItem.dataset.ticker;
                    if (ticker) {
                        logger.info(`Kliknięto sygnał H3 (Sidebar): ${ticker}`);
                        Logic.showSignalDetails(ticker);
                    }
                }
            });
        }

        // Przyciski w Sidebarze (Sterowanie Workerem)
        if (UI.btnPhase1) {
            UI.btnPhase1.addEventListener('click', async () => {
                UI.btnPhase1.disabled = true;
                try { await api.sendWorkerControl('start_phase1'); } catch(e) {}
            });
        }
        if (UI.btnPhase3) {
            UI.btnPhase3.addEventListener('click', () => {
                Logic.showH3LiveParamsModal();
            });
        }
        
        // Modal H3 Live (Konfiguracja)
        if (UI.h3LiveModal.cancelBtn) {
            UI.h3LiveModal.cancelBtn.addEventListener('click', Logic.hideH3LiveParamsModal);
        }
        if (UI.h3LiveModal.startBtn) {
            UI.h3LiveModal.startBtn.addEventListener('click', Logic.handleRunH3LiveScan);
        }
    
        // Detale Sygnału
        if (UI.signalDetails && UI.signalDetails.closeBtn) {
            UI.signalDetails.closeBtn.addEventListener('click', Logic.hideSignalDetails);
        }
        if (UI.signalDetails && UI.signalDetails.backdrop) {
            UI.signalDetails.backdrop.addEventListener('click', (e) => {
                if (e.target === UI.signalDetails.backdrop) Logic.hideSignalDetails();
            });
        }
        
        // Quantum Lab Listeners
        if (UI.quantumModal.cancelBtn) UI.quantumModal.cancelBtn.addEventListener('click', Logic.hideQuantumModal);
        if (UI.quantumModal.startBtn) UI.quantumModal.startBtn.addEventListener('click', Logic.handleStartQuantumOptimization);
        if (UI.optimizationResultsModal.closeBtn) UI.optimizationResultsModal.closeBtn.addEventListener('click', Logic.hideOptimizationResults);
    
        // === NAWIGACJA GŁÓWNA ===
        if (UI.dashboardLink) UI.dashboardLink.addEventListener('click', (e) => { e.preventDefault(); Logic.showDashboard(); });
        if (UI.portfolioLink) UI.portfolioLink.addEventListener('click', (e) => { e.preventDefault(); Logic.showPortfolio(); });
        if (UI.transactionsLink) UI.transactionsLink.addEventListener('click', (e) => { e.preventDefault(); Logic.showTransactions(); });
        if (UI.agentReportLink) UI.agentReportLink.addEventListener('click', (e) => { e.preventDefault(); Logic.showAgentReport(); });
        
        // NOWOŚĆ: Obsługa kliknięcia w link "Sygnały H3 Live"
        if (UI.h3SignalsLink) {
            UI.h3SignalsLink.addEventListener('click', (e) => {
                e.preventDefault();
                Logic.showH3Signals();
            });
        }
    
        // Modale Kupna/Sprzedaży
        if(UI.buyModal.cancelBtn) UI.buyModal.cancelBtn.addEventListener('click', Logic.hideBuyModal);
        if(UI.buyModal.confirmBtn) UI.buyModal.confirmBtn.addEventListener('click', Logic.handleBuyConfirm);
        
        if(UI.sellModal.cancelBtn) UI.sellModal.cancelBtn.addEventListener('click', Logic.hideSellModal);
        if(UI.sellModal.confirmBtn) UI.sellModal.confirmBtn.addEventListener('click', Logic.handleSellConfirm);
        
        // Inne Modale
        if(UI.aiReportModal.closeBtn) UI.aiReportModal.closeBtn.addEventListener('click', Logic.hideAIReportModal);
        
        if(UI.h3DeepDiveModal.closeBtn) UI.h3DeepDiveModal.closeBtn.addEventListener('click', Logic.hideH3DeepDiveModal);
        if(UI.h3DeepDiveModal.runBtn) UI.h3DeepDiveModal.runBtn.addEventListener('click', Logic.handleRunH3DeepDive);
        
        // Mobile Menu
        if(UI.mobileMenuBtn) UI.mobileMenuBtn.addEventListener('click', () => { UI.sidebar.classList.remove('-translate-x-full'); UI.sidebarBackdrop.classList.remove('hidden'); });
        if(UI.mobileSidebarCloseBtn) UI.mobileSidebarCloseBtn.addEventListener('click', () => { UI.sidebar.classList.add('-translate-x-full'); UI.sidebarBackdrop.classList.add('hidden'); });
        if(UI.sidebarBackdrop) UI.sidebarBackdrop.addEventListener('click', () => { UI.sidebar.classList.add('-translate-x-full'); UI.sidebarBackdrop.classList.add('hidden'); });
    
        // Auto-Login Loop
        const intervalId = setInterval(async () => {
            try {
                const status = await api.getApiRootStatus();
                if (status && status.status && status.status.includes("running")) {
                    clearInterval(intervalId);
                    if (UI.loginStatusText) UI.loginStatusText.textContent = 'System gotowy.';
                    if (UI.loginButton) {
                        UI.loginButton.disabled = false;
                        UI.loginButton.textContent = 'Wejdź do Aplikacji';
                    }
                }
            } catch (e) {}
        }, 3000);

    } catch (error) {
        console.error("CRITICAL ERROR in app.js:", error);
        const statusEl = document.getElementById('login-status-text');
        if (statusEl) statusEl.textContent = "Błąd inicjalizacji aplikacji. Sprawdź konsolę.";
    }
});
