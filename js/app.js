import { ui, renderers } from './ui.js';
import { api } from './api.js';
import { logger, state } from './state.js';
import * as Logic from './logic.js';

document.addEventListener('DOMContentLoaded', () => {
    logger.info("DOM loaded. Initializing APEX Predator...");

    const UI = ui.init(); 
    Logic.setUI(UI);      

    function startApp() {
        logger.info("Starting App...");
        UI.loginScreen.classList.add('hidden');
        UI.dashboardScreen.classList.remove('hidden');
        UI.apiStatus.innerHTML = '<span class="h-2 w-2 rounded-full bg-green-500 mr-2"></span>Online';
        
        Logic.showDashboard();
        Logic.pollWorkerStatus();
        Logic.refreshSidebarData(); 
        Logic.pollSystemAlerts();   
        try { lucide.createIcons(); } catch(e) {}
    }

    document.getElementById('login-form').addEventListener('submit', (e) => {
        e.preventDefault();
        if (!UI.loginButton.disabled) startApp();
    });

    UI.mainContent.addEventListener('click', async e => {
        const target = e.target;
        const sellBtn = target.closest('.sell-stock-btn');
        const prevBtn = target.closest('#report-prev-btn');
        const nextBtn = target.closest('#report-next-btn');
        
        if (target.closest('#run-backtest-year-btn')) Logic.handleYearBacktestRequest();
        else if (target.closest('#run-h3-deep-dive-modal-btn')) Logic.showH3DeepDiveModal();
        else if (target.closest('#run-csv-export-btn')) Logic.handleCsvExport();
        else if (target.closest('#run-ai-optimizer-btn')) Logic.handleRunAIOptimizer();
        else if (target.closest('#view-ai-report-btn')) Logic.handleViewAIOptimizerReport();
        else if (target.closest('#toggle-h3-params')) {
             const container = document.getElementById('h3-params-container');
             const icon = document.getElementById('h3-params-icon');
             if (container) { container.classList.toggle('hidden'); icon.classList.toggle('rotate-180'); }
        }
        else if (sellBtn) {
             const ticker = sellBtn.dataset.ticker;
             const quantity = parseInt(sellBtn.dataset.quantity, 10);
             if (ticker) Logic.showSellModal(ticker, quantity);
        }
        else if (prevBtn && !prevBtn.disabled) Logic.loadAgentReportPage(state.currentReportPage - 1);
        else if (nextBtn && !nextBtn.disabled) Logic.loadAgentReportPage(state.currentReportPage + 1);
    });

    // --- ZMIANA: Obsługa przycisków F1/F3 ---
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
    
    // --- ZMIANA: Obsługa modala H3 Live ---
    if (UI.h3LiveModal.cancelBtn) {
        UI.h3LiveModal.cancelBtn.addEventListener('click', Logic.hideH3LiveParamsModal);
    }
    if (UI.h3LiveModal.startBtn) {
        UI.h3LiveModal.startBtn.addEventListener('click', Logic.handleRunH3LiveScan);
    }

    UI.sidebarPhasesContainer.addEventListener('click', (e) => {
        const toggle = e.target.closest('.accordion-toggle');
        if (toggle) {
            const content = toggle.nextElementSibling;
            const icon = toggle.querySelector('.accordion-icon');
            if (content) { content.classList.toggle('hidden'); icon.classList.toggle('rotate-180'); }
        }
    });

    UI.dashboardLink.addEventListener('click', (e) => { e.preventDefault(); Logic.showDashboard(); });
    UI.portfolioLink.addEventListener('click', (e) => { e.preventDefault(); Logic.showPortfolio(); });
    UI.transactionsLink.addEventListener('click', (e) => { e.preventDefault(); Logic.showTransactions(); });
    UI.agentReportLink.addEventListener('click', (e) => { e.preventDefault(); Logic.showAgentReport(); });

    if(UI.buyModal.cancelBtn) UI.buyModal.cancelBtn.addEventListener('click', Logic.hideBuyModal);
    if(UI.buyModal.confirmBtn) UI.buyModal.confirmBtn.addEventListener('click', Logic.handleBuyConfirm);
    if(UI.sellModal.cancelBtn) UI.sellModal.cancelBtn.addEventListener('click', Logic.hideSellModal);
    if(UI.sellModal.confirmBtn) UI.sellModal.confirmBtn.addEventListener('click', Logic.handleSellConfirm);
    if(UI.aiReportModal.closeBtn) UI.aiReportModal.closeBtn.addEventListener('click', Logic.hideAIReportModal);
    if(UI.h3DeepDiveModal.closeBtn) UI.h3DeepDiveModal.closeBtn.addEventListener('click', Logic.hideH3DeepDiveModal);
    if(UI.h3DeepDiveModal.runBtn) UI.h3DeepDiveModal.runBtn.addEventListener('click', Logic.handleRunH3DeepDive);
    if(UI.mobileMenuBtn) UI.mobileMenuBtn.addEventListener('click', () => { UI.sidebar.classList.remove('-translate-x-full'); UI.sidebarBackdrop.classList.remove('hidden'); });
    if(UI.mobileSidebarCloseBtn) UI.mobileSidebarCloseBtn.addEventListener('click', () => { UI.sidebar.classList.add('-translate-x-full'); UI.sidebarBackdrop.classList.add('hidden'); });
    if(UI.sidebarBackdrop) UI.sidebarBackdrop.addEventListener('click', () => { UI.sidebar.classList.add('-translate-x-full'); UI.sidebarBackdrop.classList.add('hidden'); });

    const intervalId = setInterval(async () => {
        try {
            const status = await api.getApiRootStatus();
            if (status && status.status.includes("running")) {
                clearInterval(intervalId);
                UI.loginStatusText.textContent = 'System gotowy.';
                UI.loginButton.disabled = false;
                UI.loginButton.textContent = 'Wejdź do Aplikacji';
            }
        } catch (e) {}
    }, 3000);
});
