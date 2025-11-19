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
        // ... (reszta handlerów bez zmian) ...
        // Skopiuj resztę logic z poprzedniej wersji lub logic.js
        // Tu skupiamy się na przyciskach bocznych
    });

    if (UI.btnPhase1) {
        UI.btnPhase1.addEventListener('click', async () => {
            console.log("Kliknięto Start Fazy 1"); // DEBUG LOG
            UI.btnPhase1.disabled = true;
            try { 
                const res = await api.sendWorkerControl('start_phase1'); 
                console.log("API odpowiedziało:", res); // DEBUG LOG
            } catch(e) {
                console.error("Błąd F1:", e);
            }
        });
    }
    if (UI.btnPhase3) {
        UI.btnPhase3.addEventListener('click', async () => {
            console.log("Kliknięto Start Fazy 3"); // DEBUG LOG
            UI.btnPhase3.disabled = true;
            try { 
                const res = await api.sendWorkerControl('start_phase3'); 
                console.log("API odpowiedziało:", res); // DEBUG LOG
            } catch(e) {
                console.error("Błąd F3:", e);
            }
        });
    }

    // ... (reszta listenerów bez zmian) ...
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
    
    // ... (reszta modali) ...
    
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
