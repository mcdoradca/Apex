import { ui, renderers } from './ui.js';
import { api } from './api.js';
import { logger, state } from './state.js';
import * as Logic from './logic.js';

document.addEventListener('DOMContentLoaded', () => {
    logger.info("DOM loaded. Initializing APEX Predator...");
    const UI = ui.init(); 
    Logic.setUI(UI);      
    // ... (init bez zmian) ...

    // ZMIANA W OBSŁUDZE PRZYCISKÓW:

    if (UI.btnPhase1) {
        UI.btnPhase1.addEventListener('click', async () => {
            UI.btnPhase1.disabled = true;
            try { await api.sendWorkerControl('start_phase1'); } catch(e) {}
        });
    }
    if (UI.btnPhase3) {
        UI.btnPhase3.addEventListener('click', () => {
            // Zamiast od razu startować, pokazujemy modal
            Logic.showH3LiveParamsModal();
        });
    }
    
    // Obsługa przycisków w modalu H3 Live
    if (UI.h3LiveModal.cancelBtn) {
        UI.h3LiveModal.cancelBtn.addEventListener('click', Logic.hideH3LiveParamsModal);
    }
    if (UI.h3LiveModal.startBtn) {
        UI.h3LiveModal.startBtn.addEventListener('click', Logic.handleRunH3LiveScan);
    }
    
    // ... (reszta listenerów bez zmian)
});
