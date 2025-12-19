// ui.js - Zarządzanie interfejsem użytkownika

// ==================================================================
// 1. RENDEROWANIE ZAKŁADEK I PANELI
// ==================================================================

function renderTabContent(tabName) {
    const contentDiv = document.getElementById('tab-content');
    contentDiv.innerHTML = ''; // Wyczyść

    if (tabName === 'dashboard') {
        renderDashboard(contentDiv);
    } else if (tabName === 'signals') {
        renderSignalsTab(contentDiv);
    } else if (tabName === 'scanner') {
        renderScannerTab(contentDiv);
    } else if (tabName === 'portfolio') {
        renderPortfolioTab(contentDiv);
    } else if (tabName === 'settings') {
        renderSettingsTab(contentDiv);
    } else if (tabName === 'analysis') { // Nowa zakładka Analysis/Optimizer
        renderAnalysisTab(contentDiv);
    }
}

// ==================================================================
// 2. ZAKŁADKA DASHBOARD
// ==================================================================

function renderDashboard(container) {
    container.innerHTML = `
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
            <!-- Kafelki Statusu -->
            <div class="bg-gray-800 p-4 rounded shadow border-l-4 border-blue-500">
                <h3 class="text-gray-400 text-sm">Aktywne Sygnały</h3>
                <p class="text-2xl font-bold text-white" id="stat-active-signals">-</p>
            </div>
            <div class="bg-gray-800 p-4 rounded shadow border-l-4 border-green-500">
                <h3 class="text-gray-400 text-sm">Dzisiejszy Zysk (Sym)</h3>
                <p class="text-2xl font-bold text-green-400" id="stat-daily-pnl">-</p>
            </div>
            <div class="bg-gray-800 p-4 rounded shadow border-l-4 border-purple-500">
                <h3 class="text-gray-400 text-sm">Status Systemu</h3>
                <p class="text-lg font-bold text-white truncate" id="stat-system-status">ONLINE</p>
            </div>
            <div class="bg-gray-800 p-4 rounded shadow border-l-4 border-yellow-500">
                <h3 class="text-gray-400 text-sm">Ostatni Skan</h3>
                <p class="text-sm font-bold text-white mt-2" id="stat-last-scan">-</p>
            </div>
        </div>

        <!-- Pasek Postępu Skanowania -->
        <div id="scan-progress-container" class="hidden mb-6 bg-gray-800 p-4 rounded shadow border border-gray-700">
            <h3 class="text-sm font-bold text-blue-400 mb-2">Trwa Skanowanie Rynku...</h3>
            <div class="w-full bg-gray-700 rounded-full h-2.5 dark:bg-gray-700">
                <div id="scan-progress-bar" class="bg-blue-600 h-2.5 rounded-full" style="width: 0%"></div>
            </div>
            <p class="text-xs text-gray-400 mt-1 text-right" id="scan-progress-text">0%</p>
        </div>

        <!-- Dziennik Operacyjny (Logs) -->
        <div class="bg-gray-800 p-4 rounded shadow border border-gray-700">
            <h3 class="text-lg font-bold text-gray-200 mb-2">Dziennik Operacyjny</h3>
            <div id="scan-log-container" class="bg-black text-green-400 font-mono text-xs p-2 h-64 overflow-y-auto rounded">
                <p>Ładowanie logów...</p>
            </div>
        </div>
    `;
    
    // Inicjalizacja logów po wyrenderowaniu
    if (window.updateScanLogs) window.updateScanLogs();
}

// ==================================================================
// 3. ZAKŁADKA ANALYSIS (OPTIMIZER) - POPRAWIONA PEŁNA WERSJA
// ==================================================================

function renderAnalysisTab(container) {
    container.innerHTML = `
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <!-- LEWA KOLUMNA: KONFIGURACJA -->
            <div class="bg-gray-800 p-6 rounded-lg shadow-lg border border-gray-700">
                <h2 class="text-xl font-bold mb-4 text-blue-400 flex items-center">
                    <i class="fas fa-microchip mr-2"></i> Konfiguracja Symulacji
                </h2>
                
                <div class="space-y-4">
                    <!-- Wybór Strategii -->
                    <div>
                        <label class="block text-sm font-medium text-gray-400 mb-1">Strategia</label>
                        <select id="opt-strategy" class="w-full bg-gray-900 border border-gray-600 rounded px-3 py-2 text-white focus:outline-none focus:border-blue-500">
                            <option value="H3">H3 Elite Sniper (Cisza/Wolumen)</option>
                            <option value="AQM">AQM Adaptive Quantum (Wektorowa)</option>
                        </select>
                    </div>

                    <!-- Wybór Roku -->
                    <div>
                        <label class="block text-sm font-medium text-gray-400 mb-1">Rok Docelowy</label>
                        <select id="opt-year" class="w-full bg-gray-900 border border-gray-600 rounded px-3 py-2 text-white focus:outline-none focus:border-blue-500">
                            <option value="2025">2025 (Obecny)</option>
                            <option value="2024">2024</option>
                            <option value="2023" selected>2023 (Bliźniak)</option>
                            <option value="2022">2022 (Bessa)</option>
                            <option value="2021">2021 (Hossa/Szczyt)</option>
                        </select>
                    </div>

                    <!-- Wybór Okresu (Scan Period) -->
                    <div>
                        <label class="block text-sm font-medium text-gray-400 mb-1">Okres Badania</label>
                        <select id="opt-period" class="w-full bg-gray-900 border border-gray-600 rounded px-3 py-2 text-white focus:outline-none focus:border-blue-500">
                            <option value="FULL">Cały Rok (Full Year)</option>
                            <option value="Q1">Q1 (Styczeń - Marzec)</option>
                            <option value="Q2">Q2 (Kwiecień - Czerwiec)</option>
                            <option value="Q3">Q3 (Lipiec - Wrzesień)</option>
                            <option value="Q4" selected>Q4 (Październik - Grudzień)</option>
                        </select>
                    </div>

                    <!-- Liczba Prób (Trials) -->
                    <div>
                        <label class="block text-sm font-medium text-gray-400 mb-1">Liczba Prób (Trials)</label>
                        <input type="number" id="opt-trials" value="50" min="10" max="500" class="w-full bg-gray-900 border border-gray-600 rounded px-3 py-2 text-white focus:outline-none focus:border-blue-500">
                    </div>

                    <!-- Przycisk Start -->
                    <button id="btn-start-opt" class="w-full bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-500 hover:to-purple-500 text-white font-bold py-3 px-4 rounded transition transform hover:scale-105 shadow-lg flex items-center justify-center">
                        <i class="fas fa-play mr-2"></i> Uruchom Optymalizację
                    </button>
                </div>
            </div>

            <!-- PRAWA KOLUMNA: WYNIKI I LOGI -->
            <div class="bg-gray-800 p-6 rounded-lg shadow-lg border border-gray-700 flex flex-col h-full">
                <h2 class="text-xl font-bold mb-4 text-green-400 flex items-center">
                    <i class="fas fa-chart-line mr-2"></i> Status i Wyniki
                </h2>
                
                <!-- Status Box -->
                <div id="opt-status-box" class="bg-gray-900 p-4 rounded mb-4 border border-gray-600">
                    <p class="text-sm text-gray-400">Status Workera:</p>
                    <p id="opt-worker-status" class="text-lg font-mono font-bold text-yellow-400">IDLE</p>
                </div>

                <!-- Logi -->
                <div class="flex-1 bg-black rounded border border-gray-700 p-2 overflow-y-auto font-mono text-xs text-gray-300" style="max-height: 300px;" id="opt-logs">
                    <p class="text-gray-500">Oczekiwanie na start...</p>
                </div>
            </div>
        </div>
    `;

    // Obsługa zdarzenia kliknięcia przycisku START
    const startBtn = document.getElementById('btn-start-opt');
    if (startBtn) {
        startBtn.addEventListener('click', () => {
            const strategy = document.getElementById('opt-strategy').value;
            const year = document.getElementById('opt-year').value;
            const period = document.getElementById('opt-period').value; // Pobieramy wartość z selecta
            const trials = document.getElementById('opt-trials').value;

            // Wywołanie funkcji z logic.js (lub app.js)
            if (window.startOptimization) {
                window.startOptimization(strategy, year, period, trials);
            } else {
                console.error("Funkcja startOptimization nie jest dostępna globalnie!");
                alert("Błąd interfejsu: Brak funkcji startOptimization.");
            }
        });
    }
}

// ==================================================================
// 4. ZAKŁADKA SIGNALS (TABELA)
// ==================================================================

function renderSignalsTab(container) {
    container.innerHTML = `
        <div class="bg-gray-800 p-4 rounded shadow border border-gray-700">
            <div class="flex justify-between items-center mb-4">
                <h2 class="text-xl font-bold text-blue-400">Sygnały Handlowe (Live)</h2>
                <button id="btn-refresh-signals" class="bg-gray-700 hover:bg-gray-600 text-white px-3 py-1 rounded text-sm">
                    <i class="fas fa-sync"></i> Odśwież
                </button>
            </div>
            <div class="overflow-x-auto">
                <table class="min-w-full text-left text-sm text-gray-400">
                    <thead class="bg-gray-900 text-gray-200 uppercase font-medium">
                        <tr>
                            <th class="px-4 py-3">Ticker</th>
                            <th class="px-4 py-3">Strategia</th>
                            <th class="px-4 py-3">Status</th>
                            <th class="px-4 py-3">Cena Wejścia</th>
                            <th class="px-4 py-3">TP / SL</th>
                            <th class="px-4 py-3">Score</th>
                            <th class="px-4 py-3">Data</th>
                            <th class="px-4 py-3">Akcje</th>
                        </tr>
                    </thead>
                    <tbody id="signals-table-body" class="divide-y divide-gray-700">
                        <tr><td colspan="8" class="text-center py-4">Ładowanie danych...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    `;
    
    if (window.loadSignalsTable) window.loadSignalsTable();
    
    const refreshBtn = document.getElementById('btn-refresh-signals');
    if(refreshBtn) {
        refreshBtn.addEventListener('click', () => { if(window.loadSignalsTable) window.loadSignalsTable(); });
    }
}

// ==================================================================
// 5. ZAKŁADKA SCANNER (RĘCZNY)
// ==================================================================

function renderScannerTab(container) {
    container.innerHTML = `
        <div class="bg-gray-800 p-6 rounded-lg shadow-lg border border-gray-700">
            <h2 class="text-xl font-bold mb-4 text-purple-400">Skaner Rynku (Manualny)</h2>
            <p class="text-gray-400 mb-6">Uruchomienie skanera spowoduje przeszukanie rynku pod kątem strategii H3 oraz AQM.</p>
            
            <button id="btn-start-scan" class="w-full md:w-auto bg-purple-600 hover:bg-purple-500 text-white font-bold py-3 px-8 rounded shadow-lg transition flex items-center justify-center">
                <i class="fas fa-radar mr-2"></i> Uruchom Pełny Skan
            </button>
            
            <div id="manual-scan-status" class="mt-4 text-gray-300 hidden">
                <p>Status: <span class="font-bold text-yellow-400">Inicjalizacja...</span></p>
            </div>
        </div>
    `;
    
    const btn = document.getElementById('btn-start-scan');
    if(btn) {
        btn.addEventListener('click', () => {
            if(window.startManualScan) window.startManualScan();
        });
    }
}

// ==================================================================
// 6. ZAKŁADKA PORTFOLIO
// ==================================================================

function renderPortfolioTab(container) {
    container.innerHTML = `
        <div class="bg-gray-800 p-4 rounded shadow border border-gray-700">
            <h2 class="text-xl font-bold text-green-400 mb-4">Portfel Inwestycyjny</h2>
            <p class="text-gray-500">Funkcjonalność w budowie...</p>
        </div>
    `;
}

// ==================================================================
// 7. ZAKŁADKA SETTINGS
// ==================================================================

function renderSettingsTab(container) {
    container.innerHTML = `
        <div class="bg-gray-800 p-6 rounded-lg shadow-lg border border-gray-700">
            <h2 class="text-xl font-bold text-gray-200 mb-6">Ustawienia Systemu</h2>
            
            <div class="space-y-6">
                <!-- Sekcja API -->
                <div>
                    <h3 class="text-lg font-medium text-white mb-2">Konfiguracja API</h3>
                    <div class="bg-gray-900 p-4 rounded border border-gray-600">
                        <label class="block text-sm text-gray-400 mb-1">Alpha Vantage API Key</label>
                        <input type="password" value="****************" disabled class="w-full bg-gray-800 text-gray-500 border border-gray-700 rounded px-3 py-2 cursor-not-allowed">
                        <p class="text-xs text-gray-500 mt-1">Klucz jest zarządzany przez zmienne środowiskowe.</p>
                    </div>
                </div>

                <!-- Sekcja Telegram -->
                <div>
                    <h3 class="text-lg font-medium text-white mb-2">Powiadomienia Telegram</h3>
                    <div class="bg-gray-900 p-4 rounded border border-gray-600">
                        <label class="block text-sm text-gray-400 mb-1">Chat ID</label>
                        <input type="text" placeholder="Wpisz Chat ID" class="w-full bg-gray-800 text-white border border-gray-600 rounded px-3 py-2 mb-2">
                        <button class="bg-blue-600 hover:bg-blue-500 text-white px-4 py-2 rounded text-sm">Zapisz</button>
                    </div>
                </div>
            </div>
        </div>
    `;
}
