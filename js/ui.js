import { logger, state, REPORT_PAGE_SIZE } from './state.js';

// === CSS INJECTION ===
const style = document.createElement('style');
style.textContent = `
    @keyframes heartbeat-idle { 0% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.4); transform: scale(1); } 70% { box-shadow: 0 0 0 10px rgba(16, 185, 129, 0); transform: scale(1.02); } 100% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); transform: scale(1); } }
    @keyframes heartbeat-busy { 0% { box-shadow: 0 0 0 0 rgba(234, 179, 8, 0.7); transform: scale(1.03); } 50% { box-shadow: 0 0 0 15px rgba(234, 179, 8, 0); transform: scale(1); } 100% { box-shadow: 0 0 0 0 rgba(234, 179, 8, 0); transform: scale(1); } }
    .pulse-idle { animation: heartbeat-idle 3s infinite ease-in-out; }
    .pulse-busy { animation: heartbeat-busy 0.8s infinite ease-in-out; }
    .glass-panel { background: rgba(22, 27, 34, 0.85); backdrop-filter: blur(12px); -webkit-backdrop-filter: blur(12px); border: 1px solid rgba(48, 54, 61, 0.8); box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.5); }
    .sniper-scope-container { height: 14px; background: #0f172a; border-radius: 4px; position: relative; overflow: hidden; margin-top: 15px; border: 1px solid #374151; display: flex; box-shadow: inset 0 2px 6px 0 rgba(0, 0, 0, 0.6); }
    .scope-zone-risk { background: linear-gradient(90deg, rgba(220, 38, 38, 0.9) 0%, rgba(127, 29, 29, 0.4) 100%); height: 100%; border-right: 1px solid rgba(255,255,255,0.1); } 
    .scope-zone-reward { background: linear-gradient(90deg, rgba(6, 78, 59, 0.4) 0%, rgba(16, 185, 129, 0.9) 100%); height: 100%; flex-grow: 1; }
    .scope-marker { position: absolute; top: -1px; bottom: -1px; width: 3px; background: #ffffff; box-shadow: 0 0 8px 3px rgba(255, 255, 255, 0.9); z-index: 30; transform: translateX(-50%); transition: left 0.8s cubic-bezier(0.22, 1, 0.36, 1); }
    .entry-marker { position: absolute; top: 0; bottom: 0; width: 2px; background: rgba(250, 204, 21, 0.8); z-index: 20; box-shadow: 0 0 5px rgba(250, 204, 21, 0.5); }
    .sector-badge-up { background-color: rgba(6, 78, 59, 0.6); color: #6ee7b7; border: 1px solid rgba(16, 185, 129, 0.3); }
    .sector-badge-down { background-color: rgba(127, 29, 29, 0.6); color: #fca5a5; border: 1px solid rgba(239, 68, 68, 0.3); }
    .extended-hours-text { color: #c084fc; font-weight: bold; text-shadow: 0 0 5px rgba(192, 132, 252, 0.3); }
    .strat-badge { font-size: 10px; font-weight: 800; padding: 2px 6px; border-radius: 4px; text-transform: uppercase; letter-spacing: 0.05em; border: 1px solid transparent; }
    .strat-badge-h3 { background-color: rgba(124, 58, 237, 0.2); color: #a78bfa; border-color: rgba(124, 58, 237, 0.4); box-shadow: 0 0 5px rgba(124, 58, 237, 0.2); }
    .strat-badge-aqm { background-color: rgba(6, 182, 212, 0.2); color: #22d3ee; border-color: rgba(6, 182, 212, 0.4); box-shadow: 0 0 5px rgba(6, 182, 212, 0.2); }
    .strat-badge-biox { background-color: rgba(236, 72, 153, 0.2); color: #f472b6; border-color: rgba(236, 72, 153, 0.4); box-shadow: 0 0 5px rgba(236, 72, 153, 0.2); }
    .strat-badge-h4 { background-color: rgba(245, 158, 11, 0.2); color: #fbbf24; border-color: rgba(245, 158, 11, 0.4); box-shadow: 0 0 5px rgba(245, 158, 11, 0.2); }
    .strat-badge-flux { background-color: rgba(16, 185, 129, 0.2); color: #6ee7b7; border-color: rgba(16, 185, 129, 0.4); box-shadow: 0 0 8px rgba(16, 185, 129, 0.3); }
    .strat-badge-unknown { background-color: rgba(75, 85, 99, 0.3); color: #9ca3af; border-color: rgba(75, 85, 99, 0.5); }
    .kinetic-bar-bg { background: rgba(255,255,255,0.1); height: 6px; width: 100%; border-radius: 3px; overflow: hidden; margin-top: 4px; }
    .kinetic-bar-fill { height: 100%; border-radius: 3px; transition: width 0.5s ease; }
    .flux-pulse { animation: text-pulse 1.5s infinite; }
    @keyframes text-pulse { 0% { opacity: 1; text-shadow: 0 0 5px rgba(110, 231, 183, 0.5); } 50% { opacity: 0.8; text-shadow: 0 0 15px rgba(110, 231, 183, 0.8); } 100% { opacity: 1; text-shadow: 0 0 5px rgba(110, 231, 183, 0.5); } }
    .pool-slot { background: #1f2937; border: 2px solid #374151; border-radius: 8px; padding: 12px; position: relative; overflow: hidden; transition: all 0.3s ease; display: flex; flex-direction: column; justify-content: space-between; min-height: 140px; }
    .flux-state-wait { border-color: #4b5563; background: #1f2937; opacity: 0.8; }
    .flux-state-ready { border-color: #fbbf24; background: linear-gradient(135deg, #1f2937 0%, #451a03 100%); box-shadow: 0 0 10px rgba(251, 191, 36, 0.2); }
    .flux-state-action { border-color: #10b981; background: linear-gradient(135deg, #1f2937 0%, #064e3b 100%); box-shadow: 0 0 15px rgba(16, 185, 129, 0.4); animation: border-pulse 2s infinite; }
    @keyframes border-pulse { 0% { box-shadow: 0 0 5px rgba(16, 185, 129, 0.4); border-color: #10b981; } 50% { box-shadow: 0 0 20px rgba(16, 185, 129, 0.7); border-color: #34d399; } 100% { box-shadow: 0 0 5px rgba(16, 185, 129, 0.4); border-color: #10b981; } }
    .flux-action-text { font-size: 1.5rem; font-weight: 900; letter-spacing: 0.05em; text-transform: uppercase; text-align: center; margin-top: auto; margin-bottom: auto; }
    .text-action-green { color: #34d399; text-shadow: 0 0 10px rgba(52, 211, 153, 0.6); }
    .text-action-yellow { color: #fcd34d; text-shadow: 0 0 10px rgba(252, 211, 77, 0.6); }
    .text-action-gray { color: #6b7280; }
`;
document.head.appendChild(style);

// === AUDIO & HELPERS ===
let lastSpokenSignal = "";
const playTacticalAlert = (ticker, score) => {
    try {
        if (!window.speechSynthesis || state.h3SortBy !== 'score' || lastSpokenSignal === ticker) return;
        const u = new SpeechSynthesisUtterance(`Target: ${ticker}. Score: ${score}.`);
        u.rate = 1.1; u.volume = 0.8;
        window.speechSynthesis.speak(u);
        lastSpokenSignal = ticker;
    } catch(e) {}
};

const getStrategyInfo = (notes) => {
    if (!notes) return { name: 'UNK', class: 'strat-badge-unknown' };
    const n = notes.toUpperCase();
    if (n.includes("FLUX") || n.includes("OMNI-FLUX")) return { name: 'FLUX', class: 'strat-badge-flux' };
    if (n.includes("H3")) return { name: 'H3', class: 'strat-badge-h3' };
    if (n.includes("AQM")) return { name: 'AQM', class: 'strat-badge-aqm' };
    if (n.includes("BIOX")) return { name: 'BIOX', class: 'strat-badge-biox' };
    return { name: 'MANUAL', class: 'strat-badge-unknown' };
};

// =========================================================================
// === RENDERERS (Zdefiniowane jako stałe) ===
// =========================================================================

const renderLoading = (text) => `<div class="text-center py-10"><p class="text-sky-400 animate-pulse">${text}</p></div>`;

const renderPhase1List = (candidates) => {
    try {
        if (!candidates || !Array.isArray(candidates)) return `<p class="text-xs text-gray-500 p-2">Brak danych.</p>`;
        return candidates.map(c => {
            const isUp = (c.sector_trend_score || 0) > 0;
            const badge = c.sector_ticker ? `<span class="text-[9px] ml-2 px-1 rounded ${isUp ? 'sector-badge-up' : 'sector-badge-down'}">${c.sector_ticker}</span>` : "";
            return `<div class="candidate-item flex justify-between items-center text-xs p-2 rounded hover:bg-gray-800 border-b border-gray-800"><div><span class="font-bold text-sky-400">${c.ticker}</span>${badge}</div><span class="text-gray-500 font-mono">${c.price ? c.price.toFixed(2) : '-'}</span></div>`;
        }).join('') || `<p class="text-xs text-gray-500 p-2">Brak wyników.</p>`;
    } catch (e) { return `<p class="text-red-500 text-xs">Błąd F1</p>`; }
};

const renderPhase3List = (signals) => {
    try {
        if (!signals || !Array.isArray(signals)) return `<p class="text-xs text-gray-500 p-2">Brak sygnałów.</p>`;
        return signals.map(s => {
            const isActive = s.status === 'ACTIVE';
            const strat = getStrategyInfo(s.notes);
            let scoreDisplay = "";
            if (s.notes && s.notes.includes("SCORE:")) {
                const parts = s.notes.split("SCORE:");
                if (parts.length > 1) {
                    const val = parseInt(parts[1]);
                    if (!isNaN(val)) {
                        scoreDisplay = `<span class="ml-2 text-xs bg-blue-900/30 text-blue-300 px-1 rounded">SC:${val}</span>`;
                        if (isActive && val >= 80) playTacticalAlert(s.ticker, val);
                    }
                }
            }
            return `<div class="candidate-item phase3-item flex items-center text-xs p-2 rounded cursor-pointer ${isActive ? 'text-green-400' : 'text-yellow-400'} hover:bg-gray-800" data-ticker="${s.ticker}"><i data-lucide="${isActive ? 'zap' : 'hourglass'}" class="w-4 h-4 mr-2"></i><span class="font-bold">${s.ticker}</span><span class="ml-2 strat-badge ${strat.class}">${strat.name}</span>${scoreDisplay}<span class="ml-auto text-gray-500">${s.status}</span></div>`;
        }).join('') || `<p class="text-xs text-gray-500 p-2">Brak sygnałów.</p>`;
    } catch (e) { return `<p class="text-red-500 text-xs">Błąd F3</p>`; }
};

const renderPhase4View = (candidates) => {
    if (!candidates || !Array.isArray(candidates)) return '<div class="text-center py-10 text-gray-500">Brak danych H4.</div>';
    const rows = candidates.map(c => {
        const s = c.kinetic_score || 0;
        const color = s >= 80 ? 'text-amber-400 font-black' : (s >= 50 ? 'text-yellow-200' : 'text-gray-400');
        return `<tr class="border-b border-gray-800 hover:bg-[#1f2937]"><td class="p-3 font-bold text-amber-500">${c.ticker}</td><td class="p-3 text-right font-mono text-white">${c.price ? c.price.toFixed(2) : '-'}</td><td class="p-3"><span class="${color}">${s}</span><div class="kinetic-bar-bg"><div class="kinetic-bar-fill" style="width: ${Math.min(100, s)}%; background-color: ${s >= 70 ? '#fbbf24' : '#d97706'};"></div></div></td><td class="p-3 text-right text-gray-300">${c.total_2pct_shots_ytd||0}</td><td class="p-3 text-right text-gray-300">${c.max_daily_shots||0}</td><td class="p-3 text-right text-sky-300">~${(c.avg_swing_size||0).toFixed(2)}%</td><td class="p-3 text-right text-gray-500">${c.hard_floor_violations||0}</td></tr>`;
    }).join('');
    return `<div id="phase4-view" class="max-w-6xl mx-auto"><div class="flex justify-between items-center mb-6 border-b border-gray-700 pb-4"><h2 class="text-2xl font-bold text-white">Faza 4: Kinetic Alpha</h2><button id="run-phase4-scan-btn" class="modal-button modal-button-primary bg-amber-600 hover:bg-amber-700"><i data-lucide="radar" class="w-4 h-4 mr-2"></i> Skanuj H4</button></div><div class="overflow-x-auto bg-[#161B22] rounded border border-gray-700"><table class="w-full text-sm text-left text-gray-300"><thead class="text-xs text-gray-400 uppercase bg-[#0D1117]"><tr><th class="p-3">Ticker</th><th class="p-3 text-right">Cena</th><th class="p-3">Score</th><th class="p-3 text-right">Strzały</th><th class="p-3 text-right">Max/Dzień</th><th class="p-3 text-right">Zasięg</th><th class="p-3 text-right">Błędy</th></tr></thead><tbody>${rows}</tbody></table></div></div>`;
};

const renderPhase5View = (poolData) => {
    try {
        const activeSlots = (poolData && Array.isArray(poolData)) ? poolData.slice(0, 8) : [];
        
        const slotsHtml = activeSlots.map((item, index) => {
            // --- 1. BEZPIECZNA EKSTRAKCJA DANYCH ---
            const safeNum = (val) => (typeof val === 'number' && !isNaN(val)) ? val : 0;
            const ticker = item.ticker || "???";
            const price = safeNum(item.price);
            const score = safeNum(item.flux_score);
            const vel = safeNum(item.velocity);
            const ofp = safeNum(item.ofp);
            const elast = safeNum(item.elasticity);
            
            // --- 2. SL / TP / RR (NAPRAWA) ---
            const sl = safeNum(item.stop_loss);
            const tp = safeNum(item.take_profit);
            let rrStr = "---";
            
            if (sl > 0 && tp > 0 && price > 0) {
                const risk = Math.abs(price - sl);
                const reward = Math.abs(tp - price);
                if (risk > 0.0001) {
                    rrStr = (reward / risk).toFixed(1) + "R";
                }
            }
            
            // Formatowanie stringów
            const pStr = price > 0 ? price.toFixed(2) : "---";
            const slStr = sl > 0 ? sl.toFixed(2) : "---";
            const tpStr = tp > 0 ? tp.toFixed(2) : "---";

            // --- 3. LOGIKA KARTY ---
            let cardState = "flux-state-wait";
            let actText = "CZEKAJ";
            let actColor = "text-action-gray";
            let actDesc = "Monitorowanie...";
            let ofpColor = "text-gray-500";
            let ofpIcon = "—";

            if (ofp > 0.1) { ofpColor = "text-green-400"; ofpIcon = "↑"; }
            else if (ofp < -0.1) { ofpColor = "text-red-400"; ofpIcon = "↓"; }

            if (score >= 65) {
                cardState = "flux-state-action";
                actText = "KUPUJ";
                actColor = "text-action-green";
                actDesc = "Setup Potwierdzony!";
                if (elast < -1.0) actText = "DIP BUY";
                else if (elast > 0.5) actText = "BREAKOUT";
            } else if (score >= 40 || vel > 1.5 || Math.abs(ofp) > 0.2) {
                cardState = "flux-state-ready";
                actText = "GOTOWY";
                actColor = "text-action-yellow";
                actDesc = "Szukam wejścia...";
            }

            const isActive = (item.fails === 0);
            const statusIcon = isActive 
                ? '<span class="flex h-3 w-3 relative"><span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span><span class="relative inline-flex rounded-full h-3 w-3 bg-emerald-500"></span></span>' 
                : '<span class="h-3 w-3 rounded-full bg-red-500"></span>';

            return `
            <div class="pool-slot ${cardState} relative overflow-hidden group">
                <div class="absolute top-[-10px] right-[-10px] p-4 opacity-10 font-black text-6xl text-white pointer-events-none">#${index+1}</div>
                <div class="flex justify-between items-start z-10">
                    <div>
                        <div class="flex items-center gap-2 mb-1">
                            ${statusIcon}
                            <span class="font-black text-2xl text-white tracking-wide">${ticker}</span>
                        </div>
                        <div class="text-xs font-mono text-gray-400">Cena: <span class="text-white font-bold">${pStr}</span></div>
                    </div>
                    <div class="text-right">
                        <div class="text-[10px] uppercase text-gray-500 font-bold">Flux Score</div>
                        <div class="text-xl font-black ${actionColor}">${score}</div>
                    </div>
                </div>
                
                <div class="flux-action-text ${actionColor} py-2">${actText}</div>
                
                <!-- SEKCJA RYZYKA (TERAZ WIDOCZNA I BEZPIECZNA) -->
                <div class="flex flex-col gap-1 my-2 px-2 py-1 bg-black/20 rounded border border-white/5">
                    <div class="flex justify-between items-center">
                        <span class="text-[10px] uppercase text-red-500 font-bold">SL (Stop)</span>
                        <span class="text-sm font-bold text-red-400 font-mono">${slStr}</span>
                    </div>
                    <div class="flex justify-between items-center">
                        <span class="text-[10px] uppercase text-green-500 font-bold">TP (Target)</span>
                        <span class="text-sm font-bold text-green-400 font-mono">${tpStr}</span>
                    </div>
                     <div class="flex justify-between items-center pt-1 mt-1 border-t border-white/10">
                        <span class="text-[9px] uppercase text-gray-500">R:R</span>
                        <span class="text-[10px] font-bold text-yellow-500 font-mono">${rrStr}</span>
                    </div>
                </div>

                <div class="flex justify-center items-center mb-2 mt-auto">
                    <span class="text-[10px] uppercase text-gray-500 font-bold mr-2">Presja (OFP):</span>
                    <span class="text-sm font-black font-mono ${ofpColor}">${ofpIcon} ${ofp.toFixed(2)}</span>
                </div>
                <div class="mt-auto z-10">
                    <div class="flex justify-between items-end mb-1">
                        <span class="text-[10px] text-gray-400 font-mono uppercase">${actDesc}</span>
                        <span class="text-[10px] font-mono ${vel > 1.0 ? 'text-green-400' : 'text-gray-500'}">Vol: ${vel.toFixed(1)}x</span>
                    </div>
                    <div class="h-2 w-full bg-gray-800 rounded-full overflow-hidden border border-gray-700">
                        <div class="h-full ${score >= 65 ? 'bg-emerald-500' : (score >= 40 ? 'bg-yellow-500' : 'bg-gray-600')} transition-all duration-500" style="width: ${Math.min(100, score)}%"></div>
                    </div>
                </div>
            </div>`;
        }).join('');

        const emptyCount = 8 - activeSlots.length;
        const emptyHtml = Array.from({length: Math.max(0, emptyCount)}, () => `
            <div class="pool-slot bg-[#0d1117] border border-gray-800 border-dashed rounded-lg p-4 flex flex-col items-center justify-center text-gray-600 opacity-50 min-h-[140px]">
                <span class="text-xs font-mono">Slot Wolny</span>
            </div>
        `).join('');

        return `
        <div id="phase5-monitor-view" class="max-w-7xl mx-auto">
            <div class="flex flex-col md:flex-row justify-between items-center mb-6 border-b border-gray-700 pb-4 gap-4">
                <div>
                    <h2 class="text-3xl font-black text-white flex items-center tracking-tight">OMNI-FLUX MONITOR</h2>
                    <p class="text-sm text-gray-500 mt-1 font-mono">RADAR MODE: BULK SCAN | <span class="text-emerald-400">OFP ENABLED</span></p>
                </div>
                <div class="flex items-center gap-4">
                    <div class="text-right mr-4">
                        <p class="text-[10px] text-gray-500 uppercase font-bold">Market Bias</p>
                        <p class="text-lg font-black font-mono ${state.macroBias === 'BEARISH' ? 'text-red-500' : 'text-green-500'}">${state.macroBias || 'NEUTRAL'}</p>
                    </div>
                    <button id="stop-phase5-btn" class="bg-red-900/30 hover:bg-red-800/50 text-red-300 border border-red-800 px-4 py-3 rounded-lg text-xs font-bold flex items-center transition-colors shadow-lg"><i data-lucide="square" class="w-4 h-4 mr-2 fill-current"></i> ZATRZYMAJ</button>
                </div>
            </div>
            <div class="mb-4"><div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">${slotsHtml}${emptyHtml}</div></div>
            <div class="mt-4 p-3 bg-[#161B22] border border-gray-700 rounded text-xs text-gray-400">v5.5 Fixed UI (SL/TP)</div>
        </div>`;

    } catch (e) {
        console.error("Błąd renderowania F5:", e);
        return `<div class="p-4 text-red-500">Błąd F5: ${e.message}</div>`;
    }
};

const renderDashboard = () => {
    const activeCount = state.phase3.filter(s => s.status === 'ACTIVE').length;
    const pendingCount = state.phase3.filter(s => s.status === 'PENDING').length;
    let pulseClass = "pulse-idle";
    let statusColor = "text-green-500";
    const wStatus = state.workerStatus.status || "IDLE";
    if (wStatus.includes("RUNNING") || wStatus.includes("BUSY")) {
        pulseClass = "pulse-busy";
        statusColor = "text-yellow-400";
    } else if (wStatus.includes("PAUSED")) {
        pulseClass = "";
        statusColor = "text-red-500";
    }
    return `<div id="dashboard-view" class="max-w-6xl mx-auto"><div class="mb-6 relative"><i data-lucide="search" class="absolute left-3 top-3 w-5 h-5 text-gray-500"></i><input type="text" placeholder="Wpisz ticker (np. AAPL) i naciśnij Enter" class="w-full bg-[#161B22] border border-gray-700 text-gray-300 rounded-lg pl-10 pr-4 py-3 focus:ring-sky-500 focus:border-sky-500 outline-none shadow-lg placeholder-gray-600"></div><h2 class="text-2xl font-bold text-sky-400 mb-6 border-b border-gray-700 pb-2 flex items-center"><i data-lucide="activity" class="w-6 h-6 mr-3"></i>Centrum Dowodzenia</h2><div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8"><div class="glass-panel p-6 rounded-xl relative overflow-hidden ${pulseClass} transition-all duration-500"><h3 class="font-semibold text-gray-400 flex items-center text-sm mb-3 uppercase tracking-wider"><i data-lucide="cpu" class="w-4 h-4 mr-2 text-sky-400"></i>Status Silnika</h3><p id="dashboard-worker-status" class="text-5xl font-black ${statusColor} tracking-tighter drop-shadow-lg">${wStatus}</p><p id="dashboard-current-phase" class="text-xs text-gray-500 mt-2 font-mono bg-black/30 inline-block px-2 py-1 rounded">Faza: ${state.workerStatus.phase || 'NONE'}</p></div><div class="glass-panel p-6 rounded-xl"><h3 class="font-semibold text-gray-400 flex items-center text-sm mb-3 uppercase tracking-wider"><i data-lucide="bar-chart-2" class="w-4 h-4 mr-2 text-yellow-400"></i>Postęp Skanowania</h3><div class="mt-2 flex items-baseline gap-2"><span id="progress-text" class="text-3xl font-extrabold text-white">0 / 0</span><span class="text-gray-500 text-sm">analiz</span></div><div class="w-full bg-gray-800 rounded-full h-3 mt-4 overflow-hidden border border-gray-700"><div id="progress-bar" class="bg-gradient-to-r from-sky-600 to-blue-500 h-full rounded-full transition-all duration-500 shadow-[0_0_10px_rgba(14,165,233,0.5)]" style="width: 0%"></div></div></div><div class="glass-panel p-6 rounded-xl relative"><div class="absolute top-0 right-0 p-3 opacity-10"><i data-lucide="crosshair" class="w-16 h-16 text-white"></i></div><h3 class="font-semibold text-gray-400 flex items-center text-sm mb-4 uppercase tracking-wider"><i data-lucide="trending-up" class="w-4 h-4 mr-2 text-red-500"></i>Sygnały</h3><div class="flex justify-between items-center"><div class="text-center pr-6 border-r border-gray-700 z-10"><p id="dashboard-active-signals" class="text-5xl font-black text-white">${activeCount + pendingCount}</p><p class="text-[10px] text-gray-500 mt-1 uppercase font-bold tracking-widest text-green-400">Aktywne</p></div><div class="text-center pl-4 flex-grow z-10"><p id="dashboard-discarded-signals" class="text-5xl font-black text-gray-500">${state.discardedSignalCount || 0}</p><p class="text-[10px] text-gray-600 mt-1 uppercase font-bold tracking-widest">Odrzucone</p></div></div></div></div><h3 class="text-sm font-bold text-gray-500 mb-3 uppercase tracking-wider flex items-center"><i data-lucide="terminal" class="w-4 h-4 mr-2"></i>Dziennik Operacyjny</h3><div id="scan-log-container" class="bg-[#0d1117] p-4 rounded-lg inner-shadow h-96 overflow-y-scroll border border-gray-800 custom-scrollbar font-mono text-xs text-gray-400 leading-relaxed shadow-inner"><pre id="scan-log">Inicjalizacja systemu...</pre></div></div>`;
};

const renderH3SignalsPanel = (signals, quotes = {}) => {
    const activeCount = signals.filter(s => s.status === 'ACTIVE').length;
    const pendingCount = signals.filter(s => s.status === 'PENDING').length;
    const cardsHtml = signals.length > 0 ? signals.map(s => {
        let score = 0;
        const strat = getStrategyInfo(s.notes);
        if (s.notes && s.notes.includes("SCORE:")) {
            const match = s.notes.match(/SCORE:\s*(\d+)/);
            if (match) score = parseInt(match[1]);
        }
        let currentPrice = 0;
        if (quotes && quotes[s.ticker] && quotes[s.ticker]['05. price']) {
            const lp = parseFloat(quotes[s.ticker]['05. price']);
            if (!isNaN(lp) && lp > 0) currentPrice = lp;
        }
        if (currentPrice === 0 && s.entry_price) currentPrice = parseFloat(s.entry_price);
        
        const tp = parseFloat(s.take_profit || 0);
        const sl = parseFloat(s.stop_loss || 0);
        const entry = parseFloat(s.entry_price || 0);
        
        let entryPercent = "0%", scopeLeft = "0%";
        if (entry > 0 && sl > 0 && tp > 0) {
            const totalDist = tp - sl;
            if (totalDist > 0) {
                let ep = ((entry - sl) / totalDist) * 100;
                entryPercent = Math.max(0, Math.min(100, ep)) + "%";
                let prog = ((currentPrice - sl) / totalDist) * 100;
                scopeLeft = Math.max(0, Math.min(100, prog)) + "%";
            }
        }
        
        const statusColor = s.status === 'ACTIVE' ? 'border-green-500 shadow-[0_0_15px_rgba(16,185,129,0.2)]' : 'border-yellow-500';
        const priceClass = currentPrice > entry ? "text-green-400 font-bold" : (currentPrice < entry ? "text-red-400 font-bold" : "text-gray-500");

        return `
        <div class="phase3-item bg-[#161B22] rounded-lg p-4 border-l-4 ${statusColor} hover:bg-[#1f2937] transition-all cursor-pointer group" data-ticker="${s.ticker}">
            <div class="flex justify-between items-start mb-3">
                <div><div class="flex items-center gap-2"><h4 class="font-bold text-white text-xl">${s.ticker}</h4><span class="strat-badge ${strat.class}">${strat.name}</span></div><div class="text-xs text-gray-500 mt-1">Wejście: <span class="text-gray-300">${entry.toFixed(2)}</span></div></div>
                <div class="text-right"><span class="text-xs bg-gray-800 border border-gray-700 px-2 py-1 rounded text-sky-300">AQM: ${score}</span></div>
            </div>
            <div class="flex justify-between items-end text-[10px] font-mono text-gray-500 mb-1 mt-2">
                <div class="text-left"><span class="block text-[9px] uppercase text-red-500/70">SL</span><span class="text-red-400 font-bold text-xs">${sl.toFixed(2)}</span></div>
                <div class="text-center pb-1"><span class="${priceClass} text-base">${currentPrice.toFixed(2)}</span></div>
                <div class="text-right"><span class="block text-[9px] uppercase text-green-500/70">TP</span><span class="text-green-400 font-bold text-xs">${tp.toFixed(2)}</span></div>
            </div>
            <div class="sniper-scope-container"><div class="scope-zone-risk" style="width: ${entryPercent}"></div><div class="scope-zone-reward" style="width: calc(100% - ${entryPercent})"></div><div class="entry-marker" style="left: ${entryPercent}"></div><div class="scope-marker" style="left: ${scopeLeft}"></div></div>
        </div>`;
    }).join('') : `<p class="text-center text-gray-500 col-span-full py-20">Brak aktywnych sygnałów H3.</p>`;
    return `<div id="h3-signals-view" class="max-w-7xl mx-auto"><div class="flex justify-between items-center mb-6 border-b border-gray-700 pb-4"><h2 class="text-2xl font-bold text-white">Sygnały H3 Live</h2><div class="flex gap-2"><select id="h3-sort-select" class="bg-[#161B22] border border-gray-700 text-xs rounded text-gray-300"><option value="score">Sort: Score</option><option value="ticker">Sort: Ticker</option></select><button id="h3-refresh-btn" class="p-1 hover:bg-gray-800 rounded text-gray-300">Odśwież</button></div></div><div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">${cardsHtml}</div></div>`;
};

const renderPhaseXView = (candidates) => {
    if (!candidates || !Array.isArray(candidates)) return '<div class="text-center text-gray-500">Brak danych FX.</div>';
    const rows = candidates.map(c => {
        const pumpVal = c.last_pump_percent || 0.0;
        const pumpColor = pumpVal >= 100 ? 'text-purple-400 font-black' : (pumpVal >= 50 ? 'text-pink-400 font-bold' : 'text-gray-400');
        return `<tr class="border-b border-gray-800 hover:bg-[#1f2937] transition-colors"><td class="p-3 font-bold text-pink-500">${c.ticker}</td><td class="p-3 text-right font-mono text-white">${c.price ? c.price.toFixed(4) : '0.0000'}</td><td class="p-3 text-right text-gray-400">${(c.volume_avg/1000000).toFixed(1)}M</td><td class="p-3 text-center font-bold text-white bg-gray-800/50 rounded">${c.pump_count_1y}</td><td class="p-3 text-right ${pumpColor}">+${pumpVal.toFixed(0)}%</td></tr>`;
    }).join('');
    return `<div id="phasex-view" class="max-w-6xl mx-auto"><div class="flex justify-between items-center mb-6 border-b border-gray-700 pb-4"><h2 class="text-2xl font-bold text-white">Faza X: BioX Hunter</h2><button id="run-phasex-scan-btn" class="modal-button modal-button-primary bg-pink-600">Skanuj BioX</button></div><div class="overflow-x-auto bg-[#161B22] rounded border border-gray-700"><table class="w-full text-sm text-left text-gray-300"><thead class="text-xs text-gray-400 uppercase bg-[#0D1117]"><tr><th class="p-3">Ticker</th><th class="p-3 text-right">Cena</th><th class="p-3 text-right">Vol</th><th class="p-3 text-center">Pompy</th><th class="p-3 text-right">Moc</th></tr></thead><tbody>${rows}</tbody></table></div></div>`;
};

const renderPortfolio = (holdings, quotes) => {
    if (!holdings || !Array.isArray(holdings)) return '<p class="text-gray-500">Brak danych portfela.</p>';
    const rows = holdings.map(h => {
        const price = quotes[h.ticker]?.['05. price'] ? parseFloat(quotes[h.ticker]['05. price']) : 0;
        const priceDisplay = price > 0 ? price.toFixed(2) : "---";
        return `<tr class="border-b border-gray-800 hover:bg-[#1f2937]"><td class="p-3 font-bold text-sky-400">${h.ticker}</td><td class="p-3 text-right">${h.quantity}</td><td class="p-3 text-right">${h.average_buy_price.toFixed(4)}</td><td class="p-3 text-right text-white">${priceDisplay}</td><td class="p-3 text-right"><button data-ticker="${h.ticker}" data-quantity="${h.quantity}" class="sell-stock-btn text-xs bg-red-600/20 text-red-300 py-1 px-3 rounded">Sprzedaj</button></td></tr>`;
    }).join('');
    return `<div id="portfolio-view" class="max-w-6xl mx-auto"><h2 class="text-2xl font-bold text-sky-400 mb-6 border-b border-gray-700 pb-2">Portfel</h2><div class="overflow-x-auto bg-[#161B22] rounded border border-gray-700"><table class="w-full text-sm text-left text-gray-300"><thead class="text-xs text-gray-400 uppercase bg-[#0D1117]"><tr><th class="p-3">Ticker</th><th class="p-3 text-right">Ilość</th><th class="p-3 text-right">Cena Zakupu</th><th class="p-3 text-right">Kurs</th><th class="p-3 text-right">Akcja</th></tr></thead><tbody>${rows}</tbody></table></div></div>`;
};

const renderTransactions = (txHistory) => {
    if (!txHistory || !Array.isArray(txHistory)) return '<p class="text-gray-500">Brak historii.</p>';
    const rows = txHistory.map(t => {
        return `<tr class="border-b border-gray-800 hover:bg-[#1f2937]"><td class="p-3 text-gray-400 text-xs">${new Date(t.transaction_date).toLocaleDateString()}</td><td class="p-3 font-bold text-sky-400">${t.ticker}</td><td class="p-3">${t.transaction_type}</td><td class="p-3 text-right">${t.quantity}</td><td class="p-3 text-right">${t.price_per_share.toFixed(4)}</td><td class="p-3 text-right">${t.profit_loss_usd ? t.profit_loss_usd.toFixed(2) : '-'}</td></tr>`;
    }).join('');
    return `<div id="transactions-view" class="max-w-6xl mx-auto"><h2 class="text-2xl font-bold text-sky-400 mb-6 border-b border-gray-700 pb-2">Historia Transakcji</h2><div class="overflow-x-auto bg-[#161B22] rounded border border-gray-700"><table class="w-full text-sm text-left text-gray-300"><thead class="text-xs text-gray-400 uppercase bg-[#0D1117]"><tr><th class="p-3">Data</th><th class="p-3">Ticker</th><th class="p-3">Typ</th><th class="p-3 text-right">Ilość</th><th class="p-3 text-right">Cena</th><th class="p-3 text-right">PnL</th></tr></thead><tbody>${rows}</tbody></table></div></div>`;
};

const renderAgentReport = (report) => {
    return `<div id="agent-report-view" class="max-w-6xl mx-auto"><h2 class="text-2xl font-bold text-sky-400 mb-6">Raport Agenta</h2><p class="text-gray-400">Transakcji: ${report.total_trades_count}</p></div>`;
};

const renderOptimizationResults = (job) => {
    if (!job) return `<p class="text-gray-500">Brak danych.</p>`;
    return `<div class="space-y-6"><h4 class="text-sm text-gray-400 font-bold">Wyniki Optymalizacji</h4><p class="text-gray-300">Best Score: ${job.best_score}</p></div>`;
};

// === EXPORT ===
export const renderers = {
    loading: renderLoading,
    phase1List: renderPhase1List,
    phase3List: renderPhase3List,
    phase4View: renderPhase4View,
    phase5View: renderPhase5View,
    dashboard: renderDashboard,
    h3SignalsPanel: renderH3SignalsPanel,
    phaseXView: renderPhaseXView,
    portfolio: renderPortfolio,
    transactions: renderTransactions,
    agentReport: renderAgentReport,
    optimizationResults: renderOptimizationResults
};

export const ui = {
    init: () => {
        const get = (id) => document.getElementById(id);
        
        const h3ModalContent = document.querySelector('#h3-live-modal .grid');
        if (h3ModalContent && !document.getElementById('h3-live-strategy-mode')) {
            const stratDiv = document.createElement('div');
            stratDiv.innerHTML = `<label class="block text-xs font-bold text-gray-400 mb-1 uppercase">Tryb Strategii</label><select id="h3-live-strategy-mode" class="modal-input cursor-pointer hover:bg-gray-800 transition-colors"><option value="H3">H3 (Elite Sniper)</option><option value="AQM">AQM (Adaptive Quantum)</option></select><p class="text-[10px] text-gray-600 mt-1">Wymusza logikę obliczeń (H3 vs AQM).</p>`;
            h3ModalContent.insertBefore(stratDiv, h3ModalContent.firstChild);
        }
        if (h3ModalContent && !document.getElementById('h3-live-aqm-min')) {
            const newDiv = document.createElement('div');
            newDiv.innerHTML = `<label class="block text-xs font-bold text-gray-400 mb-1 uppercase">Min. Component Score (AQM)</label><input type="number" id="h3-live-aqm-min" class="modal-input" placeholder="0.5" step="0.1" value="0.5"><p class="text-[10px] text-gray-600 mt-1">Próg dla QPS, VES, MRS.</p>`;
            h3ModalContent.appendChild(newDiv);
        }

        const quantumModalContent = document.querySelector('#quantum-optimization-modal .space-y-3');
        if (quantumModalContent && !document.getElementById('qo-period-select')) {
            const periodDiv = document.createElement('div');
            periodDiv.innerHTML = `<label class="block text-xs font-bold text-gray-400 mb-1 uppercase">Okres Analizy</label><select id="qo-period-select" class="modal-input cursor-pointer hover:bg-gray-800 transition-colors"><option value="FULL">Pełny Rok (Standard)</option><option value="Q1">Q1 (Styczeń - Marzec)</option><option value="Q2">Q2 (Kwiecień - Czerwiec)</option><option value="Q3">Q3 (Lipiec - Wrzesień)</option><option value="Q4">Q4 (Październik - Grudzień)</option></select><p class="text-xs text-gray-500 mt-1">Wybierz sezonowość.</p>`;
            quantumModalContent.insertBefore(periodDiv, quantumModalContent.children[1]);
        }

        const sidebarControls = document.querySelector('#app-sidebar .pt-4 .space-y-2');
        if (sidebarControls) {
            if (!document.getElementById('btn-phasex-scan')) {
                 const btn = document.createElement('button');
                 btn.id = 'btn-phasex-scan';
                 btn.className = 'w-full text-left flex items-center bg-pink-600/20 hover:bg-pink-600/40 text-pink-300 py-2 px-3 rounded-md text-sm transition-colors disabled:opacity-50 disabled:cursor-not-allowed mt-2';
                 btn.innerHTML = '<i data-lucide="biohazard" class="mr-2 h-4 w-4"></i>Skanuj Faza X (BioX)';
                 sidebarControls.appendChild(btn);
            }
            if (!document.getElementById('btn-phase4-scan')) {
                 const btnH4 = document.createElement('button');
                 btnH4.id = 'btn-phase4-scan';
                 btnH4.className = 'w-full text-left flex items-center bg-amber-600/20 hover:bg-amber-600/40 text-amber-300 py-2 px-3 rounded-md text-sm transition-colors disabled:opacity-50 disabled:cursor-not-allowed mt-2';
                 btnH4.innerHTML = '<i data-lucide="zap" class="mr-2 h-4 w-4"></i>Skanuj H4 (Kinetic)';
                 sidebarControls.appendChild(btnH4);
            }
            if (!document.getElementById('btn-phase5-scan')) {
                 const btnF5 = document.createElement('button');
                 btnF5.id = 'btn-phase5-scan';
                 btnF5.className = 'w-full text-left flex items-center bg-emerald-600/20 hover:bg-emerald-600/40 text-emerald-300 py-2 px-3 rounded-md text-sm transition-colors disabled:opacity-50 disabled:cursor-not-allowed mt-2';
                 btnF5.innerHTML = '<i data-lucide="waves" class="mr-2 h-4 w-4"></i>Start F5 (Omni-Flux)';
                 sidebarControls.appendChild(btnF5);
            }
        }

        return {
            loginScreen: get('login-screen'),
            dashboardScreen: get('dashboard'),
            loginButton: get('login-button'),
            loginStatusText: get('login-status-text'),
            mainContent: get('main-content'),
            
            btnPhase1: get('btn-phase-1'),
            btnPhase3: get('btn-phase-3'),
            btnPhaseX: get('btn-phasex-scan'),
            btnPhase4: get('btn-phase4-scan'),
            btnPhase5: get('btn-phase5-scan'),
            
            h3LiveModal: {
                backdrop: get('h3-live-modal'),
                strategyMode: get('h3-live-strategy-mode'),
                percentile: get('h3-live-percentile'),
                mass: get('h3-live-mass'),
                minScore: get('h3-live-min-score'),
                tp: get('h3-live-tp'),
                sl: get('h3-live-sl'),
                maxHold: get('h3-live-hold'),
                cancelBtn: get('h3-live-cancel-btn'),
                startBtn: get('h3-live-start-btn')
            },

            signalDetails: {
                backdrop: get('signal-details-modal'),
                ticker: get('sd-ticker'),
                companyName: get('sd-company-name'),
                validityBadge: get('sd-validity-badge'),
                currentPrice: get('sd-current-price'),
                changePercent: get('sd-change-percent'),
                marketStatus: get('sd-market-status'),
                nyTime: get('sd-ny-time'),
                countdown: get('sd-countdown'),
                entry: get('sd-entry-price'),
                tp: get('sd-take-profit'),
                sl: get('sd-stop-loss'),
                rr: get('sd-risk-reward'),
                sector: get('sd-sector'),
                industry: get('sd-industry'),
                description: get('sd-description'), 
                generationDate: get('sd-generation-date'),
                validityMessage: get('sd-validity-message'),
                closeBtn: get('sd-close-btn'),
                buyBtn: get('sd-buy-btn'),
                ghostBtn: null
            },

            quantumModal: {
                backdrop: get('quantum-optimization-modal'),
                strategySelect: get('qo-strategy-select'),
                periodSelect: get('qo-period-select'),
                yearInput: get('qo-year-input'),
                trialsInput: get('qo-trials-input'),
                cancelBtn: get('qo-cancel-btn'),
                startBtn: get('qo-start-btn'),
                statusMessage: get('qo-status-message')
            },
            optimizationResultsModal: {
                backdrop: get('optimization-results-modal'),
                content: get('optimization-results-content'),
                closeBtn: get('optimization-results-close-btn')
            },
            tradeAuditModal: {
                backdrop: get('trade-audit-modal'),
                closeBtn: get('ta-close-btn'),
                expPf: get('ta-exp-pf'),
                expWr: get('ta-exp-wr'),
                actPl: get('ta-act-pl'),
                status: get('ta-status'),
                reportContent: get('ta-report-content'),
                suggestionBox: get('ta-suggestion-box'),
                suggestionText: get('ta-suggestion-text')
            },

            startBtn: get('start-btn'),
            pauseBtn: get('pause-btn'),
            resumeBtn: get('resume-btn'),
            apiStatus: get('api-status'),
            workerStatusText: get('worker-status-text'),
            dashboardLink: get('dashboard-link'),
            h3SignalsLink: get('h3-signals-link'),
            portfolioLink: get('portfolio-link'),
            transactionsLink: get('transactions-link'),
            agentReportLink: get('agent-report-link'),
            heartbeatStatus: get('heartbeat-status'),
            alertContainer: get('system-alert-container'),
            phase1: { list: get('phase-1-list'), count: get('phase-1-count') },
            phase3: { list: get('phase-3-list'), count: get('phase-3-count') },
            buyModal: { backdrop: get('buy-modal'), tickerSpan: get('buy-modal-ticker'), quantityInput: get('buy-quantity'), priceInput: get('buy-price'), cancelBtn: get('buy-cancel-btn'), confirmBtn: get('buy-confirm-btn') },
            sellModal: { backdrop: get('sell-modal'), tickerSpan: get('sell-modal-ticker'), maxQuantitySpan: get('sell-max-quantity'), quantityInput: get('sell-quantity'), priceInput: get('sell-price'), cancelBtn: get('sell-cancel-btn'), confirmBtn: get('sell-confirm-btn') },
            aiReportModal: { backdrop: get('ai-report-modal'), content: get('ai-report-content'), closeBtn: get('ai-report-close-btn') },
            h3DeepDiveModal: { backdrop: get('h3-deep-dive-modal'), yearInput: get('h3-deep-dive-year-input'), runBtn: get('run-h3-deep-dive-btn'), statusMsg: get('h3-deep-dive-status-message'), content: get('h3-deep-dive-report-content'), closeBtn: get('h3-deep-dive-close-btn') },
            
            sidebar: get('app-sidebar'),
            sidebarBackdrop: get('sidebar-backdrop'),
            mobileMenuBtn: get('mobile-menu-btn'),
            mobileSidebarCloseBtn: get('mobile-sidebar-close'),
            sidebarNav: document.querySelector('#app-sidebar nav'),
            sidebarPhasesContainer: get('phases-container')
        };
    }
};
