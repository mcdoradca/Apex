import { logger, state, REPORT_PAGE_SIZE } from './state.js';

// === CSS INJECTION: HUD, ANIMACJE, SNIPER SCOPE, GLASSMORPHISM ===
const style = document.createElement('style');
style.textContent = `
    /* Animacje Pulsu */
    @keyframes heartbeat-idle {
        0% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.4); transform: scale(1); }
        70% { box-shadow: 0 0 0 10px rgba(16, 185, 129, 0); transform: scale(1.02); }
        100% { box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); transform: scale(1); }
    }
    @keyframes heartbeat-busy {
        0% { box-shadow: 0 0 0 0 rgba(234, 179, 8, 0.7); transform: scale(1); }
        25% { transform: scale(1.03); }
        50% { box-shadow: 0 0 0 15px rgba(234, 179, 8, 0); transform: scale(1); }
        100% { box-shadow: 0 0 0 0 rgba(234, 179, 8, 0); transform: scale(1); }
    }
    .pulse-idle { animation: heartbeat-idle 3s infinite ease-in-out; }
    .pulse-busy { animation: heartbeat-busy 0.8s infinite ease-in-out; }
    
    /* Efekt Szkła (HUD) */
    .glass-panel {
        background: rgba(22, 27, 34, 0.85);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid rgba(48, 54, 61, 0.8);
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.5);
    }

    /* Sniper Scope Bar (Pasek Setupu) */
    .sniper-scope-container {
        height: 14px;
        background: #0f172a;
        border-radius: 4px;
        position: relative;
        overflow: hidden;
        margin-top: 15px;
        border: 1px solid #374151;
        display: flex;
        box-shadow: inset 0 2px 6px 0 rgba(0, 0, 0, 0.6);
    }
    /* Strefa SL (Czerwona Gradient) - Lewa strona */
    .scope-zone-risk { 
        background: linear-gradient(90deg, rgba(220, 38, 38, 0.9) 0%, rgba(127, 29, 29, 0.4) 100%);
        height: 100%; 
        border-right: 1px solid rgba(255,255,255,0.1);
    } 
    /* Strefa TP (Zielona Gradient) - Prawa strona */
    .scope-zone-reward { 
        background: linear-gradient(90deg, rgba(6, 78, 59, 0.4) 0%, rgba(16, 185, 129, 0.9) 100%);
        height: 100%; 
        flex-grow: 1; 
    }
    
    /* Biały Celownik (Aktualna Cena) */
    .scope-marker {
        position: absolute;
        top: -1px;
        bottom: -1px;
        width: 3px;
        background: #ffffff;
        box-shadow: 0 0 8px 3px rgba(255, 255, 255, 0.9);
        z-index: 30;
        transform: translateX(-50%);
        transition: left 0.8s cubic-bezier(0.22, 1, 0.36, 1);
    }
    
    /* Żółta Linia (Entry Price) */
    .entry-marker {
        position: absolute;
        top: 0;
        bottom: 0;
        width: 2px;
        background: rgba(250, 204, 21, 0.8);
        z-index: 20;
        box-shadow: 0 0 5px rgba(250, 204, 21, 0.5);
    }
    
    .sector-badge-up { background-color: rgba(6, 78, 59, 0.6); color: #6ee7b7; border: 1px solid rgba(16, 185, 129, 0.3); }
    .sector-badge-down { background-color: rgba(127, 29, 29, 0.6); color: #fca5a5; border: 1px solid rgba(239, 68, 68, 0.3); }
    .extended-hours-text { color: #c084fc; font-weight: bold; text-shadow: 0 0 5px rgba(192, 132, 252, 0.3); }

    /* Nowe Badges Strategii */
    .strat-badge { font-size: 10px; font-weight: 800; padding: 2px 6px; border-radius: 4px; text-transform: uppercase; letter-spacing: 0.05em; border: 1px solid transparent; }
    .strat-badge-h3 { background-color: rgba(124, 58, 237, 0.2); color: #a78bfa; border-color: rgba(124, 58, 237, 0.4); box-shadow: 0 0 5px rgba(124, 58, 237, 0.2); }
    .strat-badge-aqm { background-color: rgba(6, 182, 212, 0.2); color: #22d3ee; border-color: rgba(6, 182, 212, 0.4); box-shadow: 0 0 5px rgba(6, 182, 212, 0.2); }
    .strat-badge-biox { background-color: rgba(236, 72, 153, 0.2); color: #f472b6; border-color: rgba(236, 72, 153, 0.4); box-shadow: 0 0 5px rgba(236, 72, 153, 0.2); }
    
    /* H4 Kinetic Badge */
    .strat-badge-h4 { background-color: rgba(245, 158, 11, 0.2); color: #fbbf24; border-color: rgba(245, 158, 11, 0.4); box-shadow: 0 0 5px rgba(245, 158, 11, 0.2); }
    
    /* V5 Flux Badge (NOWOŚĆ) */
    .strat-badge-flux { background-color: rgba(16, 185, 129, 0.2); color: #6ee7b7; border-color: rgba(16, 185, 129, 0.4); box-shadow: 0 0 8px rgba(16, 185, 129, 0.3); }
    
    .strat-badge-unknown { background-color: rgba(75, 85, 99, 0.3); color: #9ca3af; border-color: rgba(75, 85, 99, 0.5); }
    
    /* Kinetic Score Bar */
    .kinetic-bar-bg { background: rgba(255,255,255,0.1); height: 6px; width: 100%; border-radius: 3px; overflow: hidden; margin-top: 4px; }
    .kinetic-bar-fill { height: 100%; border-radius: 3px; transition: width 0.5s ease; }

    /* Flux Pulse Animation (V5) */
    .flux-pulse { animation: text-pulse 1.5s infinite; }
    @keyframes text-pulse {
        0% { opacity: 1; text-shadow: 0 0 5px rgba(110, 231, 183, 0.5); }
        50% { opacity: 0.8; text-shadow: 0 0 15px rgba(110, 231, 183, 0.8); }
        100% { opacity: 1; text-shadow: 0 0 5px rgba(110, 231, 183, 0.5); }
    }
    
    /* Active Pool Slot (V5) */
    .pool-slot {
        background: #1f2937;
        border: 2px solid #374151; /* Grubsza ramka */
        border-radius: 8px;
        padding: 12px;
        position: relative;
        overflow: hidden;
        transition: all 0.3s ease;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        min-height: 140px; /* Stała wysokość */
    }
    
    /* Kolory Stanów (Flux States) */
    .flux-state-wait { border-color: #4b5563; background: #1f2937; opacity: 0.8; }
    .flux-state-ready { border-color: #fbbf24; background: linear-gradient(135deg, #1f2937 0%, #451a03 100%); box-shadow: 0 0 10px rgba(251, 191, 36, 0.2); }
    .flux-state-action { border-color: #10b981; background: linear-gradient(135deg, #1f2937 0%, #064e3b 100%); box-shadow: 0 0 15px rgba(16, 185, 129, 0.4); animation: border-pulse 2s infinite; }
    
    @keyframes border-pulse {
        0% { box-shadow: 0 0 5px rgba(16, 185, 129, 0.4); border-color: #10b981; }
        50% { box-shadow: 0 0 20px rgba(16, 185, 129, 0.7); border-color: #34d399; }
        100% { box-shadow: 0 0 5px rgba(16, 185, 129, 0.4); border-color: #10b981; }
    }

    .flux-action-text { font-size: 1.5rem; font-weight: 900; letter-spacing: 0.05em; text-transform: uppercase; text-align: center; margin-top: auto; margin-bottom: auto; }
    .text-action-green { color: #34d399; text-shadow: 0 0 10px rgba(52, 211, 153, 0.6); }
    .text-action-yellow { color: #fcd34d; text-shadow: 0 0 10px rgba(252, 211, 77, 0.6); }
    .text-action-gray { color: #6b7280; }

`;
document.head.appendChild(style);

// === TACTICAL AUDIO SYSTEM ===
const synth = window.speechSynthesis;
let lastSpokenSignal = "";

const playTacticalAlert = (ticker, score) => {
    if (!synth || state.h3SortBy !== 'score') return;
    if (lastSpokenSignal === ticker) return;
    
    const text = `Commander. Target acquired: ${ticker}. Score: ${score}.`;
    const utterance = new SpeechSynthesisUtterance(text);
    utterance.rate = 1.1; 
    utterance.pitch = 0.9; 
    utterance.volume = 0.8;
    
    const voices = synth.getVoices();
    const enVoice = voices.find(v => v.lang.includes('en-US') && v.name.includes('Google')) || voices[0];
    if (enVoice) utterance.voice = enVoice;
    
    synth.speak(utterance);
    lastSpokenSignal = ticker;
};

// === HELPER: Wykrywanie Strategii ===
const getStrategyInfo = (notes) => {
    if (!notes) return { name: 'UNK', class: 'strat-badge-unknown', full: 'Unknown' };
    
    const n = notes.toUpperCase();
    if (n.includes("STRATEGIA: FLUX") || n.includes("STRATEGIA: OMNI-FLUX")) return { name: 'FLUX', class: 'strat-badge-flux', full: 'Apex Flux (AF1)' };
    if (n.includes("STRATEGIA: H3") || n.includes("STRATEGY: H3")) return { name: 'H3', class: 'strat-badge-h3', full: 'H3 Elite Sniper' };
    if (n.includes("STRATEGIA: AQM") || n.includes("STRATEGY: AQM")) return { name: 'AQM', class: 'strat-badge-aqm', full: 'AQM V4' };
    if (n.includes("STRATEGIA: BIOX") || n.includes("STRATEGY: BIOX")) return { name: 'BIOX', class: 'strat-badge-biox', full: 'BioX Pump' };
    
    return { name: 'MANUAL', class: 'strat-badge-unknown', full: 'Manual/Other' };
};

// =========================================================================
// === EXPORT 1: RENDERERS (Wyciągnięte na zewnątrz dla Logic.js) ===
// =========================================================================
export const renderers = {
    loading: (text) => `<div class="text-center py-10"><div role="status" class="flex flex-col items-center"><svg aria-hidden="true" class="inline w-8 h-8 text-gray-600 animate-spin fill-sky-500" viewBox="0 0 100 101" fill="none" xmlns="http://www.w3.org/2000/svg"><path d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z" fill="currentColor"/><path d="M93.9676 39.0409C96.393 38.4038 97.8624 35.9116 97.0079 33.5539C95.2932 28.8227 92.871 24.3692 89.8167 20.348C85.8452 15.1192 80.8826 10.7238 75.2124 7.41289C69.5422 4.10194 63.2754 1.94025 56.7698 1.05124C51.7666 0.367541 46.6976 0.446843 41.7345 1.27873C39.2613 1.69328 37.813 4.19778 38.4501 6.62326C39.0873 9.04874 41.5694 10.4717 44.0505 10.1071C47.8511 9.54855 51.7191 9.52689 55.5402 10.0491C60.8642 10.7766 65.9928 12.5457 70.6331 15.2552C75.2735 17.9648 79.3347 21.5619 82.5849 25.841C84.9175 28.9121 86.7997 32.2913 88.1811 35.8758C89.083 38.2158 91.5421 39.6781 93.9676 39.0409Z" fill="currentColor"/></svg><p class="text-sky-400 mt-4">${text}</p></div></div>`,
    
    phase1List: (candidates) => candidates.map(c => {
        let sectorBadge = "";
        if (c.sector_ticker) {
            const isTrendUp = parseFloat(c.sector_trend_score || 0) > 0;
            const badgeClass = isTrendUp ? "sector-badge-up" : "sector-badge-down";
            const icon = isTrendUp ? "↗" : "↘";
            sectorBadge = `<span class="text-[9px] ml-2 px-1.5 py-0.5 rounded ${badgeClass} font-mono tracking-tighter" title="Kondycja Sektora">${c.sector_ticker} ${icon}</span>`;
        }
        return `<div class="candidate-item flex justify-between items-center text-xs p-2 rounded-md cursor-default transition-colors phase-1-text border-b border-gray-800 last:border-0 hover:bg-gray-800"><div><span class="font-bold text-sky-400">${c.ticker}</span>${sectorBadge}</div><span class="text-gray-500 font-mono">${c.price ? c.price.toFixed(2) : '-'}</span></div>`;
    }).join('') || `<p class="text-xs text-gray-500 p-2">Brak wyników.</p>`,
    
    phase3List: (signals) => signals.map(s => {
        let statusClass = s.status === 'ACTIVE' ? 'text-green-400' : 'text-yellow-400';
        let icon = s.status === 'ACTIVE' ? 'zap' : 'hourglass';
        let scoreDisplay = "";
        let scoreVal = 0;
        const strat = getStrategyInfo(s.notes);
        if (s.notes && s.notes.includes("SCORE:")) {
            try {
                const parts = s.notes.split("SCORE:");
                if (parts.length > 1) {
                    const scorePart = parts[1].trim().split(/[\s\/]/)[0].replace(",", "").replace(".", "."); 
                    scoreVal = parseFloat(scorePart);
                    
                    let scoreBg = "bg-blue-900/30 text-blue-300";
                    if (strat.name === 'FLUX') {
                        scoreBg = "bg-emerald-900/30 text-emerald-300 flux-pulse";
                    }
                    
                    if (strat.name !== 'BIOX') {
                        scoreDisplay = `<span class="ml-2 text-xs ${scoreBg} px-1 rounded">SC: ${scoreVal.toFixed(0)}</span>`;
                    } else {
                        scoreDisplay = `<span class="ml-2 text-xs text-pink-300 bg-pink-900/30 px-1 rounded">MOC: ${scoreVal.toFixed(0)}%</span>`;
                    }
                }
            } catch(e) {}
        }
        if (s.status === 'ACTIVE' && scoreVal >= 0.80) { playTacticalAlert(s.ticker, (scoreVal * 100).toFixed(0)); }
        return `<div class="candidate-item phase3-item flex items-center text-xs p-2 rounded-md cursor-pointer transition-colors ${statusClass} hover:bg-gray-800" data-ticker="${s.ticker}"><i data-lucide="${icon}" class="w-4 h-4 mr-2"></i><span class="font-bold">${s.ticker}</span><span class="ml-2 strat-badge ${strat.class}">${strat.name}</span>${scoreDisplay}<span class="ml-auto text-gray-500">${s.status}</span></div>`;
    }).join('') || `<p class="text-xs text-gray-500 p-2">Brak sygnałów.</p>`,

    phase4View: (candidates) => {
        const rows = candidates.map(c => {
            const scoreColor = c.kinetic_score >= 80 ? 'text-amber-400 font-black' : (c.kinetic_score >= 50 ? 'text-yellow-200 font-bold' : 'text-gray-400');
            const shotsClass = c.max_daily_shots >= 3 ? 'text-green-400 font-bold' : 'text-gray-300';
            const floorClass = c.hard_floor_violations > 0 ? 'text-red-500 font-bold' : 'text-gray-500';
            const barWidth = Math.min(100, c.kinetic_score);
            const barColor = c.kinetic_score >= 70 ? '#fbbf24' : '#d97706';
            return `<tr class="border-b border-gray-800 hover:bg-[#1f2937] transition-colors"><td class="p-3 font-bold text-amber-500">${c.ticker}</td><td class="p-3 text-right font-mono text-white">${c.price ? c.price.toFixed(2) : '---'}</td><td class="p-3"><div class="flex items-center justify-between"><span class="${scoreColor}">${c.kinetic_score}</span><span class="text-xs text-gray-600 ml-2">PKT</span></div><div class="kinetic-bar-bg"><div class="kinetic-bar-fill" style="width: ${barWidth}%; background-color: ${barColor};"></div></div></td><td class="p-3 text-right text-gray-300 font-mono">${c.total_2pct_shots_ytd || 0}</td><td class="p-3 text-right ${shotsClass} font-mono">${c.max_daily_shots || 0}</td><td class="p-3 text-right text-sky-300 font-mono">~${c.avg_swing_size ? c.avg_swing_size.toFixed(2) : '0.00'}%</td><td class="p-3 text-right ${floorClass}">${c.hard_floor_violations}</td></tr>`;
        }).join('');
        const tableHeader = `<thead class="text-xs text-gray-400 uppercase bg-[#0D1117] sticky top-0"><tr><th class="p-3 text-left">Ticker</th><th class="p-3 text-right">Cena</th><th class="p-3 text-left">Kinetic Score</th><th class="p-3 text-right">Strzały (30d)</th><th class="p-3 text-right">Max Dziennie</th><th class="p-3 text-right">Śr. Zasięg</th><th class="p-3 text-right">Podłoga (-5%)</th></tr></thead>`;
        return `<div id="phase4-view" class="max-w-6xl mx-auto"><div class="flex justify-between items-center mb-6 border-b border-gray-700 pb-4"><div><h2 class="text-2xl font-bold text-white flex items-center"><i data-lucide="zap" class="w-6 h-6 mr-3 text-amber-500"></i>Faza 4: Kinetic Alpha</h2><p class="text-sm text-gray-500 mt-1">Ranking "Petard": Akcje z największą liczbą impulsów intraday >2%.</p></div><button id="run-phase4-scan-btn" class="modal-button modal-button-primary bg-amber-600 hover:bg-amber-700 flex items-center shadow-[0_0_15px_rgba(217,119,6,0.3)]"><i data-lucide="radar" class="w-4 h-4 mr-2"></i> Skanuj H4</button></div>${candidates.length === 0 ? '<div class="text-center py-10 bg-[#161B22] rounded-lg border border-gray-700"><i data-lucide="search" class="w-12 h-12 mx-auto text-gray-600 mb-3"></i><p class="text-gray-500">Brak danych. Uruchom skaner, aby znaleźć petardy.</p></div>' : `<div class="overflow-x-auto bg-[#161B22] rounded-lg border border-gray-700 shadow-xl"><table class="w-full text-sm text-left text-gray-300">${tableHeader}<tbody>${rows}</tbody></table></div>`}</div>`;
    },

    phase5View: (poolData) => {
        const activeSlots = poolData.slice(0, 8); 
        
        const slotsHtml = activeSlots.map((item, index) => {
            const elast = item.elasticity || 0;
            const vel = item.velocity || 0;
            const score = item.flux_score || 0;
            const price = item.price || 0;
            const ofp = item.ofp || 0.0;
            
            const slPrice = item.stop_loss || 0.0;
            const tpPrice = item.take_profit || 0.0;
            const rrRatio = item.risk_reward || 0.0;
            
            let cardState = "flux-state-wait";
            let actionText = "CZEKAJ";
            let actionColor = "text-action-gray";
            let actionDescription = "Monitorowanie...";
            let ofpColor = "text-gray-500";
            let ofpIcon = "";

            if (ofp > 0.1) {
                ofpColor = "text-green-400";
                ofpIcon = "↑";
            } else if (ofp < -0.1) {
                ofpColor = "text-red-400";
                ofpIcon = "↓";
            } else {
                ofpIcon = "—";
            }
            
            if (score >= 65) {
                cardState = "flux-state-action";
                actionText = "KUPUJ";
                actionColor = "text-action-green";
                actionDescription = "Setup Potwierdzony!";
                
                if (elast < -1.0) actionText = "DIP BUY";
                else if (elast > 0.5) actionText = "BREAKOUT";
                
            } 
            else if (score >= 40 || vel > 1.5 || Math.abs(ofp) > 0.2) {
                cardState = "flux-state-ready";
                actionText = "GOTOWY";
                actionColor = "text-action-yellow";
                actionDescription = "Szukam wejścia...";
                
                if (ofp > 0.3) actionDescription = "Silna Presja Popytu!";
                else if (ofp < -0.3) actionDescription = "Silna Presja Podaży!";
            }
            
            const isActive = item.fails === 0;
            const statusIcon = isActive ? '<span class="flex h-3 w-3 relative"><span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span><span class="relative inline-flex rounded-full h-3 w-3 bg-emerald-500"></span></span>' : '<span class="h-3 w-3 rounded-full bg-red-500"></span>';
            
            return `
            <div class="pool-slot ${cardState} relative overflow-hidden group">
                <div class="absolute top-[-10px] right-[-10px] p-4 opacity-10 font-black text-6xl text-white pointer-events-none">#${index+1}</div>
                <div class="flex justify-between items-start z-10">
                    <div>
                        <div class="flex items-center gap-2 mb-1">
                            ${statusIcon}
                            <span class="font-black text-2xl text-white tracking-wide">${item.ticker}</span>
                        </div>
                        <div class="text-xs font-mono text-gray-400">Cena: <span class="text-white font-bold">${price.toFixed(2)}</span></div>
                    </div>
                    <div class="text-right">
                        <div class="text-[10px] uppercase text-gray-500 font-bold">Flux Score</div>
                        <div class="text-xl font-black ${actionColor}">${score.toFixed(0)}</div>
                    </div>
                </div>
                <div class="flux-action-text ${actionColor} py-2">
                    ${actionText}
                </div>
                <div class="flex flex-col gap-1 my-2">
                    <div class="flex justify-between items-center">
                        <span class="text-[10px] uppercase text-red-500 font-bold">SL (${rrRatio.toFixed(1)}R)</span>
                        <span class="text-sm font-bold text-red-400 font-mono">${slPrice > 0 ? slPrice.toFixed(2) : '---'}</span>
                    </div>
                    <div class="flex justify-between items-center">
                        <span class="text-[10px] uppercase text-green-500 font-bold">TP (${rrRatio.toFixed(1)}R)</span>
                        <span class="text-sm font-bold text-green-400 font-mono">${tpPrice > 0 ? tpPrice.toFixed(2) : '---'}</span>
                    </div>
                </div>
                <div class="flex justify-center items-center mb-2 mt-auto">
                    <span class="text-[10px] uppercase text-gray-500 font-bold mr-2">Presja (OFP):</span>
                    <span class="text-sm font-black font-mono ${ofpColor}">${ofpIcon} ${ofp.toFixed(2)}</span>
                </div>
                <div class="mt-auto z-10">
                    <div class="flex justify-between items-end mb-1">
                        <span class="text-[10px] text-gray-400 font-mono uppercase">${actionDescription}</span>
                        <span class="text-[10px] font-mono ${vel > 1.0 ? 'text-green-400' : 'text-gray-500'}">Vol: ${vel.toFixed(1)}x</span>
                    </div>
                    <div class="h-2 w-full bg-gray-800 rounded-full overflow-hidden border border-gray-700">
                        <div class="h-full ${score >= 65 ? 'bg-emerald-500' : (score >= 40 ? 'bg-yellow-500' : 'bg-gray-600')} transition-all duration-500" style="width: ${Math.min(100, score)}%"></div>
                    </div>
                </div>
            </div>`;
        }).join('');

        const emptySlotsCount = 8 - activeSlots.length;
        const emptySlotsHtml = Array(Math.max(0, emptySlotsCount)).fill(0).map((_, i) => `
            <div class="pool-slot bg-[#0d1117] border border-gray-800 border-dashed rounded-lg p-4 flex flex-col items-center justify-center text-gray-600 opacity-50 min-h-[140px]">
                <i data-lucide="loader" class="w-8 h-8 mb-2 animate-spin text-gray-700"></i>
                <span class="text-xs font-mono">Slot Wolny</span>
            </div>
        `).join('');

        return `
        <div id="phase5-monitor-view" class="max-w-7xl mx-auto">
            <div class="flex flex-col md:flex-row justify-between items-center mb-6 border-b border-gray-700 pb-4 gap-4">
                <div>
                    <h2 class="text-3xl font-black text-white flex items-center tracking-tight">
                        <i data-lucide="waves" class="w-8 h-8 mr-3 text-emerald-500"></i>
                        OMNI-FLUX MONITOR
                    </h2>
                    <p class="text-sm text-gray-500 mt-1 font-mono">RADAR MODE: BULK SCAN | <span class="text-emerald-400">OFP ENABLED</span></p>
                </div>
                
                <div class="flex items-center gap-4">
                    <div class="text-right mr-4">
                        <p class="text-[10px] text-gray-500 uppercase font-bold">Market Bias</p>
                        <p class="text-lg font-black font-mono ${state.macroBias === 'BEARISH' ? 'text-red-500' : 'text-green-500'}">
                            ${state.macroBias || 'NEUTRAL'}
                        </p>
                    </div>
                    <button id="stop-phase5-btn" class="bg-red-900/30 hover:bg-red-800/50 text-red-300 border border-red-800 px-4 py-3 rounded-lg text-xs font-bold flex items-center transition-colors shadow-lg">
                        <i data-lucide="square" class="w-4 h-4 mr-2 fill-current"></i> ZATRZYMAJ
                    </button>
                </div>
            </div>

            <div class="mb-4">
                <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                    ${slotsHtml}
                    ${emptySlotsHtml}
                </div>
            </div>
            
            <div class="mt-4 p-3 bg-[#161B22] border border-gray-700 rounded text-xs text-gray-400 flex items-center justify-between">
                <div class="flex items-center">
                    <i data-lucide="info" class="w-4 h-4 mr-2 text-blue-400"></i>
                    <span>System automatycznie rotuje spółki. OFP (Order Flow Pressure) wskazuje przewagę <span class="text-green-400">Kupujących (↑)</span> lub <span class="text-red-400">Sprzedających (↓)</span>.</span>
                </div>
                <div class="font-mono text-gray-600">v5.2 Radar Engine</div>
            </div>
        </div>`;
    },

    dashboard: () => {
        const activeSignalsCount = state.phase3.filter(s => s.status === 'ACTIVE').length;
        const pendingSignalsCount = state.phase3.filter(s => s.status === 'PENDING').length;
        let pulseClass = "pulse-idle";
        let statusColor = "text-green-500";
        const workerStatus = state.workerStatus.status || "IDLE";
        if (workerStatus.includes("RUNNING") || workerStatus.includes("BUSY")) {
            pulseClass = "pulse-busy";
            statusColor = "text-yellow-400";
        } else if (workerStatus.includes("PAUSED")) {
            pulseClass = "";
            statusColor = "text-red-500";
        }
        return `<div id="dashboard-view" class="max-w-6xl mx-auto"><div class="mb-6 relative"><i data-lucide="search" class="absolute left-3 top-3 w-5 h-5 text-gray-500"></i><input type="text" placeholder="Wpisz ticker (np. AAPL) i naciśnij Enter" class="w-full bg-[#161B22] border border-gray-700 text-gray-300 rounded-lg pl-10 pr-4 py-3 focus:ring-sky-500 focus:border-sky-500 outline-none shadow-lg placeholder-gray-600"></div><h2 class="text-2xl font-bold text-sky-400 mb-6 border-b border-gray-700 pb-2 flex items-center"><i data-lucide="activity" class="w-6 h-6 mr-3"></i>Centrum Dowodzenia</h2><div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8"><div class="glass-panel p-6 rounded-xl relative overflow-hidden ${pulseClass} transition-all duration-500"><h3 class="font-semibold text-gray-400 flex items-center text-sm mb-3 uppercase tracking-wider"><i data-lucide="cpu" class="w-4 h-4 mr-2 text-sky-400"></i>Status Silnika</h3><p id="dashboard-worker-status" class="text-5xl font-black ${statusColor} tracking-tighter drop-shadow-lg">${workerStatus}</p><p id="dashboard-current-phase" class="text-xs text-gray-500 mt-2 font-mono bg-black/30 inline-block px-2 py-1 rounded">Faza: ${state.workerStatus.phase || 'NONE'}</p></div><div class="glass-panel p-6 rounded-xl"><h3 class="font-semibold text-gray-400 flex items-center text-sm mb-3 uppercase tracking-wider"><i data-lucide="bar-chart-2" class="w-4 h-4 mr-2 text-yellow-400"></i>Postęp Skanowania</h3><div class="mt-2 flex items-baseline gap-2"><span id="progress-text" class="text-3xl font-extrabold text-white">0 / 0</span><span class="text-gray-500 text-sm">analiz</span></div><div class="w-full bg-gray-800 rounded-full h-3 mt-4 overflow-hidden border border-gray-700"><div id="progress-bar" class="bg-gradient-to-r from-sky-600 to-blue-500 h-full rounded-full transition-all duration-500 shadow-[0_0_10px_rgba(14,165,233,0.5)]" style="width: 0%"></div></div></div><div class="glass-panel p-6 rounded-xl relative"><div class="absolute top-0 right-0 p-3 opacity-10"><i data-lucide="crosshair" class="w-16 h-16 text-white"></i></div><h3 class="font-semibold text-gray-400 flex items-center text-sm mb-4 uppercase tracking-wider"><i data-lucide="trending-up" class="w-4 h-4 mr-2 text-red-500"></i>Sygnały</h3><div class="flex justify-between items-center"><div class="text-center pr-6 border-r border-gray-700 z-10"><p id="dashboard-active-signals" class="text-5xl font-black text-white">${activeSignalsCount + pendingSignalsCount}</p><p class="text-[10px] text-gray-500 mt-1 uppercase font-bold tracking-widest text-green-400">Aktywne</p></div><div class="text-center pl-4 flex-grow z-10"><p id="dashboard-discarded-signals" class="text-5xl font-black text-gray-500">${state.discardedSignalCount || 0}</p><p class="text-[10px] text-gray-600 mt-1 uppercase font-bold tracking-widest">Odrzucone</p></div></div></div></div><h3 class="text-sm font-bold text-gray-500 mb-3 uppercase tracking-wider flex items-center"><i data-lucide="terminal" class="w-4 h-4 mr-2"></i>Dziennik Operacyjny</h3><div id="scan-log-container" class="bg-[#0d1117] p-4 rounded-lg inner-shadow h-96 overflow-y-scroll border border-gray-800 custom-scrollbar font-mono text-xs text-gray-400 leading-relaxed shadow-inner"><pre id="scan-log">Inicjalizacja systemu...</pre></div></div>`;
    },
    
    h3SignalsPanel: (signals, quotes = {}) => {
        const activeCount = signals.filter(s => s.status === 'ACTIVE').length;
        const pendingCount = signals.filter(s => s.status === 'PENDING').length;
        const cardsHtml = signals.length > 0 ? signals.map(s => {
            let score = "N/A";
            const strat = getStrategyInfo(s.notes);
            if (s.notes && s.notes.includes("SCORE:")) {
                const match = s.notes.match(/SCORE:\s*(\d+)/);
                if (match) score = match[1];
            }
            let currentPrice = 0;
            let isLive = false;
            if (quotes && quotes[s.ticker] && quotes[s.ticker]['05. price']) {
                const liveP = parseFloat(quotes[s.ticker]['05. price']);
                if (!isNaN(liveP) && liveP > 0) {
                    currentPrice = liveP;
                    isLive = true;
                }
            }
            if (currentPrice === 0 && s.entry_price) {
                currentPrice = parseFloat(s.entry_price);
            }
            let rValueDisplay = "---";
            let rValueClass = "text-gray-400";
            let scopeLeft = "0%"; 
            let entryPercent = "0%";
            let priceDisplayClass = "text-gray-500"; 
            const tp = parseFloat(s.take_profit || 0);
            const sl = parseFloat(s.stop_loss || 0);
            const entry = parseFloat(s.entry_price || 0);
            if (entry > 0 && sl > 0 && tp > 0) {
                 const totalDistance = tp - sl;
                 const riskDistance = entry - sl;
                 if (totalDistance > 0 && riskDistance > 0) {
                     let ep = ((entry - sl) / totalDistance) * 100;
                     ep = Math.max(0, Math.min(100, ep));
                     entryPercent = `${ep}%`;
                     if (currentPrice > 0) {
                        const profitLossAmount = currentPrice - entry;
                        const rValue = profitLossAmount / riskDistance;
                        if (rValue > 0) {
                            rValueDisplay = `+${rValue.toFixed(2)} R`;
                            rValueClass = "text-green-400 font-black";
                        } else if (rValue < 0) {
                            rValueDisplay = `${rValue.toFixed(2)} R`;
                            rValueClass = "text-red-500 font-black animate-pulse";
                        } else {
                            rValueDisplay = "0.00 R";
                            rValueClass = "text-gray-300";
                        }
                        if (currentPrice > entry) priceDisplayClass = "text-green-400 font-bold";
                        else if (currentPrice < entry) priceDisplayClass = "text-red-400 font-bold";
                        let progress = ((currentPrice - sl) / totalDistance) * 100;
                        progress = Math.max(0, Math.min(100, progress));
                        scopeLeft = `${progress}%`;
                     }
                 }
            }
            let timeRemaining = "---";
            let timeBarWidth = 100;
            if (s.expiration_date) {
                const now = new Date();
                const exp = new Date(s.expiration_date);
                const gen = new Date(s.generation_date);
                const totalLife = exp.getTime() - gen.getTime();
                const timeLeft = exp.getTime() - now.getTime();
                if (timeLeft > 0) {
                    const daysLeft = Math.floor(timeLeft / (1000 * 60 * 60 * 24));
                    const hoursLeft = Math.floor((timeLeft % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
                    timeRemaining = `${daysLeft}d ${hoursLeft}h`; 
                    if (totalLife > 0) timeBarWidth = Math.max(0, Math.min(100, (timeLeft / totalLife) * 100));
                } else {
                    timeRemaining = "Wygasł";
                    timeBarWidth = 0;
                }
            } else if (s.status === 'PENDING') {
                timeRemaining = "Oczekiwanie";
            }
            
            let statusColor = s.status === 'ACTIVE' ? 'border-green-500 shadow-[0_0_15px_rgba(16,185,129,0.2)]' : 'border-yellow-500';
            if (strat.name === 'FLUX') {
                statusColor = 'border-emerald-500 shadow-[0_0_15px_rgba(16,185,129,0.3)]';
            }
            
            const statusIcon = s.status === 'ACTIVE' ? 'zap' : 'hourglass';
            return `<div class="phase3-item bg-[#161B22] rounded-lg p-4 border-l-4 ${statusColor} hover:bg-[#1f2937] transition-all cursor-pointer relative overflow-hidden group" data-ticker="${s.ticker}"><div class="absolute bottom-0 left-0 h-1 bg-gray-700 w-full"><div class="bg-sky-600 h-full transition-all duration-1000" style="width: ${timeBarWidth}%"></div></div><div class="flex justify-between items-start mb-3"><div><div class="flex items-center gap-2"><h4 class="font-bold text-white text-xl tracking-wide">${s.ticker}</h4><span class="strat-badge ${strat.class}">${strat.name}</span><i data-lucide="${statusIcon}" class="w-4 h-4 ${s.status === 'ACTIVE' ? 'text-green-400' : 'text-yellow-400'}"></i></div><div class="text-xs text-gray-500 mt-1 font-mono">Wejście: <span class="text-gray-300">${s.entry_price ? parseFloat(s.entry_price).toFixed(2) : '---'}</span></div></div><div class="text-right"><div class="flex flex-col items-end"><span class="text-xs bg-gray-800 border border-gray-700 px-2 py-1 rounded text-sky-300 font-mono mb-1 shadow-sm">AQM: ${score}</span><span class="text-sm ${rValueClass} font-mono mt-1 flex items-center gap-1 bg-black/40 px-2 rounded border border-white/10">${rValueDisplay}${isLive ? '<span class="relative flex h-2 w-2"><span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span><span class="relative inline-flex rounded-full h-2 w-2 bg-green-500"></span></span>' : ''}</span></div></div></div><div class="flex justify-between items-end text-[10px] font-mono text-gray-500 mb-1 mt-2"><div class="text-left"><span class="block text-[9px] uppercase text-red-500/70">Stop Loss</span><span class="text-red-400 font-bold text-xs">${s.stop_loss ? parseFloat(s.stop_loss).toFixed(2) : '---'}</span></div><div class="text-center pb-1"><span class="${priceDisplayClass} text-base tracking-wider drop-shadow-md">${currentPrice > 0 ? currentPrice.toFixed(2) : '---'}</span></div><div class="text-right"><span class="block text-[9px] uppercase text-green-500/70">Take Profit</span><span class="text-green-400 font-bold text-xs">${s.take_profit ? parseFloat(s.take_profit).toFixed(2) : '---'}</span></div></div><div class="sniper-scope-container" title="Zakres: SL (Lewo) | TP (Prawo)"><div class="scope-zone-risk" style="width: ${entryPercent}"></div><div class="scope-zone-reward" style="width: calc(100% - ${entryPercent})"></div><div class="entry-marker" style="left: ${entryPercent}"></div><div class="scope-marker" style="left: ${scopeLeft}"></div></div><div class="mt-3 flex justify-between items-center"><span class="text-[10px] text-gray-500 font-mono flex items-center" title="Czas do wygaśnięcia setupu"><i data-lucide="clock" class="w-3 h-3 mr-1"></i>TTL: ${timeRemaining}</span><button class="text-xs bg-sky-600/10 hover:bg-sky-600/30 text-sky-400 px-2 py-1 rounded transition-colors">Szczegóły ></button></div></div>`;
        }).join('') || `<p class="text-xs text-gray-500 p-2">Brak sygnałów.</p>`;

        return `<div id="h3-signals-view" class="max-w-6xl mx-auto">
            <div class="flex justify-between items-center mb-6 border-b border-gray-700 pb-4">
                <h2 class="text-2xl font-bold text-white flex items-center">
                    <i data-lucide="target" class="w-6 h-6 mr-3 text-sky-500"></i>
                    Sygnały H3 Live
                </h2>
                <div class="flex gap-2">
                    <select id="h3-sort-select" class="bg-[#161B22] border border-gray-700 text-xs text-gray-300 rounded px-2 py-1 focus:outline-none">
                        <option value="score">Sort: Score</option>
                        <option value="rr">Sort: R:R</option>
                        <option value="time">Sort: Czas</option>
                        <option value="ticker">Sort: Ticker</option>
                    </select>
                    <button id="h3-refresh-btn" class="p-1 hover:bg-gray-800 rounded"><i data-lucide="refresh-cw" class="w-4 h-4 text-gray-400"></i></button>
                </div>
            </div>
            <div class="space-y-4">
                ${cardsHtml}
            </div>
        </div>`;
    },

    phaseXView: (candidates) => {
        const rows = candidates.map(c => {
            const dateStr = c.last_pump_date ? new Date(c.last_pump_date).toLocaleDateString() : '-';
            const pumpVal = c.last_pump_percent || 0.0;
            const pumpColor = pumpVal >= 100 ? 'text-purple-400 font-black' : (pumpVal >= 50 ? 'text-pink-400 font-bold' : 'text-gray-400');
            return `<tr class="border-b border-gray-800 hover:bg-[#1f2937] transition-colors"><td class="p-3 font-bold text-pink-500">${c.ticker}</td><td class="p-3 text-right font-mono text-white">${c.price ? c.price.toFixed(4) : '0.0000'}</td><td class="p-3 text-right text-gray-400">${c.volume_avg ? (c.volume_avg / 1000000).toFixed(1) : '0.0'}M</td><td class="p-3 text-center font-bold text-white bg-gray-800/50 rounded">${c.pump_count_1y || 0}</td><td class="p-3 text-right text-gray-300">${dateStr}</td><td class="p-3 text-right ${pumpColor}">+${pumpVal.toFixed(0)}%</td></tr>`;
        }).join('');
        const tableHeader = `<thead class="text-xs text-gray-400 uppercase bg-[#0D1117] sticky top-0"><tr><th class="p-3 text-left">Ticker</th><th class="p-3 text-right">Cena ($)</th><th class="p-3 text-right">Vol (Avg)</th><th class="p-3 text-center">Pompy (1Y)</th><th class="p-3 text-right">Ost. Wybuch</th><th class="p-3 text-right">Moc (%)</th></tr></thead>`;
        return `<div id="phasex-view" class="max-w-6xl mx-auto"><div class="flex justify-between items-center mb-6 border-b border-gray-700 pb-4"><div><h2 class="text-2xl font-bold text-white flex items-center"><i data-lucide="biohazard" class="w-6 h-6 mr-3 text-pink-500"></i>Faza X: BioX Hunter</h2><p class="text-sm text-gray-500 mt-1">Biotech Penny Stocks (0.5$ - 4.0$) z historią wybuchów >20%.</p></div><button id="run-phasex-scan-btn" class="modal-button modal-button-primary bg-pink-600 hover:bg-pink-700 flex items-center shadow-[0_0_15px_rgba(219,39,119,0.3)]"><i data-lucide="radar" class="w-4 h-4 mr-2"></i> Skanuj BioX</button></div>${candidates.length === 0 ? '<div class="text-center py-10 bg-[#161B22] rounded-lg border border-gray-700"><i data-lucide="search-x" class="w-12 h-12 mx-auto text-gray-600 mb-3"></i><p class="text-gray-500">Brak danych. Uruchom skaner, aby znaleźć kandydatów.</p></div>' : `<div class="overflow-x-auto bg-[#161B22] rounded-lg border border-gray-700 shadow-xl"><table class="w-full text-sm text-left text-gray-300">${tableHeader}<tbody>${rows}</tbody></table></div>`}</div>`;
    },

    portfolio: (holdings, quotes) => {
        let totalPortfolioValue = 0;
        let totalProfitLoss = 0;
        const rows = holdings.map(h => {
            const quote = quotes[h.ticker];
            let currentPrice = null, dayChangePercent = null, profitLoss = null, currentValue = null;
            let priceClass = 'text-gray-400';
            let changePercentDisplay = '---';
            let changePercentClass = 'text-gray-500';
            let priceSource = 'close';
            const strat = getStrategyInfo(h.notes || h.strategy || "");
            if (quote && quote['05. price']) {
                try {
                    currentPrice = parseFloat(quote['05. price']);
                    priceSource = quote['_price_source'] || 'close'; 
                    dayChangePercent = parseFloat(quote['change percent'] ? quote['change percent'].replace('%', '') : '0');
                    priceClass = dayChangePercent >= 0 ? 'text-green-500' : 'text-red-500';
                    currentValue = h.quantity * currentPrice;
                    const costBasis = h.quantity * h.average_buy_price;
                    profitLoss = currentValue - costBasis;
                    totalPortfolioValue += currentValue;
                    totalProfitLoss += profitLoss;
                    if (h.average_buy_price > 0) {
                        const pctChange = ((currentPrice - h.average_buy_price) / h.average_buy_price) * 100;
                        changePercentDisplay = `${pctChange > 0 ? '+' : ''}${pctChange.toFixed(2)}%`;
                        changePercentClass = pctChange >= 0 ? 'text-green-400 font-bold' : 'text-red-400 font-bold';
                    }
                } catch (e) { console.error(`Błąd obliczeń dla ${h.ticker} w portfelu:`, e); }
            }
            if (priceSource === 'extended_hours') { priceClass = 'extended-hours-text'; }
            const profitLossClass = profitLoss == null ? 'text-gray-500' : (profitLoss >= 0 ? 'text-green-500' : 'text-red-500');
            const takeProfitFormatted = h.take_profit ? h.take_profit.toFixed(2) : '---';
            const priceDisplay = (priceSource === 'extended_hours') ? `🌙 ${currentPrice.toFixed(2)}` : (currentPrice ? currentPrice.toFixed(2) : '---');
            return `<tr class="border-b border-gray-800 hover:bg-[#1f2937]"><td class="p-3 font-bold text-sky-400 flex items-center gap-2">${h.ticker}<span class="strat-badge ${strat.class}" style="font-size: 8px;">${strat.name}</span></td><td class="p-3 text-right">${h.quantity}</td><td class="p-3 text-right">${h.average_buy_price.toFixed(4)}</td><td class="p-3 text-right ${priceClass}">${priceDisplay}</td><td class="p-3 text-right ${changePercentClass}">${changePercentDisplay}</td><td class="p-3 text-right text-cyan-400 font-bold">${takeProfitFormatted}</td><td class="p-3 text-right ${profitLossClass}">${profitLoss != null ? profitLoss.toFixed(2) + ' USD' : '---'}</td><td class="p-3 text-right"><button data-ticker="${h.ticker}" data-quantity="${h.quantity}" class="sell-stock-btn text-xs bg-red-600/20 hover:bg-red-600/40 text-red-300 py-1 px-3 rounded">Sprzedaj</button></td></tr>`;
        }).join('');
        const totalProfitLossClass = totalProfitLoss >= 0 ? 'text-green-500' : 'text-red-500';
        const tableHeader = `<thead class="text-xs text-gray-400 uppercase bg-[#0D1117]"><tr><th scope="col" class="p-3">Ticker / Strategia</th><th scope="col" class="p-3 text-right">Ilość</th><th scope="col" class="p-3 text-right">Cena Zakupu</th><th scope="col" class="p-3 text-right">Kurs (USD)</th><th scope="col" class="p-3 text-right">Zmiana %</th><th scope="col" class="p-3 text-right">Cel (TP)</th><th scope="col" class="p-3 text-right">Zysk / Strata</th><th scope="col" class="p-3 text-right">Akcja</th></tr></thead>`;
        return `<div id="portfolio-view" class="max-w-6xl mx-auto"><h2 class="text-2xl font-bold text-sky-400 mb-6 border-b border-gray-700 pb-2 flex justify-between items-center">Portfel Inwestycyjny<span class="text-lg text-gray-400">Wartość: ${totalPortfolioValue.toFixed(2)} USD | Z/S: <span class="${totalProfitLossClass}">${totalProfitLoss.toFixed(2)} USD</span></span></h2>${holdings.length === 0 ? '<p class="text-center text-gray-500 py-10">Twój portfel jest pusty.</p>' : `<div class="overflow-x-auto bg-[#161B22] rounded-lg border border-gray-700"><table class="w-full text-sm text-left text-gray-300">${tableHeader}<tbody>${rows}</tbody></table></div>` }</div>`;
    },
    
    transactions: (transactions) => {
            const rows = transactions.map(t => {
            const typeClass = t.transaction_type === 'BUY' ? 'text-green-400' : 'text-red-400';
            const profitLossClass = t.profit_loss_usd == null ? '' : (t.profit_loss_usd >= 0 ? 'text-green-500' : 'text-red-500');
            const transactionDate = new Date(t.transaction_date).toLocaleString('pl-PL');
            return `<tr class="border-b border-gray-800 hover:bg-[#1f2937]"><td class="p-3 text-gray-400 text-xs">${transactionDate}</td><td class="p-3 font-bold text-sky-400">${t.ticker}</td><td class="p-3 font-semibold ${typeClass}">${t.transaction_type}</td><td class="p-3 text-right">${t.quantity}</td><td class="p-3 text-right">${t.price_per_share.toFixed(4)}</td><td class="p-3 text-right ${profitLossClass}">${t.profit_loss_usd != null ? t.profit_loss_usd.toFixed(2) + ' USD' : '---'}</td></tr>`;
        }).join('');
        return `<div id="transactions-view" class="max-w-6xl mx-auto"><h2 class="text-2xl font-bold text-sky-400 mb-6 border-b border-gray-700 pb-2">Historia Transakcji</h2>${transactions.length === 0 ? '<p class="text-center text-gray-500 py-10">Brak historii transakcji.</p>' : `<div class="overflow-x-auto bg-[#161B22] rounded-lg border border-gray-700"><table class="w-full text-sm text-left text-gray-300"><thead class="text-xs text-gray-400 uppercase bg-[#0D1117]"><tr><th scope="col" class="p-3">Data</th><th scope="col" class="p-3">Ticker</th><th scope="col" class="p-3">Typ</th><th scope="col" class="p-3 text-right">Ilość</th><th scope="col" class="p-3 text-right">Cena (USD)</th><th scope="col" class="p-3 text-right">Zysk / Strata (USD)</th></tr></thead><tbody>${rows}</tbody></table></div>` }</div>`;
    },
    
    agentReport: (report) => {
        const stats = report.stats;
        const trades = report.trades;
        const total_trades_count = report.total_trades_count;
        const totalPages = Math.ceil(total_trades_count / REPORT_PAGE_SIZE);
        const startTrade = (state.currentReportPage - 1) * REPORT_PAGE_SIZE + 1;
        const endTrade = Math.min(startTrade + REPORT_PAGE_SIZE - 1, total_trades_count);

        // Helper functions for formatting
        const formatMetric = (val) => (typeof val !== 'number' || isNaN(val)) ? `<span class="text-gray-600">---</span>` : val.toFixed(3);
        const formatPercent = (val) => { 
            if (typeof val !== 'number' || isNaN(val)) return `<span class="text-gray-500">---</span>`; 
            const color = val >= 0 ? 'text-green-500' : 'text-red-500'; 
            return `<span class="${color}">${val.toFixed(2)}%</span>`; 
        };
        const formatProfitFactor = (val) => { 
            if (typeof val !== 'number' || isNaN(val)) return `<span class="text-gray-500">---</span>`; 
            const color = val >= 1 ? 'text-green-500' : 'text-red-500'; 
            return `<span class="${color}">${val.toFixed(2)}</span>`; 
        };
        const formatNumber = (val) => (typeof val !== 'number' || isNaN(val)) ? `<span class="text-gray-500">---</span>` : val.toFixed(2);
        const createStatCard = (label, value, icon) => `<div class="bg-[#161B22] p-4 rounded-lg shadow-lg border border-gray-700"><h3 class="font-semibold text-gray-400 flex items-center text-sm"><i data-lucide="${icon}" class="w-4 h-4 mr-2 text-sky-400"></i>${label}</h3><p class="text-3xl font-extrabold mt-2 text-white">${value}</p></div>`;

        // Setup Summary Table Rows
        const setupRows = Object.entries(stats.by_setup).map(([setupName, setupStats]) => {
            return `<tr class="border-b border-gray-800 hover:bg-[#1f2937]"><td class="p-3 font-semibold text-sky-400">${setupName}</td><td class="p-3 text-right">${setupStats.total_trades}</td><td class="p-3 text-right">${formatPercent(setupStats.win_rate_percent)}</td><td class="p-3 text-right">${formatPercent(setupStats.total_p_l_percent)}</td><td class="p-3 text-right">${formatProfitFactor(setupStats.profit_factor)}</td></tr>`;
        }).join('');

        const setupTable = setupRows.length > 0 
            ? `<div class="overflow-x-auto bg-[#161B22] rounded-lg border border-gray-700"><table class="w-full text-sm text-left text-gray-300"><thead class="text-xs text-gray-400 uppercase bg-[#0D1117] sticky top-0 z-10"><tr><th scope="col" class="p-3">Strategia</th><th scope="col" class="p-3 text-right">Ilość Transakcji</th><th scope="col" class="p-3 text-right">Win Rate (%)</th><th scope="col" class="p-3 text-right">Całkowity P/L (%)</th><th scope="col" class="p-3 text-right">Profit Factor</th></tr></thead><tbody>${setupRows}</tbody></table></div>` 
            : `<p class="text-center text-gray-500 py-10">Brak danych per strategia.</p>`;

        // Trade History Table Headers
        const tradeHeaders = ['Akcja', 'Data Otwarcia', 'Ticker', 'Strategia', 'Status', 'Cena Wejścia', 'Cena Zamknięcia', 'P/L (%)', 'ATR', 'T. Dil.', 'P. Grav.', 'TD %tile', 'PG %tile', 'Inst. Sync', 'Retail Herd.', 'AQM H3', 'AQM %tile', 'J (Norm)', '∇² (Norm)', 'm² (Norm)', 'J (H4)', 'J Thresh.'];
        const headerClasses = ['sticky left-0 bg-[#0D1117] z-20', 'sticky left-[50px] bg-[#0D1117]', 'sticky left-[140px] bg-[#0D1117]', 'sticky left-[210px] bg-[#0D1117]', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right', 'text-right'];

        // Trade History Rows
        const tradeRows = trades.map(t => {
            const statusClass = t.status === 'CLOSED_TP' ? 'text-green-400' : (t.status === 'CLOSED_SL' ? 'text-red-400' : 'text-yellow-400');
            const setupNameShort = (t.setup_type || 'UNKNOWN').replace('BACKTEST_', '').replace('_AQM_V3_', ' ').replace('QUANTUM_FIELD', 'H3').replace('INFO_THERMO', 'H4').replace('CONTRARIAN_ENTANGLEMENT', 'H2').replace('GRAVITY_MEAN_REVERSION', 'H1');
            
            const auditBtn = t.ai_audit_report 
                ? `<button class="text-xs bg-purple-600 hover:bg-purple-500 text-white px-2 py-1 rounded flex items-center gap-1 recheck-btn" data-trade-id="${t.id}"><i data-lucide="check-circle" class="w-3 h-3"></i>Wynik</button>`
                : (t.expected_profit_factor ? `<button class="text-xs bg-gray-700 hover:bg-gray-600 text-gray-300 px-2 py-1 rounded recheck-btn" data-trade-id="${t.id}" title="Czekam na audyt...">🕒</button>` : `<span class="text-gray-600">-</span>`);

            return `<tr class="border-b border-gray-800 hover:bg-[#1f2937] text-xs font-mono"><td class="p-2 whitespace-nowrap sticky left-0 bg-[#161B22] hover:bg-[#1f2937] z-10 border-r border-gray-700">${auditBtn}</td><td class="p-2 whitespace-nowrap text-gray-400 sticky left-[50px] bg-[#161B22] hover:bg-[#1f2937] border-r border-gray-700">${new Date(t.open_date).toLocaleDateString('pl-PL')}</td><td class="p-2 whitespace-nowrap font-bold text-sky-400 sticky left-[140px] bg-[#161B22] hover:bg-[#1f2937] border-r border-gray-700">${t.ticker}</td><td class="p-2 whitespace-nowrap text-gray-300 sticky left-[210px] bg-[#161B22] hover:bg-[#1f2937] border-r border-gray-700">${setupNameShort}</td><td class="p-2 whitespace-nowrap text-right ${statusClass}">${t.status.replace('CLOSED_', '')}</td><td class="p-2 whitespace-nowrap text-right">${formatNumber(t.entry_price)}</td><td class="p-2 whitespace-nowrap text-right">${formatNumber(t.close_price)}</td><td class="p-2 whitespace-nowrap text-right font-bold">${formatPercent(t.final_profit_loss_percent)}</td><td class="p-2 whitespace-nowrap text-right text-purple-300">${formatMetric(t.metric_atr_14)}</td><td class="p-2 whitespace-nowrap text-right text-blue-300">${formatMetric(t.metric_time_dilation)}</td><td class="p-2 whitespace-nowrap text-right text-blue-300">${formatMetric(t.metric_price_gravity)}</td><td class="p-2 whitespace-nowrap text-right text-gray-500">${formatMetric(t.metric_td_percentile_90)}</td><td class="p-2 whitespace-nowrap text-right text-gray-500">${formatMetric(t.metric_pg_percentile_90)}</td><td class="p-2 whitespace-nowrap text-right text-green-300">${formatMetric(t.metric_inst_sync)}</td><td class="p-2 whitespace-nowrap text-right text-red-300">${formatMetric(t.metric_retail_herding)}</td><td class="p-2 whitespace-nowrap text-right text-yellow-300 font-bold">${formatMetric(t.metric_aqm_score_h3)}</td><td class="p-2 whitespace-nowrap text-right text-gray-500">${formatMetric(t.metric_aqm_percentile_95)}</td><td class="p-2 whitespace-nowrap text-right text-yellow-400">${formatMetric(t.metric_J_norm)}</td><td class="p-2 whitespace-nowrap text-right text-yellow-400">${formatMetric(t.metric_nabla_sq_norm)}</td><td class="p-2 whitespace-nowrap text-right text-yellow-400">${formatMetric(t.metric_m_sq_norm)}</td><td class="p-2 whitespace-nowrap text-right text-pink-300">${formatMetric(t.metric_J)}</td><td class="p-2 whitespace-nowrap text-right text-gray-500">${formatMetric(t.metric_J_threshold_2sigma)}</td></tr>`;
        }).join('');

        const tradeTable = trades.length > 0 
            ? `<div class="overflow-x-auto bg-[#161B22] rounded-lg border border-gray-700 max-h-[500px] overflow-y-auto"><table class="w-full text-sm text-left text-gray-300 min-w-[2400px]"><thead class="text-xs text-gray-400 uppercase bg-[#0D1117] sticky top-0 z-20"><tr>${tradeHeaders.map((h, index) => `<th scope="col" class="p-2 whitespace-nowrap ${headerClasses[index]}">${h}</th>`).join('')}</tr></thead><tbody>${tradeRows}</tbody></table></div>` 
            : `<p class="text-center text-gray-500 py-10">Brak zamkniętych transakcji do wyświetlenia.</p>`;

        // UI Sections
        const backtestSection = `<div class="bg-[#161B22] p-6 rounded-lg shadow-lg border border-gray-700"><h4 class="text-lg font-semibold text-gray-300 mb-3">Uruchom Nowy Test Historyczny</h4><p class="text-sm text-gray-500 mb-4">Wpisz rok (np. 2010), aby przetestować strategie na historycznych danych dla tego roku.</p><div class="mb-4"><label class="block text-xs font-bold text-gray-400 mb-1 uppercase">Strategia Backtestu</label><select id="backtest-strategy-select" class="modal-input w-full cursor-pointer hover:bg-gray-800 transition-colors text-xs"><option value="H3">H3 (Elite Sniper)</option><option value="AQM">AQM (Adaptive Quantum)</option><option value="BIOX">BioX (Pump Hunter >20%)</option></select></div><div class="flex items-start gap-3"><input type="number" id="backtest-year-input" class="modal-input w-32 !mb-0" placeholder="YYYY" min="2000" max="${new Date().getFullYear()}"><button id="run-backtest-year-btn" class="modal-button modal-button-primary flex items-center flex-shrink-0 bg-sky-600 hover:bg-sky-700"><i data-lucide="play" class="w-4 h-4 mr-2"></i>Uruchom Test</button></div><button id="toggle-h3-params" class="text-xs text-gray-400 hover:text-white flex items-center focus:outline-none border border-gray-700 px-3 py-1 rounded bg-[#0D1117]"><span class="font-bold text-sky-500 mr-2">Zaawansowana Konfiguracja H3 (Symulator)</span><i data-lucide="chevron-down" id="h3-params-icon" class="w-4 h-4 transition-transform"></i></button><div id="h3-params-container" class="mt-3 p-4 bg-[#0D1117] border border-gray-700 rounded hidden grid grid-cols-1 md:grid-cols-3 gap-4"><div><label class="block text-xs font-bold text-gray-500 mb-1 uppercase">Percentyl AQM</label><input type="number" id="h3-param-percentile" class="modal-input !mb-0 text-xs" placeholder="0.95" step="0.01" value="0.95"><p class="text-[10px] text-gray-600 mt-1">Domyślny: 0.95</p></div><div><label class="block text-xs font-bold text-gray-500 mb-1 uppercase">Próg Masy m²</label><input type="number" id="h3-param-mass" class="modal-input !mb-0 text-xs" placeholder="-0.5" step="0.1" value="-0.5"><p class="text-[10px] text-gray-600 mt-1">Domyślny: -0.5</p></div><div><label class="block text-xs font-bold text-gray-500 mb-1 uppercase">Min. AQM Score</label><input type="number" id="h3-param-min-score" class="modal-input !mb-0 text-xs" placeholder="0.0" step="0.1" value="0.0"><p class="text-[10px] text-gray-600 mt-1">Hard Floor (V4)</p></div><div><label class="block text-xs font-bold text-gray-500 mb-1 uppercase">Mnożnik TP (ATR)</label><input type="number" id="h3-param-tp" class="modal-input !mb-0 text-xs" placeholder="5.0" step="0.5" value="5.0"><p class="text-[10px] text-gray-600 mt-1">Domyślny: 5.0</p></div><div><label class="block text-xs font-bold text-gray-500 mb-1 uppercase">Mnożnik SL (ATR)</label><input type="number" id="h3-param-sl" class="modal-input !mb-0 text-xs" placeholder="2.0" step="0.5" value="2.0"><p class="text-[10px] text-gray-600 mt-1">Domyślny: 2.0</p></div><div><label class="block text-xs font-bold text-gray-500 mb-1 uppercase">Max Hold (Dni)</label><input type="number" id="h3-param-hold" class="modal-input !mb-0 text-xs" placeholder="5" step="1" value="5"><p class="text-[10px] text-gray-600 mt-1">Nowe w V4</p></div><div class="md:col-span-3 border-t border-gray-800 pt-3 mt-1"><label class="block text-xs font-bold text-gray-500 mb-1 uppercase">Nazwa Setupu (Suffix)</label><input type="text" id="h3-param-name" class="modal-input !mb-0 text-xs" placeholder="CUSTOM_TEST_1"><p class="text-[10px] text-gray-600 mt-1">Oznaczenie w raportach</p></div></div><div id="backtest-status-message" class="text-sm mt-3 h-4"></div></div>`;
        const quantumLabSection = `<div class="bg-[#161B22] p-6 rounded-lg shadow-lg border border-gray-700 relative overflow-hidden"><div class="absolute top-0 right-0 p-2 opacity-5 pointer-events-none"><i data-lucide="atom" class="w-32 h-32 text-purple-500"></i></div><h4 class="text-lg font-semibold text-purple-400 mb-3 flex items-center"><i data-lucide="flask-conical" class="w-5 h-5 mr-2"></i>Quantum Lab (Apex V4)</h4><p class="text-sm text-gray-500 mb-4">Uruchom optymalizację bayesowską (Optuna), aby znaleźć idealne parametry H3 dla wybranego roku.</p><div class="flex flex-wrap gap-3"><button id="open-quantum-modal-btn" class="modal-button modal-button-primary bg-purple-600 hover:bg-purple-700 flex items-center flex-shrink-0"><i data-lucide="cpu" class="w-4 h-4 mr-2"></i>Konfiguruj Optymalizację</button><button id="view-optimization-results-btn" class="modal-button modal-button-secondary flex items-center flex-shrink-0"><i data-lucide="list" class="w-4 h-4 mr-2"></i>Wyniki</button></div><div id="quantum-lab-status" class="text-sm mt-3 h-4"></div></div>`;
        const aiOptimizerSection = `<div class="bg-[#161B22] p-6 rounded-lg shadow-lg border border-gray-700"><h4 class="text-lg font-semibold text-gray-300 mb-3">Analiza Mega Agenta AI</h4><p class="text-sm text-gray-500 mb-4">Uruchom Mega Agenta, aby przeanalizował wszystkie zebrane dane i zasugerował optymalizacje strategii.</p><div class="flex items-start gap-3"><button id="run-ai-optimizer-btn" class="modal-button modal-button-primary flex items-center flex-shrink-0"><i data-lucide="brain-circuit" class="w-4 h-4 mr-2"></i>Analiza AI</button><button id="view-ai-report-btn" class="modal-button modal-button-secondary flex items-center flex-shrink-0"><i data-lucide="eye" class="w-4 h-4 mr-2"></i>Raport</button></div><div id="ai-optimizer-status-message" class="text-sm mt-3 h-4"></div></div>`;
        const exportSection = `<div class="bg-[#161B22] p-6 rounded-lg shadow-lg border border-gray-700"><h4 class="text-lg font-semibold text-gray-300 mb-3">Eksport Danych</h4><p class="text-sm text-gray-500 mb-4">Pobierz *wszystkie* ${total_trades_count} transakcje jako CSV.</p><div class="flex items-start gap-3"><button id="run-csv-export-btn" class="modal-button modal-button-primary flex items-center flex-shrink-0"><i data-lucide="download-cloud" class="w-4 h-4 mr-2"></i>Eksport CSV</button></div><div id="csv-export-status-message" class="text-sm mt-3 h-4"></div></div>`;
        const h3DeepDiveSection = `<div class="bg-[#161B22] p-6 rounded-lg shadow-lg border border-gray-700"><h4 class="text-lg font-semibold text-gray-300 mb-3">Analiza Porażek H3</h4><p class="text-sm text-gray-500 mb-4">Analiza "słabego roku" (Deep Dive).</p><div class="flex items-start gap-3"><button id="run-h3-deep-dive-modal-btn" class="modal-button modal-button-primary flex items-center flex-shrink-0"><i data-lucide="search-check" class="w-4 h-4 mr-2"></i>Analiza Deep Dive</button></div><div id="h3-deep-dive-main-status" class="text-sm mt-3 h-4"></div></div>`;
        
        const paginationControls = totalPages > 1 
            ? `<div class="flex justify-between items-center mt-4"><span class="text-sm text-gray-400">Wyświetlanie ${startTrade}-${endTrade} z ${total_trades_count} transakcji</span><div class="flex gap-2"><button id="report-prev-btn" class="modal-button modal-button-secondary" ${state.currentReportPage === 1 ? 'disabled' : ''}><i data-lucide="arrow-left" class="w-4 h-4"></i></button><span class="text-sm text-gray-400 p-2">Strona ${state.currentReportPage} / ${totalPages}</span><button id="report-next-btn" class="modal-button modal-button-secondary" ${state.currentReportPage === totalPages ? 'disabled' : ''}><i data-lucide="arrow-right" class="w-4 h-4"></i></button></div></div>` 
            : '';

        return `<div id="agent-report-view" class="max-w-6xl mx-auto"><h2 class="text-2xl font-bold text-sky-400 mb-6 border-b border-gray-700 pb-2">Raport Wydajności Agenta</h2><h3 class="text-xl font-bold text-gray-300 mb-4">Kluczowe Wskaźniki (Wszystkie ${stats.total_trades} Transakcji)</h3><div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">${createStatCard('Całkowity P/L (%)', formatPercent(stats.total_p_l_percent), 'percent')}${createStatCard('Win Rate (%)', formatPercent(stats.win_rate_percent), 'target')}${createStatCard('Profit Factor', formatProfitFactor(stats.profit_factor), 'ratio')}${createStatCard('Ilość Transakcji', stats.total_trades, 'bar-chart-2')}</div><h3 class="text-xl font-bold text-gray-300 mb-4">Podsumowanie wg Strategii</h3>${setupTable}<h3 class="text-xl font-bold text-gray-300 mt-8 mb-4">Narzędzia Analityczne</h3><div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 mt-6">${backtestSection}${quantumLabSection}${aiOptimizerSection}${h3DeepDiveSection}${exportSection}</div><h3 class="text-xl font-bold text-gray-300 mt-8 mb-4">Historia Zamkniętych Transakcji</h3>${paginationControls}${tradeTable}${paginationControls}</div>`;
    },

    optimizationResults: (job) => {
        if (!job) return `<p class="text-gray-500">Brak danych o optymalizacji.</p>`;
        const trials = job.trials || [];
        trials.sort((a, b) => (b.profit_factor || 0) - (a.profit_factor || 0));
        const trialsRows = trials.map(t => {
            const isBest = t.id === job.best_trial_id;
            const rowClass = isBest ? "bg-green-900/20 border-l-4 border-green-500" : "border-b border-gray-800 hover:bg-[#1f2937]";
            const paramsStr = Object.entries(t.params).map(([k, v]) => `<span class="text-gray-400">${k}:</span> <span class="text-sky-300">${typeof v === 'number' ? v.toFixed(2) : v}</span>`).join(', ');
            const paramsJson = JSON.stringify(t.params).replace(/"/g, '&quot;');
            return `<tr class="${rowClass}"><td class="p-2 text-center font-mono text-gray-500">#${t.trial_number}</td><td class="p-2 text-right font-bold ${t.profit_factor >= 1.5 ? 'text-green-400' : 'text-gray-300'}">${t.profit_factor ? t.profit_factor.toFixed(2) : '0.00'}</td><td class="p-2 text-right">${t.win_rate ? t.win_rate.toFixed(1) : '0.0'}%</td><td class="p-2 text-right">${t.total_trades || 0}</td><td class="p-2 text-xs font-mono">${paramsStr}</td><td class="p-2 text-right"><button class="use-params-btn bg-purple-600 hover:bg-purple-700 text-white text-xs px-2 py-1 rounded flex items-center ml-auto" data-params="${paramsJson}"><i data-lucide="play-circle" class="w-3 h-3 mr-1"></i> Użyj</button></td></tr>`;
        }).join('');
        return `<div class="space-y-6"><div class="flex justify-between items-center bg-[#0D1117] p-4 rounded border border-gray-700"><div><h4 class="text-sm text-gray-400 uppercase font-bold">Zadanie: ${job.target_year}</h4><p class="text-xs text-gray-500">ID: ${job.id}</p></div><div class="text-right"><div class="text-2xl font-bold ${job.best_score >= 2.0 ? 'text-green-400' : 'text-yellow-400'}">Best Score: ${job.best_score ? job.best_score.toFixed(4) : '---'}</div><div class="text-xs text-gray-500">Status: ${job.status}</div></div></div><h4 class="text-sm text-gray-400 uppercase font-bold border-b border-gray-700 pb-1">Ranking Prób (Top Wyniki)</h4><div class="overflow-x-auto max-h-64 border border-gray-700 rounded"><table class="w-full text-sm text-left text-gray-300"><thead class="text-xs text-gray-400 uppercase bg-[#0D1117] sticky top-0"><tr><th class="p-2 text-center">#</th><th class="p-2 text-right">PF</th><th class="p-2 text-right">Win Rate</th><th class="p-2 text-right">Trades</th><th class="p-2">Parametry</th><th class="p-2 text-right">Akcja</th></tr></thead><tbody>${trialsRows}</tbody></table></div></div>`;
    }
};

// =========================================================================
// === EXPORT 2: UI (Konstrukcja i Inicjalizacja) ===
// =========================================================================
export const ui = {
    init: () => {
        const get = (id) => document.getElementById(id);
        
        // Iniekcja elementów do modalu H3
        const h3ModalContent = document.querySelector('#h3-live-modal .grid');
        
        if (h3ModalContent && !document.getElementById('h3-live-strategy-mode')) {
            const stratDiv = document.createElement('div');
            stratDiv.innerHTML = `
                <label class="block text-xs font-bold text-gray-400 mb-1 uppercase">Tryb Strategii</label>
                <select id="h3-live-strategy-mode" class="modal-input cursor-pointer hover:bg-gray-800 transition-colors">
                    <option value="H3">H3 (Elite Sniper)</option>
                    <option value="AQM">AQM (Adaptive Quantum)</option>
                </select>
                <p class="text-[10px] text-gray-600 mt-1">Wymusza logikę obliczeń (H3 vs AQM).</p>
            `;
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
            periodDiv.innerHTML = `
                <label class="block text-xs font-bold text-gray-400 mb-1 uppercase">Okres Analizy</label>
                <select id="qo-period-select" class="modal-input cursor-pointer hover:bg-gray-800 transition-colors">
                    <option value="FULL">Pełny Rok (Standard)</option>
                    <option value="Q1">Q1 (Styczeń - Marzec)</option>
                    <option value="Q2">Q2 (Kwiecień - Czerwiec)</option>
                    <option value="Q3">Q3 (Lipiec - Wrzesień)</option>
                    <option value="Q4">Q4 (Październik - Grudzień)</option>
                </select>
                <p class="text-xs text-gray-500 mt-1">Wybierz sezonowość.</p>
            `;
            quantumModalContent.insertBefore(periodDiv, quantumModalContent.children[1]);
        }

        // === INIEKCJA PRZYCISKU FAZY X (BIOX) ===
        const sidebarControls = document.querySelector('#app-sidebar .pt-4 .space-y-2');
        if (sidebarControls && !document.getElementById('btn-phasex-scan')) {
             const btn = document.createElement('button');
             btn.id = 'btn-phasex-scan';
             btn.className = 'w-full text-left flex items-center bg-pink-600/20 hover:bg-pink-600/40 text-pink-300 py-2 px-3 rounded-md text-sm transition-colors disabled:opacity-50 disabled:cursor-not-allowed mt-2';
             btn.innerHTML = '<i data-lucide="biohazard" class="mr-2 h-4 w-4"></i>Skanuj Faza X (BioX)';
             sidebarControls.appendChild(btn);
        }

        // === INIEKCJA PRZYCISKU FAZY 4 (H4 KINETIC) ===
        if (sidebarControls && !document.getElementById('btn-phase4-scan')) {
             const btnH4 = document.createElement('button');
             btnH4.id = 'btn-phase4-scan';
             btnH4.className = 'w-full text-left flex items-center bg-amber-600/20 hover:bg-amber-600/40 text-amber-300 py-2 px-3 rounded-md text-sm transition-colors disabled:opacity-50 disabled:cursor-not-allowed mt-2';
             btnH4.innerHTML = '<i data-lucide="zap" class="mr-2 h-4 w-4"></i>Skanuj H4 (Kinetic)';
             sidebarControls.appendChild(btnH4);
        }

        // === INIEKCJA PRZYCISKU FAZY 5 (OMNI-FLUX) ===
        if (sidebarControls && !document.getElementById('btn-phase5-scan')) {
             const btnF5 = document.createElement('button');
             btnF5.id = 'btn-phase5-scan';
             // Używamy koloru szmaragdowego (Emerald) dla odróżnienia
             btnF5.className = 'w-full text-left flex items-center bg-emerald-600/20 hover:bg-emerald-600/40 text-emerald-300 py-2 px-3 rounded-md text-sm transition-colors disabled:opacity-50 disabled:cursor-not-allowed mt-2';
             btnF5.innerHTML = '<i data-lucide="waves" class="mr-2 h-4 w-4"></i>Start F5 (Omni-Flux)';
             sidebarControls.appendChild(btnF5);
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
            
            buyModal: { 
                backdrop: get('buy-modal'), tickerSpan: get('buy-modal-ticker'), 
                quantityInput: get('buy-quantity'), priceInput: get('buy-price'),
                cancelBtn: get('buy-cancel-btn'), confirmBtn: get('buy-confirm-btn')
            },
            sellModal: { 
                backdrop: get('sell-modal'), tickerSpan: get('sell-modal-ticker'), 
                maxQuantitySpan: get('sell-max-quantity'), quantityInput: get('sell-quantity'), 
                priceInput: get('sell-price'), cancelBtn: get('sell-cancel-btn'), confirmBtn: get('sell-confirm-btn')
            },
            aiReportModal: {
                backdrop: get('ai-report-modal'), content: get('ai-report-content'), closeBtn: get('ai-report-close-btn')
            },
            h3DeepDiveModal: {
                backdrop: get('h3-deep-dive-modal'), yearInput: get('h3-deep-dive-year-input'),
                runBtn: get('run-h3-deep-dive-btn'), statusMsg: get('h3-deep-dive-status-message'),
                content: get('h3-deep-dive-report-content'), closeBtn: get('h3-deep-dive-close-btn')
            },
            sidebar: get('app-sidebar'),
            sidebarBackdrop: get('sidebar-backdrop'),
            mobileMenuBtn: get('mobile-menu-btn'),
            mobileSidebarCloseBtn: get('mobile-sidebar-close'),
            sidebarNav: document.querySelector('#app-sidebar nav'),
            sidebarPhasesContainer: get('phases-container')
        };
    },
    
    // Podpinamy wyeksportowany obiekt renderers, aby zachować strukturę ui.renderers
    renderers: renderers
};
