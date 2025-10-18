// Medusir 3.1 - Backend Predictive Engine (Pre-Trained Hybrid)
// Developed by FrostyMedusir

const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');

// --- Configuration ---
const API_TOKEN = '9TSAgtfLRXRAlfj';
const APP_ID = 1089;
const WEIGHTS_FILE = './medusir_weights.json';
const VOLATILITY_INDEXES = {
    '1HZ100V': 'Vol 100 (1s)', '1HZ75V': 'Vol 75 (1s)', '1HZ50V': 'Vol 50 (1s)', '1HZ25V': 'Vol 25 (1s)', '1HZ10V': 'Vol 10 (1s)',
};
const ANALYSIS_TICKS = 100;
const STABILITY_ANALYSIS_TICKS = 150;
const ENTRY_WINDOW_SECONDS = 5;
const KALMAN_R = 0.01, KALMAN_Q = 0.1;
const ATR_PERIOD = 14;
const LOSS_STREAK_THRESHOLD = 3;
const COOLDOWN_PERIOD_MS = 5 * 60 * 1000;
const RISK_OFF_THRESHOLD = 3;

// --- Pre-trained Weights from Historical Records ---
const PRE_TRAINED_WEIGHTS = {
    TRENDING: { ldf: 0.6, recency: 0.8, pf: 1.5, stoch: 0.7 },
    REVERTING: { ldf: 1.4, recency: 1.2, pf: 0.6, stoch: 1.1 },
    BALANCED: { ldf: 1.0, recency: 1.0, pf: 1.0, stoch: 1.0 }
};

let instruments = {};
let portfolioRiskMode = 'NORMAL';

// --- Initialization ---
function initializeInstruments() {
    console.log("Initializing Medusir 3.1 Engine...");
    const savedWeights = loadWeights();
    for (const symbol in VOLATILITY_INDEXES) {
        instruments[symbol] = {
            symbol,
            priceHistory: { high: [], low: [], close: [] },
            smoothedPriceHistory: [],
            kalmanState: { x: null, p: 1 },
            weights: savedWeights[symbol] || { ...PRE_TRAINED_WEIGHTS.BALANCED },
            marketPersona: 'BALANCED',
            activeSignal: null,
            lossStreak: 0,
            cooldownUntil: null,
        };
    }
}

// --- Persistent Learning & File IO ---
function loadWeights() {
    try {
        if (fs.existsSync(WEIGHTS_FILE)) {
            console.log("Loading saved AI weights from records...");
            const data = fs.readFileSync(WEIGHTS_FILE, 'utf8');
            return JSON.parse(data);
        }
        console.log("No records found. Starting with pre-trained intelligence.");
        return {};
    } catch (e) {
        console.error("Error loading weights file, starting fresh:", e.message);
        return {};
    }
}

function saveWeights() {
    const weightsToSave = {};
    for (const symbol in instruments) {
        weightsToSave[symbol] = instruments[symbol].weights;
    }
    try {
        fs.writeFileSync(WEIGHTS_FILE, JSON.stringify(weightsToSave, null, 2));
    } catch (e) {
        console.error("Error saving weights to file:", e.message);
    }
}

// --- Server & Comms ---
const server = http.createServer((req, res) => {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    res.end('Medusir 3.1 Backend is running.');
});
const wss = new WebSocket.Server({ server });

wss.on('connection', ws => console.log('Frontend client connected.'));

function broadcast(data) {
    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) client.send(JSON.stringify(data));
    });
}

// --- Deriv API Connection ---
function connectToDeriv() {
    const wsDeriv = new WebSocket(`wss://ws.binaryws.com/websockets/v3?app_id=${APP_ID}`);
    wsDeriv.onopen = () => { console.log('Connected to Deriv API.'); wsDeriv.send(JSON.stringify({ authorize: API_TOKEN })); };
    wsDeriv.onmessage = (event) => {
        const data = JSON.parse(event.data);
        if (data.error) { console.error('Deriv API Error:', data.error.message); wsDeriv.close(); return; }
        if (data.msg_type === 'authorize' && data.authorize) {
            console.log('Authorized with Deriv API. Subscribing to ticks...');
            for (const symbol in VOLATILITY_INDEXES) wsDeriv.send(JSON.stringify({ ticks: symbol, subscribe: 1 }));
        }
        if (data.msg_type === 'tick') handleTick(data.tick);
    };
    wsDeriv.onclose = () => { console.log('Disconnected from Deriv API. Reconnecting in 5 seconds...'); setTimeout(connectToDeriv, 5000); };
    wsDeriv.onerror = (error) => console.error('Deriv WebSocket error:', error.message);
}

// --- Main Logic & Analysis ---
function handleTick(tick) {
    const symbol = tick.symbol;
    if (!instruments[symbol]) return;
    const instrument = instruments[symbol];
    const newPrice = tick.quote;

    // Update price history for all indicators
    const lastClose = instrument.priceHistory.close.length > 0 ? instrument.priceHistory.close[instrument.priceHistory.close.length - 1] : newPrice;
    instrument.priceHistory.high.push(Math.max(lastClose, newPrice));
    instrument.priceHistory.low.push(Math.min(lastClose, newPrice));
    instrument.priceHistory.close.push(newPrice);
    
    // Smooth data with Kalman Filter
    const smoothedPrice = kalmanUpdate(instrument.kalmanState, newPrice);
    instrument.smoothedPriceHistory.push(smoothedPrice);

    if (instrument.priceHistory.close.length > STABILITY_ANALYSIS_TICKS + 5) {
       Object.keys(instrument.priceHistory).forEach(key => instrument.priceHistory[key].shift());
       instrument.smoothedPriceHistory.shift();
    }
    
    broadcast({ type: 'tick', symbol, digit: newPrice.toFixed(2).slice(-1) });

    // --- INSTANT ANALYSIS ON EVERY TICK ---
    runAnalysis(symbol);
}

function runAnalysis(symbol) {
    const instrument = instruments[symbol];
    if (instrument.activeSignal) return; // Don't analyze if a signal is already active

    if (instrument.cooldownUntil && Date.now() < instrument.cooldownUntil) {
        broadcast({ type: 'status', symbol, status: 'COOLING DOWN', persona: instrument.marketPersona });
        return;
    }
    if(instrument.cooldownUntil) {
        instrument.cooldownUntil = null; instrument.lossStreak = 0;
        console.log(`Instrument ${symbol} has cooled down. Resuming analysis.`);
    }

    const prediction = getAIConfluencePrediction(symbol);

    if (prediction.signal === 'CONFIRMED') {
        instrument.activeSignal = {
            predictedDigit: prediction.digit,
            modelsUsed: prediction.modelsUsed,
            entryTime: Date.now()
        };
        broadcast({
            type: 'signal', symbol, digit: prediction.digit,
            confidence: prediction.confidence, models: prediction.modelsUsed, persona: instrument.marketPersona
        });
        setTimeout(() => {
            backtestAndLearn(symbol);
            instrument.activeSignal = null;
        }, ENTRY_WINDOW_SECONDS * 1000);
    } else {
         broadcast({ type: 'status', symbol, status: 'ANALYZING', persona: instrument.marketPersona, reason: prediction.reason });
    }
}

// --- AI Engine & Helpers ---
function getAIConfluencePrediction(symbol) { 
    const instrument = instruments[symbol]; 
    if (instrument.smoothedPriceHistory.length < ANALYSIS_TICKS) return { signal: 'HOLD', reason: 'Calibrating...' }; 

    const marketPersona = detectMarketPersona(instrument.priceHistory); 
    instrument.marketPersona = marketPersona; 
    
    if (marketPersona === 'ERRATIC') return { signal: 'HOLD', reason: 'Erratic Market' }; 
    
    instrument.weights = PRE_TRAINED_WEIGHTS[marketPersona];
    const requiredConfluence = portfolioRiskMode === 'RISK_OFF' ? 4 : 3;

    const lastDigits = instrument.smoothedPriceHistory.map(price => parseInt(price.toFixed(2).slice(-1))); 
    const experts = [
        getLDFPrediction(lastDigits), 
        getRecencyPrediction(lastDigits), 
        getPairingFrequencyPrediction(lastDigits), 
        getStochasticPrediction(instrument.priceHistory)
    ];
    
    const predictions = {}; 
    experts.forEach(p => { 
        if (p && p.digit !== null) { 
            if (!predictions[p.digit]) predictions[p.digit] = { score: 0, models: [] }; 
            predictions[p.digit].score += p.score * instrument.weights[p.model]; 
            predictions[p.digit].models.push(p.model.toUpperCase()); 
        } 
    }); 

    let bestDigit = null, maxScore = -Infinity, finalModels = []; 
    for (const digit in predictions) { 
        if (predictions[digit].models.length >= requiredConfluence && predictions[digit].score > maxScore) { 
            maxScore = predictions[digit].score; 
            bestDigit = parseInt(digit); 
            finalModels = predictions[digit].models; 
        } 
    } 
    if (bestDigit !== null) { 
        const confidence = Math.min(99, Math.floor(60 + maxScore / 10)); 
        return { signal: 'CONFIRMED', digit: bestDigit, confidence: confidence, modelsUsed: finalModels }; 
    } 
    return { signal: 'HOLD', reason: 'No Confluence' }; 
}

function detectMarketPersona(priceHistory) { /* ... same as before ... */ }
function getLDFPrediction(digits) { /* ... same as before ... */ }
function getRecencyPrediction(digits) { /* ... same as before ... */ }
function getPairingFrequencyPrediction(digits) { /* ... same as before ... */ }
function getStochasticPrediction(priceHistory) { /* ... same as before ... */ }
function kalmanUpdate(state, z) { /* ... same as before ... */ }
function detectMarketPersona(priceHistory) { const slice = priceHistory.close.slice(-50); if (slice.length < 50) return 'BALANCED'; const mean = slice.reduce((a, b) => a + b, 0) / slice.length; const stdDev = Math.sqrt(slice.map(x => Math.pow(x - mean, 2)).reduce((a, b) => a + b) / slice.length); const relativeStdDev = stdDev / mean; if (relativeStdDev > 0.0008) return 'ERRATIC'; if (relativeStdDev < 0.0003) return 'REVERTING'; return 'TRENDING'; }
function getLDFPrediction(digits) { const counts = Array(10).fill(0); digits.slice(-50).forEach(d => counts[d]++); const min = Math.min(...counts); const digit = counts.indexOf(min); const score = 100 - (min/50*100*2); return { digit, score, model: 'ldf' }; }
function getRecencyPrediction(digits) { let scores = []; for (let i=0; i<=9; i++) { scores[i] = digits.lastIndexOf(i); } const min = Math.min(...scores); const digit = scores.indexOf(min); const score = (digits.length - min) / ANALYSIS_TICKS * 50; return { digit, score, model: 'recency' }; }
function getPairingFrequencyPrediction(digits) { const last = digits[digits.length-1]; const transitions = {}; for(let i=0; i<digits.length-1; i++) { if (digits[i] === last) { const next = digits[i+1]; transitions[next] = (transitions[next] || 0) + 1; } } let mostCommon = null, max = -1; for(let i=0;i<=9;i++){ if((transitions[i]||0) > max){ max = transitions[i]||0; mostCommon = i; } } return { digit: mostCommon, score: max*10, model: 'pf' }; }
function getStochasticPrediction(priceHistory) { const period = 14; if (priceHistory.close.length < period) return null; const highs = priceHistory.high.slice(-period); const lows = priceHistory.low.slice(-period); const highestHigh = Math.max(...highs); const lowestLow = Math.min(...lows); const k = ((priceHistory.close[priceHistory.close.length-1] - lowestLow) / (highestHigh-lowestLow)) * 100; if (k < 10) return {digit: parseInt(lowestLow.toFixed(2).slice(-1)), score: 30, model: 'stoch'}; if (k > 90) return {digit: parseInt(highestHigh.toFixed(2).slice(-1)), score: 30, model: 'stoch'}; return null; }
function kalmanUpdate(state, z) { if (state.x === null) { state.x = z; } const p_pred = state.p + KALMAN_R; const K = p_pred / (p_pred + KALMAN_Q); state.x = state.x + K * (z - state.x); state.p = (1 - K) * p_pred; return state.x; }

function backtestAndLearn(symbol) {
    const instrument = instruments[symbol];
    if (!instrument.activeSignal) return;

    const finalPrice = instrument.priceHistory.close[instrument.priceHistory.close.length - 1];
    const finalDigit = parseInt(finalPrice.toFixed(2).slice(-1));
    const { predictedDigit, modelsUsed } = instrument.activeSignal;
    const wasCorrect = finalDigit === predictedDigit;
    const adj = 0.1;

    if (wasCorrect) {
        instrument.lossStreak = 0;
        console.log(`✅ WIN on ${symbol}. Predicted: ${predictedDigit}. Actual: ${finalDigit}. Reinforcing ${modelsUsed.join(', ')}.`);
        modelsUsed.forEach(model => { instrument.weights[model.toLowerCase()] += adj; });
    } else {
        instrument.lossStreak++;
        console.log(`❌ LOSS on ${symbol}. Predicted: ${predictedDigit}. Actual: ${finalDigit}. Penalizing ${modelsUsed.join(', ')}.`);
        modelsUsed.forEach(model => { instrument.weights[model.toLowerCase()] -= adj; });
    }
    
    if (instrument.lossStreak >= LOSS_STREAK_THRESHOLD) {
        instrument.cooldownUntil = Date.now() + COOLDOWN_PERIOD_MS;
        console.log(`-- ${symbol} deactivated. Cooling down for 5 minutes. --`);
    }

    Object.keys(instrument.weights).forEach(key => {
        instrument.weights[key] = Math.max(0.5, Math.min(2.0, instrument.weights[key]));
    });
    saveWeights();
}

function checkPortfolioRisk() {
    const cooledDownCount = Object.values(instruments).filter(inst => inst.cooldownUntil && Date.now() < inst.cooldownUntil).length;
    const newMode = cooledDownCount >= RISK_OFF_THRESHOLD ? 'RISK_OFF' : 'NORMAL';
    if (newMode !== portfolioRiskMode) {
        portfolioRiskMode = newMode;
        console.log(`PORTFOLIO RISK MODE CHANGED TO: ${portfolioRiskMode}.`);
    }
}

// --- Start the Engine ---
initializeInstruments();
connectToDeriv();
setInterval(checkPortfolioRisk, 10000);

server.listen(process.env.PORT || 8080, () => {
    console.log(`Medusir 3.1 Backend is running on port ${process.env.PORT || 8080}`);
});

