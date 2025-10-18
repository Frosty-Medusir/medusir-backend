// Medusir 3.2 - Backend Predictive Engine (Dynamic Probability)
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
const CERTAINTY_THRESHOLD = 1.5; // Top score must be 50% higher than the next best

// --- Initial Weights from Historical Records ---
const INITIAL_WEIGHTS = { ldf: 1.0, recency: 1.0, pf: 1.0 };

let instruments = {};

// --- Initialization ---
function initializeInstruments() {
    console.log("Initializing Medusir 3.2 Engine...");
    const savedWeights = loadWeights();
    for (const symbol in VOLATILITY_INDEXES) {
        instruments[symbol] = {
            symbol,
            priceHistory: [],
            kalmanState: { x: null, p: 1 },
            weights: savedWeights[symbol] || { ...INITIAL_WEIGHTS },
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
        console.log("No records found. Starting with initial intelligence.");
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
    res.end('Medusir 3.2 Backend is running.');
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

    // Smooth data with Kalman Filter and update history
    const smoothedPrice = kalmanUpdate(instrument.kalmanState, newPrice);
    instrument.priceHistory.push(smoothedPrice);

    if (instrument.priceHistory.length > STABILITY_ANALYSIS_TICKS + 5) {
       instrument.priceHistory.shift();
    }
    
    broadcast({ type: 'tick', symbol, digit: newPrice.toFixed(2).slice(-1) });

    // --- INSTANT ANALYSIS ON EVERY TICK ---
    runAnalysis(symbol);
}

function runAnalysis(symbol) {
    const instrument = instruments[symbol];
    if (instrument.activeSignal) return; 

    const prediction = getAIProbabilityPrediction(symbol);

    if (prediction.signal === 'CONFIRMED') {
        instrument.activeSignal = {
            predictedDigit: prediction.digit,
            modelScores: prediction.modelScores,
            entryTime: Date.now()
        };
        broadcast({
            type: 'signal', symbol, digit: prediction.digit,
            confidence: prediction.confidence, models: prediction.topModels
        });
        setTimeout(() => {
            if (instrument.activeSignal) {
                backtestAndLearn(symbol);
                instrument.activeSignal = null;
            }
        }, ENTRY_WINDOW_SECONDS * 1000);
    } else {
         broadcast({ type: 'status', symbol, status: 'ANALYZING', reason: prediction.reason });
    }
}

// --- AI Engine & Helpers ---
function getAIProbabilityPrediction(symbol) {
    const instrument = instruments[symbol];
    if (instrument.priceHistory.length < ANALYSIS_TICKS) return { signal: 'HOLD', reason: 'Calibrating...' };

    const lastDigits = instrument.priceHistory.map(price => parseInt(price.toFixed(2).slice(-1)));
    
    const isStable = getVolatilityCheck(lastDigits);
    if (!isStable) return { signal: 'HOLD', reason: 'Market too volatile' };
    
    // --- Feature Scoring ---
    const ldfScores = getLDFScores(lastDigits);
    const recencyScores = getRecencyScores(lastDigits);
    const pfScores = getPairingFrequencyScores(lastDigits);

    // --- Weighted Aggregation ---
    const finalScores = Array(10).fill(0);
    for (let i = 0; i < 10; i++) {
        finalScores[i] += ldfScores[i] * instrument.weights.ldf;
        finalScores[i] += recencyScores[i] * instrument.weights.recency;
        finalScores[i] += pfScores[i] * instrument.weights.pf;
    }

    // --- Certainty Threshold ---
    const sortedScores = [...finalScores].map((score, index) => ({ score, index })).sort((a, b) => b.score - a.score);
    const best = sortedScores[0];
    const runnerUp = sortedScores[1];

    if (best.score > runnerUp.score * CERTAINTY_THRESHOLD) {
        const confidence = Math.min(99, Math.floor(50 + (best.score - runnerUp.score) * 2));
        const topModels = [
            { score: ldfScores[best.index], name: 'LDF'},
            { score: recencyScores[best.index], name: 'RECENCY'},
            { score: pfScores[best.index], name: 'PF'}
        ].sort((a,b) => b.score - a.score).slice(0,2).map(m => m.name);

        return {
            signal: 'CONFIRMED',
            digit: best.index,
            confidence: confidence,
            topModels: topModels,
            modelScores: { ldf: ldfScores, recency: recencyScores, pf: pfScores }
        };
    }

    return { signal: 'HOLD', reason: 'No High-Certainty Signal' };
}

function getLDFScores(digits) {
    const counts = Array(10).fill(0);
    digits.forEach(d => counts[d]++);
    const maxCount = Math.max(...counts);
    return counts.map(count => (maxCount - count) * 1.5); 
}

function getRecencyScores(digits) {
    const scores = Array(10).fill(0);
    for (let i=0; i<10; i++) {
        const lastSeen = digits.lastIndexOf(i);
        scores[i] = lastSeen === -1 ? ANALYSIS_TICKS : digits.length - 1 - lastSeen;
    }
    return scores;
}

function getPairingFrequencyScores(digits) {
    const last = digits[digits.length - 1];
    const transitions = Array(10).fill(0);
    for (let i = 0; i < digits.length - 1; i++) {
        if (digits[i] === last) {
            transitions[digits[i+1]]++;
        }
    }
    const maxTransition = Math.max(...transitions);
    return transitions.map(count => (count / (maxTransition || 1)) * 30); 
}

function getVolatilityCheck(digits) { 
    const slice = digits.slice(-30); 
    const mean = slice.reduce((a,b)=>a+b,0) / slice.length; 
    const stdDev = Math.sqrt(slice.map(x => Math.pow(x-mean, 2)).reduce((a,b) => a+b) / slice.length); 
    return stdDev < 2.85; 
}
function kalmanUpdate(state, z) { if (state.x === null) { state.x = z; } const p_pred = state.p + KALMAN_R; const K = p_pred / (p_pred + KALMAN_Q); state.x = state.x + K * (z - state.x); state.p = (1 - K) * p_pred; return state.x; }

function backtestAndLearn(symbol) {
    const instrument = instruments[symbol];
    if (!instrument.activeSignal) return;

    const finalPrice = instrument.priceHistory[instrument.priceHistory.length - 1];
    const finalDigit = parseInt(finalPrice.toFixed(2).slice(-1));
    const { predictedDigit, modelScores } = instrument.activeSignal;
    const wasCorrect = finalDigit === predictedDigit;
    const adj = 0.05;

    // Check which models were "most correct" for the actual outcome
    const finalScores = [
        { model: 'ldf', score: modelScores.ldf[finalDigit] },
        { model: 'recency', score: modelScores.recency[finalDigit] },
        { model: 'pf', score: modelScores.pf[finalDigit] },
    ].sort((a, b) => b.score - a.score);

    if (wasCorrect) {
        console.log(`✅ WIN on ${symbol}. Predicted: ${predictedDigit}. Actual: ${finalDigit}.`);
        // Reinforce the model that had the highest score for the CORRECT digit
        instrument.weights[finalScores[0].model] += adj * 2;
        instrument.weights[finalScores[1].model] += adj;
    } else {
        console.log(`❌ LOSS on ${symbol}. Predicted: ${predictedDigit}. Actual: ${finalDigit}.`);
        // Penalize the model that had the highest score for the WRONG (predicted) digit
        const predictedScores = [
            { model: 'ldf', score: modelScores.ldf[predictedDigit] },
            { model: 'recency', score: modelScores.recency[predictedDigit] },
            { model: 'pf', score: modelScores.pf[predictedDigit] },
        ].sort((a, b) => b.score - a.score);
        instrument.weights[predictedScores[0].model] -= adj * 2;
    }

    Object.keys(instrument.weights).forEach(key => {
        instrument.weights[key] = Math.max(0.5, Math.min(2.0, instrument.weights[key]));
    });
    console.log(`New weights for ${symbol}: LDF=${instrument.weights.ldf.toFixed(2)}, RECENCY=${instrument.weights.recency.toFixed(2)}, PF=${instrument.weights.pf.toFixed(2)}`);
    saveWeights();
}

// --- Start the Engine ---
initializeInstruments();
connectToDeriv();

server.listen(process.env.PORT || 8080, () => {
    console.log(`Medusir 3.2 Backend is running on port ${process.env.PORT || 8080}`);
});

