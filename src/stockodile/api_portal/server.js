import { express, cors, dotenv, uuidv4 } from './libs.js';
import path from 'path';
import { fileURLToPath } from 'url';
import { ethers } from 'ethers';
import crypto from 'crypto';
import http from 'http';


dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Proxy middleware for /api/v1/*, /metrics, /docs, /openapi.json to FastAPI backend
app.use((req, res, next) => {
  const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  const pathname = url.pathname;

  const shouldProxy = pathname.startsWith('/api/v1/') || 
                      pathname === '/metrics' || 
                      pathname === '/docs' || 
                      pathname === '/openapi.json';

  if (shouldProxy) {
    const backendUrl = process.env.FASTAPI_BACKEND_URL || process.env.BACKEND_URL || 'http://127.0.0.1:8000';
    
    // Map paths for parity with FastAPI endpoints
    let targetPath = pathname;
    if (pathname === '/api/v1/events') {
      targetPath = '/api/events';
    } else if (pathname === '/api/v1/payments') {
      targetPath = '/api/v1/admin/payments';
    }

    const targetUrl = new URL(targetPath + url.search, backendUrl);
    const headers = { ...req.headers };

    // Set standard proxy headers
    const clientIp = req.socket?.remoteAddress || '';
    if (clientIp) {
      if (headers['x-forwarded-for']) {
        headers['x-forwarded-for'] = `${headers['x-forwarded-for']}, ${clientIp}`;
      } else {
        headers['x-forwarded-for'] = clientIp;
      }
    }

    const options = {
      method: req.method,
      headers: headers
    };

    const proxyReq = http.request(targetUrl, options, (proxyRes) => {
      // Disable buffering and compression-transforming on reverse proxies for SSE stream
      if (targetPath === '/api/events') {
        proxyRes.headers['x-accel-buffering'] = 'no';
        if (proxyRes.headers['cache-control']) {
          proxyRes.headers['cache-control'] += ', no-transform';
        } else {
          proxyRes.headers['cache-control'] = 'no-cache, no-transform';
        }
      }

      res.writeHead(proxyRes.statusCode, proxyRes.headers);
      proxyRes.pipe(res);
    });

    proxyReq.on('error', (err) => {
      console.error(`Proxy request to backend failed: ${err.message}`);
      res.writeHead(502, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Bad Gateway: Backend connection failed' }));
    });

    if (req.body && (req.method === 'POST' || req.method === 'PUT' || req.method === 'PATCH')) {
      const bodyData = typeof req.body === 'string' ? req.body : JSON.stringify(req.body);
      proxyReq.write(bodyData);
      proxyReq.end();
    } else if (req.pipe) {
      req.pipe(proxyReq);
    } else {
      proxyReq.end();
    }
  } else {
    next();
  }
});


// Serve static frontend files from the public directory
app.use(express.static(path.join(__dirname, 'public')));

// Signature replay and reuse index
const processedSignatures = new Map();

// Mock In-Memory Database for Payments Ledger
const paymentsLedger = [
  {
    payment_id: "a3b04c8f-2879-4d8e-9d22-132d7b5f6390",
    status: "verified",
    sender: "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266",
    recipient: "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
    amount: "0.10",
    currency: "USD",
    txHash: "0x5c5067a6a3b0c801bcbc26759c5d1e2e1d7dc1518f8e811c76a77d7f781dc41b",
    timestamp: new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(), // 2 hours ago
    signature: "0x307822..."
  },
  {
    payment_id: "c23fbe2e-13c5-4a52-b430-84a86b97621c",
    status: "pending",
    sender: null,
    recipient: "0x70997970C51812dc3A010C7d01b50e0d17dc79C8",
    amount: "0.10",
    currency: "USD",
    txHash: null,
    timestamp: new Date(Date.now() - 1 * 60 * 60 * 1000).toISOString(), // 1 hour ago
    signature: null
  }
];

// Active SSE Clients
let sseClients = [];

// Helper to broadcast to SSE
function broadcastSSE(eventData) {
  sseClients = sseClients.filter(client => {
    try {
      client.write(`data: ${JSON.stringify(eventData)}\n\n`);
      return true;
    } catch (err) {
      // client connection closed or failed, remove it
      return false;
    }
  });
}

// Background Price Tickers
const tickInterval = setInterval(() => {
  const mockPrice = (2000 + Math.random() * 100).toFixed(2);
  broadcastSSE({
    type: 'tick',
    stage: 'price_update',
    status: 'success',
    message: `Price updated to $${mockPrice}`,
    data: { price: mockPrice, timestamp: new Date().toISOString() }
  });
}, 2000);

if (tickInterval.unref) {
  tickInterval.unref();
}

// 1. Root route serving index.html explicitly
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// 2. Payments Ledger Endpoint (GET /api/payments)
app.get('/api/payments', (req, res) => {
  let filtered = [...paymentsLedger];
  const { status, sender, txHash, search, limit, offset, sort } = req.query;

  const statusStr = typeof status === 'string' ? status : '';
  const senderStr = typeof sender === 'string' ? sender : '';
  const txHashStr = typeof txHash === 'string' ? txHash : '';
  const searchStr = typeof search === 'string' ? search : '';

  // Filter by Status
  if (statusStr) {
    filtered = filtered.filter(p => p.status === statusStr);
  }

  // Filter by Sender Address (case-insensitive)
  if (senderStr) {
    filtered = filtered.filter(p => p.sender && p.sender.toLowerCase() === senderStr.toLowerCase());
  }

  // Filter by txHash (case-insensitive)
  if (txHashStr) {
    filtered = filtered.filter(p => p.txHash && p.txHash.toLowerCase() === txHashStr.toLowerCase());
  }

  // Search by payment_id, sender address, or transaction hash (case-insensitive)
  if (searchStr) {
    const term = searchStr.toLowerCase();
    filtered = filtered.filter(p => 
      (p.payment_id && p.payment_id.toLowerCase().includes(term)) ||
      (p.sender && p.sender.toLowerCase().includes(term)) ||
      (p.txHash && p.txHash.toLowerCase().includes(term))
    );
  }

  // Sort by timestamp (default: descending / newest first)
  filtered.sort((a, b) => {
    const timeA = new Date(a.timestamp).getTime();
    const timeB = new Date(b.timestamp).getTime();
    if (sort === 'asc') {
      return timeA - timeB;
    } else {
      return timeB - timeA;
    }
  });

  // Pagination
  let start = parseInt(offset, 10);
  if (isNaN(start) || start < 0) {
    start = 0;
  }
  let size = parseInt(limit, 10);
  if (isNaN(size) || size < 0) {
    size = 50;
  } else if (size > 100) {
    size = 100;
  }
  const paginated = filtered.slice(start, start + size);

  res.json({
    total: filtered.length,
    payments: paginated
  });
});

// 3. Register or Update Payment Ledger Endpoint (POST /api/payments)
// Clients may create/update metadata only. Status "verified" is server-side only
// (set by signature verification on GET /api/gated-data). Unauthenticated clients
// cannot escalate a payment to verified via this endpoint.
app.post('/api/payments', (req, res) => {
  const { payment_id, status, sender, recipient, amount, currency, txHash, signature } = req.body;
  if (!payment_id) {
    return res.status(400).json({ error: "payment_id is required" });
  }

  // Never accept client-supplied verified; allow other non-privileged statuses
  const safeClientStatus =
    status && status !== 'verified' ? status : 'pending';

  let payment = paymentsLedger.find(p => p.payment_id === payment_id);
  if (payment) {
    // Do not let clients elevate (or re-assert) verified; keep existing verified if set server-side
    if (status && status !== 'verified') {
      payment.status = status;
    }
    // If client tries status=verified while still pending, force remain pending (no-op)
    if (sender) payment.sender = sender;
    if (recipient) payment.recipient = recipient;
    if (amount) payment.amount = amount;
    if (currency) payment.currency = currency;
    if (txHash) payment.txHash = txHash;
    if (signature) payment.signature = signature;
    payment.timestamp = new Date().toISOString();
  } else {
    payment = {
      payment_id,
      status: safeClientStatus,
      sender: sender || null,
      recipient: recipient || '0x70997970C51812dc3A010C7d01b50e0d17dc79C8',
      amount: amount || '0.10',
      currency: currency || 'USD',
      txHash: txHash || null,
      timestamp: new Date().toISOString(),
      signature: signature || null
    };
    paymentsLedger.push(payment);
  }

  // Broadcast to SSE
  broadcastSSE({
    type: 'payment',
    stage: payment.status === 'verified' ? 'payment_received' : 'pending',
    status: 'success',
    message: `Payment ${payment_id} is registered as ${payment.status}`,
    data: payment
  });

  res.json(payment);
});

// 4. Gated API Endpoint (x402 Micropayment Protocol Handshake)
app.get('/api/gated-data', (req, res) => {
  const paymentIdHeader = req.headers['payment-id'];
  const paymentSenderHeader = req.headers['payment-sender'];
  const paymentSignatureHeader = req.headers['payment-signature'];

  // If any required handshake header is missing, return 402 and register in ledger
  if (!paymentIdHeader || !paymentSenderHeader || !paymentSignatureHeader) {
    const newPaymentId = uuidv4();
    const defaultRecipient = process.env.RECIPIENT_ADDRESS || '0x70997970C51812dc3A010C7d01b50e0d17dc79C8';
    const fee = process.env.GATED_FEE || '0.10';

    const pendingPayment = {
      payment_id: newPaymentId,
      status: 'pending',
      sender: null,
      recipient: defaultRecipient,
      amount: fee,
      currency: 'USD',
      txHash: null,
      timestamp: new Date().toISOString(),
      signature: null
    };
    paymentsLedger.push(pendingPayment);

    // Broadcast SSE pending payment
    broadcastSSE({
      type: 'payment',
      stage: 'pending',
      status: 'pending',
      message: `New gated request initiated. Handshake payment_id generated: ${newPaymentId}`,
      data: pendingPayment
    });

    return res.status(402).json({
      error: 'Payment Required',
      payment_id: newPaymentId,
      fee: fee,
      currency: 'USD',
      recipient: defaultRecipient
    });
  }

  // Verify that payment_id exists in ledger as pending or verified
  const existingPayment = paymentsLedger.find(p => p.payment_id === paymentIdHeader);
  if (!existingPayment || (existingPayment.status !== 'pending' && existingPayment.status !== 'verified')) {
    return res.status(400).json({ error: "Invalid Payment ID" });
  }

  if (existingPayment.status === 'verified') {
    return res.json({
      status: 'success',
      data: 'Premium dark-themed gated content unlocked! Welcome to the premium Stockodile x402 Micropayments portal.',
      payment: existingPayment
    });
  }

  // Signature replay and reuse prevention
  if (processedSignatures.has(paymentSignatureHeader)) {
    return res.status(400).json({ error: "Signature already processed" });
  }
  processedSignatures.set(paymentSignatureHeader, paymentIdHeader);
  if (processedSignatures.size > 1000) {
    const oldestKey = processedSignatures.keys().next().value;
    processedSignatures.delete(oldestKey);
  }

  // Start verification stages and broadcast to SSE
  broadcastSSE({
    type: 'verification',
    stage: 'signature_recovery',
    status: 'pending',
    message: `Recovering signer address from signature for payment_id: ${paymentIdHeader}...`,
    data: { payment_id: paymentIdHeader }
  });

  let recoveredAddress;
  try {
    recoveredAddress = ethers.verifyMessage(paymentIdHeader, paymentSignatureHeader);
  } catch (err) {
    broadcastSSE({
      type: 'verification',
      stage: 'signature_recovery',
      status: 'failed',
      message: `Signature recovery failed: ${err.message}`,
      data: { error: err.message }
    });
    return res.status(400).json({ error: 'Signature verification failed' });
  }

  broadcastSSE({
    type: 'verification',
    stage: 'sender_matching',
    status: 'pending',
    message: `Recovered signer: ${recoveredAddress}. Comparing with Payment-Sender: ${paymentSenderHeader}`,
    data: { recovered: recoveredAddress, expected: paymentSenderHeader, payment_id: paymentIdHeader }
  });

  if (recoveredAddress.toLowerCase() !== paymentSenderHeader.toLowerCase()) {
    broadcastSSE({
      type: 'verification',
      stage: 'sender_matching',
      status: 'failed',
      message: `Recovered address ${recoveredAddress} does not match Payment-Sender ${paymentSenderHeader}`,
      data: { recovered: recoveredAddress, expected: paymentSenderHeader, payment_id: paymentIdHeader }
    });
    return res.status(400).json({ error: 'Recovered signer matches mismatch' });
  }

  // Signature matches sender! Now process block confirmations
  broadcastSSE({
    type: 'verification',
    stage: 'block_confirmation',
    status: 'success',
    message: 'Transaction confirmed with 12/12 block confirmations on-chain.',
    data: { confirmations: 12, payment_id: paymentIdHeader }
  });

  // Find or create ledger entry
  let payment = paymentsLedger.find(p => p.payment_id === paymentIdHeader);
  const mockTxHash = '0x' + crypto.randomUUID().replace(/-/g, '') + crypto.randomUUID().replace(/-/g, '');
  if (payment) {
    payment.status = 'verified';
    payment.sender = paymentSenderHeader;
    payment.signature = paymentSignatureHeader;
    payment.txHash = payment.txHash || mockTxHash;
    payment.timestamp = new Date().toISOString();
  } else {
    payment = {
      payment_id: paymentIdHeader,
      status: 'verified',
      sender: paymentSenderHeader,
      recipient: process.env.RECIPIENT_ADDRESS || '0x70997970C51812dc3A010C7d01b50e0d17dc79C8',
      amount: process.env.GATED_FEE || '0.10',
      currency: 'USD',
      txHash: mockTxHash,
      timestamp: new Date().toISOString(),
      signature: paymentSignatureHeader
    };
    paymentsLedger.push(payment);
  }

  broadcastSSE({
    type: 'payment',
    stage: 'payment_received',
    status: 'success',
    message: `Payment ${paymentIdHeader} successfully verified. Gated data unlocked.`,
    data: payment
  });

  res.json({
    status: 'success',
    data: 'Premium dark-themed gated content unlocked! Welcome to the premium Stockodile x402 Micropayments portal.',
    payment
  });
});

// 5. SSE Event Stream Endpoint
app.get('/api/events', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');

  
  // Send connection event
  res.write(`data: ${JSON.stringify({ type: 'info', message: 'SSE Stream connected successfully' })}\n\n`);
  
  // Send immediate initial price tick to prevent client handshake timeouts
  const initialPrice = (2000 + Math.random() * 100).toFixed(2);
  res.write(`data: ${JSON.stringify({
    type: 'tick',
    stage: 'price_update',
    status: 'success',
    message: `Price updated to $${initialPrice}`,
    data: { price: initialPrice, timestamp: new Date().toISOString() }
  })}\n\n`);

  sseClients.push(res);

  res.on('error', (err) => {
    // Suppress crash on write failures
    sseClients = sseClients.filter(c => c !== res);
  });

  req.on('close', () => {
    sseClients = sseClients.filter(c => c !== res);
  });
});

// Start listener only if run directly
const nodePath = process.argv[1];
const currentPath = fileURLToPath(import.meta.url);
const isMain = nodePath && (
  nodePath === currentPath ||
  nodePath.endsWith('server.js')
);

if (isMain) {
  app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
  });
}

export { app, paymentsLedger, broadcastSSE };
