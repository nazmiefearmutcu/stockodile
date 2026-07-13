import test from 'node:test';
import assert from 'node:assert';
import { supertest as request } from '../libs.js';
import { app } from '../server.js';
import { ethers } from 'ethers';
import { Readable, Writable } from 'stream';

test('GET / returns 200 and static content', async (t) => {
  const response = await request(app)
    .get('/')
    .expect(200);
  
  assert.match(response.text, /Stockodile x402/);
});

test('GET /api/payments returns the seed data', async (t) => {
  const response = await request(app)
    .get('/api/payments')
    .expect(200);

  const body = response.body;
  assert.strictEqual(typeof body, 'object');
  assert.ok(body.total >= 2);
  assert.ok(Array.isArray(body.payments));
  
  // Verify that the seed data is present in the response
  const verifiedPayment = body.payments.find(p => p.payment_id === 'a3b04c8f-2879-4d8e-9d22-132d7b5f6390');
  assert.ok(verifiedPayment, 'Should find the pre-seeded verified payment');
  assert.strictEqual(verifiedPayment.status, 'verified');
  assert.strictEqual(verifiedPayment.sender, '0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266');
});

test('GET /api/gated-data returns 402 with UUID and correct body structure when headers are missing', async (t) => {
  const response = await request(app)
    .get('/api/gated-data')
    .expect(402);

  const body = response.body;
  assert.strictEqual(body.error, 'Payment Required');
  assert.ok(body.payment_id);
  
  // Verify it is a valid UUID format
  const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  assert.ok(uuidRegex.test(body.payment_id), 'payment_id should be a valid UUID');
  assert.strictEqual(body.fee, '0.10');
  assert.strictEqual(body.currency, 'USD');
  assert.strictEqual(body.recipient, '0x70997970C51812dc3A010C7d01b50e0d17dc79C8');
});

test('Calling GET /api/gated-data adds a new pending record to the ledger', async (t) => {
  // Get initial count of payments
  const initialResponse = await request(app)
    .get('/api/payments')
    .expect(200);
  const initialCount = initialResponse.body.total;

  // Make request to gated-data to trigger handshake
  const gatedResponse = await request(app)
    .get('/api/gated-data')
    .expect(402);
  const newPaymentId = gatedResponse.body.payment_id;

  // Verify that the count has increased by 1
  const finalResponse = await request(app)
    .get('/api/payments')
    .expect(200);
  const finalCount = finalResponse.body.total;
  assert.strictEqual(finalCount, initialCount + 1, 'Total payments should increase by 1');

  // Verify that the new pending payment is in the ledger
  const newPayment = finalResponse.body.payments.find(p => p.payment_id === newPaymentId);
  assert.ok(newPayment, 'Should find the newly generated payment in the ledger');
  assert.strictEqual(newPayment.status, 'pending');
  assert.strictEqual(newPayment.amount, '0.10');
  assert.strictEqual(newPayment.currency, 'USD');
  assert.strictEqual(newPayment.sender, null);
});

test('GET /api/payments with array query parameters (QPP) does not crash the server', async (t) => {
  const response = await request(app)
    .get('/api/payments?search[]=test&sender[]=0x123')
    .expect(200);

  const body = response.body;
  assert.strictEqual(typeof body, 'object');
  assert.ok(Array.isArray(body.payments));
});

test('GET /api/payments with invalid pagination bounds falls back to valid range', async (t) => {
  const response = await request(app)
    .get('/api/payments?limit=-10&offset=-5')
    .expect(200);

  const body = response.body;
  assert.strictEqual(typeof body, 'object');
  assert.ok(Array.isArray(body.payments));

  const responseLarge = await request(app)
    .get('/api/payments?limit=200')
    .expect(200);
  assert.ok(responseLarge.body.payments.length <= 100);
});

test('GET /js/app.js returns 200 and static JS content', async (t) => {
  const response = await request(app)
    .get('/js/app.js')
    .expect(200);
  
  assert.match(response.headers['content-type'], /javascript/);
  assert.match(response.text, /Stockodile x402 Micropayments Gated Portal Redesign/);
});

function postPayments(body) {
  // Polyfill supertest only supports GET; drive app handler directly for POST
  return new Promise((resolve) => {
    const req = new Readable({
      read() {
        this.push(Buffer.from(JSON.stringify(body)));
        this.push(null);
      }
    });
    req.url = '/api/payments';
    req.method = 'POST';
    req.headers = {
      'content-type': 'application/json',
      'content-length': String(JSON.stringify(body).length)
    };
    req.socket = { destroy: () => {}, remoteAddress: '127.0.0.1' };
    // express polyfill may read req.body if pre-parsed
    req.body = body;

    const chunks = [];
    const res = new Writable({
      write(chunk, _enc, cb) {
        chunks.push(Buffer.from(chunk));
        cb();
      }
    });
    res.statusCode = 200;
    res.headers = {};
    res.setHeader = (n, v) => { res.headers[n.toLowerCase()] = v; return res; };
    res.getHeader = (n) => res.headers[n.toLowerCase()];
    res.removeHeader = (n) => { delete res.headers[n.toLowerCase()]; };
    res.writeHead = (status, headers) => {
      res.statusCode = status;
      if (headers) Object.entries(headers).forEach(([k, v]) => res.setHeader(k, v));
      return res;
    };
    res.end = (chunk) => {
      if (chunk) chunks.push(Buffer.from(chunk));
      const text = Buffer.concat(chunks).toString('utf8');
      let parsed = null;
      try { parsed = JSON.parse(text); } catch (_) {}
      resolve({ status: res.statusCode, body: parsed, text });
    };
    app(req, res);
  });
}

test('POST /api/payments rejects client-set status=verified (stays pending)', async (t) => {
  const paymentId = 'client-cannot-verify-' + Date.now();
  const postBody = {
    payment_id: paymentId,
    status: 'verified',
    sender: '0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266',
    amount: '0.10',
    currency: 'USD',
    txHash: '0x' + 'ab'.repeat(32)
  };

  const response = await postPayments(postBody);
  assert.strictEqual(response.status, 200);
  assert.strictEqual(response.body.status, 'pending');
  assert.strictEqual(response.body.payment_id, paymentId);

  // Gated short-circuit must not unlock without server-side verification
  const gated = await request(app)
    .get('/api/gated-data')
    .set('Payment-Id', paymentId)
    .set('Payment-Sender', '0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266')
    .set('Payment-Signature', '0x' + '11'.repeat(65))
    .expect(400);
  assert.ok(gated.body.error);
});

test('POST /api/payments demote fails (status stays verified after client sets pending)', async (t) => {
  // Seed a verified payment via gated-data signature path, then attempt client demote
  const handshake = await request(app).get('/api/gated-data').expect(402);
  const paymentId = handshake.body.payment_id;
  const wallet = ethers.Wallet.createRandom();
  const signature = await wallet.signMessage(paymentId);

  const unlock = await request(app)
    .get('/api/gated-data')
    .set('Payment-Id', paymentId)
    .set('Payment-Sender', wallet.address)
    .set('Payment-Signature', signature)
    .expect(200);
  assert.strictEqual(unlock.body.payment.status, 'verified');

  // Client tries to demote verified → pending via POST update
  const demote = await postPayments({
    payment_id: paymentId,
    status: 'pending',
    sender: wallet.address,
    amount: '0.10',
    currency: 'USD'
  });
  assert.strictEqual(demote.status, 200);
  assert.strictEqual(demote.body.status, 'verified', 'Client must not demote verified status via POST');

  // Ledger still reflects verified
  const ledger = await request(app)
    .get(`/api/payments?search=${paymentId}`)
    .expect(200);
  const record = ledger.body.payments.find(p => p.payment_id === paymentId);
  assert.ok(record);
  assert.strictEqual(record.status, 'verified');
});

test('GET /js/utils.js exposes escapeHtml helper', async (t) => {
  const response = await request(app)
    .get('/js/utils.js')
    .expect(200);
  assert.match(response.text, /function escapeHtml/);
  assert.match(response.text, /window\.escapeHtml/);
});

test('Challenger Stress & Empirical Verification Test Suite', async (t) => {
  class MockReq extends Readable {
    constructor(url, method, headers, body = null) {
      super();
      this.url = url;
      this.method = method || 'GET';
      this.headers = {};
      for (const key in headers) {
        this.headers[key.toLowerCase()] = headers[key];
      }
      this.socket = { destroy: () => {} };
      this.bodyData = body;
    }
    _read() {
      if (this.bodyData) {
        const chunk = typeof this.bodyData === 'string' ? this.bodyData : JSON.stringify(this.bodyData);
        this.push(Buffer.from(chunk));
      }
      this.push(null);
    }
  }

  class MockRes extends Writable {
    constructor(callback) {
      super();
      this.statusCode = 200;
      this.headers = {};
      this.chunks = [];
      this.callback = callback;
      this.resolved = false;

      // Instance methods to survive Express prototype re-setting
      this.setHeader = (name, value) => {
        this.headers[name.toLowerCase()] = value;
        if (name.toLowerCase() === 'content-type' && value === 'text/event-stream') {
          setTimeout(() => {
            if (!this.resolved) {
              this.resolved = true;
              this.callback({
                status: this.statusCode,
                headers: this.headers,
                text: '',
                body: null
              });
            }
          }, 10);
        }
        return this;
      };

      this.getHeader = (name) => {
        return this.headers[name.toLowerCase()];
      };

      this.removeHeader = (name) => {
        delete this.headers[name.toLowerCase()];
      };

      this.writeHead = (status, headers) => {
        this.statusCode = status;
        if (headers) {
          for (const key in headers) {
            this.setHeader(key, headers[key]);
          }
        }
        return this;
      };

      this.end = (chunk) => {
        if (this.resolved) return;
        this.resolved = true;
        if (chunk) {
          this.chunks.push(Buffer.from(chunk));
        }
        const bodyBuffer = Buffer.concat(this.chunks);
        const text = bodyBuffer.toString('utf8');
        let body = null;
        try {
          body = JSON.parse(text);
        } catch (e) {}
        this.callback({
          status: this.statusCode,
          headers: this.headers,
          text,
          body
        });
      };
    }
    _write(chunk, encoding, callback) {
      this.chunks.push(Buffer.from(chunk));
      callback();
    }
  }

  function inMemoryRequest(url, method, headers = {}, body = null) {
    return new Promise((resolve) => {
      const req = new MockReq(url, method, headers, body);
      const res = new MockRes(resolve);
      app(req, res);
    });
  }

  // 1. Static Assets Compilation & Syntax Check (via HTTP request)
  const resAppJs = await inMemoryRequest('/js/app.js', 'GET');
  assert.strictEqual(resAppJs.status, 200);
  assert.match(resAppJs.headers['content-type'], /javascript/);
  
  // Verify no unresolved imports/requires in browser JS
  const appJsContent = resAppJs.text;
  const hasRequire = appJsContent.includes('require(') && !appJsContent.includes('// require(');
  const hasESMImport = /^\s*import\s+[\s\S]*?from\s+['"].*?['"]/m.test(appJsContent);
  assert.strictEqual(hasRequire, false, 'Should not contain node require');
  assert.strictEqual(hasESMImport, false, 'Should not contain ESM imports');

  const resHtml = await inMemoryRequest('/', 'GET');
  assert.strictEqual(resHtml.status, 200);
  const requiredIds = [
    'connect-wallet-btn', 'one-click-sim-btn', 'price-chart-canvas',
    'metrics-live-price', 'metrics-total-fees', 'metrics-verified-count', 'metrics-pending-count',
    'settings-rpc-input', 'settings-contract-input', 'settings-fee-input', 'settings-save-btn',
    'api-method-select', 'api-path-input', 'api-send-btn', 'api-add-param-btn', 'api-params-container',
    'api-headers-preview', 'api-response-console', 'api-status-badge',
    'debugger-step-handshake', 'debugger-step-recovery', 'debugger-step-matching', 'debugger-step-confirmation', 'debugger-step-unlocked', 'debugger-message',
    'ledger-search-input', 'ledger-status-filter', 'ledger-sort-timestamp', 'ledger-sort-amount', 'ledger-table-body',
    'ledger-export-json', 'ledger-export-csv', 'ledger-prev-btn', 'ledger-page-info', 'ledger-next-btn',
    'sse-status-dot', 'sse-status-text', 'sse-reconnect-btn', 'sse-clear-btn', 'sse-autoscroll-chk', 'sse-log-console'
  ];
  const missingIds = requiredIds.filter(id => !resHtml.text.includes(`id="${id}"`));
  assert.strictEqual(missingIds.length, 0, `Missing DOM IDs: ${missingIds.join(', ')}`);
  console.log('   [CHALLENGER] Static assets validation: PASS');

  // 2. Empirical Verification of Endpoints
  // Test GET /api/payments
  const resPayments = await inMemoryRequest('/api/payments', 'GET');
  assert.strictEqual(resPayments.status, 200);
  assert.ok(resPayments.body.total >= 2);
  assert.ok(Array.isArray(resPayments.body.payments));

  // Test GET /api/gated-data (Handshake)
  const resHandshake = await inMemoryRequest('/api/gated-data', 'GET');
  assert.strictEqual(resHandshake.status, 402);
  assert.strictEqual(resHandshake.body.error, 'Payment Required');
  assert.ok(resHandshake.body.payment_id);
  const handshakePaymentId = resHandshake.body.payment_id;
  const gatedFee = resHandshake.body.fee;
  const gatedRecipient = resHandshake.body.recipient;

  // Verify in ledger as pending
  const resSearch = await inMemoryRequest(`/api/payments?search=${handshakePaymentId}`, 'GET');
  assert.strictEqual(resSearch.status, 200);
  const record = resSearch.body.payments.find(p => p.payment_id === handshakePaymentId);
  assert.ok(record);
  assert.strictEqual(record.status, 'pending');

  // Sign and submit payment
  const newHandshakeRes = await inMemoryRequest('/api/gated-data', 'GET');
  const verifiedPaymentId = newHandshakeRes.body.payment_id;
  
  const clientWallet = ethers.Wallet.createRandom();
  const verifiedSignature = await clientWallet.signMessage(verifiedPaymentId);

  const mockTxHash = '0x' + Array.from({length: 64}, () => Math.floor(Math.random()*16).toString(16)).join('');
  
  // Client POST cannot elevate to verified — only pending metadata is accepted
  const postRes = await inMemoryRequest('/api/payments', 'POST', { 'Content-Type': 'application/json' }, {
    payment_id: verifiedPaymentId,
    status: 'verified',
    sender: clientWallet.address,
    recipient: gatedRecipient || '0x70997970C51812dc3A010C7d01b50e0d17dc79C8',
    amount: gatedFee || '0.10',
    currency: 'USD',
    txHash: mockTxHash,
    signature: verifiedSignature
  });
  assert.strictEqual(postRes.status, 200);
  assert.strictEqual(postRes.body.status, 'pending', 'Client must not be able to set status=verified via POST');

  // Call /api/gated-data with credentials — server-side verification sets verified
  const getGatedRes = await inMemoryRequest('/api/gated-data', 'GET', {
    'Payment-Id': verifiedPaymentId,
    'Payment-Sender': clientWallet.address,
    'Payment-Signature': verifiedSignature
  });
  assert.strictEqual(getGatedRes.status, 200);
  assert.strictEqual(getGatedRes.body.status, 'success');
  assert.strictEqual(getGatedRes.body.payment.status, 'verified');

  // Test SSE Headers
  const resSSE = await inMemoryRequest('/api/events', 'GET');
  assert.strictEqual(resSSE.status, 200);
  assert.strictEqual(resSSE.headers['content-type'], 'text/event-stream');
  assert.strictEqual(resSSE.headers['cache-control'], 'no-cache');
  assert.strictEqual(resSSE.headers['connection'], 'keep-alive');
  console.log('   [CHALLENGER] Empirical endpoints verification: PASS');

  // 3. Stress Tests
  // A. Concurrent handshakes
  console.log('   [CHALLENGER] Sending 200 concurrent handshake requests...');
  const promisesHandshake = [];
  for (let i = 0; i < 200; i++) {
    promisesHandshake.push(inMemoryRequest('/api/gated-data', 'GET'));
  }
  const resultsHandshake = await Promise.all(promisesHandshake);
  const successHandshake = resultsHandshake.filter(r => r.status === 402 && r.body.payment_id).length;
  assert.strictEqual(successHandshake, 200);

  // B. Concurrent resource access
  console.log('   [CHALLENGER] Sending 100 concurrent resource access requests...');
  const promisesAccess = [];
  for (let i = 0; i < 100; i++) {
    promisesAccess.push(
      inMemoryRequest('/api/gated-data', 'GET', {
        'Payment-Id': verifiedPaymentId,
        'Payment-Sender': clientWallet.address,
        'Payment-Signature': verifiedSignature
      })
    );
  }
  const resultsAccess = await Promise.all(promisesAccess);
  const successAccess = resultsAccess.filter(r => r.status === 200 && r.body.status === 'success').length;
  assert.strictEqual(successAccess, 100);
  console.log('   [CHALLENGER] Concurrent requests stress test: PASS');

  // C. Malformed/incomplete parameters
  const resErr1 = await inMemoryRequest('/api/payments', 'POST', { 'Content-Type': 'application/json' }, { status: 'verified' });
  assert.strictEqual(resErr1.status, 400);

  // GET /api/payments with QPP (query parameters as object)
  const resErr3 = await inMemoryRequest('/api/payments?status[foo]=bar&sender[a]=b&limit[x]=y', 'GET');
  assert.strictEqual(resErr3.status, 200);
  assert.ok(Array.isArray(resErr3.body.payments));

  const resErr4 = await inMemoryRequest('/api/gated-data', 'GET', {
    'Payment-Id': 'non-existent-uuid',
    'Payment-Sender': 'invalid-eth-address',
    'Payment-Signature': 'invalid-signature-value'
  });
  assert.strictEqual(resErr4.status, 400);
  console.log('   [CHALLENGER] Malformed parameters stress test: PASS');

  // D. Massive headers or query strings
  console.log('   [CHALLENGER] Testing massive header and query stability...');
  const massiveQuery = 'x'.repeat(50 * 1024);
  const resMass1 = await inMemoryRequest(`/api/payments?search=${massiveQuery}`, 'GET');
  assert.ok(resMass1.status === 200 || resMass1.status === 414 || resMass1.status === 400);

  const massiveHeader = 'y'.repeat(10 * 1024);
  const resMass2 = await inMemoryRequest('/api/gated-data', 'GET', {
    'Payment-Id': handshakePaymentId || 'a3b04c8f-2879-4d8e-9d22-132d7b5f6390',
    'Payment-Sender': '0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266',
    'Payment-Signature': massiveHeader
  });
  assert.ok(resMass2.status === 400 || resMass2.status === 431);
  console.log('   [CHALLENGER] Massive header/query stress test: PASS');
});
