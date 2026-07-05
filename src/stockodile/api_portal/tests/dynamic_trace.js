import { app, paymentsLedger } from '../server.js';
import { express } from '../libs.js';
import { ethers } from 'ethers';
import assert from 'assert';
import http from 'http';
import { Socket } from 'net';

class MockReq extends http.IncomingMessage {
  constructor(url, method, headers, body = null) {
    super(new Socket());
    this.url = url;
    this.method = method || 'GET';
    this.headers = {};
    for (const key in headers) {
      this.headers[key.toLowerCase()] = headers[key];
    }
    this.body = body;
    
    if (body) {
      const chunk = typeof body === 'string' ? body : JSON.stringify(body);
      this.headers['content-type'] = this.headers['content-type'] || 'application/json';
      this.headers['content-length'] = String(Buffer.byteLength(chunk));
      this.push(Buffer.from(chunk));
    }
    this.push(null);
  }
}

class MockRes extends http.ServerResponse {
  constructor(callback) {
    super(new http.IncomingMessage(new Socket()));
    this.chunks = [];
    this.callback = callback;

    this.write = (chunk, encoding, callback) => {
      this.chunks.push(Buffer.from(chunk));
      if (typeof encoding === 'function') {
        encoding();
      } else if (callback) {
        callback();
      }
      return true;
    };

    this.end = (chunk, encoding, callback) => {
      if (chunk && typeof chunk !== 'function') {
        this.chunks.push(Buffer.from(chunk));
      }
      
      let cb = callback;
      if (typeof encoding === 'function') {
        cb = encoding;
      } else if (typeof chunk === 'function') {
        cb = chunk;
      }

      const bodyBuffer = Buffer.concat(this.chunks);
      const text = bodyBuffer.toString('utf8');
      let body = null;
      try {
        body = JSON.parse(text);
      } catch (e) {}

      this.callback({
        status: this.statusCode,
        headers: this.getHeaders(),
        text,
        body
      });

      if (cb) cb();
      return this;
    };
  }
}

function request(url, method, headers = {}, body = null) {
  return new Promise((resolve) => {
    const req = new MockReq(url, method, headers, body);
    const res = new MockRes(resolve);
    app(req, res);
  });
}

async function runTrace() {
  console.log('--- START DYNAMIC TRACE AUDIT ---');

  // Test 1: Path Traversal Protection
  console.log('[TRACE 1] Testing Path Traversal on express.static...');
  const resTraversal1 = await request('/../package.json', 'GET');
  console.log(`  GET /../package.json -> Status: ${resTraversal1.status}`);
  assert.strictEqual(resTraversal1.status, 403, 'Should return 403 Forbidden for traversal path');

  const resTraversal2 = await request('/css/../../package.json', 'GET');
  console.log(`  GET /css/../../package.json -> Status: ${resTraversal2.status}`);
  assert.strictEqual(resTraversal2.status, 403, 'Should return 403 Forbidden for traversal path');

  const resNormalStatic = await request('/css/style.css', 'GET');
  console.log(`  GET /css/style.css -> Status: ${resNormalStatic.status}`);
  assert.strictEqual(resNormalStatic.status, 200, 'Should allow valid static files');

  // Test 2: Gated API Bypass Prevention
  console.log('[TRACE 2] Testing Gated API Handshake (No credentials)...');
  const resGatedInit = await request('/api/gated-data', 'GET');
  console.log(`  GET /api/gated-data (No headers) -> Status: ${resGatedInit.status}`);
  assert.strictEqual(resGatedInit.status, 402, 'Should return 402 Payment Required');
  assert.ok(resGatedInit.body.payment_id, 'Should generate new payment_id');

  const paymentId = resGatedInit.body.payment_id;

  console.log('[TRACE 3] Testing Gated API Bypass with invalid payment_id...');
  const resBadId = await request('/api/gated-data', 'GET', {
    'Payment-Id': 'invalid-id-1234',
    'Payment-Sender': '0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266',
    'Payment-Signature': '0x' + '00'.repeat(65)
  });
  console.log(`  GET /api/gated-data (Bad ID) -> Status: ${resBadId.status}`);
  assert.strictEqual(resBadId.status, 400, 'Should block invalid payment ID with 400');

  // Deterministic wallets
  const wallet = new ethers.Wallet("0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80");
  const signature = await wallet.signMessage(paymentId);

  const mismatchedWallet = new ethers.Wallet("0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d");
  const mismatchedSignature = await mismatchedWallet.signMessage(paymentId);

  console.log('[TRACE 4] Testing Gated API Bypass with mismatched sender address...');
  const resMismatchedSender = await request('/api/gated-data', 'GET', {
    'Payment-Id': paymentId,
    'Payment-Sender': wallet.address, // Expected address
    'Payment-Signature': mismatchedSignature // Mismatched signature
  });
  console.log(`  GET /api/gated-data (Mismatched Sender) -> Status: ${resMismatchedSender.status}`);
  assert.strictEqual(resMismatchedSender.status, 400, 'Should block mismatched sender address with 400');
  assert.strictEqual(resMismatchedSender.body.error, 'Recovered signer matches mismatch');

  console.log('[TRACE 5] Testing valid signature and credentials...');
  const resValid = await request('/api/gated-data', 'GET', {
    'Payment-Id': paymentId,
    'Payment-Sender': wallet.address,
    'Payment-Signature': signature
  });
  console.log(`  GET /api/gated-data (Valid credentials) -> Status: ${resValid.status}`);
  assert.strictEqual(resValid.status, 200, 'Should successfully unlock premium content');

  // Test 3: Signature Replay and Reuse Prevention
  console.log('[TRACE 6] Testing Signature Replay on a DIFFERENT payment ID...');
  const resGatedInit2 = await request('/api/gated-data', 'GET');
  const paymentId2 = resGatedInit2.body.payment_id;

  const resReplayDiffId = await request('/api/gated-data', 'GET', {
    'Payment-Id': paymentId2,
    'Payment-Sender': wallet.address,
    'Payment-Signature': signature // Reusing signature of paymentId
  });
  console.log(`  GET /api/gated-data (Signature Replay / Diff ID) -> Status: ${resReplayDiffId.status}`);
  // This will fail signature verification since signature was made for paymentId, not paymentId2.
  assert.strictEqual(resReplayDiffId.status, 400, 'Should block signature replay on different ID');

  console.log('[TRACE 7] Testing Signature Reuse on the SAME payment ID...');
  const resReplaySameId = await request('/api/gated-data', 'GET', {
    'Payment-Id': paymentId,
    'Payment-Sender': wallet.address,
    'Payment-Signature': signature // Reusing same signature on same paymentId
  });
  console.log(`  GET /api/gated-data (Signature Reuse / Same ID) -> Status: ${resReplaySameId.status}`);
  assert.strictEqual(resReplaySameId.status, 200, 'Should allow re-accessing data for already verified payment ID');

  console.log('--- ALL TRACES VERIFIED SUCCESSFULLY ---');
}

runTrace().catch(err => {
  console.error('[TRACE FAILED]', err);
  process.exit(1);
});
