/**
 * Stockodile Adversarial Stress Test Suite
 * Run command: node tests/adversarial_stress.js
 */

import { app, paymentsLedger, broadcastSSE } from '../server.js';
import { ethers } from 'ethers';
import assert from 'assert';
import crypto from 'crypto';
import { Readable, Writable } from 'stream';
import { performance } from 'perf_hooks';

class MockRequest extends Readable {
  constructor(urlPath, method, headers, body) {
    super();
    this.url = urlPath;
    this.method = method || 'GET';
    this.headers = {};
    for (const key in headers) {
      this.headers[key.toLowerCase()] = headers[key];
    }
    this.body = body;
    this.socket = { destroy: () => {} };
  }
  
  _read() {
    if (this.body) {
      this.push(typeof this.body === 'string' ? this.body : JSON.stringify(this.body));
    }
    this.push(null);
  }
}

class MockResponse extends Writable {
  constructor(onComplete) {
    super();
    this.statusCode = 200;
    this.headers = {};
    this.chunks = [];
    this.onComplete = onComplete;
  }
  
  setHeader(name, value) {
    this.headers[name.toLowerCase()] = value;
    return this;
  }
  
  getHeader(name) {
    return this.headers[name.toLowerCase()];
  }
  
  writeHead(status, headers) {
    this.statusCode = status;
    if (headers) {
      for (const key in headers) {
        this.setHeader(key, headers[key]);
      }
    }
    return this;
  }
  
  _write(chunk, encoding, callback) {
    this.chunks.push(Buffer.from(chunk));
    callback();
  }
  
  end(chunk) {
    if (chunk) {
      this.chunks.push(Buffer.from(chunk));
    }
    const bodyBuffer = Buffer.concat(this.chunks);
    const text = bodyBuffer.toString('utf8');
    
    this.onComplete({
      status: this.statusCode,
      text,
      headers: this.headers
    });
  }
}

async function fetch(url, options = {}) {
  const parsedUrl = new URL(url, 'http://localhost');
  const path = parsedUrl.pathname + parsedUrl.search;
  
  return new Promise((resolve, reject) => {
    const req = new MockRequest(path, options.method, options.headers, options.body);
    const res = new MockResponse((response) => {
      resolve({
        status: response.status,
        headers: {
          get: (name) => response.headers[name.toLowerCase()] || null
        },
        text: async () => response.text,
        json: async () => JSON.parse(response.text)
      });
    });
    
    try {
      app(req, res);
    } catch (e) {
      reject(e);
    }
  });
}

async function runAdversarialTests() {
  console.log('================================================================');
  console.log('      STOCKODILE PORTAL ADVERSARIAL & STRESS TESTING SUITE      ');
  console.log('================================================================\n');

  let allTestsPassed = true;

  // Helper to log test outcomes
  function logTestResult(name, passed, details = '') {
    if (passed) {
      console.log(`[PASS] ${name}`);
    } else {
      console.log(`[FAIL] ${name} - ${details}`);
      allTestsPassed = false;
    }
  }

  // ----------------------------------------------------
  // TEST 1: Event Loop Blockage during Signature Verification
  // ----------------------------------------------------
  try {
    console.log('[STRESS] Running Test 1: Event loop blockage check...');
    const wallet = ethers.Wallet.createRandom();
    const paymentId = crypto.randomUUID();
    const sig = await wallet.signMessage(paymentId);

    // Track event loop delay
    let loopBlockedTime = 0;
    const intervalStart = performance.now();
    
    // Schedule a small delay that should run after verifyMessage if loop is free
    let timeoutFired = false;
    setTimeout(() => {
      timeoutFired = true;
      loopBlockedTime = performance.now() - intervalStart;
    }, 5);

    const start = performance.now();
    const iterations = 100;
    for (let i = 0; i < iterations; i++) {
      ethers.verifyMessage(paymentId, sig);
    }
    const end = performance.now();
    const elapsed = end - start;
    const avgTime = elapsed / iterations;

    // Wait for the timeout to fire
    await new Promise(resolve => setTimeout(resolve, 50));
    
    // Event loop blockage is essentially elapsed time since Timeout was set
    const actualDelay = loopBlockedTime - 5;
    
    console.log(` -> Verification speed: ${avgTime.toFixed(2)}ms per signature`);
    console.log(` -> Total verification CPU block: ${elapsed.toFixed(2)}ms for ${iterations} verifications`);
    console.log(` -> Measured event loop delay: ${actualDelay.toFixed(2)}ms`);
    
    // If the event loop delay is close to the elapsed verification time, it proves blockage
    assert.ok(actualDelay >= elapsed * 0.5, 'Event loop should be blocked during synchronous cryptography');
    logTestResult('Event Loop Blockage Test', true, `Synchronous crypto blocks event loop by ~${actualDelay.toFixed(2)}ms`);
  } catch (err) {
    logTestResult('Event Loop Blockage Test', false, err.message);
  }

  // ----------------------------------------------------
  // TEST 2: Gated API Authentication Bypass Vulnerability
  // ----------------------------------------------------
  try {
    console.log('[STRESS] Running Test 2: Authentication bypass vulnerability check...');
    
    // 1. Generate a random UUID and wallet that the server has NEVER seen
    const fakePaymentId = crypto.randomUUID();
    const fakeWallet = ethers.Wallet.createRandom();
    const fakeSig = await fakeWallet.signMessage(fakePaymentId);
    
    // Confirm it is not in the database first
    const initialCheck = paymentsLedger.find(p => p.payment_id === fakePaymentId);
    assert.strictEqual(initialCheck, undefined, 'Fake payment ID should not exist in ledger');

    // 2. Call gated-data directly with these credentials
    const res = await fetch('http://localhost/api/gated-data', {
      headers: {
        'Payment-Id': fakePaymentId,
        'Payment-Sender': fakeWallet.address,
        'Payment-Signature': fakeSig
      }
    });

    // 3. The server should ideally reject this because it never authorized this payment ID!
    // But since it just recovers signature on the spot and creates a verified record if not found, it passes!
    const body = await res.json();
    const inLedgerNow = paymentsLedger.find(p => p.payment_id === fakePaymentId);

    if (res.status === 200 && inLedgerNow && inLedgerNow.status === 'verified') {
      logTestResult('Authentication Bypass Vulnerability Check', false, 
        `CRITICAL SECURITY VULNERABILITY CONFIRMED: Gated data unlocked for self-generated payment ID! Record added to database as verified.`
      );
    } else {
      logTestResult('Authentication Bypass Vulnerability Check', true, `Access rejected or not registered.`);
    }
  } catch (err) {
    logTestResult('Authentication Bypass Vulnerability Check (Error)', false, err.message);
  }

  // ----------------------------------------------------
  // TEST 3: Query Parameter Pollution and Type Abuse (QPP)
  // ----------------------------------------------------
  try {
    console.log('[STRESS] Running Test 3: Query Parameter Type Abuse (QPP)...');
    
    // Pass status, limit, offset as array/objects to /api/payments
    const res = await fetch('http://localhost/api/payments?limit[]=10&offset[]=0&status[]=verified&sender[]=0x123');
    assert.strictEqual(res.status, 200);
    const body = await res.json();
    assert.ok(Array.isArray(body.payments), 'Payments array should be returned');
    
    // Test prototype pollution attempt through query string
    const res2 = await fetch('http://localhost/api/payments?__proto__[polluted]=true');
    assert.strictEqual(res2.status, 200);
    assert.strictEqual({}.polluted, undefined, 'Prototype must not be polluted');

    logTestResult('Query Parameter type abuse & pollution check', true);
  } catch (err) {
    logTestResult('Query Parameter type abuse & pollution check', false, err.message);
  }

  // ----------------------------------------------------
  // TEST 4: High Concurrency Payments Ledger Registration
  // ----------------------------------------------------
  try {
    console.log('[STRESS] Running Test 4: Concurrency and ledger data consistency...');
    
    const initialLedgerSize = paymentsLedger.length;
    const concurrencyCount = 500;
    const promises = [];

    // Register 500 payments concurrently
    for (let i = 0; i < concurrencyCount; i++) {
      const pId = `concurrent-pid-${i}`;
      promises.push(
        fetch('http://localhost/api/payments', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: {
            payment_id: pId,
            status: 'pending',
            amount: '0.10'
          }
        })
      );
    }

    const results = await Promise.all(promises);
    const successCount = results.filter(r => r.status === 200).length;
    
    assert.strictEqual(successCount, concurrencyCount, 'All concurrent registrations should succeed');
    
    // Check ledger size
    const newLedgerSize = paymentsLedger.length;
    assert.strictEqual(newLedgerSize, initialLedgerSize + concurrencyCount, 'Ledger size should increase by exactly 500');
    
    // Attempt concurrent updates to the SAME payment ID to check for duplication issues
    const updatePromises = [];
    const sharedPid = 'shared-concurrent-pid';
    
    // Initialize shared record
    await fetch('http://localhost/api/payments', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: { payment_id: sharedPid, status: 'pending' }
    });

    for (let i = 0; i < 200; i++) {
      updatePromises.push(
        fetch('http://localhost/api/payments', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: { payment_id: sharedPid, status: 'verified', amount: `${i}.00` }
        })
      );
    }

    await Promise.all(updatePromises);
    
    const finalMatches = paymentsLedger.filter(p => p.payment_id === sharedPid);
    assert.strictEqual(finalMatches.length, 1, 'Should never create duplicate ledger rows for same payment ID');
    
    logTestResult('Ledger Concurrency & Consistency Test', true, 'Ledger remained consistent with no duplicates.');
  } catch (err) {
    logTestResult('Ledger Concurrency & Consistency Test', false, err.message);
  }

  // ----------------------------------------------------
  // TEST 5: SSE Event Stream Response Lifecycle & Client Tracking
  // ----------------------------------------------------
  try {
    console.log('[STRESS] Running Test 5: SSE Client Connection Lifecycle...');
    
    // Check initial SSE pool size
    const req = new MockRequest('/api/events', 'GET', { 'Accept': 'text/event-stream' });
    let sseHeaderReceived = false;
    
    const res = new MockResponse((response) => {});
    res.setHeader = (name, value) => {
      if (name.toLowerCase() === 'content-type' && value.includes('event-stream')) {
        sseHeaderReceived = true;
      }
    };
    
    // Trigger handler
    app(req, res);
    
    // Wait for connection to be registered
    await new Promise(resolve => setTimeout(resolve, 10));
    assert.ok(sseHeaderReceived, 'SSE connection must initialize correctly');
    
    // Close connection
    req.emit('close');
    
    logTestResult('SSE Client Connection Lifecycle Test', true);
  } catch (err) {
    logTestResult('SSE Client Connection Lifecycle Test', false, err.message);
  }

  console.log('\n================================================================');
  console.log(`Adversarial verification run finished. Verdict: ${allTestsPassed ? 'SUCCESS' : 'WARNING/FAIL'}`);
  console.log('================================================================');
}

runAdversarialTests().catch(err => {
  console.error('Fatal crash during stress testing:', err);
  process.exit(1);
});
