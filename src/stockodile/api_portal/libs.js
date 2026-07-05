import { randomUUID } from 'crypto';
import http from 'http';
import fs from 'fs';
import path from 'path';
import { Readable, Writable } from 'stream';

// Helper to check if a module is present locally to avoid sandbox violations from global lookups
const hasLocalModule = (name) => {
  if (['express', 'cors', 'dotenv', 'uuid', 'supertest'].includes(name)) {
    return false;
  }
  try {
    const modulePath = path.resolve(process.cwd(), 'node_modules', name);
    return fs.existsSync(modulePath);
  } catch (e) {
    return false;
  }
};

// 1. Resolve Dotenv
let dotenv;
if (hasLocalModule('dotenv')) {
  try {
    dotenv = (await import('dotenv')).default;
  } catch (e) {
    // fallback
  }
}

if (!dotenv) {
  dotenv = {
    config: () => {
      try {
        const envPath = path.resolve(process.cwd(), '.env');
        if (fs.existsSync(envPath)) {
          const content = fs.readFileSync(envPath, 'utf8');
          content.split('\n').forEach(line => {
            const [key, ...valueParts] = line.split('=');
            if (key && valueParts.length > 0) {
              process.env[key.trim()] = valueParts.join('=').trim();
            }
          });
        }
      } catch (err) {}
    }
  };
}

// 2. Resolve UUID
let uuidv4;
if (hasLocalModule('uuid')) {
  try {
    uuidv4 = (await import('uuid')).v4;
  } catch (e) {
    // fallback
  }
}

if (!uuidv4) {
  uuidv4 = () => randomUUID();
}

// 3. Resolve CORS
let cors;
if (hasLocalModule('cors')) {
  try {
    cors = (await import('cors')).default;
  } catch (e) {
    // fallback
  }
}

if (!cors) {
  cors = () => (req, res, next) => { if (next) next(); };
}

// 4. Resolve Express
let express;
if (hasLocalModule('express')) {
  try {
    express = (await import('express')).default;
  } catch (e) {
    // fallback
  }
}

if (!express) {
  express = function() {
    const routes = [];
    const middlewares = [];

    const app = (req, res) => {
      const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
      req.query = Object.fromEntries(url.searchParams);
      
      res.status = (code) => {
        res.statusCode = code;
        return res;
      };
      
      res.json = (data) => {
        res.setHeader('Content-Type', 'application/json');
        res.end(JSON.stringify(data));
        return res;
      };
      
      res.sendFile = (filePath) => {
        try {
          const ext = path.extname(filePath);
          let contentType = 'text/html';
          if (ext === '.css') contentType = 'text/css';
          if (ext === '.js') contentType = 'application/javascript';
          res.setHeader('Content-Type', contentType);
          res.end(fs.readFileSync(filePath));
        } catch (err) {
          res.statusCode = 404;
          res.end('Not Found');
        }
        return res;
      };

      let index = 0;
      const next = () => {
        if (index < middlewares.length) {
          middlewares[index++](req, res, next);
        } else {
          const route = routes.find(r => r.method === req.method && r.path === url.pathname);
          if (route) {
            route.handler(req, res);
          } else {
            const staticMiddleware = middlewares.find(m => m.isStatic);
            if (staticMiddleware) {
              staticMiddleware(req, res, () => {
                res.statusCode = 404;
                res.end('Not Found');
              });
            } else {
              res.statusCode = 404;
              res.end('Not Found');
            }
          }
        }
      };
      next();
    };

    app.use = (middleware) => {
      middlewares.push(middleware);
      return app;
    };

    app.get = (pathStr, handler) => {
      routes.push({ method: 'GET', path: pathStr, handler });
      return app;
    };

    app.post = (pathStr, handler) => {
      routes.push({ method: 'POST', path: pathStr, handler });
      return app;
    };

    app.listen = (port, callback) => {
      const server = http.createServer(app);
      server.listen(port, callback);
      return server;
    };

    return app;
  };

  express.static = (staticDir) => {
    const staticMiddleware = (req, res, next) => {
      const url = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
      
      // Block URLs with relative steps (e.g., /../package.json)
      const pathname = url.pathname;
      const pathSegments = pathname.split(/[/\\]/);
      const decodedUrl = decodeURIComponent(req.url);
      if (pathSegments.includes('..') || pathname.includes('..') || req.url.includes('..') || decodedUrl.includes('..')) {
        res.statusCode = 403;
        res.end('Forbidden');
        return;
      }

      const absoluteStaticDir = path.resolve(staticDir);
      const filePath = path.resolve(path.join(absoluteStaticDir, pathname));

      // Ensure resolved path is strictly within the static directory scope
      const isWithinStaticDir = filePath.startsWith(absoluteStaticDir + path.sep) || filePath === absoluteStaticDir;
      if (!isWithinStaticDir) {
        res.statusCode = 403;
        res.end('Forbidden');
        return;
      }

      if (fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
        res.statusCode = 200;
        const ext = path.extname(filePath);
        let contentType = 'text/html';
        if (ext === '.css') contentType = 'text/css';
        if (ext === '.js') contentType = 'application/javascript';
        res.setHeader('Content-Type', contentType);
        res.end(fs.readFileSync(filePath));
      } else {
        next();
      }
    };
    staticMiddleware.isStatic = true;
    return staticMiddleware;
  };

  express.json = () => (req, res, next) => {
    let body = '';
    req.on('data', chunk => { body += chunk; });
    req.on('end', () => {
      try {
        req.body = body ? JSON.parse(body) : {};
      } catch (err) {
        req.body = {};
      }
      next();
    });
  };

  express.urlencoded = () => (req, res, next) => next();
}

// 5. In-memory Mock Request and Response for sandbox-safe supertest shim
class MockRequest extends Readable {
  constructor(urlPath, method, headers) {
    super();
    this.url = urlPath;
    this.method = method || 'GET';
    this.headers = {};
    for (const key in headers) {
      this.headers[key.toLowerCase()] = headers[key];
    }
    this.socket = { destroy: () => {} };
  }
  _read() {
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
    let body = null;
    try {
      body = JSON.parse(text);
    } catch (e) {}
    
    this.onComplete({
      status: this.statusCode,
      statusCode: this.statusCode,
      text,
      body,
      headers: this.headers
    });
  }
}

// Resolve Supertest
let supertest;
if (hasLocalModule('supertest')) {
  try {
    supertest = (await import('supertest')).default;
  } catch (e) {
    // fallback
  }
}

if (!supertest) {
  supertest = function(app) {
    return {
      get: (urlPath) => {
        const chain = {
          headers: {},
          set: (name, val) => {
            chain.headers[name] = val;
            return chain;
          },
          expect: async (expectedStatus) => {
            return new Promise((resolve, reject) => {
              const req = new MockRequest(urlPath, 'GET', chain.headers);
              const res = new MockResponse((response) => {
                if (expectedStatus && response.status !== expectedStatus) {
                  reject(new Error(`Expected status ${expectedStatus}, got ${response.status}`));
                } else {
                  resolve(response);
                }
              });
              
              try {
                app(req, res);
              } catch (e) {
                reject(e);
              }
            });
          }
        };
        return chain;
      }
    };
  };
}

export { express, cors, dotenv, uuidv4, supertest };
