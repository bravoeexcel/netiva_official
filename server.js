/**
 * Netiva Store — server.js
 * Backend for Zeabur PostgreSQL deployment.
 *
 * Environment variables (set in Zeabur dashboard):
 *   DATABASE_URL  — PostgreSQL connection string provided by Zeabur
 *                   e.g. postgres://user:pass@host:5432/dbname
 */

require('dotenv').config();
const express  = require('express');
const { Pool } = require('pg');
const cors     = require('cors');
const multer   = require('multer');
const path     = require('path');
const fs       = require('fs');

const app  = express();
const port = process.env.PORT || 3000;

// ── Resolve frontend directory ────────────────────────────────────────────────
// Zeabur copies repo files to /src. We support two layouts:
//   1. index.html lives in  <repo>/public/index.html  → served from public/
//   2. index.html lives in  <repo>/index.html          → served from root
const ROOT = __dirname; // /src inside Zeabur container
const PUBLIC_DIR = fs.existsSync(path.join(ROOT, 'public', 'index.html'))
  ? path.join(ROOT, 'public')
  : ROOT;

console.log('Serving frontend from:', PUBLIC_DIR);

// ── Database ─────────────────────────────────────────────────────────────────
// Zeabur PostgreSQL runs on an internal private network — no SSL needed.
// Only enable SSL if DATABASE_URL explicitly contains "sslmode=require".
function buildSSL(url) {
  if (!url) return false;
  if (url.includes('sslmode=require')) return { rejectUnauthorized: false };
  return false;
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: buildSSL(process.env.DATABASE_URL)
});

async function initDB() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS netiva_store (
      doc_id     TEXT PRIMARY KEY,
      data       JSONB NOT NULL DEFAULT '{}',
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS netiva_images (
      id         TEXT PRIMARY KEY,
      bucket     TEXT NOT NULL DEFAULT 'netiva',
      path       TEXT NOT NULL,
      mime_type  TEXT NOT NULL DEFAULT 'image/jpeg',
      data       TEXT NOT NULL,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      UNIQUE(bucket, path)
    );
  `);
  console.log('Database tables ready.');
}

// ── Middleware ────────────────────────────────────────────────────────────────
app.use(cors());
app.use(express.json({ limit: '10mb' }));
app.use(express.static(PUBLIC_DIR));

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 8 * 1024 * 1024 }
});

// ── SSE (real-time updates) ───────────────────────────────────────────────────
const sseClients = new Map();

function notifyClients(docId) {
  const clients = sseClients.get(docId);
  if (!clients) return;
  for (const res of clients) {
    try { res.write(`data: ${JSON.stringify({ docId })}\n\n`); } catch (_) {}
  }
}

app.get('/api/events/:docId', (req, res) => {
  const { docId } = req.params;
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  if (!sseClients.has(docId)) sseClients.set(docId, new Set());
  sseClients.get(docId).add(res);

  const hb = setInterval(() => {
    try { res.write(': ping\n\n'); } catch (_) { clearInterval(hb); }
  }, 30000);

  req.on('close', () => {
    clearInterval(hb);
    sseClients.get(docId)?.delete(res);
  });
});

// ── API: Get document ─────────────────────────────────────────────────────────
app.get('/api/doc/:docId', async (req, res) => {
  try {
    const { rows } = await pool.query(
      'SELECT data FROM netiva_store WHERE doc_id = $1',
      [req.params.docId]
    );
    if (rows.length === 0) return res.json({ exists: false, data: null });
    res.json({ exists: true, data: rows[0].data });
  } catch (err) {
    console.error('GET /api/doc error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── API: Upsert document ──────────────────────────────────────────────────────
app.post('/api/doc/:docId', async (req, res) => {
  const { docId } = req.params;
  const { data, merge } = req.body;

  try {
    let toWrite = data;

    if (merge) {
      const { rows } = await pool.query(
        'SELECT data FROM netiva_store WHERE doc_id = $1',
        [docId]
      );
      if (rows.length > 0) toWrite = deepMerge(rows[0].data, data);
    }

    await pool.query(`
      INSERT INTO netiva_store (doc_id, data, updated_at)
      VALUES ($1, $2, NOW())
      ON CONFLICT (doc_id) DO UPDATE
        SET data = $2, updated_at = NOW()
    `, [docId, JSON.stringify(toWrite)]);

    notifyClients(docId);
    res.json({ ok: true });
  } catch (err) {
    console.error('POST /api/doc error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── API: Upload image ─────────────────────────────────────────────────────────
app.post('/api/storage/upload', upload.single('file'), async (req, res) => {
  try {
    const bucket   = req.body.bucket || req.query.bucket || 'netiva';
    const filePath = req.body.path   || req.query.path   || `uploads/${Date.now()}`;
    const mime     = req.file.mimetype || 'image/jpeg';
    const b64      = req.file.buffer.toString('base64');
    const id       = `${bucket}__${filePath.replace(/[^a-zA-Z0-9._-]/g, '_')}`;

    await pool.query(`
      INSERT INTO netiva_images (id, bucket, path, mime_type, data, created_at)
      VALUES ($1, $2, $3, $4, $5, NOW())
      ON CONFLICT (bucket, path) DO UPDATE
        SET data = $5, mime_type = $4, created_at = NOW()
    `, [id, bucket, filePath, mime, b64]);

    const publicUrl = `/api/storage/object/${bucket}/${filePath}`;
    res.json({ ok: true, path: filePath, publicUrl });
  } catch (err) {
    console.error('POST /api/storage/upload error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ── API: Serve stored image ───────────────────────────────────────────────────
app.get('/api/storage/object/:bucket/*', async (req, res) => {
  try {
    const bucket   = req.params.bucket;
    const filePath = req.params[0];
    const { rows } = await pool.query(
      'SELECT data, mime_type FROM netiva_images WHERE bucket = $1 AND path = $2',
      [bucket, filePath]
    );
    if (rows.length === 0) return res.status(404).send('Not found');
    const buf = Buffer.from(rows[0].data, 'base64');
    res.setHeader('Content-Type', rows[0].mime_type);
    res.setHeader('Cache-Control', 'public, max-age=31536000');
    res.send(buf);
  } catch (err) {
    console.error('GET /api/storage/object error:', err);
    res.status(500).send('Server error');
  }
});

// ── Catch-all: serve index.html for SPA routes ────────────────────────────────
app.get('*', (req, res) => {
  const indexPath = path.join(PUBLIC_DIR, 'index.html');
  if (fs.existsSync(indexPath)) {
    res.sendFile(indexPath);
  } else {
    res.status(404).send('index.html not found. Make sure it is in the repo root or public/ folder.');
  }
});

// ── Utilities ─────────────────────────────────────────────────────────────────
function deepMerge(target, source) {
  if (!target || typeof target !== 'object') return source;
  const out = { ...target };
  for (const k of Object.keys(source)) {
    if (source[k] && typeof source[k] === 'object' && !Array.isArray(source[k])) {
      out[k] = deepMerge(target[k], source[k]);
    } else {
      out[k] = source[k];
    }
  }
  return out;
}

// ── Start ─────────────────────────────────────────────────────────────────────
initDB()
  .then(() => {
    app.listen(port, () => {
      console.log(`Netiva server running on port ${port}`);
    });
  })
  .catch(err => {
    console.error('Failed to initialize DB:', err);
    process.exit(1);
  });
