// ─────────────────────────────────────────────────────────────────────────────
// ventas-service — Microservicio Leoncito
// Extraído de sheets-api (patrón strangler-fig). Maneja VENTAS y MARGEN DE VENTAS:
// carga de Excel → Postgres/Neon (misma BD y mismas tablas que usaba el monolito).
// Endpoints:
//   POST /ventas/import           (multipart "archivo")   → upsert por CodigoCV
//   GET  /ventas/estado                                    → total + última carga
//   GET  /ventas?anio=&mes=&sede=                          → filas para consumidores
//   POST /margen-ventas/import    (multipart "archivo")   → reemplazo por CodigoCV
//   GET  /margen-ventas/estado
//   GET  /margen-ventas?anio=&mes=&sede=
//   GET  /health
// ─────────────────────────────────────────────────────────────────────────────
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const compression = require('compression');
const multer = require('multer');
const XLSX = require('xlsx');
const { Pool } = require('pg');

const app = express();
const PORT = process.env.PORT || 4003;

app.use(compression());   // gzip: comprime las respuestas JSON (5-10× menos bytes)
app.use(cors());
app.use(express.json({ limit: '20mb' }));

// 🐘 PostgreSQL (Neon) — misma cadena que el monolito (variable DATABASE_URL).
const pgPool = process.env.DATABASE_URL
  ? new Pool({ connectionString: process.env.DATABASE_URL, ssl: { rejectUnauthorized: false } })
  : null;

// 🗃️ Caché en memoria de queries de LECTURA pesadas (p. ej. /ventas trae ~16k filas
// del año, iguales para todos los usuarios). La 1ª petición va a Postgres/Supabase y
// las siguientes dentro del TTL se sirven de memoria → MENOS EGRESS de Supabase y
// respuestas más rápidas. Se invalida en cada escritura (import/consolidar/cruzar/PUT/
// metas). Bypass puntual con ?fresh=1. TTL por env (def. 60s).
const DB_CACHE_TTL_MS = parseInt(process.env.DB_CACHE_TTL_MS || '60000', 10);
const queryCache = new Map(); // key -> { ts, data }
function cacheGet(key) {
  const hit = queryCache.get(key);
  if (hit && (Date.now() - hit.ts) < DB_CACHE_TTL_MS) return hit.data;
  if (hit) queryCache.delete(key);   // expiró → libera memoria
  return null;
}
function cacheSet(key, data) {
  // Tope de entradas para no presionar la RAM del contenedor (Render). Map conserva
  // orden de inserción → borra la más antigua.
  if (queryCache.size >= 60) { const oldest = queryCache.keys().next().value; queryCache.delete(oldest); }
  queryCache.set(key, { ts: Date.now(), data });
  return data;
}
function cacheClear() { queryCache.clear(); }
// Sirve `key` desde caché o ejecuta `run()` (async) y lo cachea. `fresh` fuerza refresco.
async function cached(key, fresh, run) {
  if (!fresh) { const hit = cacheGet(key); if (hit !== null) return hit; }
  return cacheSet(key, await run());
}
// Invalida TODA la caché de lectura tras cualquier escritura exitosa (import/consolidar/
// cruzar/PUT/metas). Así una edición se refleja al instante, sin esperar el TTL. Simple y
// seguro: en el peor caso la siguiente lectura re-consulta la base.
app.use((req, res, next) => {
  if (req.method === 'GET' || req.method === 'HEAD' || req.method === 'OPTIONS') return next();
  res.on('finish', () => { if (res.statusCode < 400) cacheClear(); });
  next();
});

// ⚡ Índices de expresión sobre el DNI normalizado. Los cruces de derivación (LATERAL
// con regexp_replace en /modulo, /atribucion y el GET genérico) sin índice tardaban ~40s;
// con ellos bajan a ~1s. Idempotente (IF NOT EXISTS) y tolerante a fallos (las tablas
// gestion_* las crea gestion-service; si aún no existen, se reintenta en el próximo boot).
async function ensurePerfIndexes() {
  if (!pgPool) return;
  const idx = [
    [`ix_gr_dni_norm`, `gestion_realzza`, `((regexp_replace(dni_cliente,'\\D','','g')))`],
    [`ix_gc_dni_norm`, `gestion_call`,    `((regexp_replace(dni_cliente,'\\D','','g')))`],
    [`ix_ventas_docid_norm`, `ventas`,     `((regexp_replace(doc_identidad,'\\D','','g')))`],
  ];
  for (const [name, tabla, expr] of idx) {
    try { await pgPool.query(`CREATE INDEX IF NOT EXISTS ${name} ON ${tabla} ${expr}`); }
    catch (e) { console.error(`⚠️ ensurePerfIndexes ${name}:`, e.message); }
  }
}
ensurePerfIndexes();

// 📤 Subida de archivos en memoria (Excel). Límite 200 MB.
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 200 * 1024 * 1024 } });

// ── Helpers de parseo (tolera ñ/acentos y cabeceras alternativas) ────────────
function pickCol(row, ...keys) {
  for (const k of keys) {
    const v = row[k];
    if (v !== undefined && v !== null && String(v).trim() !== '') return v;
  }
  return null;
}
function toInt(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = parseInt(String(v).replace(/[^0-9-]/g, ''), 10);
  return Number.isFinite(n) ? n : null;
}
function toNum(v) {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(String(v).replace(/,/g, '').replace(/[^0-9.-]/g, ''));
  return Number.isFinite(n) ? n : null;
}
function toStr(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).trim();
  return s === '' ? null : s;
}
// Normaliza Fecha a 'YYYY-MM-DD' (Date, serial Excel, dd/mm/yyyy o ISO).
function toFechaISO(v) {
  if (v === null || v === undefined || v === '') return null;
  if (v instanceof Date && !isNaN(v)) {
    return `${v.getFullYear()}-${String(v.getMonth() + 1).padStart(2, '0')}-${String(v.getDate()).padStart(2, '0')}`;
  }
  if (typeof v === 'number') {
    const d = new Date(Math.round((v - 25569) * 86400 * 1000));
    if (!isNaN(d)) return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
    return null;
  }
  const s = String(v).trim();
  let m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (m) return `${m[3]}-${String(m[2]).padStart(2, '0')}-${String(m[1]).padStart(2, '0')}`;
  m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})/);
  if (m) return `${m[1]}-${String(m[2]).padStart(2, '0')}-${String(m[3]).padStart(2, '0')}`;
  const d = new Date(s);
  if (!isNaN(d)) return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
  return null;
}

// ─────────────────────────────────────────────────────────────────────────────
// 💰 VENTAS (upsert por codigo_cv)
// ─────────────────────────────────────────────────────────────────────────────
const VENTAS_COLS = [
  'codigo_cv', 'dia_cv', 'mes_cv', 'anio_cv', 'cliente_venta', 'sede',
  'monto_consolidado', 'cuota_inicial', 'productos', 'cuotas', 'doc_identidad',
  'estado_venta', 'entidad', 'vendedor', 'tipo_credito', 'estado_tipo_producto',
  'tipo_cliente', 'dia_af', 'mes_af', 'anio_af',
];

let ventasSchemaLista = false;
async function ensureVentasSchema() {
  if (!pgPool || ventasSchemaLista) return;
  await pgPool.query(`
    CREATE TABLE IF NOT EXISTS ventas (
      codigo_cv            BIGINT       PRIMARY KEY,
      dia_cv               SMALLINT,
      mes_cv               SMALLINT,
      anio_cv              SMALLINT,
      cliente_venta        TEXT,
      sede                 TEXT,
      monto_consolidado    NUMERIC(14,2),
      cuota_inicial        NUMERIC(14,2),
      productos            TEXT,
      cuotas               INTEGER,
      doc_identidad        TEXT,
      estado_venta         TEXT,
      entidad              TEXT,
      vendedor             TEXT,
      tipo_credito         TEXT,
      estado_tipo_producto TEXT,
      tipo_cliente         TEXT,
      dia_af               SMALLINT,
      mes_af               SMALLINT,
      anio_af              SMALLINT,
      fecha_cv  DATE GENERATED ALWAYS AS (
                  make_date(NULLIF(anio_cv,0), NULLIF(mes_cv,0), NULLIF(dia_cv,0))) STORED,
      fecha_af  DATE GENERATED ALWAYS AS (
                  make_date(NULLIF(anio_af,0), NULLIF(mes_af,0), NULLIF(dia_af,0))) STORED,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS ix_ventas_anio_mes ON ventas (anio_cv, mes_cv);
    CREATE INDEX IF NOT EXISTS ix_ventas_sede     ON ventas (sede);
    CREATE INDEX IF NOT EXISTS ix_ventas_fecha_cv ON ventas (fecha_cv);
    CREATE TABLE IF NOT EXISTS ventas_cargas (
      id           BIGSERIAL PRIMARY KEY,
      cargado_por  TEXT,
      archivo      TEXT,
      filas        INTEGER,
      insertados   INTEGER,
      actualizados INTEGER,
      creado_en    TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    -- Nueva columna del Excel de afectaciones: TipoCliente (no borra la tabla).
    ALTER TABLE ventas ADD COLUMN IF NOT EXISTS tipo_cliente TEXT;
    -- Migración: la columna se llamaba tipo_venta; la fuente real es TipoCredito.
    DO $$ BEGIN
      IF EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_name='ventas' AND column_name='tipo_venta')
         AND NOT EXISTS (SELECT 1 FROM information_schema.columns
                 WHERE table_name='ventas' AND column_name='tipo_credito')
      THEN ALTER TABLE ventas RENAME COLUMN tipo_venta TO tipo_credito; END IF;
    END $$;
  `);
  ventasSchemaLista = true;
}

// Convierte una fila cruda del Excel al arreglo ordenado según VENTAS_COLS.
function mapVentaRow(r) {
  const codigo = toInt(pickCol(r, 'CodigoCV', 'codigo_cv', 'CODIGOCV', 'Codigo CV'));
  if (codigo === null) return null;
  return [
    codigo,
    toInt(pickCol(r, 'DiaCV', 'dia_cv')),
    toInt(pickCol(r, 'MesCV', 'mes_cv')),
    toInt(pickCol(r, 'AñoCV', 'AnioCV', 'AnoCV', 'anio_cv')),
    toStr(pickCol(r, 'ClienteVenta', 'cliente_venta')),
    toStr(pickCol(r, 'Sede', 'sede')),
    toNum(pickCol(r, 'MontoConsolidado', 'monto_consolidado')),
    toNum(pickCol(r, 'CuotaInicial', 'cuota_inicial')),
    toStr(pickCol(r, 'Productos', 'productos')),
    toInt(pickCol(r, 'Cuotas', 'cuotas')),
    toStr(pickCol(r, 'DocIdentidad', 'doc_identidad')),
    toStr(pickCol(r, 'EstadoVenta', 'estado_venta')),
    toStr(pickCol(r, 'Entidad', 'entidad')),
    toStr(pickCol(r, 'Vendedor', 'vendedor')),
    toStr(pickCol(r, 'TipoCredito', 'TipoVenta', 'tipo_credito')),
    toStr(pickCol(r, 'EstadoTipoProducto', 'estado_tipo_producto')),
    toStr(pickCol(r, 'TipoCliente', 'tipo_cliente')),
    toInt(pickCol(r, 'DiaAF', 'dia_af')),
    toInt(pickCol(r, 'MesAF', 'mes_af')),
    toInt(pickCol(r, 'AñoAF', 'AnioAF', 'AnoAF', 'anio_af')),
  ];
}

const VENTAS_SET = VENTAS_COLS.slice(1).map(c => `${c} = EXCLUDED.${c}`).join(', ') + ', updated_at = now()';
async function upsertVentasChunk(client, chunk) {
  const params = [];
  const tuples = chunk.map((row, i) => {
    const base = i * VENTAS_COLS.length;
    params.push(...row);
    return '(' + VENTAS_COLS.map((_, j) => `$${base + j + 1}`).join(',') + ')';
  });
  const sql = `INSERT INTO ventas (${VENTAS_COLS.join(',')}) VALUES ${tuples.join(',')}
    ON CONFLICT (codigo_cv) DO UPDATE SET ${VENTAS_SET}
    RETURNING (xmax = 0) AS inserted`;
  const { rows } = await client.query(sql, params);
  let inserted = 0;
  for (const r of rows) if (r.inserted) inserted++;
  return { inserted, updated: rows.length - inserted };
}

app.post('/ventas/import', upload.single('archivo'), async (req, res) => {
  if (!pgPool) {
    return res.status(500).json({ success: false, message: 'Base de datos no configurada (falta DATABASE_URL).' });
  }
  if (!req.file) {
    return res.status(400).json({ success: false, message: 'No se recibió archivo (campo "archivo").' });
  }
  try {
    const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
    const ws = wb.Sheets[wb.SheetNames[0]];
    const raw = XLSX.utils.sheet_to_json(ws, { defval: '' });

    // Dedupe por codigo_cv (la última ocurrencia gana). Evita el error de Postgres
    // "ON CONFLICT DO UPDATE cannot affect row a second time".
    const byCode = new Map();
    for (const r of raw) {
      const m = mapVentaRow(r);
      if (m) byCode.set(m[0], m);
    }
    const rows = Array.from(byCode.values());
    if (rows.length === 0) {
      return res.status(400).json({ success: false, message: 'El archivo no tiene filas válidas (falta la columna CodigoCV).' });
    }

    await ensureVentasSchema();
    const client = await pgPool.connect();
    let insertados = 0, actualizados = 0;
    try {
      await client.query('BEGIN');
      const CHUNK = 1000;
      for (let i = 0; i < rows.length; i += CHUNK) {
        const r = await upsertVentasChunk(client, rows.slice(i, i + CHUNK));
        insertados += r.inserted;
        actualizados += r.updated;
      }
      await client.query(
        `INSERT INTO ventas_cargas (cargado_por, archivo, filas, insertados, actualizados)
         VALUES ($1,$2,$3,$4,$5)`,
        [toStr(req.body && req.body.cargado_por), req.file.originalname || null, rows.length, insertados, actualizados]
      );
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }

    res.json({ success: true, filas: rows.length, insertados, actualizados, updated_at: new Date().toISOString() });
  } catch (error) {
    console.error('❌ Error en POST /ventas/import:', error);
    res.status(500).json({ success: false, message: 'No se pudo importar el archivo de ventas.' });
  }
});

app.get('/ventas/estado', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureVentasSchema();
    const { rows } = await pgPool.query('SELECT COUNT(*)::int AS total, MAX(updated_at) AS updated_at FROM ventas');
    const { rows: cargas } = await pgPool.query(
      'SELECT cargado_por, archivo, filas, insertados, actualizados, creado_en FROM ventas_cargas ORDER BY id DESC LIMIT 1'
    );
    res.json({ success: true, total: rows[0].total, updated_at: rows[0].updated_at, ultimaCarga: cargas[0] || null });
  } catch (error) {
    console.error('❌ Error en GET /ventas/estado:', error);
    res.status(500).json({ success: false, message: 'No se pudo obtener el estado de ventas.' });
  }
});

app.get('/ventas', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureVentasSchema();
    const cond = [];
    const params = [];
    const anio = req.query.anio ? parseInt(req.query.anio, 10) : null;
    const mes  = req.query.mes  ? parseInt(req.query.mes, 10)  : null;
    if (anio && mes) {
      // Trae ventas del mes por su fecha de venta (CV) Y las afectaciones (NC/INC)
      // cuya fecha de afectación (AF) cae en ese mes. Necesario para que las NC cuadren.
      params.push(anio); const pa = params.length;
      params.push(mes);  const pm = params.length;
      cond.push(`((anio_cv = $${pa} AND mes_cv = $${pm}) OR (anio_af = $${pa} AND mes_af = $${pm}))`);
    } else if (anio) {
      params.push(anio);
      cond.push(`anio_cv = $${params.length}`);
    }
    if (req.query.sede) { params.push(`%${String(req.query.sede)}%`); cond.push(`sede ILIKE $${params.length}`); }
    // Filtro por vendedor (exacto, sin distinguir mayúsculas) → usado por "Mi Panel".
    if (req.query.vendedor) { params.push(String(req.query.vendedor).trim()); cond.push(`vendedor ILIKE $${params.length}`); }
    const where = cond.length ? 'WHERE ' + cond.join(' AND ') : '';
    const key = `ventas|${anio}|${mes}|${req.query.sede || ''}|${req.query.vendedor || ''}`;
    const rows = await cached(key, req.query.fresh, async () =>
      (await pgPool.query(
        `SELECT * FROM ventas ${where} ORDER BY fecha_cv DESC NULLS LAST, codigo_cv DESC`, params
      )).rows);
    res.json(rows);
  } catch (error) {
    console.error('❌ Error en GET /ventas:', error);
    res.status(500).json({ success: false, message: 'No se pudieron obtener las ventas.' });
  }
});

// GET /ventas/evolutivo — neto REAL mensual por SEDE (todos los meses/años) para el gráfico
// evolutivo de Ventas Sedes: ventas (por mes CV) menos NC arrastradas (por mes AF) menos
// incautaciones arrastradas (por mes AF). Las NC/INC del mismo mes (CV=AF) netean a 0.
// Devuelve [{sede, anio, mes, ventas, nc, inc, neto}].
app.get('/ventas/evolutivo', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureVentasSchema();
    const rows = await cached('ventas/evolutivo', req.query.fresh, async () => (await pgPool.query(`
      WITH ven AS (
        SELECT sede, anio_cv AS anio, mes_cv AS mes, SUM(monto_consolidado) AS monto FROM ventas
        WHERE UPPER(COALESCE(estado_venta,'')) NOT LIKE '%NOTA DE%' AND UPPER(COALESCE(estado_venta,'')) NOT LIKE '%INCAUTAC%'
          AND monto_consolidado > 0 AND anio_cv IS NOT NULL AND mes_cv IS NOT NULL
        GROUP BY sede, anio_cv, mes_cv
      ),
      ncnr AS (
        -- Solo restan las NC ARRASTRADAS (venta de un mes anterior anulada este mes).
        -- Las del mismo mes (CV = AF) netean a 0: su venta no está en 'ven' (que
        -- excluye los registros NC), así que sumarlas y restarlas se cancela → no restan.
        SELECT n.sede, n.anio_af AS anio, n.mes_af AS mes, SUM(n.monto_consolidado) AS monto FROM ventas n
        WHERE UPPER(COALESCE(n.estado_venta,'')) LIKE '%NOTA DE%' AND n.anio_af IS NOT NULL AND n.mes_af IS NOT NULL
          AND (n.anio_cv IS DISTINCT FROM n.anio_af OR n.mes_cv IS DISTINCT FROM n.mes_af)
        GROUP BY n.sede, n.anio_af, n.mes_af
      ),
      inc AS (
        -- Igual que las NC: solo restan las incautaciones arrastradas; las del mismo mes netean.
        SELECT sede, anio_af AS anio, mes_af AS mes, SUM(monto_consolidado) AS monto FROM ventas
        WHERE UPPER(COALESCE(estado_venta,'')) LIKE '%INCAUTAC%' AND anio_af IS NOT NULL AND mes_af IS NOT NULL
          AND (anio_cv IS DISTINCT FROM anio_af OR mes_cv IS DISTINCT FROM mes_af)
        GROUP BY sede, anio_af, mes_af
      ),
      claves AS (
        SELECT sede, anio, mes FROM ven
        UNION SELECT sede, anio, mes FROM ncnr
        UNION SELECT sede, anio, mes FROM inc
      )
      SELECT k.sede, k.anio, k.mes,
        ROUND(COALESCE(ven.monto,0))::int  AS ventas,
        ROUND(COALESCE(ncnr.monto,0))::int AS nc,
        ROUND(COALESCE(inc.monto,0))::int  AS inc,
        ROUND(COALESCE(ven.monto,0) - COALESCE(ncnr.monto,0) - COALESCE(inc.monto,0))::int AS neto
      FROM claves k
      LEFT JOIN ven  ON ven.sede  = k.sede AND ven.anio  = k.anio AND ven.mes  = k.mes
      LEFT JOIN ncnr ON ncnr.sede = k.sede AND ncnr.anio = k.anio AND ncnr.mes = k.mes
      LEFT JOIN inc  ON inc.sede  = k.sede AND inc.anio  = k.anio AND inc.mes  = k.mes
      WHERE k.anio > 0 AND k.mes > 0
      ORDER BY k.anio, k.mes`)).rows);
    res.json(rows);
  } catch (e) { console.error('❌ GET /ventas/evolutivo:', e); res.status(500).json({ success: false, message: e.message }); }
});

// ─────────────────────────────────────────────────────────────────────────────
// 🏷️  VENTAS POR CANAL — Call y Realzza (evolutivo propio, tablas separadas)
// Cada canal tiene su tabla, su carga y su ESQUEMA propio (los Excel difieren):
//   POST /ventas-call/import    · GET /ventas-call/estado    · GET /ventas-call
//   POST /ventas-realzza/import · GET /ventas-realzza/estado · GET /ventas-realzza
// Call    → Excel propio (CodigoCV, FECHAVENTA, Sede, MontoConsolidado, CuotaInicial,
//           Productos, Cuotas, DocIdentidad, TipoVenta, AsesorVenta, EstadoVenta,
//           TipoBase, TipoCliente, Entidad). La identidad del asesor es AsesorVenta =
//           el CÓDIGO CC (→ columna `vendedor`). No hay fecha de afectación separada:
//           AF = fecha de venta. Mi Panel del asesor lee siempre de aquí.
// Realzza → Excel de afectaciones (igual que `ventas`): el "vendedor" es la sede y
//           las NC/refacturaciones se aplican por fecha de afectación (mes_af/anio_af).
//           El GET con anio+mes trae ventas del mes (CV) ∪ afectaciones (AF) del mes.
// ─────────────────────────────────────────────────────────────────────────────

// Mapeo del Excel de Ventas Call. AsesorVenta → vendedor (código CC). FECHAVENTA
// se descompone en dia/mes/anio y se replica en AF (Call no tiene afectación aparte).
const CALL_COLS = [
  'codigo_cv', 'dia_cv', 'mes_cv', 'anio_cv', 'sede', 'monto_consolidado', 'cuota_inicial',
  'productos', 'cuotas', 'doc_identidad', 'tipo_credito', 'vendedor', 'estado_venta',
  'dni_txt', 'contacto', 'tipo_base', 'tipo_cliente', 'entidad', 'dia_af', 'mes_af', 'anio_af',
];
function mapVentaCallRow(r) {
  const codigo = toInt(pickCol(r, 'IDVENTA', 'CodigoCV', 'codigo_cv', 'CODIGOCV', 'Codigo CV'));
  if (codigo === null) return null;
  const iso = toFechaISO(pickCol(r, 'FECHAVENTA', 'FechaVenta', 'Fecha Venta', 'FECHA VENTA', 'fecha_venta'));
  let y = null, m = null, d = null;
  if (iso) { const p = iso.split('-'); y = +p[0]; m = +p[1]; d = +p[2]; }
  return [
    codigo, d, m, y,
    toStr(pickCol(r, 'Sede', 'sede')),
    toNum(pickCol(r, 'MontoConsolidado', 'monto_consolidado')),
    toNum(pickCol(r, 'CuotaInicial', 'CuotaIinicial', 'cuota_inicial')),
    toStr(pickCol(r, 'Productos', 'productos')),
    toInt(pickCol(r, 'Cuotas', 'cuotas')),
    toStr(pickCol(r, 'DocIdentidad', 'doc_identidad')),
    toStr(pickCol(r, 'TipoVenta', 'TipoCredito', 'tipo_credito')),
    toStr(pickCol(r, 'AsesorVenta', 'asesor_venta', 'vendedor')),   // → código CC
    toStr(pickCol(r, 'EstadoVenta', 'estado_venta')),
    toStr(pickCol(r, 'DNITXT', 'DNITxt', 'dni_txt')),
    toStr(pickCol(r, 'CONTACTO', 'Contacto', 'contacto')),
    toStr(pickCol(r, 'TipoBase', 'tipo_base')),
    toStr(pickCol(r, 'TipoCliente', 'tipo_cliente')),
    toStr(pickCol(r, 'Entidad', 'entidad')),
    d, m, y,   // AF = fecha de venta (Call no tiene afectación separada)
  ];
}

// Mapeo del Excel de Ventas Realzza. La identidad es VENDEDOR (se compara con los
// asesores Realzza registrados). También trae "Asesor Venta" (se guarda aparte).
// Sin fecha de afectación separada: AF = fecha de venta (las NC restan en su mes).
const REALZZA_COLS = [
  'codigo_cv', 'dia_cv', 'mes_cv', 'anio_cv', 'sede', 'monto_consolidado', 'cuota_inicial',
  'doc_identidad', 'productos', 'cuotas', 'estado_venta', 'asesor_venta', 'vendedor', 'entidad',
  'tipo_base', 'tipo_credito', 'tipo_producto', 'dia_af', 'mes_af', 'anio_af',
];
function mapVentaRealzzaRow(r) {
  const codigo = toInt(pickCol(r, 'CodigoCV', 'codigo_cv', 'CODIGOCV', 'Codigo CV'));
  if (codigo === null) return null;
  const iso = toFechaISO(pickCol(r, 'FECHAVENTA', 'FechaVenta', 'Fecha Venta', 'FECHA VENTA', 'fecha_venta'));
  let y = null, m = null, d = null;
  if (iso) { const p = iso.split('-'); y = +p[0]; m = +p[1]; d = +p[2]; }
  return [
    codigo, d, m, y,
    toStr(pickCol(r, 'SEDE', 'Sede', 'sede')),
    toNum(pickCol(r, 'MONTO CONSOLIDADO', 'MontoConsolidado', 'monto_consolidado')),
    toNum(pickCol(r, 'CUOTA INICIAL', 'CuotaInicial', 'cuota_inicial')),
    toStr(pickCol(r, 'DNI CLIENTE', 'DocIdentidad', 'DNI', 'doc_identidad')),
    toStr(pickCol(r, 'PRODUCTOS', 'Productos', 'productos')),
    toInt(pickCol(r, 'Nº CUOTAS', 'N° CUOTAS', 'Nro Cuotas', 'Cuotas', 'cuotas')),
    toStr(pickCol(r, 'ESTADO VENTA', 'EstadoVenta', 'estado_venta')),
    toStr(pickCol(r, 'Asesor Venta', 'AsesorVenta', 'asesor_venta')),
    toStr(pickCol(r, 'VENDEDOR', 'Vendedor', 'vendedor')),   // ← identidad Realzza
    toStr(pickCol(r, 'ENTIDAD', 'Entidad', 'entidad')),
    toStr(pickCol(r, 'TipoBase', 'TIPO BASE', 'tipo_base')),
    toStr(pickCol(r, 'TipoCredito', 'TipoVenta', 'TIPO CREDITO', 'tipo_credito')),
    toStr(pickCol(r, 'TipoProducto', 'TIPO PRODUCTO', 'tipo_producto')),
    d, m, y,   // AF = fecha de venta (Realzza no tiene afectación separada en el Excel)
  ];
}

// SET del UPSERT a partir de la lista de columnas (todas menos la PK).
const setDe = (cols) => cols.slice(1).map(c => `${c} = EXCLUDED.${c}`).join(', ') + ', updated_at = now()';

const CANALES = {
  call: {
    tabla: 'ventas_call', cargas: 'ventas_call_cargas',
    cols: CALL_COLS, mapper: mapVentaCallRow, set: setDe(CALL_COLS),
    ddl: (t) => `
      CREATE TABLE IF NOT EXISTS ${t} (
        codigo_cv         BIGINT PRIMARY KEY,
        dia_cv SMALLINT, mes_cv SMALLINT, anio_cv SMALLINT,
        sede TEXT, monto_consolidado NUMERIC(14,2), cuota_inicial NUMERIC(14,2),
        productos TEXT, cuotas INTEGER, doc_identidad TEXT, tipo_credito TEXT,
        vendedor TEXT, estado_venta TEXT, dni_txt TEXT, contacto TEXT,
        tipo_base TEXT, tipo_cliente TEXT, entidad TEXT,
        dia_af SMALLINT, mes_af SMALLINT, anio_af SMALLINT,
        fecha_cv DATE GENERATED ALWAYS AS (make_date(NULLIF(anio_cv,0),NULLIF(mes_cv,0),NULLIF(dia_cv,0))) STORED,
        fecha_af DATE GENERATED ALWAYS AS (make_date(NULLIF(anio_af,0),NULLIF(mes_af,0),NULLIF(dia_af,0))) STORED,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
      CREATE INDEX IF NOT EXISTS ix_${t}_anio_mes ON ${t} (anio_cv, mes_cv);
      CREATE INDEX IF NOT EXISTS ix_${t}_vendedor ON ${t} (vendedor);
      CREATE INDEX IF NOT EXISTS ix_${t}_sede     ON ${t} (sede);`,
  },
  realzza: {
    tabla: 'ventas_realzza', cargas: 'ventas_realzza_cargas',
    cols: REALZZA_COLS, mapper: mapVentaRealzzaRow, set: setDe(REALZZA_COLS),
    joinAfect: true,   // cruza con `ventas` (afectaciones) por CodigoCV para AF real

    ddl: (t) => `
      CREATE TABLE IF NOT EXISTS ${t} (
        codigo_cv         BIGINT PRIMARY KEY,
        dia_cv SMALLINT, mes_cv SMALLINT, anio_cv SMALLINT,
        sede TEXT, monto_consolidado NUMERIC(14,2), cuota_inicial NUMERIC(14,2),
        doc_identidad TEXT, productos TEXT, cuotas INTEGER, estado_venta TEXT,
        asesor_venta TEXT, vendedor TEXT, entidad TEXT, tipo_base TEXT,
        tipo_credito TEXT, tipo_producto TEXT,
        dia_af SMALLINT, mes_af SMALLINT, anio_af SMALLINT,
        fecha_cv DATE GENERATED ALWAYS AS (make_date(NULLIF(anio_cv,0),NULLIF(mes_cv,0),NULLIF(dia_cv,0))) STORED,
        fecha_af DATE GENERATED ALWAYS AS (make_date(NULLIF(anio_af,0),NULLIF(mes_af,0),NULLIF(dia_af,0))) STORED,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
      CREATE INDEX IF NOT EXISTS ix_${t}_anio_mes ON ${t} (anio_cv, mes_cv);
      CREATE INDEX IF NOT EXISTS ix_${t}_vendedor ON ${t} (vendedor);
      CREATE INDEX IF NOT EXISTS ix_${t}_sede     ON ${t} (sede);`,
  },
};
const canalSchemaLista = {};
async function ensureCanalSchema(canal) {
  const c = CANALES[canal];
  if (!pgPool || !c || canalSchemaLista[canal]) return;
  await pgPool.query(`
    ${c.ddl(c.tabla)}
    CREATE TABLE IF NOT EXISTS ${c.cargas} (
      id           BIGSERIAL PRIMARY KEY,
      cargado_por  TEXT,
      archivo      TEXT,
      filas        INTEGER,
      insertados   INTEGER,
      actualizados INTEGER,
      creado_en    TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  // Tablas creadas antes de que existiera la columna: la agrega sin recrear la tabla.
  await pgPool.query(`ALTER TABLE ${c.tabla} ADD COLUMN IF NOT EXISTS tipo_producto TEXT`);
  // Marca de venta "sin derivación" (ago-2026+): suma al global pero NO al asesor.
  await pgPool.query(`ALTER TABLE ${c.tabla} ADD COLUMN IF NOT EXISTS sin_derivacion BOOLEAN NOT NULL DEFAULT false`);
  // Marca manual "extranjero": una venta sin derivación marcada SÍ cuenta al vendedor.
  await pgPool.query(`ALTER TABLE ${c.tabla} ADD COLUMN IF NOT EXISTS extranjero BOOLEAN NOT NULL DEFAULT false`);
  // Fila atribuida a mano (se copia de ventas.asesor_manual al consolidar). Sirve para que
  // una venta con DNI vacío (RUC/empresa) atribuida manualmente SÍ cuente al vendedor.
  await pgPool.query(`ALTER TABLE ${c.tabla} ADD COLUMN IF NOT EXISTS asesor_manual BOOLEAN NOT NULL DEFAULT false`);
  canalSchemaLista[canal] = true;
}

async function upsertCanalChunk(client, c, chunk) {
  const params = [];
  const tuples = chunk.map((row, i) => {
    const base = i * c.cols.length;
    params.push(...row);
    return '(' + c.cols.map((_, j) => `$${base + j + 1}`).join(',') + ')';
  });
  const sql = `INSERT INTO ${c.tabla} (${c.cols.join(',')}) VALUES ${tuples.join(',')}
    ON CONFLICT (codigo_cv) DO UPDATE SET ${c.set}
    RETURNING (xmax = 0) AS inserted`;
  const { rows } = await client.query(sql, params);
  let inserted = 0;
  for (const r of rows) if (r.inserted) inserted++;
  return { inserted, updated: rows.length - inserted };
}

function registrarCanal(canal, ruta) {
  const c = CANALES[canal];

  app.post(`/${ruta}/import`, upload.single('archivo'), async (req, res) => {
    if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada (falta DATABASE_URL).' });
    if (!req.file) return res.status(400).json({ success: false, message: 'No se recibió archivo (campo "archivo").' });
    try {
      const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
      const ws = wb.Sheets[wb.SheetNames[0]];
      const raw = XLSX.utils.sheet_to_json(ws, { defval: '' });
      const byCode = new Map();
      for (const r of raw) { const m = c.mapper(r); if (m) byCode.set(m[0], m); }
      const rows = Array.from(byCode.values());
      if (rows.length === 0) return res.status(400).json({ success: false, message: 'El archivo no tiene filas válidas (falta la columna CodigoCV).' });

      await ensureCanalSchema(canal);
      const client = await pgPool.connect();
      let insertados = 0, actualizados = 0;
      try {
        await client.query('BEGIN');
        const CHUNK = 1000;
        for (let i = 0; i < rows.length; i += CHUNK) {
          const r = await upsertCanalChunk(client, c, rows.slice(i, i + CHUNK));
          insertados += r.inserted; actualizados += r.updated;
        }
        await client.query(
          `INSERT INTO ${c.cargas} (cargado_por, archivo, filas, insertados, actualizados) VALUES ($1,$2,$3,$4,$5)`,
          [toStr(req.body && req.body.cargado_por), req.file.originalname || null, rows.length, insertados, actualizados]
        );
        await client.query('COMMIT');
      } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }

      res.json({ success: true, filas: rows.length, insertados, actualizados, updated_at: new Date().toISOString() });
    } catch (error) {
      console.error(`❌ Error en POST /${ruta}/import:`, error);
      res.status(500).json({ success: false, message: 'No se pudo importar el archivo.' });
    }
  });

  app.get(`/${ruta}/estado`, async (req, res) => {
    if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
    try {
      await ensureCanalSchema(canal);
      const { rows } = await pgPool.query(`SELECT COUNT(*)::int AS total, MAX(updated_at) AS updated_at FROM ${c.tabla}`);
      const { rows: cargas } = await pgPool.query(
        `SELECT cargado_por, archivo, filas, insertados, actualizados, creado_en FROM ${c.cargas} ORDER BY id DESC LIMIT 1`
      );
      res.json({ success: true, total: rows[0].total, updated_at: rows[0].updated_at, ultimaCarga: cargas[0] || null });
    } catch (error) {
      console.error(`❌ Error en GET /${ruta}/estado:`, error);
      res.status(500).json({ success: false, message: 'No se pudo obtener el estado.' });
    }
  });

  app.get(`/${ruta}`, async (req, res) => {
    if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
    try {
      await ensureCanalSchema(canal);
      // Realzza: superpone la AFECTACIÓN autoritativa (tabla `ventas`, con dia/mes/anio_af
      // reales) sobre cada venta cruzando por CodigoCV. Así el estado y la fecha de
      // afectación (NC/refacturación/incautación) no dependen solo del Excel Realzza,
      // y el monto real por mes queda exacto (la NC resta en su mes de afectación real).
      const j = !!c.joinAfect;
      const px = j ? 'r.' : '';                          // prefijo de columnas base
      const afA = j ? 'COALESCE(v.anio_af, r.anio_af)' : 'anio_af';
      const afM = j ? 'COALESCE(v.mes_af,  r.mes_af)'  : 'mes_af';
      // Realzza: el vendedor autoritativo es el de `ventas` (base editable) — igual que el
      // módulo (/modulo). Así una edición del vendedor en `ventas` se refleja en Mi Panel.
      const vendCol = j ? 'COALESCE(v.vendedor, r.vendedor)' : `${px}vendedor`;
      const cond = [], params = [];
      const anio = req.query.anio ? parseInt(req.query.anio, 10) : null;
      const mes  = req.query.mes  ? parseInt(req.query.mes, 10)  : null;
      if (anio && mes) {
        params.push(anio); const pa = params.length;
        params.push(mes);  const pm = params.length;
        cond.push(`((${px}anio_cv = $${pa} AND ${px}mes_cv = $${pm}) OR (${afA} = $${pa} AND ${afM} = $${pm}))`);
      } else if (anio) { params.push(anio); cond.push(`${px}anio_cv = $${params.length}`); }
      if (req.query.sede)     { params.push(`%${String(req.query.sede)}%`); cond.push(`${px}sede ILIKE $${params.length}`); }
      if (req.query.vendedor) { params.push(String(req.query.vendedor).trim()); cond.push(`${vendCol} ILIKE $${params.length}`); }
      const where = cond.length ? 'WHERE ' + cond.join(' AND ') : '';

      // Realzza (joinAfect): sin_derivacion se calcula EN VIVO cruzando gestion_realzza
      // (igual que /modulo y Atribución), NO desde la columna congelada de ventas_realzza.
      // Así una derivación agregada tras consolidar se refleja en Mi Panel al recargar.
      let derivLateral = '';
      let sinDerivExpr = 'r.sin_derivacion';
      if (j) {
        params.push(DERIV_MOTIVOS_RZ); const pmot = params.length;
        derivLateral = `
           LEFT JOIN LATERAL (
             SELECT gr.marca_temporal FROM gestion_realzza gr
             WHERE regexp_replace(gr.dni_cliente, '\\D', '', 'g') = regexp_replace(r.doc_identidad, '\\D', '', 'g')
               AND gr.motivo_interes = ANY($${pmot})
               AND gr.marca_temporal::date <= r.fecha_cv
               AND r.fecha_cv - gr.marca_temporal::date <= 31
             ORDER BY gr.marca_temporal DESC LIMIT 1
           ) grz ON true`;
        sinDerivExpr = '(grz.marca_temporal IS NULL AND (r.anio_cv > 2026 OR (r.anio_cv = 2026 AND r.mes_cv >= 8)))';
      }

      const sql = j
        ? `SELECT r.codigo_cv, r.dia_cv, r.mes_cv, r.anio_cv, r.sede, r.monto_consolidado, r.cuota_inicial,
                  r.doc_identidad, r.productos, r.cuotas, r.asesor_venta, COALESCE(v.vendedor, r.vendedor) AS vendedor, r.entidad, r.tipo_base, r.tipo_credito, r.tipo_producto, ${sinDerivExpr} AS sin_derivacion, r.extranjero, r.asesor_manual, r.fecha_cv,
                  COALESCE(v.estado_venta, r.estado_venta) AS estado_venta,
                  COALESCE(v.dia_af,  r.dia_af)  AS dia_af,
                  COALESCE(v.mes_af,  r.mes_af)  AS mes_af,
                  COALESCE(v.anio_af, r.anio_af) AS anio_af,
                  make_date(NULLIF(COALESCE(v.anio_af, r.anio_af),0),
                            NULLIF(COALESCE(v.mes_af,  r.mes_af),0),
                            NULLIF(COALESCE(v.dia_af,  r.dia_af),0)) AS fecha_af
           FROM ${c.tabla} r
           LEFT JOIN ventas v ON v.codigo_cv = r.codigo_cv${derivLateral}
           ${where}
           ORDER BY r.fecha_cv DESC NULLS LAST, r.codigo_cv DESC`
        : `SELECT * FROM ${c.tabla} ${where} ORDER BY fecha_cv DESC NULLS LAST, codigo_cv DESC`;

      const key = `${ruta}|${req.query.anio || ''}|${req.query.mes || ''}|${req.query.sede || ''}|${req.query.vendedor || ''}`;
      const rows = await cached(key, req.query.fresh, async () => (await pgPool.query(sql, params)).rows);
      res.json(rows);
    } catch (error) {
      console.error(`❌ Error en GET /${ruta}:`, error);
      res.status(500).json({ success: false, message: 'No se pudieron obtener las ventas.' });
    }
  });
}

registrarCanal('call', 'ventas-call');
registrarCanal('realzza', 'ventas-realzza');

// ─────────────────────────────────────────────────────────────────────────────
// 🎯 ATRIBUCIÓN DE VENTAS CALL (AsesorVenta) por la última gestión de DERIVACIÓN.
// Reemplaza el VLOOKUP del Excel "GESTION CONTACT CENTER": cruza el DNI de la
// venta con gestion_call, toma la ÚLTIMA gestión de derivación dentro de 31 días
// y le pone el código CC del asesor. Lo que se edita a mano queda protegido
// (asesor_manual=true) para que el cruce no lo pise.
// ─────────────────────────────────────────────────────────────────────────────
const DERIV_MOTIVOS = ['VENTA DERIVADA PARA CIERRE A SEDE', 'VISITARÁ TIENDA', 'SE ENVIÓ A ASESOR VISITA A DOMICILIO'];
// Mapa nombre del asesor Call → código CC (fuente: lista de asesores del dashboard).
// Debe incluir a TODOS los asesores Call; si falta uno, sus derivaciones no se atribuyen.
const ASESORES_CC = [
  ['MORETO DELGADO PATRICIA ESTEFANY', 'CC1'], ['UCHOFEN VIGO FELICITA', 'CC3'],
  ['QUISPE FONSECA KAREN AIMEE', 'CC5'], ['MORALES ÑIQUE MARIA CANDELARIA', 'CC6'],
  ['CHANTA CAMPOS KELLY KARINTIA', 'CC8'], ['SAMAME HUAMAN ARIADNE', 'CC11'],
  ['BERNAL BAZAN BRENDA NICOL', 'CC12'], ['CARBONEL GUERRERO FRANCIS JHON', 'CC13'],
  ['TORRES ALVARADO JUDY ESMERALDA', 'CC15'], ['BONILLA CHUMACERO VILMA ROSSMERY', 'CC16'],
  ['SANDOVAL OTINIANO JUANA DEL PILAR', 'CC19'], ['CHANAME SOTO ANITA NOEMI', 'CC21'],
  ['BERNAL BAZAN FABRICIO ROLANDO', 'CC22'], ['RUIZ SAMPEN LUCRECIA NOEMI', 'CC26'],
];
const MAPA_SQL = `mapa(nombre, cc) AS (VALUES ${ASESORES_CC.map(([n, c]) => `('${n.replace(/'/g, "''")}','${c}')`).join(',')})`;
// LATERAL que devuelve la última gestión de derivación (≤31 días) del DNI de la venta
// (tabla `ventas` = afectaciones PB, alias v). Se cruza contra gestion_call.
const DERIV_LATERAL = `
  LEFT JOIN LATERAL (
    SELECT gc.asesor_contact, gc.marca_temporal, gc.tipo_cliente FROM gestion_call gc
    WHERE regexp_replace(gc.dni_cliente, '\\D', '', 'g') = regexp_replace(v.doc_identidad, '\\D', '', 'g')
      AND gc.motivo_interes = ANY($1)
      AND gc.marca_temporal::date <= v.fecha_cv
      AND v.fecha_cv - gc.marca_temporal::date <= 31
    ORDER BY gc.marca_temporal DESC LIMIT 1
  ) g ON true
  LEFT JOIN mapa m ON UPPER(TRIM(g.asesor_contact)) = m.nombre`;

// Columnas de atribución sobre la tabla `ventas` (sin tocar `vendedor`/sede/tipo_cliente del import):
//   asesor_venta       = CC del cruce/manual (AsesorVenta)
//   atrib_contacto     = origen del contacto (lista: BD, MARKET PLACE, KOMMO, …) — editable
//   atrib_tipo_cliente = tipo de cliente de la gestión (BRILLA, VIGENTE, …) — editable
//   atrib_tipo_base    = por defecto = tipo_cliente — editable
//   asesor_manual      = fila protegida del re-cruce (se editó a mano)
async function ensureAtribVentas() {
  await ensureVentasSchema();
  await pgPool.query(`ALTER TABLE ventas ADD COLUMN IF NOT EXISTS asesor_venta       TEXT`);
  await pgPool.query(`ALTER TABLE ventas ADD COLUMN IF NOT EXISTS atrib_contacto     TEXT`);
  await pgPool.query(`ALTER TABLE ventas ADD COLUMN IF NOT EXISTS atrib_tipo_cliente TEXT`);
  await pgPool.query(`ALTER TABLE ventas ADD COLUMN IF NOT EXISTS atrib_tipo_base    TEXT`);
  await pgPool.query(`ALTER TABLE ventas ADD COLUMN IF NOT EXISTS asesor_manual      BOOLEAN NOT NULL DEFAULT false`);
  // Marca manual "extranjero" (carnet de extranjería): la venta nunca cruza por DNI,
  // pero al marcarla sí cuenta al avance del vendedor aunque sea sin derivación.
  await pgPool.query(`ALTER TABLE ventas ADD COLUMN IF NOT EXISTS atrib_extranjero   BOOLEAN NOT NULL DEFAULT false`);
}
// Columnas comunes que devuelven los endpoints de atribución (con las columnas del Excel Call).
// Los campos atribuidos muestran el valor EFECTIVO: lo guardado (o editado a mano)
// y, si aún no se cruzó, la sugerencia de la derivación (el CC y el TipoCliente que
// la gestión ya conoce). TipoBase = TipoCliente por defecto. `consolidar` usa el
// mismo COALESCE, así lista y consolidación coinciden.
const ATRIB_SELECT = `
  v.codigo_cv, v.doc_identidad, v.dia_cv, v.mes_cv, v.anio_cv,
  v.monto_consolidado, v.cuota_inicial, v.cuotas, v.productos, v.sede,
  v.cliente_venta AS cliente, v.tipo_credito AS tipo_venta, v.estado_venta, v.entidad,
  COALESCE(v.asesor_venta, m.cc)                                 AS vendedor,
  v.atrib_contacto                                              AS contacto,
  COALESCE(v.atrib_tipo_cliente, g.tipo_cliente)                AS tipo_cliente,
  COALESCE(v.atrib_tipo_base, v.atrib_tipo_cliente, g.tipo_cliente) AS tipo_base,
  v.asesor_manual,
  (g.marca_temporal IS NULL AND (v.anio_cv > 2026 OR (v.anio_cv = 2026 AND v.mes_cv >= 8))) AS sin_derivacion,
  COALESCE(v.atrib_extranjero, false) AS extranjero,
  m.cc AS cc_sugerido, g.tipo_cliente AS tc_sugerido,
  g.asesor_contact AS asesor_derivacion, g.marca_temporal::date AS fecha_gestion`;

// POST /ventas-call/cruzar?anio=&mes= — atribuye el AsesorVenta (CC) a las ventas
// (tabla `ventas`) desde la última derivación en gestion_call. No pisa lo manual.
app.post('/ventas-call/cruzar', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureAtribVentas();
    const cond = ['NOT v.asesor_manual']; const params = [DERIV_MOTIVOS];
    if (req.query.anio) { params.push(parseInt(req.query.anio, 10)); cond.push(`v.anio_cv = $${params.length}`); }
    if (req.query.mes)  { params.push(parseInt(req.query.mes, 10));  cond.push(`v.mes_cv = $${params.length}`); }
    const where = cond.join(' AND ');
    const { rows } = await pgPool.query(`
      WITH ${MAPA_SQL},
      deriv AS (
        SELECT v.codigo_cv, m.cc, g.tipo_cliente AS tc FROM ventas v ${DERIV_LATERAL}
        WHERE ${where} AND m.cc IS NOT NULL
      )
      UPDATE ventas v SET
        asesor_venta       = deriv.cc,
        atrib_tipo_cliente = deriv.tc,
        atrib_tipo_base    = deriv.tc,
        updated_at = now()
      FROM deriv WHERE v.codigo_cv = deriv.codigo_cv
        AND (v.asesor_venta IS DISTINCT FROM deriv.cc OR v.atrib_tipo_cliente IS DISTINCT FROM deriv.tc)
      RETURNING v.codigo_cv`, params);
    // Total de ventas del mes con derivación cruzada (para mostrar el total atribuido).
    const totCond = []; const totParams = [DERIV_MOTIVOS];
    if (req.query.anio) { totParams.push(parseInt(req.query.anio, 10)); totCond.push(`v.anio_cv = $${totParams.length}`); }
    if (req.query.mes)  { totParams.push(parseInt(req.query.mes, 10));  totCond.push(`v.mes_cv = $${totParams.length}`); }
    const totWhere = totCond.length ? 'WHERE ' + totCond.join(' AND ') + ' AND m.cc IS NOT NULL' : 'WHERE m.cc IS NOT NULL';
    const { rows: tot } = await pgPool.query(`
      WITH ${MAPA_SQL}
      SELECT COUNT(*)::int n FROM ventas v ${DERIV_LATERAL} ${totWhere}`, totParams);
    res.json({ success: true, actualizados: rows.length, total: tot[0].n });
  } catch (e) { console.error('❌ POST /ventas-call/cruzar:', e); res.status(500).json({ success: false, message: e.message }); }
});

// GET /ventas-call/buscar?dni=&anio=&mes= — busca ese DNI en TODAS las afectaciones
// (tabla `ventas`) del mes indicado + la sugerencia de derivación, para asignar a mano.
app.get('/ventas-call/buscar', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureAtribVentas();
    const dni = String(req.query.dni || '').replace(/\D/g, '');
    if (!dni) return res.json([]);
    const cond = [`regexp_replace(v.doc_identidad, '\\D', '', 'g') = $2`];
    const params = [DERIV_MOTIVOS, dni];
    if (req.query.anio) { params.push(parseInt(req.query.anio, 10)); cond.push(`v.anio_cv = $${params.length}`); }
    if (req.query.mes)  { params.push(parseInt(req.query.mes, 10));  cond.push(`v.mes_cv = $${params.length}`); }
    const { rows } = await pgPool.query(`
      WITH ${MAPA_SQL}
      SELECT ${ATRIB_SELECT}
      FROM ventas v ${DERIV_LATERAL}
      WHERE ${cond.join(' AND ')}
      ORDER BY v.fecha_cv DESC NULLS LAST`, params);
    res.json(rows);
  } catch (e) { console.error('❌ GET /ventas-call/buscar:', e); res.status(500).json({ success: false, message: e.message }); }
});

// GET /ventas-call/atribucion?anio=&mes= — ventas (tabla `ventas`) del mes que ESTÁN
// atribuidas: con derivación en gestion_call o asignadas a mano. Para la lista.
app.get('/ventas-call/atribucion', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureAtribVentas();
    const cond = ['(m.cc IS NOT NULL OR v.asesor_manual)']; const params = [DERIV_MOTIVOS];
    if (req.query.anio) { params.push(parseInt(req.query.anio, 10)); cond.push(`v.anio_cv = $${params.length}`); }
    if (req.query.mes)  { params.push(parseInt(req.query.mes, 10));  cond.push(`v.mes_cv = $${params.length}`); }
    const { rows } = await pgPool.query(`
      WITH ${MAPA_SQL}
      SELECT ${ATRIB_SELECT}
      FROM ventas v ${DERIV_LATERAL}
      WHERE ${cond.join(' AND ')}
      ORDER BY v.fecha_cv DESC NULLS LAST, v.codigo_cv DESC`, params);
    res.json(rows);
  } catch (e) { console.error('❌ GET /ventas-call/atribucion:', e); res.status(500).json({ success: false, message: e.message }); }
});

// PUT /ventas-call/:codigo — edita a mano los campos de atribución (AsesorVenta,
// CONTACTO, TipoCliente, TipoBase). Solo actualiza los que llegan en el body y
// marca la fila como manual (protegida del re-cruce).
app.put('/ventas-call/:codigo', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureAtribVentas();
    const b = req.body || {};
    const sets = []; const vals = [];
    const add = (col, val, upper = false) => {
      let s = String(val ?? '').trim(); if (upper) s = s.toUpperCase();
      vals.push(s || null); sets.push(`${col} = $${vals.length}`);
    };
    if (b.vendedor     !== undefined) add('asesor_venta',       b.vendedor, true);
    if (b.contacto     !== undefined) add('atrib_contacto',     b.contacto);
    if (b.tipo_cliente !== undefined) add('atrib_tipo_cliente', b.tipo_cliente);
    if (b.tipo_base    !== undefined) add('atrib_tipo_base',    b.tipo_base);
    if (b.extranjero   !== undefined) { vals.push(!!b.extranjero); sets.push(`atrib_extranjero = $${vals.length}`); }
    if (!sets.length) return res.status(400).json({ success: false, message: 'Nada para actualizar.' });
    sets.push('asesor_manual = true', 'updated_at = now()');
    vals.push(parseInt(req.params.codigo, 10));
    const { rowCount } = await pgPool.query(
      `UPDATE ventas SET ${sets.join(', ')} WHERE codigo_cv = $${vals.length}`, vals);
    if (!rowCount) return res.status(404).json({ success: false, message: 'Venta no encontrada.' });
    res.json({ success: true });
  } catch (e) { console.error('❌ PUT /ventas-call/:codigo:', e); res.status(500).json({ success: false, message: e.message }); }
});

// POST /ventas-call/consolidar?anio=&mes= — copia (merge/upsert por IDVenta) las ventas
// ATRIBUIDAS del mes desde `ventas` (afectaciones, data viva) hacia `ventas_call` (histórico
// congelado que consume el módulo Ventas Call). Mapea asesor_venta→vendedor, atrib_*→contacto/
// tipo_base/tipo_cliente. No borra: actualiza las que existen y agrega las nuevas.
app.post('/ventas-call/consolidar', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureAtribVentas();
    await ensureCanalSchema('call');   // asegura la tabla ventas_call
    const cond = ['(m.cc IS NOT NULL OR v.asesor_manual)']; const params = [DERIV_MOTIVOS];
    if (req.query.anio) { params.push(parseInt(req.query.anio, 10)); cond.push(`v.anio_cv = $${params.length}`); }
    if (req.query.mes)  { params.push(parseInt(req.query.mes, 10));  cond.push(`v.mes_cv = $${params.length}`); }
    // Columnas destino en ventas_call (codigo_cv es la clave; no se pisa en el UPDATE).
    const cols = ['codigo_cv', 'dia_cv', 'mes_cv', 'anio_cv', 'sede', 'monto_consolidado', 'cuota_inicial',
      'productos', 'cuotas', 'doc_identidad', 'tipo_credito', 'vendedor', 'estado_venta', 'dni_txt',
      'contacto', 'tipo_base', 'tipo_cliente', 'entidad', 'tipo_producto', 'dia_af', 'mes_af', 'anio_af',
      'asesor_manual', 'sin_derivacion', 'extranjero'];
    // Se RESPETA lo que ya tiene ventas_call en estos campos (la data histórica manda):
    // solo se llenan si están vacíos. Las filas nuevas (agosto en adelante, gestionadas
    // desde el sistema) sí toman el valor del cruce al insertarse.
    const preservar = new Set(['contacto', 'tipo_base', 'tipo_cliente']);
    // sin_derivacion se CONGELA en la 1ª consolidación (el estado original manda: la
    // edición manual no crea derivación, así que no debe cambiarlo al re-consolidar).
    const mantener = new Set(['sin_derivacion']);
    const setUpd = cols.filter(c => c !== 'codigo_cv').map(c =>
      mantener.has(c)
        ? `${c} = ventas_call.${c}`
        : preservar.has(c)
          ? `${c} = COALESCE(NULLIF(ventas_call.${c}, ''), EXCLUDED.${c})`
          : `${c} = EXCLUDED.${c}`).join(', ');
    const { rows } = await pgPool.query(`
      WITH ${MAPA_SQL}
      INSERT INTO ventas_call (${cols.join(', ')})
      SELECT v.codigo_cv, v.dia_cv, v.mes_cv, v.anio_cv, v.sede, v.monto_consolidado, v.cuota_inicial,
             v.productos, v.cuotas, v.doc_identidad, v.tipo_credito,
             COALESCE(v.asesor_venta, m.cc)                 AS vendedor,
             v.estado_venta, v.doc_identidad                AS dni_txt,
             v.atrib_contacto                               AS contacto,
             COALESCE(v.atrib_tipo_base, g.tipo_cliente)    AS tipo_base,
             COALESCE(v.atrib_tipo_cliente, g.tipo_cliente) AS tipo_cliente,
             v.entidad, v.estado_tipo_producto AS tipo_producto,
             v.dia_cv AS dia_af, v.mes_cv AS mes_af, v.anio_cv AS anio_af,
             v.asesor_manual,
             (g.marca_temporal IS NULL AND (v.anio_cv > 2026 OR (v.anio_cv = 2026 AND v.mes_cv >= 8))) AS sin_derivacion,
             COALESCE(v.atrib_extranjero, false) AS extranjero
      FROM ventas v ${DERIV_LATERAL}
      WHERE ${cond.join(' AND ')}
      ON CONFLICT (codigo_cv) DO UPDATE SET ${setUpd}, updated_at = now()
      RETURNING (xmax = 0) AS inserted`, params);
    let insertados = 0; for (const r of rows) if (r.inserted) insertados++;
    res.json({ success: true, total: rows.length, insertados, actualizados: rows.length - insertados });
  } catch (e) { console.error('❌ POST /ventas-call/consolidar:', e); res.status(500).json({ success: false, message: e.message }); }
});

// ─────────────────────────────────────────────────────────────────────────────
// 🎯 ATRIBUCIÓN DE VENTAS REALZZA (TipoBase) por la última gestión de DERIVACIÓN.
// Análogo a Call pero sobre `ventas_realzza` × `gestion_realzza`: la última gestión
// de derivación (≤31 días) aporta el TipoBase. Se RESPETA el tipo_base del import:
// el cruce solo llena los vacíos; lo editado a mano queda protegido (asesor_manual).
// MargenTotal / ValorVenta / ClienteVenta salen de margen_ventas por CodigoCV.
// ─────────────────────────────────────────────────────────────────────────────
const DERIV_MOTIVOS_RZ = ['VENTA DERIVADA PARA CIERRE A SEDE'];
// LATERAL: última derivación (≤31 días) del DNI de la venta Realzza (alias v).
const DERIV_LATERAL_RZ = `
  LEFT JOIN LATERAL (
    SELECT gr.tipo_base, gr.asesor_realzza, gr.marca_temporal FROM gestion_realzza gr
    WHERE regexp_replace(gr.dni_cliente, '\\D', '', 'g') = regexp_replace(v.doc_identidad, '\\D', '', 'g')
      AND gr.motivo_interes = ANY($1)
      AND gr.marca_temporal::date <= v.fecha_cv
      AND v.fecha_cv - gr.marca_temporal::date <= 31
    ORDER BY gr.marca_temporal DESC LIMIT 1
  ) g ON true`;
// Margen agregado por CodigoCV (margen_ventas es 1 fila por línea de producto).
const MARGEN_JOIN = `
  LEFT JOIN (SELECT codigo_cv, SUM(valor_venta) AS valor_venta, SUM(margen_total) AS margen_total,
                    MAX(cliente) AS cliente
             FROM margen_ventas GROUP BY codigo_cv) mg ON mg.codigo_cv = v.codigo_cv`;
// La lista sale de `ventas` (afectaciones, sede REALZZA STORE) para que SIEMPRE liste
// TODAS las ventas del mes (derivadas o no), aunque aún no se hayan consolidado. Se
// hace LEFT JOIN a `ventas_realzza` (vr) para respetar el TipoBase/AsesorVenta ya
// guardados; si no hay, se muestra la sugerencia de la derivación.
const ATRIB_SELECT_RZ = `
  v.codigo_cv, v.dia_cv, v.mes_cv, v.anio_cv, v.sede, v.monto_consolidado, v.cuota_inicial,
  v.doc_identidad, v.productos, v.cuotas, v.estado_venta,
  COALESCE(vr.asesor_venta, v.asesor_venta)          AS asesor_venta,
  v.vendedor, v.entidad,
  COALESCE(NULLIF(vr.tipo_base, ''), g.tipo_base)     AS tipo_base,
  v.tipo_credito, vr.tipo_producto, v.dia_af, v.mes_af, v.anio_af,
  COALESCE(vr.asesor_manual, false)                  AS asesor_manual,
  g.tipo_base AS tb_sugerido, (g.marca_temporal IS NOT NULL) AS derivado,
  (g.marca_temporal IS NULL AND (v.anio_cv > 2026 OR (v.anio_cv = 2026 AND v.mes_cv >= 8))) AS sin_derivacion,
  COALESCE(vr.extranjero, false) AS extranjero,
  g.asesor_realzza AS asesor_derivacion, g.marca_temporal::date AS fecha_gestion,
  COALESCE(mg.cliente, v.cliente_venta) AS cliente, mg.valor_venta, mg.margen_total`;
// FROM común: ventas (afectaciones) + derivación + ventas_realzza (stored) + margen.
const ATRIB_FROM_RZ = `
  FROM ventas v ${DERIV_LATERAL_RZ}
  LEFT JOIN ventas_realzza vr ON vr.codigo_cv = v.codigo_cv
  ${MARGEN_JOIN}`;

async function ensureAtribRealzza() {
  await ensureCanalSchema('realzza');   // asegura la tabla ventas_realzza
  await pgPool.query(`ALTER TABLE ventas_realzza ADD COLUMN IF NOT EXISTS asesor_manual BOOLEAN NOT NULL DEFAULT false`);
}

// POST /ventas-realzza/cruzar?anio=&mes= — llena el TipoBase VACÍO desde la última
// derivación en gestion_realzza. Respeta lo que ya tiene y lo marcado a mano.
app.post('/ventas-realzza/cruzar', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureAtribRealzza();
    const cond = ["NOT v.asesor_manual", "(v.tipo_base IS NULL OR v.tipo_base = '')"]; const params = [DERIV_MOTIVOS_RZ];
    if (req.query.anio) { params.push(parseInt(req.query.anio, 10)); cond.push(`v.anio_cv = $${params.length}`); }
    if (req.query.mes)  { params.push(parseInt(req.query.mes, 10));  cond.push(`v.mes_cv = $${params.length}`); }
    const { rows } = await pgPool.query(`
      WITH deriv AS (
        SELECT v.codigo_cv, g.tipo_base AS tb FROM ventas_realzza v ${DERIV_LATERAL_RZ}
        WHERE ${cond.join(' AND ')} AND g.tipo_base IS NOT NULL AND g.tipo_base <> ''
      )
      UPDATE ventas_realzza v SET tipo_base = deriv.tb, updated_at = now()
      FROM deriv WHERE v.codigo_cv = deriv.codigo_cv
      RETURNING v.codigo_cv`, params);
    // Total de ventas del mes con derivación (para mostrar el total con cruce).
    const totCond = []; const totParams = [DERIV_MOTIVOS_RZ];
    if (req.query.anio) { totParams.push(parseInt(req.query.anio, 10)); totCond.push(`v.anio_cv = $${totParams.length}`); }
    if (req.query.mes)  { totParams.push(parseInt(req.query.mes, 10));  totCond.push(`v.mes_cv = $${totParams.length}`); }
    const totWhere = totCond.length ? 'WHERE ' + totCond.join(' AND ') + ' AND g.marca_temporal IS NOT NULL' : 'WHERE g.marca_temporal IS NOT NULL';
    const { rows: tot } = await pgPool.query(`
      SELECT COUNT(*)::int n FROM ventas_realzza v ${DERIV_LATERAL_RZ} ${totWhere}`, totParams);
    res.json({ success: true, actualizados: rows.length, total: tot[0].n });
  } catch (e) { console.error('❌ POST /ventas-realzza/cruzar:', e); res.status(500).json({ success: false, message: e.message }); }
});

// GET /ventas-realzza/atribucion?anio=&mes= — TODAS las ventas Realzza del mes con su
// TipoBase, si están derivadas, y margen/valor de venta (margen_ventas). Para la lista.
app.get('/ventas-realzza/atribucion', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureAtribRealzza();
    // TODAS las ventas SEDE REALZZA STORE del mes con monto > 0 (derivadas o no).
    const cond = ["v.sede ILIKE '%REALZZA%'", 'v.monto_consolidado > 0']; const params = [DERIV_MOTIVOS_RZ];
    if (req.query.anio) { params.push(parseInt(req.query.anio, 10)); cond.push(`v.anio_cv = $${params.length}`); }
    if (req.query.mes)  { params.push(parseInt(req.query.mes, 10));  cond.push(`v.mes_cv = $${params.length}`); }
    const { rows } = await pgPool.query(`
      SELECT ${ATRIB_SELECT_RZ}
      ${ATRIB_FROM_RZ}
      WHERE ${cond.join(' AND ')}
      ORDER BY v.fecha_cv DESC NULLS LAST, v.codigo_cv DESC`, params);
    res.json(rows);
  } catch (e) { console.error('❌ GET /ventas-realzza/atribucion:', e); res.status(500).json({ success: false, message: e.message }); }
});

// GET /ventas-realzza/buscar?dni=&anio=&mes= — busca ese DNI en las ventas Realzza del mes.
app.get('/ventas-realzza/buscar', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureAtribRealzza();
    const dni = String(req.query.dni || '').replace(/\D/g, '');
    if (!dni) return res.json([]);
    const cond = [`regexp_replace(v.doc_identidad, '\\D', '', 'g') = $2`, "v.sede ILIKE '%REALZZA%'"];
    const params = [DERIV_MOTIVOS_RZ, dni];
    if (req.query.anio) { params.push(parseInt(req.query.anio, 10)); cond.push(`v.anio_cv = $${params.length}`); }
    if (req.query.mes)  { params.push(parseInt(req.query.mes, 10));  cond.push(`v.mes_cv = $${params.length}`); }
    const { rows } = await pgPool.query(`
      SELECT ${ATRIB_SELECT_RZ}
      ${ATRIB_FROM_RZ}
      WHERE ${cond.join(' AND ')}
      ORDER BY v.fecha_cv DESC NULLS LAST`, params);
    res.json(rows);
  } catch (e) { console.error('❌ GET /ventas-realzza/buscar:', e); res.status(500).json({ success: false, message: e.message }); }
});

// PUT /ventas-realzza/:codigo — edita a mano TipoBase / AsesorVenta (protege del re-cruce).
app.put('/ventas-realzza/:codigo', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureAtribRealzza();
    const b = req.body || {};
    const sets = []; const vals = [];
    const add = (col, val, upper = false) => {
      let s = String(val ?? '').trim(); if (upper) s = s.toUpperCase();
      vals.push(s || null); sets.push(`${col} = $${vals.length}`);
    };
    if (b.tipo_base    !== undefined) add('tipo_base',    b.tipo_base);
    if (b.asesor_venta !== undefined) add('asesor_venta', b.asesor_venta);
    if (b.extranjero   !== undefined) { vals.push(!!b.extranjero); sets.push(`extranjero = $${vals.length}`); }
    if (!sets.length) return res.status(400).json({ success: false, message: 'Nada para actualizar.' });
    sets.push('asesor_manual = true', 'updated_at = now()');
    const codigo = parseInt(req.params.codigo, 10);
    // La lista sale de `ventas`; si la venta aún no está en ventas_realzza, se inserta
    // primero (desde `ventas`) para poder editarla y persistir el cambio.
    await pgPool.query(`
      INSERT INTO ventas_realzza (codigo_cv, dia_cv, mes_cv, anio_cv, sede, monto_consolidado, cuota_inicial,
        doc_identidad, productos, cuotas, estado_venta, asesor_venta, vendedor, entidad, tipo_credito, tipo_producto, dia_af, mes_af, anio_af, sin_derivacion)
      SELECT v.codigo_cv, v.dia_cv, v.mes_cv, v.anio_cv, v.sede, v.monto_consolidado, v.cuota_inicial,
        v.doc_identidad, v.productos, v.cuotas, v.estado_venta, v.asesor_venta, v.vendedor, v.entidad, v.tipo_credito, v.estado_tipo_producto, v.dia_af, v.mes_af, v.anio_af,
        (g.marca_temporal IS NULL AND (v.anio_cv > 2026 OR (v.anio_cv = 2026 AND v.mes_cv >= 8)))
      FROM ventas v ${DERIV_LATERAL_RZ}
      WHERE v.codigo_cv = $2 ON CONFLICT (codigo_cv) DO NOTHING`, [DERIV_MOTIVOS_RZ, codigo]);
    vals.push(codigo);
    const { rowCount } = await pgPool.query(
      `UPDATE ventas_realzza SET ${sets.join(', ')} WHERE codigo_cv = $${vals.length}`, vals);
    if (!rowCount) return res.status(404).json({ success: false, message: 'Venta no encontrada.' });
    res.json({ success: true });
  } catch (e) { console.error('❌ PUT /ventas-realzza/:codigo:', e); res.status(500).json({ success: false, message: e.message }); }
});

// POST /ventas-realzza/consolidar?anio=&mes= — agrega a `ventas_realzza` las ventas
// Realzza (sede 'SEDE REALZZA STORE') que estén en `ventas` (afectaciones) y aún NO
// existan. SOLO inserta lo nuevo (ON CONFLICT DO NOTHING): NO cambia lo que ya se tiene.
app.post('/ventas-realzza/consolidar', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureAtribRealzza();
    // $1 = motivos de derivación (para el LATERAL que aporta el TipoBase al insertar).
    const cond = ["v.sede ILIKE '%REALZZA%'"]; const params = [DERIV_MOTIVOS_RZ];
    if (req.query.anio) { params.push(parseInt(req.query.anio, 10)); cond.push(`v.anio_cv = $${params.length}`); }
    if (req.query.mes)  { params.push(parseInt(req.query.mes, 10));  cond.push(`v.mes_cv = $${params.length}`); }
    const cols = ['codigo_cv', 'dia_cv', 'mes_cv', 'anio_cv', 'sede', 'monto_consolidado', 'cuota_inicial',
      'doc_identidad', 'productos', 'cuotas', 'estado_venta', 'asesor_venta', 'vendedor', 'entidad',
      'tipo_credito', 'tipo_producto', 'dia_af', 'mes_af', 'anio_af', 'tipo_base', 'sin_derivacion'];
    // Al insertar lo nuevo, el TipoBase se toma de la última derivación (como el cruce).
    const { rows } = await pgPool.query(`
      INSERT INTO ventas_realzza (${cols.join(', ')})
      SELECT v.codigo_cv, v.dia_cv, v.mes_cv, v.anio_cv, v.sede, v.monto_consolidado, v.cuota_inicial,
             v.doc_identidad, v.productos, v.cuotas, v.estado_venta, v.asesor_venta, v.vendedor, v.entidad,
             v.tipo_credito, v.estado_tipo_producto, v.dia_af, v.mes_af, v.anio_af, g.tipo_base,
             (g.marca_temporal IS NULL AND (v.anio_cv > 2026 OR (v.anio_cv = 2026 AND v.mes_cv >= 8)))
      FROM ventas v ${DERIV_LATERAL_RZ}
      WHERE ${cond.join(' AND ')}
      ON CONFLICT (codigo_cv) DO NOTHING
      RETURNING codigo_cv`, params);
    res.json({ success: true, insertados: rows.length });
  } catch (e) { console.error('❌ POST /ventas-realzza/consolidar:', e); res.status(500).json({ success: false, message: e.message }); }
});

// ─────────────────────────────────────────────────────────────────────────────
// 🏬 ATRIBUCIÓN DE SEDES (Lambayeque / Ferreñafe) — cruza `ventas` × gestion_sedes_deriv
// La "fuente generadora" = TIPO DE BASE de la derivación. Se guarda en ventas.atrib_fuente_sede
// (Ventas Sedes lee `ventas` directo, no hay tabla consolidada). Solo ago-2026+ para la tabla.
// ─────────────────────────────────────────────────────────────────────────────
const MOTIVO_DERIV_SEDE = 'VENTA DERIVADA PARA CIERRE A SEDE';
function normSede(s) {
  const u = (s || '').toString().trim().toUpperCase();
  if (u.includes('FERRE')) return 'FERREÑAFE';
  if (u.includes('LAMBAYEQUE')) return 'LAMBAYEQUE';
  return u;
}
async function ensureAtribSedes() {
  await pgPool.query(`ALTER TABLE ventas ADD COLUMN IF NOT EXISTS atrib_fuente_sede TEXT`);
  await pgPool.query(`ALTER TABLE ventas ADD COLUMN IF NOT EXISTS atrib_sede_manual BOOLEAN NOT NULL DEFAULT false`);
}
// LATERAL: derivación (tabla gestion_sedes_deriv) del DNI en esa sede. El espejo se
// llena con POST /gestion-sedes-deriv/sync (botón "Sincronizar formulario" en la UI).
// Match SOLO por DNI (no por fecha) — cuenta la derivación aunque se registre después de
// la venta. $1 = motivo, $2 = sede (upper, con acento).
const DERIV_LATERAL_SEDE = `
  LEFT JOIN LATERAL (
    SELECT gsd.tipo_base, gsd.marca_temporal,
           COALESCE(NULLIF(gsd.asesor_lambayeque, ''), gsd.asesor_ferrenafe) AS asesor
    FROM gestion_sedes_deriv gsd
    WHERE regexp_replace(gsd.dni_cliente, '\\D', '', 'g') = regexp_replace(v.doc_identidad, '\\D', '', 'g')
      AND gsd.motivo_interes = $1
      AND upper(trim(gsd.sede)) = $2
    ORDER BY gsd.marca_temporal DESC LIMIT 1
  ) g ON true`;
const ATRIB_SELECT_SEDE = `
  v.codigo_cv, v.dia_cv, v.mes_cv, v.anio_cv, v.sede, v.monto_consolidado, v.cuota_inicial,
  v.doc_identidad, v.productos, v.cuotas, v.estado_venta, v.vendedor, v.entidad, v.tipo_credito,
  v.cliente_venta AS cliente, v.dia_af, v.mes_af, v.anio_af,
  COALESCE(NULLIF(v.atrib_fuente_sede, ''), g.tipo_base) AS fuente,
  COALESCE(v.atrib_sede_manual, false)                   AS manual,
  (g.marca_temporal IS NOT NULL)                         AS derivado,
  g.tipo_base AS fuente_sugerida, g.asesor AS asesor_derivacion, g.marca_temporal::date AS fecha_gestion`;

// POST /ventas-sedes/cruzar?anio=&mes=&sede= — llena atrib_fuente_sede VACÍO desde la derivación.
app.post('/ventas-sedes/cruzar', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureAtribSedes();
    const sede = normSede(req.query.sede);
    if (!sede) return res.status(400).json({ success: false, message: 'Falta la sede.' });
    const cond = ['NOT COALESCE(v.atrib_sede_manual, false)', "(v.atrib_fuente_sede IS NULL OR v.atrib_fuente_sede = '')",
      `v.sede ILIKE '%' || $2 || '%'`];
    const params = [MOTIVO_DERIV_SEDE, sede];
    if (req.query.anio) { params.push(parseInt(req.query.anio, 10)); cond.push(`v.anio_cv = $${params.length}`); }
    if (req.query.mes)  { params.push(parseInt(req.query.mes, 10));  cond.push(`v.mes_cv = $${params.length}`); }
    const { rows } = await pgPool.query(`
      WITH deriv AS (
        SELECT v.codigo_cv, g.tipo_base AS tb FROM ventas v ${DERIV_LATERAL_SEDE}
        WHERE ${cond.join(' AND ')} AND g.tipo_base IS NOT NULL AND g.tipo_base <> ''
      )
      UPDATE ventas v SET atrib_fuente_sede = deriv.tb, updated_at = now()
      FROM deriv WHERE v.codigo_cv = deriv.codigo_cv
      RETURNING v.codigo_cv`, params);
    res.json({ success: true, actualizados: rows.length });
  } catch (e) { console.error('❌ POST /ventas-sedes/cruzar:', e); res.status(500).json({ success: false, message: e.message }); }
});

// GET /ventas-sedes/atribucion?anio=&mes=&sede= — ventas de la sede con su fuente + derivado.
app.get('/ventas-sedes/atribucion', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    // Cruce dinámico contra el espejo del formulario: nunca servir cacheado (navegador/proxy).
    res.set('Cache-Control', 'no-store');
    await ensureAtribSedes();
    const sede = normSede(req.query.sede);
    if (!sede) return res.status(400).json({ success: false, message: 'Falta la sede.' });
    const cond = [`v.sede ILIKE '%' || $2 || '%'`, 'v.monto_consolidado > 0'];
    const params = [MOTIVO_DERIV_SEDE, sede];
    if (req.query.anio) { params.push(parseInt(req.query.anio, 10)); cond.push(`v.anio_cv = $${params.length}`); }
    if (req.query.mes)  { params.push(parseInt(req.query.mes, 10));  cond.push(`v.mes_cv = $${params.length}`); }
    const { rows } = await pgPool.query(`
      SELECT ${ATRIB_SELECT_SEDE} FROM ventas v ${DERIV_LATERAL_SEDE}
      WHERE ${cond.join(' AND ')}
      ORDER BY v.fecha_cv DESC NULLS LAST, v.codigo_cv DESC`, params);
    res.json(rows);
  } catch (e) { console.error('❌ GET /ventas-sedes/atribucion:', e); res.status(500).json({ success: false, message: e.message }); }
});

// GET /ventas-sedes/buscar?dni=&anio=&mes=&sede= — busca ese DNI en las ventas de la sede.
app.get('/ventas-sedes/buscar', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureAtribSedes();
    res.set('Cache-Control', 'no-store');
    const sede = normSede(req.query.sede);
    const dni = String(req.query.dni || '').replace(/\D/g, '');
    if (!sede || !dni) return res.json([]);
    const cond = [`v.sede ILIKE '%' || $2 || '%'`, `regexp_replace(v.doc_identidad, '\\D', '', 'g') = $3`];
    const params = [MOTIVO_DERIV_SEDE, sede, dni];
    if (req.query.anio) { params.push(parseInt(req.query.anio, 10)); cond.push(`v.anio_cv = $${params.length}`); }
    if (req.query.mes)  { params.push(parseInt(req.query.mes, 10));  cond.push(`v.mes_cv = $${params.length}`); }
    const { rows } = await pgPool.query(`
      SELECT ${ATRIB_SELECT_SEDE} FROM ventas v ${DERIV_LATERAL_SEDE}
      WHERE ${cond.join(' AND ')}
      ORDER BY v.fecha_cv DESC NULLS LAST`, params);
    res.json(rows);
  } catch (e) { console.error('❌ GET /ventas-sedes/buscar:', e); res.status(500).json({ success: false, message: e.message }); }
});

// PUT /ventas-sedes/:codigo — edita a mano la fuente generadora (protege del re-cruce).
app.put('/ventas-sedes/:codigo', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureAtribSedes();
    const b = req.body || {};
    if (b.fuente === undefined) return res.status(400).json({ success: false, message: 'Nada para actualizar.' });
    const fuente = String(b.fuente ?? '').trim().toUpperCase() || null;
    const { rowCount } = await pgPool.query(
      'UPDATE ventas SET atrib_fuente_sede = $2, atrib_sede_manual = true, updated_at = now() WHERE codigo_cv = $1',
      [parseInt(req.params.codigo, 10), fuente]);
    if (!rowCount) return res.status(404).json({ success: false, message: 'Venta no encontrada.' });
    res.json({ success: true });
  } catch (e) { console.error('❌ PUT /ventas-sedes/:codigo:', e); res.status(500).json({ success: false, message: e.message }); }
});

// ─────────────────────────────────────────────────────────────────────────────
// 💰 SUELDO ESTIMADO — VENDEDOR DE PISO/SEDE (réplica del Excel "Comisiones Asesores")
// Comisión sobre valor de venta: Electro 1.5% + Melamina 3% + Motos 1% (Melamina/Motos
// van CON IGV, la venta base va SIN IGV, tal cual el Excel). Neto de NC e Incautaciones
// por MES DE AFECTACIÓN. Bonos: Categoría (derivada de la venta con IGV) y Campañero (si
// el canal del CAP es CAMPAÑA). Volumen/Reto = Fase 2 (requiere meta por sede).
const IGV = 1.18;
const COM_ELECTRO = 0.015, COM_MELAMINA = 0.03, COM_MOTOS = 0.01;
const RMV_SEDE = 1130, BASICO_SEDE = 400;
// Categoría por venta CON IGV: [umbral, nombre, bono]
const CAT_SEDE = [[150000, 'Leo Master', 600], [100000, 'Top Master', 450], [65000, 'Master', 300], [49000, 'Senior', 150]];
// Bono Campañero por venta CON IGV (solo canal CAMPAÑA): [umbral, bono]
const CAMPANERO_SEDE = [[50000, 355], [45000, 275], [40000, 250], [35000, 225], [30000, 200], [25000, 100], [20000, 50]];
// Bono Volumen por venta CON IGV (gate: %logro de meta de la sede ≥ 100%): [umbral, bono]
const VOLUMEN_SEDE = [[180000, 1000], [151000, 750], [145001, 750], [140001, 700], [135001, 650], [130001, 600],
  [125001, 550], [120001, 500], [115001, 440], [110001, 420], [105001, 410], [100001, 400], [95001, 360],
  [90001, 350], [85001, 340], [80001, 300], [75001, 280], [70001, 260], [65001, 240], [60001, 220]];
const META_LOGRO_GATE = 1.0;   // el Bono Volumen requiere que la sede llegue al 100% de su meta

// Tabla `meta_sede`: meta mensual por sede (input del admin) para el %logro (Bono Volumen/Reto).
let metaSedeLista = false;
async function ensureMetaSede() {
  if (!pgPool || metaSedeLista) return;
  await pgPool.query(`
    CREATE TABLE IF NOT EXISTS meta_sede (
      id BIGSERIAL PRIMARY KEY,
      sede TEXT NOT NULL,
      anio INT NOT NULL,
      mes INT NOT NULL,
      meta NUMERIC(14,2) NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ux_meta_sede ON meta_sede (UPPER(sede), anio, mes);`);
  metaSedeLista = true;
}

// Total neto (con IGV) de una sede en un mes: ventas positivas − NC/Incautaciones (por mes de
// afectación). `sedeLike` es el nombre corto (ej. CAYALTI) que hace match ILIKE con ventas.sede.
async function sedeTotalNeto(sedeLike, anio, mes) {
  const q = await pgPool.query(
    `SELECT
       COALESCE((SELECT SUM(monto_consolidado) FROM ventas
         WHERE sede ILIKE '%' || $1 || '%' AND mes_cv = $2 AND anio_cv = $3
           AND UPPER(estado_venta) NOT LIKE '%NOTA DE CR%' AND UPPER(estado_venta) NOT LIKE '%INCAUTAC%'
           AND UPPER(estado_venta) NOT LIKE '%ANULAD%'), 0)
     - COALESCE((SELECT SUM(monto_consolidado) FROM ventas
         WHERE sede ILIKE '%' || $1 || '%' AND mes_af = $2 AND anio_af = $3
           AND (UPPER(estado_venta) LIKE '%NOTA DE CR%' OR UPPER(estado_venta) LIKE '%INCAUTAC%')), 0) AS total`,
    [sedeLike, mes, anio]);
  return +q.rows[0].total;
}

app.get('/ventas-sedes/sueldo', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    res.set('Cache-Control', 'no-store');
    const vendedor = String(req.query.vendedor || '').trim();
    const anio = parseInt(req.query.anio, 10);
    const mes  = parseInt(req.query.mes, 10);
    if (!vendedor || !anio || !mes) return res.status(400).json({ success: false, message: 'Faltan vendedor, anio y mes.' });

    // K (venta sin IGV) + melamina/motos (sin IGV) de las ventas del vendedor con mes_cv=mes,
    // cruzando el detalle por línea (margen_ventas) por codigo_cv.
    const mg = await pgPool.query(
      `SELECT COALESCE(SUM(mv.valor_venta),0) AS k,
         COALESCE(SUM(mv.valor_venta) FILTER (WHERE UPPER(mv.linea_real)='LINEA MELAMINA'),0) AS mel,
         COALESCE(SUM(mv.valor_venta) FILTER (WHERE UPPER(mv.linea_real)='LINEA MOTOCICLETAS'),0) AS mot
       FROM margen_ventas mv
       WHERE mv.codigo_cv IN (
         SELECT codigo_cv FROM ventas
         WHERE UPPER(TRIM(vendedor)) = UPPER(TRIM($1)) AND mes_cv = $2 AND anio_cv = $3)`,
      [vendedor, mes, anio]);
    // NC e Incautaciones por MES DE AFECTACIÓN (con IGV, como en el Excel).
    const af = await pgPool.query(
      `SELECT COALESCE(SUM(monto_consolidado) FILTER (WHERE UPPER(estado_venta) LIKE '%NOTA DE CR%'),0) AS nc,
              COALESCE(SUM(monto_consolidado) FILTER (WHERE UPPER(estado_venta) LIKE '%INCAUTAC%'),0) AS inc
       FROM ventas WHERE UPPER(TRIM(vendedor)) = UPPER(TRIM($1)) AND mes_af = $2 AND anio_af = $3`,
      [vendedor, mes, anio]);
    // Canal del CAP (para el Bono Campañero). CAMPAÑA* → campañero; RECP* → receptivo.
    const cap = await pgPool.query(
      `SELECT canal FROM cap_asesores WHERE UPPER(TRIM(vendedor)) = UPPER(TRIM($1)) LIMIT 1`, [vendedor]);

    const ventaSinIGV = +mg.rows[0].k;
    const melamina = (+mg.rows[0].mel) * IGV;   // con IGV
    const motos    = (+mg.rows[0].mot) * IGV;   // con IGV
    const nc  = +af.rows[0].nc;
    const inc = +af.rows[0].inc;
    const ventaConIGV = ventaSinIGV * IGV;      // = L del Excel (para categoría y escalas)

    const baseElectro = (ventaSinIGV - nc - inc) - melamina - motos;
    const comElectro  = baseElectro > 0 ? baseElectro * COM_ELECTRO : 0;
    const comMelamina = melamina * COM_MELAMINA;
    const comMotos    = motos * COM_MOTOS;
    const totalComisiones = comElectro + comMelamina + comMotos;

    // Categoría (derivada de la venta con IGV) → bono.
    const cat = CAT_SEDE.find(([u]) => ventaConIGV >= u);
    const categoria = cat ? cat[1] : 'Sin Categoría';
    const bonoCategoria = cat ? cat[2] : 0;

    // Bono Campañero: solo si el canal del CAP es CAMPAÑA.
    const canal = (cap.rows[0]?.canal || '').toString().trim();
    const canalSinTilde = canal.normalize('NFD').replace(/[̀-ͯ]/g, '');
    const esCampana = /^CAMP/i.test(canalSinTilde);
    const escC = CAMPANERO_SEDE.find(([u]) => ventaConIGV >= u);
    const bonoCampanero = esCampana && escC ? escC[1] : 0;

    // Bono Volumen: gate por %logro de la META de la SEDE del vendedor (≥100%), escala por venta.
    await ensureMetaSede();
    let sedeNombre = '', metaSede = 0, sedeTotal = 0, logroSede = 0, bonoVolumen = 0;
    const sedeRow = await pgPool.query(
      `SELECT sede FROM ventas WHERE UPPER(TRIM(vendedor)) = UPPER(TRIM($1)) AND mes_cv = $2 AND anio_cv = $3
         AND sede IS NOT NULL AND sede <> '' GROUP BY sede ORDER BY COUNT(*) DESC LIMIT 1`, [vendedor, mes, anio]);
    sedeNombre = sedeRow.rows[0]?.sede || '';
    if (sedeNombre) {
      const metaRow = await pgPool.query(
        `SELECT sede, meta FROM meta_sede WHERE $1 ILIKE '%' || sede || '%' AND anio = $2 AND mes = $3
         ORDER BY LENGTH(sede) DESC LIMIT 1`, [sedeNombre, anio, mes]);
      if (metaRow.rows[0] && +metaRow.rows[0].meta > 0) {
        metaSede = +metaRow.rows[0].meta;
        sedeTotal = await sedeTotalNeto(metaRow.rows[0].sede, anio, mes);
        logroSede = metaSede > 0 ? sedeTotal / metaSede : 0;
        if (logroSede >= META_LOGRO_GATE) {
          const ev = VOLUMEN_SEDE.find(([u]) => ventaConIGV >= u);
          bonoVolumen = ev ? ev[1] : 0;
        }
      }
    }

    // Adicional por motos GLOBAL GO (per-unit, misma lógica que Call/Realzza): ≥5 → 125 c/u, si no 100.
    const mgo = await pgPool.query(
      `SELECT COUNT(*)::int c FROM ventas
       WHERE UPPER(TRIM(vendedor)) = UPPER(TRIM($1)) AND mes_cv = $2 AND anio_cv = $3
         AND UPPER(estado_tipo_producto) LIKE '%MOTO%' AND UPPER(entidad) LIKE '%GLOBAL GO%'
         AND UPPER(estado_venta) NOT LIKE '%NOTA DE CR%' AND UPPER(estado_venta) NOT LIKE '%INCAUTAC%'`,
      [vendedor, mes, anio]);
    const motosGlobal = mgo.rows[0].c;
    const tarifaMoto = motosGlobal >= 5 ? 125 : 100;
    const pagoMotosGlobal = motosGlobal * tarifaMoto;

    // Sueldo base FIJO (RMV 1130) para todos + comisiones + bonos + adicional motos, todo aditivo.
    const sueldoBase = RMV_SEDE;
    const remTotal = sueldoBase + totalComisiones + bonoCategoria + bonoCampanero + bonoVolumen + pagoMotosGlobal;

    const r2 = n => Math.round(n * 100) / 100;
    res.json({
      vendedor, anio, mes, canal, esCampana, categoria,
      ventaSinIGV: r2(ventaSinIGV), ventaConIGV: r2(ventaConIGV),
      notasCredito: r2(nc), incautaciones: r2(inc),
      melamina: r2(melamina), motos: r2(motos), baseElectro: r2(baseElectro),
      comElectro: r2(comElectro), comMelamina: r2(comMelamina), comMotos: r2(comMotos),
      totalComisiones: r2(totalComisiones),
      sueldoBase,
      bonoCategoria, bonoCampanero, bonoVolumen,
      motosGlobal, tarifaMoto, pagoMotosGlobal: r2(pagoMotosGlobal),
      sede: sedeNombre, metaSede: r2(metaSede), sedeTotal: r2(sedeTotal), logroSede: r2(logroSede),
      remTotal: r2(remTotal),
      nota: 'Estimado a mes completo, neto de NC/Incautaciones. Bono Volumen requiere que la sede llegue al 100% de su meta.',
    });
  } catch (e) { console.error('❌ GET /ventas-sedes/sueldo:', e); res.status(500).json({ success: false, message: e.message }); }
});

// ─────────────────────────────────────────────────────────────────────────────
// 🎯 MAESTRO METAS POR SEDE (Fase 2) — meta mensual por sede + %logro (para Bono Volumen)
// GET /meta-sede?anio=&mes= — una fila por sede (del CAP) con su meta, total real y %logro.
app.get('/meta-sede', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    res.set('Cache-Control', 'no-store');
    await ensureMetaSede();
    const anio = parseInt(req.query.anio, 10), mes = parseInt(req.query.mes, 10);
    if (!anio || !mes) return res.status(400).json({ success: false, message: 'Faltan anio y mes.' });
    const sedes = await pgPool.query(
      `SELECT DISTINCT UPPER(TRIM(sede)) sede FROM cap_asesores
       WHERE sede IS NOT NULL AND TRIM(sede) <> '' AND UPPER(TRIM(sede)) NOT IN ('TODAS') ORDER BY 1`);
    const metas = await pgPool.query(`SELECT id, UPPER(sede) sede, meta FROM meta_sede WHERE anio = $1 AND mes = $2`, [anio, mes]);
    const metaMap = new Map(metas.rows.map(r => [r.sede, r]));
    const out = [];
    for (const s of sedes.rows) {
      const m = metaMap.get(s.sede);
      const meta = m ? +m.meta : 0;
      const actual = await sedeTotalNeto(s.sede, anio, mes);
      out.push({
        id: m?.id || null, sede: s.sede, anio, mes, meta,
        actual: Math.round(actual * 100) / 100,
        logro: meta > 0 ? Math.round((actual / meta) * 1000) / 10 : 0,   // %
      });
    }
    res.json(out);
  } catch (e) { console.error('❌ GET /meta-sede:', e); res.status(500).json({ success: false, message: e.message }); }
});

// POST /meta-sede — upsert de la meta de una sede/mes. Body: { sede, anio, mes, meta }.
app.post('/meta-sede', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureMetaSede();
    const b = req.body || {};
    const sede = String(b.sede || '').trim().toUpperCase();
    const anio = parseInt(b.anio, 10), mes = parseInt(b.mes, 10);
    const meta = Number(String(b.meta ?? '').replace(/[^0-9.]/g, '')) || 0;
    if (!sede || !anio || !mes) return res.status(400).json({ success: false, message: 'Faltan sede, anio y mes.' });
    await pgPool.query(
      `INSERT INTO meta_sede (sede, anio, mes, meta) VALUES ($1, $2, $3, $4)
       ON CONFLICT (UPPER(sede), anio, mes) DO UPDATE SET meta = EXCLUDED.meta, updated_at = now()`,
      [sede, anio, mes, meta]);
    res.json({ success: true });
  } catch (e) { console.error('❌ POST /meta-sede:', e); res.status(500).json({ success: false, message: e.message }); }
});

// DELETE /meta-sede/:id — borra la meta (la sede vuelve a quedar sin meta).
app.delete('/meta-sede/:id', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureMetaSede();
    const { rowCount } = await pgPool.query('DELETE FROM meta_sede WHERE id = $1', [parseInt(req.params.id, 10)]);
    if (!rowCount) return res.status(404).json({ success: false, message: 'Meta no encontrada.' });
    res.json({ success: true });
  } catch (e) { console.error('❌ DELETE /meta-sede/:id:', e); res.status(500).json({ success: false, message: e.message }); }
});

// ─────────────────────────────────────────────────────────────────────────────
// 🎯 MAESTRO METAS POR TIPO DE BASE (Realzza) — meta mensual por tipo de base.
// Alimenta la columna Meta / %avance de la tabla "Ventas por tipo de base" de Ventas
// Realzza (antes venía del Excel; ahora editable en BD).
let metaTipoBaseLista = false;
async function ensureMetaTipoBase() {
  if (!pgPool || metaTipoBaseLista) return;
  await pgPool.query(`
    CREATE TABLE IF NOT EXISTS meta_tipo_base (
      id BIGSERIAL PRIMARY KEY,
      tipo_base TEXT NOT NULL,
      anio INT NOT NULL,
      mes INT NOT NULL,
      meta NUMERIC(14,2) NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ux_meta_tipo_base ON meta_tipo_base (UPPER(tipo_base), anio, mes);`);
  metaTipoBaseLista = true;
}

// GET /meta-tipo-base?anio=&mes= — grid del maestro: un tipo de base (de ventas_realzza)
// por fila con su meta del mes. GET /meta-tipo-base?anio= (sin mes) → mapa de TODO el año
// [{tipo_base, anio, mes, meta}] para poblar la tabla de Ventas Realzza.
app.get('/meta-tipo-base', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    res.set('Cache-Control', 'no-store');
    await ensureMetaTipoBase();
    const anio = parseInt(req.query.anio, 10);
    if (!anio) return res.status(400).json({ success: false, message: 'Falta el anio.' });
    const mes = req.query.mes ? parseInt(req.query.mes, 10) : null;
    if (!mes) {
      // Mapa del año: todas las metas seteadas (para Ventas Realzza).
      const { rows } = await pgPool.query(
        `SELECT UPPER(tipo_base) tipo_base, anio, mes, meta FROM meta_tipo_base WHERE anio = $1`, [anio]);
      return res.json(rows);
    }
    // Grid del mes: lista de tipos de base (de ventas_realzza) + su meta.
    const tipos = await pgPool.query(
      `SELECT DISTINCT UPPER(TRIM(tipo_base)) tipo_base FROM ventas_realzza
       WHERE tipo_base IS NOT NULL AND TRIM(tipo_base) <> '' ORDER BY 1`);
    const metas = await pgPool.query(`SELECT id, UPPER(tipo_base) tipo_base, meta FROM meta_tipo_base WHERE anio = $1 AND mes = $2`, [anio, mes]);
    const map = new Map(metas.rows.map(r => [r.tipo_base, r]));
    const out = tipos.rows.map(t => {
      const m = map.get(t.tipo_base);
      return { id: m?.id || null, tipo_base: t.tipo_base, anio, mes, meta: m ? +m.meta : 0 };
    });
    res.json(out);
  } catch (e) { console.error('❌ GET /meta-tipo-base:', e); res.status(500).json({ success: false, message: e.message }); }
});

// POST /meta-tipo-base — upsert. Body: { tipo_base, anio, mes, meta }.
app.post('/meta-tipo-base', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureMetaTipoBase();
    const b = req.body || {};
    const tipo = String(b.tipo_base || '').trim().toUpperCase();
    const anio = parseInt(b.anio, 10), mes = parseInt(b.mes, 10);
    const meta = Number(String(b.meta ?? '').replace(/[^0-9.]/g, '')) || 0;
    if (!tipo || !anio || !mes) return res.status(400).json({ success: false, message: 'Faltan tipo_base, anio y mes.' });
    await pgPool.query(
      `INSERT INTO meta_tipo_base (tipo_base, anio, mes, meta) VALUES ($1, $2, $3, $4)
       ON CONFLICT (UPPER(tipo_base), anio, mes) DO UPDATE SET meta = EXCLUDED.meta, updated_at = now()`,
      [tipo, anio, mes, meta]);
    res.json({ success: true });
  } catch (e) { console.error('❌ POST /meta-tipo-base:', e); res.status(500).json({ success: false, message: e.message }); }
});

// DELETE /meta-tipo-base/:id — borra la meta.
app.delete('/meta-tipo-base/:id', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureMetaTipoBase();
    const { rowCount } = await pgPool.query('DELETE FROM meta_tipo_base WHERE id = $1', [parseInt(req.params.id, 10)]);
    if (!rowCount) return res.status(404).json({ success: false, message: 'Meta no encontrada.' });
    res.json({ success: true });
  } catch (e) { console.error('❌ DELETE /meta-tipo-base/:id:', e); res.status(500).json({ success: false, message: e.message }); }
});

// GET /ventas-realzza/modulo?anio= — data del MÓDULO Ventas Realzza: UNA fila por venta,
// base en `ventas` (afectaciones PB, COMPLETO para todos los meses) y cruzando con
// `ventas_realzza` (atribución) solo para el TipoBase / TipoProducto. Así:
//   • Estado (NC), fecha de afectación (dia/mes/anio_af), monto y entidad salen de `ventas`.
//   • Una venta como NC → resta en su mes de AF; si no, cuenta en su mes de venta (CV).
//   • TipoBase: respeta el de ventas_realzza (Realzza tal cual / CALL manual); si está
//     vacío, marca 'CALL' por derivación Call — EXCEPTO Brenda (vende como Realzza).
app.get('/ventas-realzza/modulo', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureAtribRealzza();
    const anio = parseInt(req.query.anio, 10) || new Date().getFullYear();
    const rows = await cached(`realzza/modulo|${anio}`, req.query.fresh, async () => (await pgPool.query(`
      WITH ${MAPA_SQL}
      SELECT v.codigo_cv, v.dia_cv, v.mes_cv, v.anio_cv, v.sede, v.monto_consolidado, v.cuota_inicial,
             v.doc_identidad, v.productos, v.cuotas, v.estado_venta, v.entidad, v.vendedor,
             COALESCE(r.asesor_venta, v.asesor_venta) AS asesor_venta,
             v.tipo_credito, COALESCE(r.tipo_producto, v.estado_tipo_producto) AS tipo_producto,
             v.cliente_venta, v.dia_af, v.mes_af, v.anio_af,
             -- sin_derivacion se calcula EN VIVO (igual que Atribución Realzza): es true
             -- solo si NO hay derivación en gestion_realzza para ese DNI (≤31 días antes de
             -- la venta) y la venta es ago-2026+. NO se lee la columna congelada de
             -- ventas_realzza (quedaba desactualizada al derivar después de consolidar).
             (grz.marca_temporal IS NULL AND (v.anio_cv > 2026 OR (v.anio_cv = 2026 AND v.mes_cv >= 8))) AS sin_derivacion,
             COALESCE(r.extranjero, false) AS extranjero,
             COALESCE(r.asesor_manual, false) AS asesor_manual,
             COALESCE(NULLIF(r.tipo_base,''),
               CASE WHEN m.cc IS NOT NULL AND UPPER(COALESCE(v.vendedor,'')) NOT LIKE '%BERNAL BAZAN BRENDA%' THEN 'CALL' END) AS tipo_base
      FROM ventas v
      LEFT JOIN ventas_realzza r ON r.codigo_cv = v.codigo_cv
      LEFT JOIN LATERAL (
        SELECT gc.asesor_contact FROM gestion_call gc
        WHERE regexp_replace(gc.dni_cliente, '\\D', '', 'g') = regexp_replace(v.doc_identidad, '\\D', '', 'g')
          AND gc.motivo_interes = ANY($1)
          AND gc.marca_temporal::date <= v.fecha_cv
          AND v.fecha_cv - gc.marca_temporal::date <= 31
        ORDER BY gc.marca_temporal DESC LIMIT 1
      ) g ON true
      LEFT JOIN LATERAL (
        SELECT gr.marca_temporal FROM gestion_realzza gr
        WHERE regexp_replace(gr.dni_cliente, '\\D', '', 'g') = regexp_replace(v.doc_identidad, '\\D', '', 'g')
          AND gr.motivo_interes = ANY($3)
          AND gr.marca_temporal::date <= v.fecha_cv
          AND v.fecha_cv - gr.marca_temporal::date <= 31
        ORDER BY gr.marca_temporal DESC LIMIT 1
      ) grz ON true
      LEFT JOIN mapa m ON UPPER(TRIM(g.asesor_contact)) = m.nombre
      WHERE v.sede ILIKE '%REALZZA%' AND (v.anio_cv = $2 OR v.anio_af = $2)`, [DERIV_MOTIVOS, anio, DERIV_MOTIVOS_RZ])).rows);
    res.json(rows);
  } catch (e) { console.error('❌ GET /ventas-realzza/modulo:', e); res.status(500).json({ success: false, message: e.message }); }
});

// GET /ventas-realzza/evolutivo — neto REAL mensual para el gráfico evolutivo.
// Por mes = ventas (Realzza-store, por mes de venta) − NC NO refacturadas (por mes de AF).
// Una NC es "refacturada" si el mismo cliente tiene otra venta (no NC) ese mismo mes → no
// resta (se reemplazó). Se acota a los meses que ya tienen ventas en ventas_realzza (evita
// meses viejos con solo-NC en negativo). Así cada mes coincide con el Monto Real (ventas −
// NC + refacturación) del módulo.
app.get('/ventas-realzza/evolutivo', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureAtribRealzza();
    const { rows } = await pgPool.query(`
      WITH meses AS (
        SELECT DISTINCT anio_cv AS anio, mes_cv AS mes FROM ventas_realzza
        WHERE UPPER(COALESCE(estado_venta,'')) NOT LIKE '%NOTA DE%' AND monto_consolidado > 0
          AND anio_cv IS NOT NULL AND mes_cv IS NOT NULL
      ),
      ven AS (
        SELECT anio_cv AS anio, mes_cv AS mes, SUM(monto_consolidado) AS monto FROM ventas
        WHERE sede ILIKE '%REALZZA%' AND UPPER(COALESCE(estado_venta,'')) NOT LIKE '%NOTA DE%' AND monto_consolidado > 0
        GROUP BY anio_cv, mes_cv
      ),
      ncnr AS (
        -- NC que restan: SOLO las "arrastradas" (venta de un mes anterior anulada este mes,
        -- CV ≠ AF). Las NC del MISMO mes (CV=AF) netean a 0 (la venta se hizo y se anuló el
        -- mismo mes; ya está incluida en el Monto total del módulo) → NO restan.
        SELECT n.anio_af AS anio, n.mes_af AS mes, SUM(n.monto_consolidado) AS monto FROM ventas n
        WHERE n.sede ILIKE '%REALZZA%' AND UPPER(COALESCE(n.estado_venta,'')) LIKE '%NOTA DE%'
          AND n.anio_af IS NOT NULL AND n.mes_af IS NOT NULL
          AND NOT (n.anio_cv = n.anio_af AND n.mes_cv = n.mes_af)
        GROUP BY n.anio_af, n.mes_af
      )
      SELECT m.anio, m.mes,
             ROUND(COALESCE(ven.monto,0))::int AS ventas,
             ROUND(COALESCE(ncnr.monto,0))::int AS nc,
             ROUND(COALESCE(ven.monto,0) - COALESCE(ncnr.monto,0))::int AS neto
      FROM meses m
      LEFT JOIN ven  ON ven.anio  = m.anio AND ven.mes  = m.mes
      LEFT JOIN ncnr ON ncnr.anio = m.anio AND ncnr.mes = m.mes
      WHERE m.anio > 0 AND m.mes > 0
      ORDER BY m.anio, m.mes`);
    res.json(rows);
  } catch (e) { console.error('❌ GET /ventas-realzza/evolutivo:', e); res.status(500).json({ success: false, message: e.message }); }
});

// ─────────────────────────────────────────────────────────────────────────────
// 🧲 LEADS KOMMO (Call / Realzza) — para la "maduración de leads"
// Solo se usan para contar los LEADS INGRESADOS por mes (fecha de creación).
// La maduración (meses lead→venta) se calcula aparte cruzando ventas con la
// gestión KOMMO por DNI (en el frontend). Estas tablas guardan la data cruda.
// ─────────────────────────────────────────────────────────────────────────────
const LEADS_COLS = [
  'id', 'nombre_lead', 'contacto_principal', 'responsable', 'embudo',
  'dia_cr', 'mes_cr', 'anio_cr', 'ultima_modificacion', 'modificado_por', 'etiquetas',
];
// Parser tolerante para la "Fecha de creación" de Kommo (dd.mm.yyyy / dd/mm/yyyy,
// con o sin hora). Devuelve {d, m, y}.
function parseFechaDMY(v) {
  if (v instanceof Date && !isNaN(v)) return { d: v.getDate(), m: v.getMonth() + 1, y: v.getFullYear() };
  if (typeof v === 'number') {
    const d = new Date(Math.round((v - 25569) * 86400 * 1000));
    if (!isNaN(d)) return { d: d.getUTCDate(), m: d.getUTCMonth() + 1, y: d.getUTCFullYear() };
  }
  const s = String(v || '').trim();
  if (!s) return { d: null, m: null, y: null };
  const parte = s.split(/[ T]/)[0];
  const p = parte.split(/[\/.\-]/).map(x => parseInt(x, 10));
  if (p.length >= 3 && !p.some(isNaN)) {
    if (p[0] > 31) return { y: p[0], m: p[1], d: p[2] };   // yyyy-mm-dd
    return { d: p[0], m: p[1], y: p[2] };                   // dd/mm/yyyy
  }
  return { d: null, m: null, y: null };
}
function mapLeadRow(r) {
  const id = toInt(pickCol(r, 'ID', 'Id', 'id'));
  if (id === null) return null;
  const f = parseFechaDMY(pickCol(r, 'Fecha de creación', 'Fecha de creacion', 'FECHA DE CREACIÓN', 'fecha_creacion'));
  return [
    id,
    toStr(pickCol(r, 'Nombre del lead', 'nombre_lead')),
    toStr(pickCol(r, 'Contacto principal', 'contacto_principal')),
    toStr(pickCol(r, 'Responsable', 'responsable')),
    toStr(pickCol(r, 'Embudo de ventas', 'embudo')),
    f.d, f.m, f.y,
    toStr(pickCol(r, 'Última modificación el', 'Ultima modificacion el', 'ultima_modificacion')),
    toStr(pickCol(r, 'Modificado por', 'modificado_por')),
    toStr(pickCol(r, 'Etiquetas del lead', 'etiquetas')),
  ];
}
const leadsDdl = (t) => `
  CREATE TABLE IF NOT EXISTS ${t} (
    id            BIGINT PRIMARY KEY,
    nombre_lead   TEXT, contacto_principal TEXT, responsable TEXT, embudo TEXT,
    dia_cr SMALLINT, mes_cr SMALLINT, anio_cr SMALLINT,
    ultima_modificacion TEXT, modificado_por TEXT, etiquetas TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
  );
  CREATE INDEX IF NOT EXISTS ix_${t}_anio_mes ON ${t} (anio_cr, mes_cr);`;
const LEADS = {
  call:    { tabla: 'leads_kommo_call',    cargas: 'leads_kommo_call_cargas' },
  realzza: { tabla: 'leads_kommo_realzza', cargas: 'leads_kommo_realzza_cargas' },
};
const leadsSet = LEADS_COLS.slice(1).map(c => `${c} = EXCLUDED.${c}`).join(', ') + ', updated_at = now()';
const leadsSchemaLista = {};
async function ensureLeadsSchema(canal) {
  const c = LEADS[canal];
  if (!pgPool || !c || leadsSchemaLista[canal]) return;
  await pgPool.query(`
    ${leadsDdl(c.tabla)}
    CREATE TABLE IF NOT EXISTS ${c.cargas} (
      id BIGSERIAL PRIMARY KEY, cargado_por TEXT, archivo TEXT, filas INTEGER,
      insertados INTEGER, actualizados INTEGER, creado_en TIMESTAMPTZ NOT NULL DEFAULT now()
    );`);
  leadsSchemaLista[canal] = true;
}
async function upsertLeadsChunk(client, tabla, chunk) {
  const params = [];
  const tuples = chunk.map((row, i) => {
    const base = i * LEADS_COLS.length; params.push(...row);
    return '(' + LEADS_COLS.map((_, j) => `$${base + j + 1}`).join(',') + ')';
  });
  const sql = `INSERT INTO ${tabla} (${LEADS_COLS.join(',')}) VALUES ${tuples.join(',')}
    ON CONFLICT (id) DO UPDATE SET ${leadsSet} RETURNING (xmax = 0) AS inserted`;
  const { rows } = await client.query(sql, params);
  let inserted = 0; for (const r of rows) if (r.inserted) inserted++;
  return { inserted, updated: rows.length - inserted };
}
function registrarLeads(canal, ruta) {
  const c = LEADS[canal];
  app.post(`/${ruta}/import`, upload.single('archivo'), async (req, res) => {
    if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
    if (!req.file) return res.status(400).json({ success: false, message: 'No se recibió archivo (campo "archivo").' });
    try {
      const wb = XLSX.read(req.file.buffer, { type: 'buffer' });
      const ws = wb.Sheets[wb.SheetNames[0]];
      const raw = XLSX.utils.sheet_to_json(ws, { defval: '' });
      const byId = new Map();
      for (const r of raw) { const m = mapLeadRow(r); if (m) byId.set(m[0], m); }
      const rows = Array.from(byId.values());
      if (rows.length === 0) return res.status(400).json({ success: false, message: 'El archivo no tiene filas válidas (falta la columna ID).' });
      await ensureLeadsSchema(canal);
      const client = await pgPool.connect();
      let insertados = 0, actualizados = 0;
      try {
        await client.query('BEGIN');
        const CHUNK = 1000;
        for (let i = 0; i < rows.length; i += CHUNK) {
          const r = await upsertLeadsChunk(client, c.tabla, rows.slice(i, i + CHUNK));
          insertados += r.inserted; actualizados += r.updated;
        }
        await client.query(`INSERT INTO ${c.cargas} (cargado_por, archivo, filas, insertados, actualizados) VALUES ($1,$2,$3,$4,$5)`,
          [toStr(req.body && req.body.cargado_por), req.file.originalname || null, rows.length, insertados, actualizados]);
        await client.query('COMMIT');
      } catch (e) { await client.query('ROLLBACK'); throw e; } finally { client.release(); }
      res.json({ success: true, filas: rows.length, insertados, actualizados, updated_at: new Date().toISOString() });
    } catch (error) {
      console.error(`❌ Error en POST /${ruta}/import:`, error);
      res.status(500).json({ success: false, message: 'No se pudo importar el archivo.' });
    }
  });
  app.get(`/${ruta}/estado`, async (req, res) => {
    if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
    try {
      await ensureLeadsSchema(canal);
      const { rows } = await pgPool.query(`SELECT COUNT(*)::int AS total, MAX(updated_at) AS updated_at FROM ${c.tabla}`);
      const { rows: cargas } = await pgPool.query(`SELECT cargado_por, archivo, filas, insertados, actualizados, creado_en FROM ${c.cargas} ORDER BY id DESC LIMIT 1`);
      res.json({ success: true, total: rows[0].total, updated_at: rows[0].updated_at, ultimaCarga: cargas[0] || null });
    } catch (error) {
      console.error(`❌ Error en GET /${ruta}/estado:`, error);
      res.status(500).json({ success: false, message: 'No se pudo obtener el estado.' });
    }
  });
  // Conteo de leads ingresados (para "Leads Ingresados"). Opcional ?anio=YYYY.
  // Con ?por=dia agrupa por día (para el gráfico de leads/día); si no, por mes.
  app.get(`/${ruta}`, async (req, res) => {
    if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
    try {
      await ensureLeadsSchema(canal);
      if (req.query.por === 'dia') {
        const params = []; let where = 'WHERE dia_cr IS NOT NULL AND mes_cr IS NOT NULL AND anio_cr IS NOT NULL';
        if (req.query.anio) { params.push(parseInt(req.query.anio, 10)); where += ` AND anio_cr = $${params.length}`; }
        const { rows } = await pgPool.query(
          `SELECT anio_cr AS anio, mes_cr AS mes, dia_cr AS dia, COUNT(*)::int AS total FROM ${c.tabla} ${where}
           GROUP BY anio_cr, mes_cr, dia_cr ORDER BY anio_cr, mes_cr, dia_cr`, params);
        return res.json(rows);
      }
      const params = []; let where = '';
      if (req.query.anio) { params.push(parseInt(req.query.anio, 10)); where = 'WHERE anio_cr = $1'; }
      const { rows } = await pgPool.query(
        `SELECT anio_cr AS anio, mes_cr AS mes, COUNT(*)::int AS total FROM ${c.tabla} ${where}
         GROUP BY anio_cr, mes_cr ORDER BY anio_cr, mes_cr`, params);
      res.json(rows);
    } catch (error) {
      console.error(`❌ Error en GET /${ruta}:`, error);
      res.status(500).json({ success: false, message: 'No se pudieron obtener los leads.' });
    }
  });
}
registrarLeads('call', 'leads-kommo-call');
registrarLeads('realzza', 'leads-kommo-realzza');

// ─────────────────────────────────────────────────────────────────────────────
// 📊 MARGEN DE VENTAS (reemplazo por codigo_cv; uno-a-muchos por producto)
// ─────────────────────────────────────────────────────────────────────────────
const MARGEN_COLS = [
  'codigo_cv', 'fecha', 'cliente', 'producto', 'marca', 'linea_producto',
  'cantidad', 'sede', 'linea_real', 'valor_venta', 'margen_total',
];

let margenSchemaLista = false;
async function ensureMargenSchema() {
  if (!pgPool || margenSchemaLista) return;
  await pgPool.query(`
    CREATE TABLE IF NOT EXISTS margen_ventas (
      id             BIGSERIAL PRIMARY KEY,
      codigo_cv      BIGINT,
      fecha          DATE,
      cliente        TEXT,
      producto       TEXT,
      marca          TEXT,
      linea_producto TEXT,
      cantidad       NUMERIC(14,2),
      sede           TEXT,
      linea_real     TEXT,
      valor_venta    NUMERIC(14,2),
      margen_total   NUMERIC(14,2),
      updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
      created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    CREATE INDEX IF NOT EXISTS ix_margen_codigo ON margen_ventas (codigo_cv);
    CREATE INDEX IF NOT EXISTS ix_margen_sede   ON margen_ventas (sede);
    CREATE INDEX IF NOT EXISTS ix_margen_fecha  ON margen_ventas (fecha);
    CREATE TABLE IF NOT EXISTS margen_ventas_cargas (
      id           BIGSERIAL PRIMARY KEY,
      cargado_por  TEXT,
      archivo      TEXT,
      filas        INTEGER,
      codigos      INTEGER,
      reemplazados INTEGER,
      creado_en    TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);
  margenSchemaLista = true;
}

function mapMargenRow(r) {
  const codigo = toInt(pickCol(r, 'CodigoCV', 'codigo_cv', 'CODIGOCV', 'Codigo CV'));
  if (codigo === null) return null;
  return [
    codigo,
    toFechaISO(pickCol(r, 'Fecha', 'fecha', 'FECHA')),
    toStr(pickCol(r, 'Cliente', 'cliente')),
    toStr(pickCol(r, 'Producto', 'producto')),
    toStr(pickCol(r, 'Marca', 'marca')),
    toStr(pickCol(r, 'LineaProducto', 'Linea Producto', 'linea_producto')),
    toNum(pickCol(r, 'Cantidad', 'cantidad')),
    toStr(pickCol(r, 'SEDE', 'Sede', 'sede')),
    toStr(pickCol(r, 'LINEA REAL', 'LineaReal', 'linea_real')),
    toNum(pickCol(r, 'VALOR VENTA', 'ValorVenta', 'valor_venta')),
    toNum(pickCol(r, 'MARGEN TOTAL', 'MargenTotal', 'margen_total')),
  ];
}

async function insertMargenChunk(client, chunk) {
  const params = [];
  const tuples = chunk.map((row, i) => {
    const base = i * MARGEN_COLS.length;
    params.push(...row);
    return '(' + MARGEN_COLS.map((_, j) => `$${base + j + 1}`).join(',') + ')';
  });
  await client.query(`INSERT INTO margen_ventas (${MARGEN_COLS.join(',')}) VALUES ${tuples.join(',')}`, params);
}

app.post('/margen-ventas/import', upload.single('archivo'), async (req, res) => {
  if (!pgPool) {
    return res.status(500).json({ success: false, message: 'Base de datos no configurada (falta DATABASE_URL).' });
  }
  if (!req.file) {
    return res.status(400).json({ success: false, message: 'No se recibió archivo (campo "archivo").' });
  }
  try {
    const wb = XLSX.read(req.file.buffer, { type: 'buffer', cellDates: true });
    const ws = wb.Sheets[wb.SheetNames[0]];
    const raw = XLSX.utils.sheet_to_json(ws, { defval: '' });

    const rows = [];
    for (const r of raw) {
      const m = mapMargenRow(r);
      if (m) rows.push(m);
    }
    if (rows.length === 0) {
      return res.status(400).json({ success: false, message: 'El archivo no tiene filas válidas (falta la columna CodigoCV).' });
    }

    const codigos = Array.from(new Set(rows.map(r => r[0])));

    await ensureMargenSchema();
    const client = await pgPool.connect();
    let reemplazados = 0;
    try {
      await client.query('BEGIN');
      // Reemplazo por CodigoCV: borra los códigos presentes en el archivo…
      for (let i = 0; i < codigos.length; i += 5000) {
        const slice = codigos.slice(i, i + 5000);
        const del = await client.query('DELETE FROM margen_ventas WHERE codigo_cv = ANY($1::bigint[])', [slice]);
        reemplazados += del.rowCount;
      }
      // …y reinserta todas las filas del archivo.
      const CHUNK = 800;
      for (let i = 0; i < rows.length; i += CHUNK) {
        await insertMargenChunk(client, rows.slice(i, i + CHUNK));
      }
      await client.query(
        `INSERT INTO margen_ventas_cargas (cargado_por, archivo, filas, codigos, reemplazados)
         VALUES ($1,$2,$3,$4,$5)`,
        [toStr(req.body && req.body.cargado_por), req.file.originalname || null, rows.length, codigos.length, reemplazados]
      );
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK');
      throw e;
    } finally {
      client.release();
    }

    res.json({ success: true, filas: rows.length, codigos: codigos.length, reemplazados, updated_at: new Date().toISOString() });
  } catch (error) {
    console.error('❌ Error en POST /margen-ventas/import:', error);
    res.status(500).json({ success: false, message: 'No se pudo importar el archivo de margen.' });
  }
});

app.get('/margen-ventas/estado', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureMargenSchema();
    const { rows } = await pgPool.query('SELECT COUNT(*)::int AS total, MAX(updated_at) AS updated_at FROM margen_ventas');
    const { rows: cargas } = await pgPool.query(
      'SELECT cargado_por, archivo, filas, codigos, reemplazados, creado_en FROM margen_ventas_cargas ORDER BY id DESC LIMIT 1'
    );
    res.json({ success: true, total: rows[0].total, updated_at: rows[0].updated_at, ultimaCarga: cargas[0] || null });
  } catch (error) {
    console.error('❌ Error en GET /margen-ventas/estado:', error);
    res.status(500).json({ success: false, message: 'No se pudo obtener el estado de margen.' });
  }
});

app.get('/margen-ventas', async (req, res) => {
  if (!pgPool) return res.status(500).json({ success: false, message: 'Base de datos no configurada.' });
  try {
    await ensureMargenSchema();
    const cond = [];
    const params = [];
    if (req.query.anio) { params.push(parseInt(req.query.anio, 10)); cond.push(`EXTRACT(YEAR FROM fecha) = $${params.length}`); }
    if (req.query.mes)  { params.push(parseInt(req.query.mes, 10));  cond.push(`EXTRACT(MONTH FROM fecha) = $${params.length}`); }
    if (req.query.sede) { params.push(`%${String(req.query.sede)}%`); cond.push(`sede ILIKE $${params.length}`); }
    const where = cond.length ? 'WHERE ' + cond.join(' AND ') : '';
    const key = `margen-ventas|${req.query.anio || ''}|${req.query.mes || ''}|${req.query.sede || ''}`;
    const rows = await cached(key, req.query.fresh, async () =>
      (await pgPool.query(
        `SELECT * FROM margen_ventas ${where} ORDER BY fecha DESC NULLS LAST, codigo_cv DESC`, params
      )).rows);
    res.json(rows);
  } catch (error) {
    console.error('❌ Error en GET /margen-ventas:', error);
    res.status(500).json({ success: false, message: 'No se pudieron obtener los márgenes.' });
  }
});

// ── Health ───────────────────────────────────────────────────────────────────
app.get('/health', (_req, res) => res.json({
  ok: true, service: 'ventas-service', db: !!pgPool, ts: new Date().toISOString(),
}));

app.listen(PORT, () => console.log(`✅ ventas-service escuchando en http://localhost:${PORT}`));
