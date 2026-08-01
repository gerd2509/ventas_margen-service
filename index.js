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
    const { rows } = await pgPool.query(
      `SELECT * FROM ventas ${where} ORDER BY fecha_cv DESC NULLS LAST, codigo_cv DESC`, params
    );
    res.json(rows);
  } catch (error) {
    console.error('❌ Error en GET /ventas:', error);
    res.status(500).json({ success: false, message: 'No se pudieron obtener las ventas.' });
  }
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
      const cond = [], params = [];
      const anio = req.query.anio ? parseInt(req.query.anio, 10) : null;
      const mes  = req.query.mes  ? parseInt(req.query.mes, 10)  : null;
      if (anio && mes) {
        params.push(anio); const pa = params.length;
        params.push(mes);  const pm = params.length;
        cond.push(`((${px}anio_cv = $${pa} AND ${px}mes_cv = $${pm}) OR (${afA} = $${pa} AND ${afM} = $${pm}))`);
      } else if (anio) { params.push(anio); cond.push(`${px}anio_cv = $${params.length}`); }
      if (req.query.sede)     { params.push(`%${String(req.query.sede)}%`); cond.push(`${px}sede ILIKE $${params.length}`); }
      if (req.query.vendedor) { params.push(String(req.query.vendedor).trim()); cond.push(`${px}vendedor ILIKE $${params.length}`); }
      const where = cond.length ? 'WHERE ' + cond.join(' AND ') : '';

      const sql = j
        ? `SELECT r.codigo_cv, r.dia_cv, r.mes_cv, r.anio_cv, r.sede, r.monto_consolidado, r.cuota_inicial,
                  r.doc_identidad, r.productos, r.cuotas, r.asesor_venta, r.vendedor, r.entidad, r.tipo_base, r.tipo_credito, r.tipo_producto, r.fecha_cv,
                  COALESCE(v.estado_venta, r.estado_venta) AS estado_venta,
                  COALESCE(v.dia_af,  r.dia_af)  AS dia_af,
                  COALESCE(v.mes_af,  r.mes_af)  AS mes_af,
                  COALESCE(v.anio_af, r.anio_af) AS anio_af,
                  make_date(NULLIF(COALESCE(v.anio_af, r.anio_af),0),
                            NULLIF(COALESCE(v.mes_af,  r.mes_af),0),
                            NULLIF(COALESCE(v.dia_af,  r.dia_af),0)) AS fecha_af
           FROM ${c.tabla} r
           LEFT JOIN ventas v ON v.codigo_cv = r.codigo_cv
           ${where}
           ORDER BY r.fecha_cv DESC NULLS LAST, r.codigo_cv DESC`
        : `SELECT * FROM ${c.tabla} ${where} ORDER BY fecha_cv DESC NULLS LAST, codigo_cv DESC`;

      const { rows } = await pgPool.query(sql, params);
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
const ASESORES_CC = [
  ['MORETO DELGADO PATRICIA ESTEFANY', 'CC1'], ['QUISPE FONSECA KAREN AIMEE', 'CC5'],
  ['MORALES ÑIQUE MARIA CANDELARIA', 'CC6'], ['CHANTA CAMPOS KELLY KARINTIA', 'CC8'],
  ['BERNAL BAZAN BRENDA NICOL', 'CC12'], ['TORRES ALVARADO JUDY ESMERALDA', 'CC15'],
  ['CHANAME SOTO ANITA NOEMI', 'CC21'], ['BERNAL BAZAN FABRICIO ROLANDO', 'CC22'],
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
}
// Columnas comunes que devuelven los endpoints de atribución (con las columnas del Excel Call).
const ATRIB_SELECT = `
  v.codigo_cv, v.doc_identidad, v.dia_cv, v.mes_cv, v.anio_cv,
  v.monto_consolidado, v.cuota_inicial, v.cuotas, v.productos, v.sede,
  v.cliente_venta AS cliente, v.tipo_credito AS tipo_venta, v.estado_venta, v.entidad,
  v.asesor_venta AS vendedor, v.atrib_contacto AS contacto,
  v.atrib_tipo_cliente AS tipo_cliente, v.atrib_tipo_base AS tipo_base, v.asesor_manual,
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
      'contacto', 'tipo_base', 'tipo_cliente', 'entidad', 'dia_af', 'mes_af', 'anio_af', 'asesor_manual'];
    // Se RESPETA lo que ya tiene ventas_call en estos campos (la data histórica manda):
    // solo se llenan si están vacíos. Las filas nuevas (agosto en adelante, gestionadas
    // desde el sistema) sí toman el valor del cruce al insertarse.
    const preservar = new Set(['contacto', 'tipo_base', 'tipo_cliente']);
    const setUpd = cols.filter(c => c !== 'codigo_cv').map(c =>
      preservar.has(c)
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
             v.entidad, v.dia_cv AS dia_af, v.mes_cv AS mes_af, v.anio_cv AS anio_af,
             v.asesor_manual
      FROM ventas v ${DERIV_LATERAL}
      WHERE ${cond.join(' AND ')}
      ON CONFLICT (codigo_cv) DO UPDATE SET ${setUpd}, updated_at = now()
      RETURNING (xmax = 0) AS inserted`, params);
    let insertados = 0; for (const r of rows) if (r.inserted) insertados++;
    res.json({ success: true, total: rows.length, insertados, actualizados: rows.length - insertados });
  } catch (e) { console.error('❌ POST /ventas-call/consolidar:', e); res.status(500).json({ success: false, message: e.message }); }
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
    const { rows } = await pgPool.query(
      `SELECT * FROM margen_ventas ${where} ORDER BY fecha DESC NULLS LAST, codigo_cv DESC`, params
    );
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
