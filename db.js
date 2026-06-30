import pg from "pg";
import { randomBytes, pbkdf2Sync, timingSafeEqual } from "crypto";

const { Pool } = pg;

if (!process.env.DATABASE_URL) {
  throw new Error(
    "DATABASE_URL mancante. Configura Heroku Postgres e imposta DATABASE_URL.",
  );
}

const isProduction = process.env.NODE_ENV === "production";
export const db = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: isProduction ? { rejectUnauthorized: false } : undefined,
});

export async function initDb() {
  await db.query(`
    CREATE TABLE IF NOT EXISTS users (
      id BIGSERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      pin_hash TEXT NOT NULL,
      role TEXT NOT NULL DEFAULT 'operator',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS items (
      id BIGSERIAL PRIMARY KEY,
      sku TEXT NOT NULL,
      description TEXT NOT NULL,
      family TEXT NOT NULL DEFAULT '',
      subfamily TEXT NOT NULL DEFAULT '',
      lot TEXT NOT NULL,
      entry_date DATE,
      uom TEXT NOT NULL DEFAULT 'PC',
      initial_qty DOUBLE PRECISION NOT NULL DEFAULT 0,
      value_amount DOUBLE PRECISION NOT NULL DEFAULT 0,
      unit_cost DOUBLE PRECISION NOT NULL DEFAULT 0,
      ownership TEXT NOT NULL DEFAULT '',
      stock_area TEXT NOT NULL DEFAULT '',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(sku, lot)
    );

    CREATE TABLE IF NOT EXISTS movements (
      id BIGSERIAL PRIMARY KEY,
      item_id BIGINT NOT NULL REFERENCES items(id),
      type TEXT NOT NULL CHECK(type IN ('IN','OUT')),
      qty DOUBLE PRECISION NOT NULL CHECK(qty > 0),
      warehouse TEXT NOT NULL DEFAULT 'MAIN',
      location TEXT NOT NULL DEFAULT 'DEFAULT',
      bin TEXT NOT NULL DEFAULT 'DEFAULT',
       equipment TEXT,
      operator_user_id BIGINT REFERENCES users(id),
      ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      note TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_movements_item ON movements(item_id);
    CREATE INDEX IF NOT EXISTS idx_items_sku ON items(sku);
    CREATE INDEX IF NOT EXISTS idx_movements_wh ON movements(warehouse, location, bin);

    CREATE TABLE IF NOT EXISTS bom_headers (
      id BIGSERIAL PRIMARY KEY,
      equipment TEXT NOT NULL UNIQUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS bom_rows (
      id BIGSERIAL PRIMARY KEY,
      bom_id BIGINT NOT NULL REFERENCES bom_headers(id) ON DELETE CASCADE,
      sku TEXT NOT NULL,
      description TEXT NOT NULL DEFAULT '',
      qty_required DOUBLE PRECISION NOT NULL DEFAULT 0,
      qty_reserved DOUBLE PRECISION NOT NULL DEFAULT 0,
      availability TEXT NOT NULL DEFAULT 'MISSING',
      reservation_note TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS stock_reservations (
      id BIGSERIAL PRIMARY KEY,
      equipment TEXT NOT NULL,
      sku TEXT NOT NULL,
      qty_reserved DOUBLE PRECISION NOT NULL CHECK(qty_reserved >= 0),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(equipment, sku)
    );

    CREATE TABLE IF NOT EXISTS printed_labels (
      item_id BIGINT PRIMARY KEY REFERENCES items(id) ON DELETE CASCADE,
      print_count INTEGER NOT NULL DEFAULT 0,
      last_printed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS item_template_options (
      id BIGSERIAL PRIMARY KEY,
      category TEXT NOT NULL,
      value TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(category, value)
    );

    CREATE INDEX IF NOT EXISTS idx_bom_rows_bom ON bom_rows(bom_id);
    CREATE INDEX IF NOT EXISTS idx_bom_rows_sku ON bom_rows(sku);
    CREATE INDEX IF NOT EXISTS idx_stock_reservations_sku ON stock_reservations(sku);
    CREATE INDEX IF NOT EXISTS idx_printed_labels_last ON printed_labels(last_printed_at DESC);
    CREATE INDEX IF NOT EXISTS idx_item_template_options_category
      ON item_template_options(category);
    ALTER TABLE bom_rows DROP CONSTRAINT IF EXISTS bom_rows_bom_id_sku_key;
   
   ALTER TABLE movements ADD COLUMN IF NOT EXISTS equipment TEXT;
    ALTER TABLE bom_rows ADD COLUMN IF NOT EXISTS qty_reserved DOUBLE PRECISION NOT NULL DEFAULT 0;
    ALTER TABLE bom_rows ADD COLUMN IF NOT EXISTS availability TEXT NOT NULL DEFAULT 'MISSING';
    ALTER TABLE bom_rows ADD COLUMN IF NOT EXISTS reservation_note TEXT;
    ALTER TABLE bom_rows ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
    ALTER TABLE bom_rows ADD COLUMN IF NOT EXISTS source_line_no INTEGER;
    ALTER TABLE bom_rows ADD COLUMN IF NOT EXISTS source_family TEXT NOT NULL DEFAULT '';
    ALTER TABLE bom_rows ADD COLUMN IF NOT EXISTS source_dimension TEXT NOT NULL DEFAULT '';
    ALTER TABLE bom_rows ADD COLUMN IF NOT EXISTS source_unit TEXT NOT NULL DEFAULT '';
    ALTER TABLE bom_rows ADD COLUMN IF NOT EXISTS matched_item_id BIGINT REFERENCES items(id);
    ALTER TABLE bom_headers ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'DRAFT';
    ALTER TABLE bom_headers ADD COLUMN IF NOT EXISTS finalized_at TIMESTAMPTZ;
    ALTER TABLE bom_rows DROP CONSTRAINT IF EXISTS bom_rows_bom_id_sku_key;
    ALTER TABLE stock_reservations ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
    ALTER TABLE stock_reservations ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
     ALTER TABLE items ADD COLUMN IF NOT EXISTS family TEXT NOT NULL DEFAULT '';
    ALTER TABLE items ADD COLUMN IF NOT EXISTS subfamily TEXT NOT NULL DEFAULT '';
    ALTER TABLE items ADD COLUMN IF NOT EXISTS value_amount DOUBLE PRECISION NOT NULL DEFAULT 0;
    ALTER TABLE items ADD COLUMN IF NOT EXISTS unit_cost DOUBLE PRECISION NOT NULL DEFAULT 0;
    ALTER TABLE items ADD COLUMN IF NOT EXISTS dimension_1 TEXT NOT NULL DEFAULT '';
    ALTER TABLE items ADD COLUMN IF NOT EXISTS dimension_2 TEXT NOT NULL DEFAULT '';
    ALTER TABLE items ADD COLUMN IF NOT EXISTS ownership TEXT NOT NULL DEFAULT '';
    ALTER TABLE items ADD COLUMN IF NOT EXISTS stock_area TEXT NOT NULL DEFAULT '';
    `);

  const { rows } = await db.query(`SELECT COUNT(*)::int AS n FROM users`);
  if (rows[0].n === 0) {
    const hash = hashPin("1234");
    await db.query(
      `INSERT INTO users (name, pin_hash, role) VALUES ($1, $2, $3)`,
      ["Admin", hash, "admin"],
    );
    console.log(
      "Seeded default admin PIN = 1234 (change it in Admin → Users).",
    );
  }
}

function normalizeEquipment(equipment) {
  return String(equipment || "")
    .trim()
    .toUpperCase();
}

export function hashPin(pin) {
  const salt = randomBytes(16).toString("hex");
  const iterations = 150000;
  const dk = pbkdf2Sync(String(pin), salt, iterations, 32, "sha256").toString(
    "hex",
  );
  return `${salt}$${iterations}$${dk}`;
}

export function verifyPin(pin, stored) {
  try {
    const [salt, itStr, dk] = String(stored).split("$");
    const iterations = Number(itStr);
    const derived = pbkdf2Sync(
      String(pin),
      salt,
      iterations,
      32,
      "sha256",
    ).toString("hex");
    return timingSafeEqual(Buffer.from(derived, "hex"), Buffer.from(dk, "hex"));
  } catch {
    return false;
  }
}

export async function listUsers() {
  const { rows } = await db.query(
    `SELECT id, name, role, created_at FROM users ORDER BY id`,
  );
  return rows;
}

export async function getUserById(id) {
  const { rows } = await db.query(
    `SELECT id, name, role FROM users WHERE id=$1`,
    [id],
  );
  return rows[0] || null;
}

export async function getUserByPin(pin) {
  const { rows } = await db.query(`SELECT id, name, role, pin_hash FROM users`);
  for (const r of rows) {
    if (verifyPin(pin, r.pin_hash)) {
      return { id: r.id, name: r.name, role: r.role };
    }
  }
  return null;
}

export async function createUser({ name, pin, role }) {
  const hash = hashPin(pin);
  await db.query(`INSERT INTO users (name, pin_hash, role) VALUES ($1,$2,$3)`, [
    name,
    hash,
    role || "operator",
  ]);
}

export async function resetUserPin({ user_id, pin }) {
  const hash = hashPin(pin);
  await db.query(`UPDATE users SET pin_hash=$1 WHERE id=$2`, [hash, user_id]);
}

export async function deleteUser({ user_id }) {
  await db.query(`DELETE FROM users WHERE id=$1`, [user_id]);
}

export async function upsertItem({
  sku,
  description,
  family = "",
  subfamily = "",
  lot,
  entry_date,
  uom = "PC",
  initial_qty = 0,
  value_amount = 0,
  unit_cost = 0,
  dimension_1 = '',
  dimension_2 = '',
  ownership = '',
  stock_area = '',
}) {
  await db.query(
    `
    INSERT INTO items (sku, description, family, subfamily, lot, entry_date, uom, initial_qty, value_amount, unit_cost, dimension_1, dimension_2, ownership, stock_area)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
    ON CONFLICT(sku, lot) DO UPDATE SET
      description=EXCLUDED.description,
      family=EXCLUDED.family,
      subfamily=EXCLUDED.subfamily,
      entry_date=EXCLUDED.entry_date,
      uom=EXCLUDED.uom,
      initial_qty=EXCLUDED.initial_qty,
      value_amount=EXCLUDED.value_amount,
      unit_cost=EXCLUDED.unit_cost,
      dimension_1=EXCLUDED.dimension_1,
      dimension_2=EXCLUDED.dimension_2,
      ownership=EXCLUDED.ownership,
      stock_area=EXCLUDED.stock_area,
      updated_at=NOW()
    `,
    [
      sku,
      description,
      family,
      subfamily,
      lot,
      entry_date || null,
      uom,
      initial_qty,
      value_amount,
      unit_cost,
      String(dimension_1 || '').trim(),
      String(dimension_2 || '').trim(),
      String(ownership || '').trim(),
      String(stock_area || '').trim(),
    ],
  );
}

export async function listItems() {
  const { rows } = await db.query(`
     SELECT
      id,
      sku,
      description,
      family,
      subfamily,
      lot,
      entry_date,
      uom,
      initial_qty,
      value_amount,
      unit_cost,
      dimension_1,
      dimension_2,
      ownership,
      stock_area,
      created_at
    FROM items
    ORDER BY created_at DESC
  `);
  return rows;
}

export async function seedItemTemplateOptions(optionsByCategory = {}) {
  const { rows } = await db.query(
    `SELECT COUNT(*)::int AS n FROM item_template_options`,
  );
  if (rows[0].n > 0) return false;

  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const categories = [];
    const optionValues = [];
    for (const [category, categoryValues] of Object.entries(optionsByCategory)) {
      for (const value of categoryValues || []) {
        const cleanValue = String(value || "").trim();
        if (!cleanValue) continue;
        categories.push(category);
        optionValues.push(cleanValue);
      }
    }
    if (optionValues.length) {
      await client.query(
        `INSERT INTO item_template_options (category, value)
         SELECT category, value
         FROM UNNEST($1::text[], $2::text[]) AS option(category, value)
         ON CONFLICT(category, value) DO NOTHING`,
        [categories, optionValues],
      );
    }
    await client.query("COMMIT");
    return true;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function seedItemTemplateOptionCategory(category, values = []) {
  const { rows } = await db.query(
    `SELECT COUNT(*)::int AS n
     FROM item_template_options
     WHERE category=$1`,
    [category],
  );
  if (rows[0].n > 0) return false;

  const cleanValues = Array.from(
    new Set(
      values
        .map((value) => String(value || "").trim())
        .filter(Boolean),
    ),
  );
  if (!cleanValues.length) return false;

  await db.query(
    `INSERT INTO item_template_options (category, value)
     SELECT $1, value
     FROM UNNEST($2::text[]) AS option(value)
     ON CONFLICT(category, value) DO NOTHING`,
    [category, cleanValues],
  );
  return true;
}

export async function listItemTemplateOptions() {
  const { rows } = await db.query(
    `SELECT id, category, value
     FROM item_template_options
     ORDER BY category, UPPER(value), value`,
  );
  return rows;
}

export async function addItemTemplateOption({ category, value }) {
  const cleanValue = String(value || "").trim();
  if (!cleanValue) return false;
  const { rowCount } = await db.query(
    `INSERT INTO item_template_options (category, value)
     SELECT $1, $2
     WHERE NOT EXISTS (
       SELECT 1
       FROM item_template_options
       WHERE category=$1 AND UPPER(TRIM(value))=UPPER(TRIM($2))
     )
     ON CONFLICT(category, value) DO NOTHING`,
    [category, cleanValue],
  );
  return rowCount > 0;
}

export async function deleteItemTemplateOption({ id, category }) {
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const countResult = await client.query(
      `SELECT COUNT(*)::int AS n
       FROM item_template_options
       WHERE category=$1`,
      [category],
    );
    if (countResult.rows[0].n <= 1) {
      await client.query("ROLLBACK");
      return { deleted: false, reason: "last_option" };
    }
    const result = await client.query(
      `DELETE FROM item_template_options WHERE id=$1 AND category=$2`,
      [id, category],
    );
    await client.query("COMMIT");
    return { deleted: result.rowCount > 0, reason: "" };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function getItemById(id) {
  const { rows } = await db.query(`SELECT * FROM items WHERE id=$1`, [id]);
  return rows[0] || null;
}

export async function getItemBySkuLot(sku, lot) {
  const { rows } = await db.query(
    `SELECT * FROM items WHERE sku=$1 AND lot=$2`,
    [sku, lot],
  );
  return rows[0] || null;
}

export async function markLabelsPrinted(itemIds = []) {
  const ids = Array.from(
    new Set(
      (Array.isArray(itemIds) ? itemIds : [])
        .map((id) => Number(id))
        .filter((id) => Number.isFinite(id) && id > 0),
    ),
  );
  if (ids.length === 0) return 0;

  const { rowCount } = await db.query(
    `
    INSERT INTO printed_labels (item_id, print_count, last_printed_at)
    SELECT id, 1, NOW()
    FROM items
    WHERE id = ANY($1::bigint[])
    ON CONFLICT (item_id) DO UPDATE SET
      print_count = printed_labels.print_count + 1,
      last_printed_at = NOW()
    `,
    [ids],
  );
  return rowCount || 0;
}

export async function listPrintedLabels({ search = "", limit = 300 } = {}) {
  const params = [];
  let where = "";
  const q = String(search || "").trim();
  if (q) {
    params.push(`%${q.toUpperCase()}%`);
    where = `WHERE (
      UPPER(i.sku) LIKE $${params.length}
      OR UPPER(i.description) LIKE $${params.length}
      OR UPPER(i.family) LIKE $${params.length}
      OR UPPER(i.subfamily) LIKE $${params.length}
      OR UPPER(i.lot) LIKE $${params.length}
    )`;
  }
  params.push(Number(limit) || 300);

  const { rows } = await db.query(
    `
    SELECT
      i.id,
      i.sku,
      i.description,
      i.family,
      i.subfamily,
      i.lot,
      i.uom,
      p.print_count,
      p.last_printed_at
    FROM printed_labels p
    JOIN items i ON i.id = p.item_id
    ${where}
    ORDER BY p.last_printed_at DESC, i.sku
    LIMIT $${params.length}
    `,
    params,
  );
  return rows;
}

export async function deleteItemById(itemId) {
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    await client.query(`DELETE FROM movements WHERE item_id=$1`, [itemId]);
    const result = await client.query(`DELETE FROM items WHERE id=$1`, [
      itemId,
    ]);
    await client.query("COMMIT");
    return result.rowCount > 0;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function clearAllStockAndMovements() {
  const client = await db.connect();
  try {
    await client.query("BEGIN");
    await client.query(`DELETE FROM movements`);
    await client.query(`DELETE FROM stock_reservations`);
    await client.query(`DELETE FROM bom_rows`);
    await client.query(`DELETE FROM bom_headers`);
    await client.query(`DELETE FROM items`);
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function getOnhandForItemAt(
  { item_id, warehouse, location, bin },
  client = db,
) {
  const { rows } = await client.query(
    `
    SELECT
    (
        MAX(CASE
          WHEN $2 = 'MAIN' AND $3 = 'DEFAULT' AND $4 = 'DEFAULT'
            THEN COALESCE(i.initial_qty, 0)
          ELSE 0
        END)
      )
      + COALESCE(SUM(CASE WHEN m.type='IN' THEN m.qty END),0)
      - COALESCE(SUM(CASE WHEN m.type='OUT' THEN m.qty END),0) AS onhand
    FROM items i
    LEFT JOIN movements m
      ON m.item_id = i.id
     AND m.warehouse = $2
     AND m.location = $3
     AND m.bin = $4
    WHERE i.id=$1
  `,
    [item_id, warehouse, location, bin],
  );
  return Number(rows[0]?.onhand || 0);
}

export async function addMovementChecked({
  item_id,
  type,
  qty,
  warehouse,
  location,
  bin,
  equipment,
  operator_user_id,
  note,
}) {
  const client = await db.connect();
  try {
    await client.query("BEGIN");

    if (type === "OUT") {
      const onhand = await getOnhandForItemAt(
        { item_id, warehouse, location, bin },
        client,
      );
      if (onhand < qty) {
        const err = new Error(
          `Stock insufficiente: on-hand=${onhand}, richiesto OUT=${qty}`,
        );
        err.code = "INSUFFICIENT_STOCK";
        throw err;
      }
    }

    const inserted = await client.query(
      `
     INSERT INTO movements (item_id, type, qty, warehouse, location, bin, equipment, operator_user_id, note)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
       RETURNING id, ts, qty
      `,
      [
        item_id,
        type,
        qty,
        warehouse,
        location,
        bin,
        equipment || null,
        operator_user_id || null,
        note || null,
      ],
    );

    await client.query("COMMIT");
    return inserted.rows[0] || null;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function listMovements(limit = 200) {
  const { rows } = await db.query(
    `
    SELECT m.ts, m.type, m.qty, m.warehouse, m.location, m.bin, m.equipment, m.note,
           i.sku, i.lot, i.description,
           u.name AS operator
    FROM movements m
    JOIN items i ON i.id = m.item_id
    LEFT JOIN users u ON u.id = m.operator_user_id
    ORDER BY m.ts DESC, m.id DESC
    LIMIT $1
    `,
    [limit],
  );
  return rows;
}

export async function getStockRows({ warehouse = null } = {}) {
  const params = [];
  let where = "";
  if (warehouse) {
    params.push(warehouse);
    where = `WHERE m.warehouse = $${params.length}`;
  }

  const { rows } = await db.query(
    `
    WITH agg AS (
      SELECT
       i.id AS item_id,
     i.sku, i.description, i.family, i.subfamily, i.dimension_1, i.dimension_2, i.lot, i.entry_date, i.uom, i.initial_qty, i.ownership, i.stock_area,
        m.warehouse, m.location, m.bin,
        COALESCE(SUM(CASE WHEN m.type='IN' THEN m.qty END),0) AS qty_in,
        COALESCE(SUM(CASE WHEN m.type='OUT' THEN m.qty END),0) AS qty_out
      FROM items i
      LEFT JOIN movements m ON m.item_id = i.id
      ${where}
       GROUP BY i.id, i.sku, i.description, i.family, i.subfamily, i.dimension_1, i.dimension_2, i.lot, i.entry_date, i.uom, i.initial_qty, i.ownership, i.stock_area, m.warehouse, m.location, m.bin
    )
    SELECT
         item_id, sku, description, family, subfamily, dimension_1, dimension_2, lot, entry_date, uom, initial_qty, ownership, stock_area,
      COALESCE(warehouse,'MAIN') AS warehouse,
      COALESCE(location,'DEFAULT') AS location,
      COALESCE(bin,'DEFAULT') AS bin,
      qty_in, qty_out,
      (initial_qty + qty_in - qty_out) AS qty_onhand
    FROM agg
    ORDER BY sku, lot, warehouse, location, bin
    `,
    params,
  );
  return rows;
}

export async function listStockReservations() {
  const { rows } = await db.query(`
    SELECT
      h.equipment,
      r.sku,
      r.qty_required,
      r.qty_reserved,
      GREATEST(r.qty_required - r.qty_reserved, 0) AS qty_to_buy,
      COALESCE(NULLIF(MAX(TRIM(i.uom)), ''), 'PC') AS uom
    FROM bom_rows r
    JOIN bom_headers h ON h.id = r.bom_id
    LEFT JOIN items i ON i.sku = r.sku
    GROUP BY h.equipment, r.sku, r.qty_required, r.qty_reserved
    ORDER BY r.sku, h.equipment
  `);
  return rows;
}

export async function listBomHeaders() {
  const { rows } = await db.query(`
    SELECT h.equipment, h.status, h.finalized_at, h.created_at, h.updated_at, COUNT(r.id)::int AS rows_count
    FROM bom_headers h
    LEFT JOIN bom_rows r ON r.bom_id = h.id
    GROUP BY h.id
    ORDER BY h.updated_at DESC, h.equipment
  `);
  return rows;
}

export async function getBomByEquipment(equipment) {
  const eq = normalizeEquipment(equipment);
  if (!eq) return null;

  const h = await db.query(
    `SELECT id, equipment, status, finalized_at, created_at, updated_at FROM bom_headers WHERE equipment=$1`,
    [eq],
  );
  const header = h.rows[0];
  if (!header) return null;

  const rows = await db.query(
    `
    SELECT id, sku, description, qty_required, qty_reserved, availability, reservation_note,
           source_line_no, source_family, source_dimension, source_unit, matched_item_id, updated_at
    FROM bom_rows
    WHERE bom_id=$1
    ORDER BY COALESCE(source_line_no, 999999), id
    `,
    [header.id],
  );

  return { ...header, rows: rows.rows };
}

export async function listOutMovementsByEquipmentAndSkus(equipment, skus = []) {
  const eq = normalizeEquipment(equipment);
  const normalizedSkus = Array.from(
    new Set(
      (Array.isArray(skus) ? skus : [])
        .map((sku) => String(sku || "").trim())
        .filter(Boolean),
    ),
  );

  if (!eq || normalizedSkus.length === 0) {
    return [];
  }

  const { rows } = await db.query(
    `
    SELECT
      m.id,
      m.ts,
      m.qty,
      i.sku
    FROM movements m
    JOIN items i ON i.id = m.item_id
    WHERE m.type = 'OUT'
      AND COALESCE(TRIM(m.equipment), '') = $1
      AND i.sku = ANY($2::text[])
    ORDER BY m.ts ASC, m.id ASC
    `,
    [eq, normalizedSkus],
  );

  return rows;
}

export async function deleteBomByEquipment(equipment) {
  const eq = normalizeEquipment(equipment);
  if (!eq) {
    throw new Error("Equipment mancante");
  }

  const client = await db.connect();
  try {
    await client.query("BEGIN");

    await client.query(`DELETE FROM stock_reservations WHERE equipment=$1`, [
      eq,
    ]);

    const deleted = await client.query(
      `DELETE FROM bom_headers WHERE equipment=$1`,
      [eq],
    );

    await client.query("COMMIT");
    return deleted.rowCount > 0;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function ensureBomHeader(equipment) {
  const eq = normalizeEquipment(equipment);
  if (!eq) {
    throw new Error("Equipment mancante");
  }

  const { rows } = await db.query(
    `
    INSERT INTO bom_headers (equipment)
    VALUES ($1)
    ON CONFLICT (equipment) DO UPDATE SET updated_at=NOW()
    RETURNING id, equipment, created_at, updated_at
    `,
    [eq],
  );

  return rows[0];
}

export async function upsertBomFromRows(equipment, rows) {
  const eq = normalizeEquipment(equipment);
  if (!eq) {
    throw new Error("Equipment mancante");
  }

  const bomRows = (Array.isArray(rows) ? rows : [])
    .map((r, idx) => {
      const qty = Number(r.qty_required || r.qty_supplier || 0);
      const sourceLineNo = Number(r.source_line_no || r.line_no || idx + 1);
      const sourceFamily = String(r.source_family || r.family || "").trim();
      const sourceDimension = String(r.source_dimension || r.dimension || "").trim();
      const sku = String(r.sku || "").trim() || `__PENDING__-${sourceLineNo}-${idx + 1}`;
      return {
        sku,
        description: String(r.description || "").trim(),
        qty_required: Number.isFinite(qty) ? qty : 0,
        source_line_no: Number.isFinite(sourceLineNo) ? sourceLineNo : idx + 1,
        source_family: sourceFamily,
        source_dimension: sourceDimension,
        source_unit: String(r.source_unit || r.unit || "").trim(),
      };
    })
    .filter((r) => r.qty_required > 0 && r.source_family && r.source_family.toUpperCase() !== "SKIP");

  const client = await db.connect();
  try {
    await client.query("BEGIN");

    const created = await client.query(
      `
      INSERT INTO bom_headers (equipment)
      VALUES ($1)
      ON CONFLICT (equipment) DO UPDATE SET updated_at=NOW(), status='DRAFT', finalized_at=NULL
      RETURNING id, equipment
      `,
      [eq],
    );
    const bomId = created.rows[0].id;

    await client.query(`DELETE FROM stock_reservations WHERE equipment=$1`, [eq]);
    await client.query(`DELETE FROM bom_rows WHERE bom_id=$1`, [bomId]);

    const freeQtyBySku = new Map();
    const stockInfoBySku = new Map();
    const propertyTokens = new Set(["GTS", "LN", "LINDE"]);
    const skuParts = (sku) =>
      String(sku || "")
        .trim()
        .split("-")
        .map((part) => part.trim())
        .filter(Boolean);
    const skuProperty = (sku) => {
      const parts = skuParts(sku);
      const last = String(parts[parts.length - 1] || "").toUpperCase();
      return propertyTokens.has(last) ? last : "";
    };
    const skuWithoutProperty = (sku) => {
      const parts = skuParts(sku);
      const last = String(parts[parts.length - 1] || "").toUpperCase();
      if (propertyTokens.has(last)) parts.pop();
      return parts.join("-").toUpperCase();
    };
    const propertyLabel = (token) => {
      const normalized = String(token || "").toUpperCase();
      if (normalized === "LN" || normalized === "LINDE") return "Linde";
      if (normalized === "GTS") return "GTS";
      return token || "";
    };
    const resolveStockSkuForBomSku = async (requestedSku) => {
      const normalizedSku = String(requestedSku || "").trim();
      const exact = await client.query(
        `SELECT sku FROM items WHERE sku=$1 LIMIT 1`,
        [normalizedSku],
      );
      if (exact.rows[0]?.sku) {
        return {
          requestedSku: normalizedSku,
          stockSku: exact.rows[0].sku,
          propertyMismatch: false,
          requestedProperty: skuProperty(normalizedSku),
          stockProperty: skuProperty(exact.rows[0].sku),
        };
      }

      const requestedBase = skuWithoutProperty(normalizedSku);
      const requestedProperty = skuProperty(normalizedSku);
      if (!requestedBase || !requestedProperty) {
        return {
          requestedSku: normalizedSku,
          stockSku: normalizedSku,
          propertyMismatch: false,
          requestedProperty,
          stockProperty: "",
        };
      }

      const candidates = await client.query(
        `
        SELECT sku
        FROM items
        WHERE UPPER(sku) LIKE $1
        ORDER BY sku
        `,
        [`${requestedBase}-%`],
      );
      const candidate = candidates.rows.find((row) => {
        const candidateSku = String(row.sku || "").trim();
        const candidateProperty = skuProperty(candidateSku);
        return (
          skuWithoutProperty(candidateSku) === requestedBase &&
          candidateProperty &&
          candidateProperty !== requestedProperty
        );
      });

      if (!candidate) {
        return {
          requestedSku: normalizedSku,
          stockSku: normalizedSku,
          propertyMismatch: false,
          requestedProperty,
          stockProperty: "",
        };
      }

      return {
        requestedSku: normalizedSku,
        stockSku: String(candidate.sku || "").trim(),
        propertyMismatch: true,
        requestedProperty,
        stockProperty: skuProperty(candidate.sku),
      };
    };
    const getFreeQtyForSku = async (sku) => {
      const normalizedSku = String(sku || "").trim();
      if (!normalizedSku || normalizedSku.startsWith("__PENDING__")) return null;
      if (freeQtyBySku.has(normalizedSku)) {
        return freeQtyBySku.get(normalizedSku);
      }

      const stockRes = await client.query(
        `
        SELECT
          MIN(i.id) AS item_id,
          MAX(NULLIF(i.description, '')) AS description,
          MAX(NULLIF(i.uom, '')) AS uom,
          COALESCE(SUM(i.initial_qty), 0)
            + COALESCE(SUM(CASE WHEN m.type='IN' THEN m.qty END), 0)
            - COALESCE(SUM(CASE WHEN m.type='OUT' THEN m.qty END), 0) AS qty_onhand
        FROM items i
        LEFT JOIN movements m ON m.item_id = i.id
        WHERE i.sku = $1
        GROUP BY i.sku
        `,
        [normalizedSku],
      );
      const stock = stockRes.rows[0];
      if (!stock) {
        stockInfoBySku.set(normalizedSku, null);
        freeQtyBySku.set(normalizedSku, 0);
        return 0;
      }

      const reservedRes = await client.query(
        `SELECT COALESCE(SUM(qty_reserved),0) AS qty_reserved FROM stock_reservations WHERE sku=$1`,
        [normalizedSku],
      );
      const onhand = Number(stock.qty_onhand || 0);
      const alreadyReserved = Number(reservedRes.rows[0]?.qty_reserved || 0);
      const freeQty = Math.max(0, onhand - alreadyReserved);
      stockInfoBySku.set(normalizedSku, stock);
      freeQtyBySku.set(normalizedSku, freeQty);
      return freeQty;
    };

    for (const row of bomRows) {
      const isPending = row.sku.startsWith("__PENDING__");
      let qtyReserved = 0;
      let availability = "TO_MATCH";
      let reservationNote =
        `Scegli item da famiglia ${row.source_family}${row.source_dimension ? ` / dimensione ${row.source_dimension}` : ""}`;
      let matchedItemId = null;
      let description = row.description || "";
      let stockSku = row.sku;
      let propertyMismatch = false;
      let propertyMismatchNote = "";

      if (!isPending) {
        const match = await resolveStockSkuForBomSku(row.sku);
        stockSku = match.stockSku;
        propertyMismatch = Boolean(match.propertyMismatch);
        propertyMismatchNote = propertyMismatch
          ? `BOQ richiede ${propertyLabel(match.requestedProperty)}, disponibile a stock ${propertyLabel(match.stockProperty)} (${stockSku}). `
          : "";
        const freeQty = await getFreeQtyForSku(stockSku);
        const stockInfo = stockInfoBySku.get(stockSku);
        qtyReserved = Math.max(0, Math.min(row.qty_required, Number(freeQty || 0)));
        freeQtyBySku.set(stockSku, Math.max(0, Number(freeQty || 0) - qtyReserved));
        matchedItemId = stockInfo?.item_id || null;
        if (!description && stockInfo?.description) description = stockInfo.description;
        if (!stockInfo) {
          availability = "MISSING";
          reservationNote = `Non presente a stock: comprare ${row.qty_required} ${row.source_unit || "pz"}`;
        } else if (qtyReserved >= row.qty_required) {
          availability = "OK";
          reservationNote = `${propertyMismatchNote}Presente a stock: riservati ${qtyReserved} ${stockInfo.uom || row.source_unit || ""}`.trim();
        } else if (qtyReserved > 0) {
          availability = "PARTIAL";
          reservationNote = `${propertyMismatchNote}Parziale: riservati ${qtyReserved}, da comprare ${row.qty_required - qtyReserved} ${stockInfo.uom || row.source_unit || ""}`.trim();
        } else {
          availability = "MISSING";
          reservationNote = `${propertyMismatchNote}Stock insufficiente: comprare ${row.qty_required} ${stockInfo?.uom || row.source_unit || ""}`.trim();
        }
      }

      await client.query(
        `
        INSERT INTO bom_rows (
          bom_id, sku, description, qty_required, qty_reserved, availability, reservation_note,
          source_line_no, source_family, source_dimension, source_unit, matched_item_id, updated_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW())
        `,
        [
          bomId,
          stockSku,
          description,
          row.qty_required,
          qtyReserved,
          availability,
          reservationNote,
          row.source_line_no,
          row.source_family,
          row.source_dimension,
          row.source_unit,
          matchedItemId,
        ],
      );

      if (qtyReserved > 0 && !isPending) {
        await client.query(
          `
          INSERT INTO stock_reservations (equipment, sku, qty_reserved, updated_at)
          VALUES ($1,$2,$3,NOW())
          ON CONFLICT (equipment, sku) DO UPDATE SET
            qty_reserved = stock_reservations.qty_reserved + EXCLUDED.qty_reserved,
            updated_at = NOW()
          `,
          [eq, stockSku, qtyReserved],
        );
      }
    }

    await client.query(`UPDATE bom_headers SET updated_at=NOW() WHERE id=$1`, [bomId]);
    await client.query("COMMIT");

    return { equipment: eq, rows_count: bomRows.length };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function consumeBomReservation({
  equipment,
  sku,
  qty,
  consumedAt,
}) {
  const eq = normalizeEquipment(equipment);
  const normalizedSku = String(sku || "").trim();
  const q = Number(qty || 0);
  if (!eq || !normalizedSku || !Number.isFinite(q) || q <= 0) return;

  const client = await db.connect();
  try {
    await client.query("BEGIN");

    await client.query(
      `
      UPDATE stock_reservations
      SET qty_reserved = GREATEST(qty_reserved - $3, 0),
          updated_at = NOW()
      WHERE equipment = $1 AND sku = $2
      `,
      [eq, normalizedSku, q],
    );

    await client.query(
      `
      UPDATE bom_rows r
      SET qty_required = GREATEST(r.qty_required - $3, 0),
          qty_reserved = GREATEST(
            LEAST(r.qty_reserved, GREATEST(r.qty_required - $3, 0)),
            0
          ),
          availability = CASE
            WHEN GREATEST(r.qty_required - $3, 0) <= 0 THEN 'OK'
            WHEN GREATEST(
              LEAST(r.qty_reserved, GREATEST(r.qty_required - $3, 0)),
              0
            ) <= 0 THEN 'MISSING'
            WHEN GREATEST(
              LEAST(r.qty_reserved, GREATEST(r.qty_required - $3, 0)),
              0
            ) < GREATEST(r.qty_required - $3, 0) THEN 'PARTIAL'
            ELSE 'OK'
          END,
          reservation_note = CASE
            WHEN GREATEST(r.qty_required - $3, 0) <= 0 THEN CONCAT(
              'Prelevati ',
              TRIM(($3::double precision)::text),
              ' ',
              COALESCE(
                NULLIF(
                  TRIM(
                    (
                      SELECT MAX(TRIM(it.uom))
                      FROM items it
                      WHERE UPPER(it.sku) = UPPER(r.sku)
                    )
                  ),
                  ''
                ),
                'PC'
              ),
              ' il ',
              TO_CHAR(COALESCE($4::timestamptz, NOW()) AT TIME ZONE 'Europe/Rome', 'DD/MM/YYYY'),
              ', ',
              TO_CHAR(COALESCE($4::timestamptz, NOW()) AT TIME ZONE 'Europe/Rome', 'HH24:MI')
            )
            ELSE CONCAT(
               'ancora da acquistare: ',
              TRIM(
                (
                  GREATEST(r.qty_required - $3, 0) - GREATEST(
                    LEAST(r.qty_reserved, GREATEST(r.qty_required - $3, 0)),
                    0
                  )
                )::text
              ),
              ' ',
              COALESCE(
                NULLIF(
                  TRIM(
                    (
                      SELECT MAX(TRIM(it.uom))
                      FROM items it
                      WHERE UPPER(it.sku) = UPPER(r.sku)
                    )
                  ),
                  ''
                ),
                'PC'
              )
            )
          END,

          updated_at = NOW()
      FROM bom_headers h
      WHERE h.id = r.bom_id
        AND h.equipment = $1
        AND r.sku = $2
      `,
      [eq, normalizedSku, q, consumedAt],
    );

    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

function splitFamilyTokens(value) {
  return String(value || "")
    .toUpperCase()
    .split(/[;,|]+/)
    .map((s) => s.trim())
    .filter(Boolean)
    .filter((s) => s !== "SKIP");
}

function expandFamilyAliases(tokens) {
  const out = new Set(tokens);
  for (const token of tokens) {
    if (token.startsWith("TUBE") || token === "TUBO") out.add("TUB");
    if (token.startsWith("ELB")) out.add("FIT-ELB");
    if (token.startsWith("TEE")) out.add("MISC");
    if (token.startsWith("FIT")) ["MISC", "FIT-END", "FIT-RED", "FIT-ELB"].forEach((x) => out.add(x));
    if (token.startsWith("VAL")) ["VAL", "VAL-DIA", "VAL-BAL", "VAL-BEL"].forEach((x) => out.add(x));
    if (token.startsWith("REG")) out.add("REG");
    if (token.startsWith("MAN")) out.add("GAU");
    if (token.startsWith("PFA")) out.add("PFA-TUB");
    if (token.startsWith("PVDF")) out.add("PVDF-TUB");
    if (token.startsWith("PP")) ["PVDF-TUB", "PFA-TUB", "MISC", "VAL"].forEach((x) => out.add(x));
  }
  return [...out];
}

function dimensionTokens(value) {
  const raw = String(value || "")
    .toUpperCase()
    .replace(/[”″]/g, '"')
    .replace(/[’′]/g, "'")
    .replace(/''/g, '"')
    .replace(/,/g, ".")
    .trim();
  const tokens = new Set();
  if (!raw) return [];

  const addInch = (inch) => {
    const map = {
      "1/4": ["1/4", "1-4", "6.35"],
      "3/8": ["3/8", "3-8", "9.53"],
      "1/2": ["1/2", "1-2", "12.70"],
      "3/4": ["3/4", "3-4", "19.05"],
      "1": ["1", "1IN", "25.40"],
    };
    for (const t of map[inch] || [inch]) tokens.add(t);
  };

  for (const m of raw.matchAll(/\b(1\/4|3\/8|1\/2|3\/4)\b/g)) addInch(m[1]);
  if (/(^|[^0-9\/])1\s*("|IN|$)/.test(raw)) addInch("1");

  for (const m of raw.matchAll(/\bDN\s*(\d+)\b/g)) {
    tokens.add(`DN${m[1]}`);
    tokens.add(`DE${m[1]}`);
  }
  for (const m of raw.matchAll(/\bDE\s*(\d+)\b/g)) {
    tokens.add(`DE${m[1]}`);
    tokens.add(`DN${m[1]}`);
  }
  for (const m of raw.matchAll(/\b(6\.35|9\.53|12\.70|19\.05|25\.40)\b/g)) tokens.add(m[1]);

  return [...tokens].filter(Boolean);
}

function normalizeFamily(value) {
  return String(value || "")
    .trim()
    .toUpperCase();
}

function familyAliases(token) {
  const value = normalizeFamily(token);
  const aliases = {
    TB: ["TB", "TUBO"],
    TUBE: ["TUBE", "TUBO", "TB"],
    EL45: ["EL45", "CURVA 45°", "CURVA 45"],
    EL90: ["EL90", "CURVA 90°", "CURVA 90"],
  };
  return aliases[value] || [value];
}

function familyTokens(value) {
  const tokens = String(value || "")
    .split(/[;,/|]+/)
    .map((v) => normalizeFamily(v))
    .filter(Boolean);
  return Array.from(new Set(tokens.flatMap((token) => familyAliases(token))));
}

function normalizeDimensionValue(value) {
  return String(value || "")
    .toUpperCase()
    .replace(/[”″]/g, '"')
    .replace(/[’′]/g, "'")
    .replace(/''/g, '"')
    .replace(/\s+/g, "")
    .replace(/\"/g, "")
    .replace(/,/g, ".")
    .trim();
}

function normalizeSearchableDimensionText(value) {
  return String(value || "")
    .toUpperCase()
    .replace(/[”″]/g, '"')
    .replace(/[’′]/g, "'")
    .replace(/''/g, '"')
    .replace(/,/g, ".")
    .replace(/\s+/g, "")
    .trim();
}

function tokenAppearsInText(token, text) {
  const t = normalizeSearchableDimensionText(text);
  const k = normalizeSearchableDimensionText(token);
  if (!k || !t) return false;
  if (t.includes(k)) return true;
  if (k.includes("/")) return t.includes(k.replace("/", "-"));
  return false;
}

function dimensionMatchesCandidate(row, requestedDimension) {
  const req = normalizeDimensionValue(requestedDimension);
  if (!req) return true;

  const tokens = dimensionTokens(requestedDimension);
  const fields = [row.dimension_1, row.dimension_2, row.description, row.sku];

  if (tokens.length > 0) {
    return tokens.some((token) => fields.some((field) => tokenAppearsInText(token, field)));
  }

  return fields.some((field) => normalizeDimensionValue(field).includes(req));
}

function scoreCandidate(row, requestedFamilies, requestedDimension) {
  let score = 0;
  const family = normalizeFamily(row.family);
  if (requestedFamilies.includes(family)) score += 70;
  if (dimensionMatchesCandidate(row, requestedDimension)) score += 25;
  if (Number(row.qty_onhand || 0) > 0) score += 5;
  return score;
}


function manualFamilyPrefixes(family) {
  const rawTokens = String(family || "")
    .toUpperCase()
    .split(/[;,/|+\s]+/)
    .map((v) => v.trim())
    .filter(Boolean);

  const prefixes = [];
  for (const token of rawTokens.length ? rawTokens : [String(family || "").toUpperCase().trim()]) {
    if (token.startsWith("FIT") && !prefixes.includes("FIT")) prefixes.push("FIT");
    if (token.startsWith("RED") && !prefixes.includes("RED")) prefixes.push("RED");
    if (token.startsWith("REG") && !prefixes.includes("REG")) prefixes.push("REG");
    if (token.startsWith("VAL") && !prefixes.includes("VAL")) prefixes.push("VAL");
    if (token.startsWith("MAN") && !prefixes.includes("MAN")) prefixes.push("MAN");
  }
  return prefixes;
}

function isManualSubfamilyFamily(family) {
  return manualFamilyPrefixes(family).length > 0;
}

export async function listBomSubfamiliesForFamily({ family }) {
  const prefixes = manualFamilyPrefixes(family);
  const families = familyTokens(family);
  if (prefixes.length === 0 && families.length === 0) return [];

  const params = [];
  let familyWhere = "";

  if (prefixes.length > 0) {
    params.push(prefixes.map((p) => `%${p}%`));
    familyWhere = `UPPER(TRIM(family)) LIKE ANY($${params.length}::text[])`;
  } else {
    params.push(families);
    familyWhere = `UPPER(TRIM(family)) = ANY($${params.length}::text[])`;
  }

  const { rows } = await db.query(
    `
    SELECT
      COALESCE(NULLIF(TRIM(subfamily), ''), '(senza sottofamiglia)') AS subfamily,
      STRING_AGG(DISTINCT NULLIF(TRIM(family), ''), ', ' ORDER BY NULLIF(TRIM(family), '')) AS families,
      COUNT(*)::int AS items_count,
      COALESCE(SUM(initial_qty), 0) AS total_initial_qty
    FROM items
    WHERE ${familyWhere}
    GROUP BY COALESCE(NULLIF(TRIM(subfamily), ''), '(senza sottofamiglia)')
    ORDER BY subfamily
    `,
    params,
  );

  return rows.map((r) => ({
    subfamily: r.subfamily,
    families: r.families || "",
    items_count: Number(r.items_count || 0),
    total_initial_qty: Number(r.total_initial_qty || 0),
  }));
}

export async function listItemsByFamilyForBom({ family, dimension, subfamily = "", subfamilies = [], search = "" }) {
  const prefixes = manualFamilyPrefixes(family);
  const families = familyTokens(family);
  if (prefixes.length === 0 && families.length === 0) return [];

  const params = [];
  let familyWhere = "";

  if (prefixes.length > 0) {
    params.push(prefixes.map((p) => `%${p}%`));
    familyWhere = `UPPER(TRIM(i.family)) LIKE ANY($${params.length}::text[])`;
  } else {
    params.push(families);
    familyWhere = `UPPER(TRIM(i.family)) = ANY($${params.length}::text[])`;
  }

  let selectedSubfamilies = Array.isArray(subfamilies) ? subfamilies : [];
  if (String(subfamily || "").trim()) selectedSubfamilies.push(String(subfamily).trim());
  selectedSubfamilies = Array.from(new Set(selectedSubfamilies.map((v) => String(v || "").trim()).filter(Boolean)));

  let subfamilyWhere = "";
  if (selectedSubfamilies.length > 0) {
    const normal = selectedSubfamilies.filter((v) => v !== "(senza sottofamiglia)");
    const wantsBlank = selectedSubfamilies.includes("(senza sottofamiglia)");
    if (normal.length && wantsBlank) {
      params.push(normal);
      subfamilyWhere = `AND (TRIM(i.subfamily) = ANY($${params.length}::text[]) OR COALESCE(NULLIF(TRIM(i.subfamily), ''), '(senza sottofamiglia)') = '(senza sottofamiglia)')`;
    } else if (normal.length) {
      params.push(normal);
      subfamilyWhere = `AND TRIM(i.subfamily) = ANY($${params.length}::text[])`;
    } else if (wantsBlank) {
      subfamilyWhere = `AND COALESCE(NULLIF(TRIM(i.subfamily), ''), '(senza sottofamiglia)') = '(senza sottofamiglia)'`;
    }
  }

  let searchWhere = "";
  const q = String(search || "").trim();
  if (q) {
    params.push(`%${q.toUpperCase()}%`);
    searchWhere = `AND (
      UPPER(i.sku) LIKE $${params.length}
      OR UPPER(i.description) LIKE $${params.length}
      OR UPPER(COALESCE(i.family,'')) LIKE $${params.length}
      OR UPPER(COALESCE(i.subfamily,'')) LIKE $${params.length}
      OR UPPER(COALESCE(i.dimension_1,'')) LIKE $${params.length}
      OR UPPER(COALESCE(i.dimension_2,'')) LIKE $${params.length}
      OR UPPER(COALESCE(i.lot,'')) LIKE $${params.length}
    )`;
  }

  const { rows } = await db.query(
    `
    WITH stock AS (
      SELECT
        i.id AS item_id,
        i.sku,
        i.description,
        i.family,
        i.subfamily,
        i.dimension_1,
        i.dimension_2,
        i.lot,
        i.uom,
        i.initial_qty,
        COALESCE(SUM(CASE WHEN m.type='IN' THEN m.qty END),0) AS qty_in,
        COALESCE(SUM(CASE WHEN m.type='OUT' THEN m.qty END),0) AS qty_out
      FROM items i
      LEFT JOIN movements m ON m.item_id = i.id
      WHERE ${familyWhere}
        ${subfamilyWhere}
        ${searchWhere}
      GROUP BY i.id, i.sku, i.description, i.family, i.subfamily, i.dimension_1, i.dimension_2, i.lot, i.uom, i.initial_qty
    )
    SELECT *, (initial_qty + qty_in - qty_out) AS qty_onhand
    FROM stock
    ORDER BY family, subfamily, sku, lot
    LIMIT 700
    `,
    params,
  );

  const scoreFamilies = prefixes.length ? rows.map((r) => normalizeFamily(r.family)) : families;
  const withScore = rows
    .map((row) => ({
      ...row,
      score: scoreCandidate(row, scoreFamilies, dimension),
      dimension_match: dimensionMatchesCandidate(row, dimension),
    }))
    .sort((a, b) => Number(b.score || 0) - Number(a.score || 0) || String(a.sku).localeCompare(String(b.sku)));

  const requestedDimension = normalizeDimensionValue(dimension);
  if (requestedDimension) {
    const dimensionRows = withScore.filter((row) => row.dimension_match);
    if (dimensionRows.length > 0) return dimensionRows;
  }

  return withScore;
}

export async function markBomRowToBuy({ bomRowId, qtyToBuy = null }) {
  const rowId = Number(bomRowId);
  if (!Number.isFinite(rowId)) throw new Error("Riga BOM non valida");

  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const rowRes = await client.query(
      `
      SELECT r.*, h.equipment
      FROM bom_rows r
      JOIN bom_headers h ON h.id = r.bom_id
      WHERE r.id=$1
      FOR UPDATE
      `,
      [rowId],
    );
    const bomRow = rowRes.rows[0];
    if (!bomRow) throw new Error("Riga BOM non trovata");

    const oldSku = String(bomRow.sku || "").trim();
    if (oldSku && !oldSku.startsWith("__PENDING__") && oldSku !== "TO_BUY") {
      await client.query(
        `DELETE FROM stock_reservations WHERE equipment=$1 AND sku=$2`,
        [bomRow.equipment, oldSku],
      );
    }

    const toBuySku = "TO_BUY";
    const uom = bomRow.source_unit || "PC";
    const requestedToBuy = Number(qtyToBuy || 0);
    const finalQtyToBuy = Number.isFinite(requestedToBuy) && requestedToBuy > 0
      ? Math.min(requestedToBuy, Number(bomRow.qty_required || 0))
      : Number(bomRow.qty_required || 0);
    await client.query(
      `
      UPDATE bom_rows
      SET sku=$2,
          qty_required=$4,
          qty_reserved=0,
          availability='TO_BUY',
          reservation_note=$3,
          matched_item_id=NULL,
          updated_at=NOW()
      WHERE id=$1
      `,
      [rowId, toBuySku, `Da acquistare: ${finalQtyToBuy} ${uom}`, finalQtyToBuy],
    );
    await client.query(`UPDATE bom_headers SET updated_at=NOW() WHERE id=$1`, [bomRow.bom_id]);
    await client.query("COMMIT");
    return { ok: true, availability: "TO_BUY" };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function finalizeBom(equipment) {
  const eq = normalizeEquipment(equipment);
  if (!eq) throw new Error("Equipment mancante");

  const client = await db.connect();
  try {
    await client.query("BEGIN");
    const h = await client.query(`SELECT id FROM bom_headers WHERE equipment=$1 FOR UPDATE`, [eq]);
    const header = h.rows[0];
    if (!header) throw new Error("BOM non trovato");

    const pending = await client.query(
      `
      SELECT COUNT(*)::int AS n
      FROM bom_rows
      WHERE bom_id=$1
        AND (availability='TO_MATCH' OR sku LIKE '__PENDING__%')
      `,
      [header.id],
    );
    const pendingCount = Number(pending.rows[0]?.n || 0);
    if (pendingCount > 0) {
      await client.query("ROLLBACK");
      return { ok: false, pending: pendingCount };
    }

    await client.query(
      `UPDATE bom_headers SET status='FINALIZED', finalized_at=NOW(), updated_at=NOW() WHERE id=$1`,
      [header.id],
    );
    await client.query("COMMIT");
    return { ok: true, pending: 0 };
  } catch (error) {
    try { await client.query("ROLLBACK"); } catch {}
    throw error;
  } finally {
    client.release();
  }
}

export async function getBomRowById(rowId) {
  const { rows } = await db.query(
    `
    SELECT r.*, h.equipment
    FROM bom_rows r
    JOIN bom_headers h ON h.id = r.bom_id
    WHERE r.id = $1
    `,
    [rowId],
  );
  return rows[0] || null;
}

export async function matchBomRowToItem({ bomRowId, itemId }) {
  const rowId = Number(bomRowId);
  const id = Number(itemId);
  if (!Number.isFinite(rowId) || !Number.isFinite(id)) {
    throw new Error("Riga BOM o item non valido");
  }

  const client = await db.connect();
  try {
    await client.query("BEGIN");

    const rowRes = await client.query(
      `
      SELECT r.*, h.equipment
      FROM bom_rows r
      JOIN bom_headers h ON h.id = r.bom_id
      WHERE r.id=$1
      FOR UPDATE
      `,
      [rowId],
    );
    const bomRow = rowRes.rows[0];
    if (!bomRow) throw new Error("Riga BOM non trovata");

    const itemRes = await client.query(`SELECT * FROM items WHERE id=$1`, [id]);
    const item = itemRes.rows[0];
    if (!item) throw new Error("Item stock non trovato");

    const originalBomQty = Number(bomRow.qty_required || 0);
    const selectedQtyTotal = selected.reduce((sum, x) => sum + Number(x.qty_required || 0), 0);

    const oldSku = String(bomRow.sku || "").trim();
    if (oldSku && !oldSku.startsWith("__PENDING__")) {
      await client.query(
        `
        UPDATE stock_reservations
        SET qty_reserved = GREATEST(qty_reserved - $3, 0), updated_at = NOW()
        WHERE equipment = $1 AND sku = $2
        `,
        [bomRow.equipment, oldSku, Number(bomRow.qty_reserved || 0)],
      );
    }

    const onhandRes = await client.query(
      `
      SELECT
        SUM(i.initial_qty)
        + COALESCE(SUM(CASE WHEN m.type='IN' THEN m.qty END),0)
        - COALESCE(SUM(CASE WHEN m.type='OUT' THEN m.qty END),0) AS qty_onhand
      FROM items i
      LEFT JOIN movements m ON m.item_id = i.id
      WHERE i.sku = $1
      `,
      [item.sku],
    );
    const onhand = Number(onhandRes.rows[0]?.qty_onhand || 0);

    const reservedRes = await client.query(
      `SELECT COALESCE(SUM(qty_reserved),0) AS qty_reserved FROM stock_reservations WHERE sku=$1`,
      [item.sku],
    );
    const alreadyReserved = Number(reservedRes.rows[0]?.qty_reserved || 0);
    const freeQty = Math.max(0, onhand - alreadyReserved);
    const qtyRequired = Number(bomRow.qty_required || 0);
    const qtyReserved = Math.max(0, Math.min(qtyRequired, freeQty));
    const qtyMissing = Math.max(0, qtyRequired - qtyReserved);
    let availability = "MISSING";
    if (qtyReserved >= qtyRequired) availability = "OK";
    else if (qtyReserved > 0) availability = "PARTIAL";

    const note =
      availability === "OK"
        ? `Match confermato: ${item.sku}`
        : `Match confermato: ${item.sku} • ancora da acquistare: ${qtyMissing} ${item.uom || bomRow.source_unit || "PC"}`;

    await client.query(
      `
      UPDATE bom_rows
      SET sku=$2,
          description=COALESCE(NULLIF(description,''), $3),
          qty_reserved=$4,
          availability=$5,
          reservation_note=$6,
          matched_item_id=$7,
          updated_at=NOW()
      WHERE id=$1
      `,
      [rowId, item.sku, item.description || "", qtyReserved, availability, note, item.id],
    );

    if (qtyReserved > 0) {
      await client.query(
        `
        INSERT INTO stock_reservations (equipment, sku, qty_reserved, updated_at)
        VALUES ($1,$2,$3,NOW())
        ON CONFLICT (equipment, sku) DO UPDATE SET
          qty_reserved = stock_reservations.qty_reserved + EXCLUDED.qty_reserved,
          updated_at = NOW()
        `,
        [bomRow.equipment, item.sku, qtyReserved],
      );
    }

    await client.query(`UPDATE bom_headers SET updated_at=NOW() WHERE id=$1`, [bomRow.bom_id]);
    await client.query("COMMIT");

    return { sku: item.sku, availability, qty_reserved: qtyReserved };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}


export async function matchBomRowToMultipleItems({ bomRowId, items = [] }) {
  const rowId = Number(bomRowId);
  const selected = (Array.isArray(items) ? items : [])
    .map((x) => ({ item_id: Number(x.item_id), qty_required: Number(x.qty_required || x.qty || 0) }))
    .filter((x) => Number.isFinite(x.item_id) && x.item_id > 0 && Number.isFinite(x.qty_required) && x.qty_required > 0);

  if (!Number.isFinite(rowId) || rowId <= 0) throw new Error("Riga BOM non valida");
  if (selected.length === 0) throw new Error("Nessun item selezionato");

  const client = await db.connect();
  try {
    await client.query("BEGIN");

    const rowRes = await client.query(
      `
      SELECT r.*, h.equipment
      FROM bom_rows r
      JOIN bom_headers h ON h.id = r.bom_id
      WHERE r.id=$1
      FOR UPDATE
      `,
      [rowId],
    );
    const bomRow = rowRes.rows[0];
    if (!bomRow) throw new Error("Riga BOM non trovata");

    const originalBomQty = Number(bomRow.qty_required || 0);
    const selectedQtyTotal = selected.reduce((sum, x) => sum + Number(x.qty_required || 0), 0);

    const oldSku = String(bomRow.sku || "").trim();
    if (oldSku && !oldSku.startsWith("__PENDING__") && oldSku !== "TO_BUY") {
      await client.query(
        `UPDATE stock_reservations SET qty_reserved = GREATEST(qty_reserved - $3, 0), updated_at=NOW() WHERE equipment=$1 AND sku=$2`,
        [bomRow.equipment, oldSku, Number(bomRow.qty_reserved || 0)],
      );
    }

    const inserted = [];
    let firstDone = false;
    for (const sel of selected) {
      const itemRes = await client.query(
        `
        SELECT
          i.*,
          COALESCE(SUM(CASE WHEN m.type='IN' THEN m.qty END),0) AS qty_in,
          COALESCE(SUM(CASE WHEN m.type='OUT' THEN m.qty END),0) AS qty_out,
          (i.initial_qty + COALESCE(SUM(CASE WHEN m.type='IN' THEN m.qty END),0) - COALESCE(SUM(CASE WHEN m.type='OUT' THEN m.qty END),0)) AS qty_onhand
        FROM items i
        LEFT JOIN movements m ON m.item_id = i.id
        WHERE i.id=$1
        GROUP BY i.id
        `,
        [sel.item_id],
      );
      const item = itemRes.rows[0];
      if (!item) continue;

      const reservedRes = await client.query(
        `SELECT COALESCE(SUM(qty_reserved),0) AS qty_reserved FROM stock_reservations WHERE sku=$1`,
        [item.sku],
      );
      const alreadyReserved = Number(reservedRes.rows[0]?.qty_reserved || 0);
      const onhand = Number(item.qty_onhand || 0);
      const freeQty = Math.max(0, onhand - alreadyReserved);
      const qtyRequired = Number(sel.qty_required || 0);
      const qtyReserved = Math.max(0, Math.min(qtyRequired, freeQty));
      const qtyMissing = Math.max(0, qtyRequired - qtyReserved);
      let availability = "MISSING";
      if (qtyReserved >= qtyRequired) availability = "OK";
      else if (qtyReserved > 0) availability = "PARTIAL";
      const note = availability === "OK"
        ? `Match confermato: ${item.sku}`
        : `Match confermato: ${item.sku} • ancora da acquistare: ${qtyMissing} ${item.uom || bomRow.source_unit || "PC"}`;

      if (!firstDone) {
        await client.query(
          `
          UPDATE bom_rows
          SET sku=$2,
              description=COALESCE(NULLIF(description,''), $3),
              qty_required=$4,
              qty_reserved=$5,
              availability=$6,
              reservation_note=$7,
              matched_item_id=$8,
              source_family=$9,
              source_dimension=$10,
              source_unit=$11,
              updated_at=NOW()
          WHERE id=$1
          `,
          [
            rowId,
            item.sku,
            item.description || "",
            qtyRequired,
            qtyReserved,
            availability,
            note,
            item.id,
            bomRow.source_family || item.family || "",
            bomRow.source_dimension || [item.dimension_1, item.dimension_2].filter(Boolean).join(" / "),
            bomRow.source_unit || item.uom || "PC",
          ],
        );
        firstDone = true;
      } else {
        const sourceLineRes = await client.query(
          `SELECT COALESCE(MAX(source_line_no), 0) + 1 AS next_line FROM bom_rows WHERE bom_id=$1`,
          [bomRow.bom_id],
        );
        const sourceLineNo = Number(sourceLineRes.rows[0]?.next_line || 1);
        const insertRes = await client.query(
          `
          INSERT INTO bom_rows (
            bom_id, sku, description, qty_required, qty_reserved, availability, reservation_note,
            source_line_no, source_family, source_dimension, source_unit, matched_item_id, source_type, updated_at
          )
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'BOM',NOW())
          RETURNING id
          `,
          [
            bomRow.bom_id,
            item.sku,
            item.description || "",
            qtyRequired,
            qtyReserved,
            availability,
            note,
            sourceLineNo,
            bomRow.source_family || item.family || "",
            bomRow.source_dimension || [item.dimension_1, item.dimension_2].filter(Boolean).join(" / "),
            bomRow.source_unit || item.uom || "PC",
            item.id,
          ],
        );
        inserted.push(insertRes.rows[0]);
      }

      if (qtyReserved > 0) {
        await client.query(
          `
          INSERT INTO stock_reservations (equipment, sku, qty_reserved, updated_at)
          VALUES ($1,$2,$3,NOW())
          ON CONFLICT (equipment, sku) DO UPDATE SET
            qty_reserved = stock_reservations.qty_reserved + EXCLUDED.qty_reserved,
            updated_at = NOW()
          `,
          [bomRow.equipment, item.sku, qtyReserved],
        );
      }
    }

    const residualToBuy = Math.max(0, originalBomQty - selectedQtyTotal);
    if (residualToBuy > 0.000001) {
      const sourceLineRes = await client.query(
        `SELECT COALESCE(MAX(source_line_no), 0) + 1 AS next_line FROM bom_rows WHERE bom_id=$1`,
        [bomRow.bom_id],
      );
      const sourceLineNo = Number(sourceLineRes.rows[0]?.next_line || 1);
      await client.query(
        `
        INSERT INTO bom_rows (
          bom_id, sku, description, qty_required, qty_reserved, availability, reservation_note,
          source_line_no, source_family, source_dimension, source_unit, matched_item_id, source_type, updated_at
        )
        VALUES ($1,'TO_BUY',$2,$3,0,'TO_BUY',$4,$5,$6,$7,$8,NULL,'BOM',NOW())
        `,
        [
          bomRow.bom_id,
          bomRow.description || '',
          residualToBuy,
          `Da acquistare: ${residualToBuy} ${bomRow.source_unit || 'PC'}`,
          sourceLineNo,
          bomRow.source_family || '',
          bomRow.source_dimension || '',
          bomRow.source_unit || 'PC',
        ],
      );
      inserted.push({ to_buy: true });
    }

    await client.query(`UPDATE bom_headers SET updated_at=NOW() WHERE id=$1`, [bomRow.bom_id]);
    await client.query("COMMIT");
    return { ok: true, inserted_count: inserted.length + 1 };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}


export async function searchStockItemsForManual({ search = "", limit = 200 } = {}) {
  const q = String(search || "").trim();
  const params = [];
  let where = "";
  if (q) {
    params.push(`%${q.toUpperCase()}%`);
    where = `WHERE UPPER(CONCAT_WS(' ', sku, description, family, subfamily, dimension_1, dimension_2, lot, uom)) LIKE $1`;
  }
  params.push(Number(limit) || 200);

  const { rows } = await db.query(
    `
    WITH agg AS (
      SELECT
        i.id AS item_id,
        i.sku,
        i.description,
        i.family,
        i.subfamily,
        i.dimension_1,
        i.dimension_2,
        i.lot,
        i.uom,
        i.initial_qty,
        COALESCE(SUM(CASE WHEN m.type='IN' THEN m.qty END),0) AS qty_in,
        COALESCE(SUM(CASE WHEN m.type='OUT' THEN m.qty END),0) AS qty_out
      FROM items i
      LEFT JOIN movements m ON m.item_id = i.id
      GROUP BY i.id, i.sku, i.description, i.family, i.subfamily, i.dimension_1, i.dimension_2, i.lot, i.uom, i.initial_qty
    )
    SELECT
      item_id, sku, description, family, subfamily, dimension_1, dimension_2, lot, uom,
      initial_qty, qty_in, qty_out, (initial_qty + qty_in - qty_out) AS qty_onhand
    FROM agg
    ${where}
    ORDER BY sku, lot
    LIMIT $${params.length}
    `,
    params,
  );
  return rows;
}

export async function addManualItemsToBom({ equipment, items = [] }) {
  const eq = normalizeEquipment(equipment);
  if (!eq) throw new Error("Equipment mancante");
  const rowsToAdd = (Array.isArray(items) ? items : [])
    .map((item) => ({
      item_id: Number(item.item_id),
      qty_required: Number(item.qty_required || item.qty || 0),
    }))
    .filter((item) => Number.isFinite(item.item_id) && item.item_id > 0 && Number.isFinite(item.qty_required) && item.qty_required > 0);

  if (rowsToAdd.length === 0) throw new Error("Nessun item valido da aggiungere");

  const client = await db.connect();
  try {
    await client.query("BEGIN");

    const header = await client.query(
      `
      INSERT INTO bom_headers (equipment)
      VALUES ($1)
      ON CONFLICT (equipment) DO UPDATE SET updated_at=NOW(), status='DRAFT', finalized_at=NULL
      RETURNING id, equipment
      `,
      [eq],
    );
    const bomId = header.rows[0].id;

    const inserted = [];
    for (const item of rowsToAdd) {
      const itemRes = await client.query(
        `
        SELECT
          i.*,
          COALESCE(SUM(CASE WHEN m.type='IN' THEN m.qty END),0) AS qty_in,
          COALESCE(SUM(CASE WHEN m.type='OUT' THEN m.qty END),0) AS qty_out,
          (i.initial_qty + COALESCE(SUM(CASE WHEN m.type='IN' THEN m.qty END),0) - COALESCE(SUM(CASE WHEN m.type='OUT' THEN m.qty END),0)) AS qty_onhand
        FROM items i
        LEFT JOIN movements m ON m.item_id = i.id
        WHERE i.id=$1
        GROUP BY i.id
        `,
        [item.item_id],
      );
      const stockItem = itemRes.rows[0];
      if (!stockItem) continue;

      const requiredQty = item.qty_required;
      const onhandQty = Number(stockItem.qty_onhand || 0);
      const reservedQty = Math.max(0, Math.min(requiredQty, onhandQty));
      const qtyToBuy = Math.max(0, requiredQty - reservedQty);
      const availability = qtyToBuy > 0 ? "TO_BUY" : "OK";
      const note = qtyToBuy > 0
        ? `TO BUY ${qtyToBuy} ${String(stockItem.uom || "PC").trim() || "PC"}`
        : `Aggiunto manualmente da stock. Disponibile ${onhandQty} ${String(stockItem.uom || "PC").trim() || "PC"}`;

      const sourceLineRes = await client.query(
        `SELECT COALESCE(MAX(source_line_no), 0) + 1 AS next_line FROM bom_rows WHERE bom_id=$1`,
        [bomId],
      );
      const sourceLineNo = Number(sourceLineRes.rows[0]?.next_line || 1);

      const insertedRow = await client.query(
        `
        INSERT INTO bom_rows (
          bom_id, sku, description, qty_required, qty_reserved, availability, reservation_note,
          source_line_no, source_family, source_dimension, source_unit, matched_item_id, source_type, updated_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'MANUAL',NOW())
        RETURNING id, sku, qty_required, qty_reserved, availability
        `,
        [
          bomId,
          stockItem.sku,
          stockItem.description || "",
          requiredQty,
          reservedQty,
          availability,
          note,
          sourceLineNo,
          stockItem.family || "",
          [stockItem.dimension_1, stockItem.dimension_2].filter(Boolean).join(" / "),
          stockItem.uom || "PC",
          stockItem.id,
        ],
      );

      if (reservedQty > 0) {
        await client.query(
          `
          INSERT INTO stock_reservations (equipment, sku, qty_reserved, updated_at)
          VALUES ($1,$2,$3,NOW())
          ON CONFLICT (equipment, sku) DO UPDATE SET
            qty_reserved = stock_reservations.qty_reserved + EXCLUDED.qty_reserved,
            updated_at = NOW()
          `,
          [eq, stockItem.sku, reservedQty],
        );
      }

      inserted.push(insertedRow.rows[0]);
    }

    await client.query(`UPDATE bom_headers SET updated_at=NOW() WHERE id=$1`, [bomId]);
    await client.query("COMMIT");
    return { equipment: eq, inserted };
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}


export async function listWarehouses() {
  const { rows } = await db.query(`
    SELECT DISTINCT warehouse FROM movements
    UNION
    SELECT 'MAIN' AS warehouse
    ORDER BY warehouse
  `);
  return rows.map((r) => r.warehouse);
}

export async function listLocations(warehouse) {
  const { rows } = await db.query(
    `
    SELECT DISTINCT location FROM movements WHERE warehouse=$1
    UNION SELECT 'DEFAULT'
    ORDER BY location
    `,
    [warehouse],
  );
  return rows.map((r) => r.location);
}

export async function listBins(warehouse, location) {
  const { rows } = await db.query(
    `
    SELECT DISTINCT bin FROM movements WHERE warehouse=$1 AND location=$2
    UNION SELECT 'DEFAULT'
    ORDER BY bin
    `,
    [warehouse, location],
  );
  return rows.map((r) => r.bin);
}

export async function findItemByTechnicalKey(technicalKey) {
  const { rows } = await db.query(
    `
    SELECT sku, description, family, subfamily
    FROM items
    WHERE subfamily = $1
    ORDER BY sku
    LIMIT 1
    `,
    [technicalKey],
  );

  return rows[0] || null;
}
