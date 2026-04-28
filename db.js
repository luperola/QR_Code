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
      lot TEXT NOT NULL,
      entry_date DATE,
      uom TEXT NOT NULL DEFAULT 'PC',
      initial_qty DOUBLE PRECISION NOT NULL DEFAULT 0,
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
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE(bom_id, sku)
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

    CREATE INDEX IF NOT EXISTS idx_bom_rows_bom ON bom_rows(bom_id);
    CREATE INDEX IF NOT EXISTS idx_stock_reservations_sku ON stock_reservations(sku);
   
   ALTER TABLE movements ADD COLUMN IF NOT EXISTS equipment TEXT;
    ALTER TABLE bom_rows ADD COLUMN IF NOT EXISTS qty_reserved DOUBLE PRECISION NOT NULL DEFAULT 0;
    ALTER TABLE bom_rows ADD COLUMN IF NOT EXISTS availability TEXT NOT NULL DEFAULT 'MISSING';
    ALTER TABLE bom_rows ADD COLUMN IF NOT EXISTS reservation_note TEXT;
    ALTER TABLE bom_rows ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
    ALTER TABLE stock_reservations ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
    ALTER TABLE stock_reservations ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
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
  lot,
  entry_date,
  uom = "PC",
  initial_qty = 0,
}) {
  await db.query(
    `
    INSERT INTO items (sku, description, lot, entry_date, uom, initial_qty)
    VALUES ($1,$2,$3,$4,$5,$6)
    ON CONFLICT(sku, lot) DO UPDATE SET
      description=EXCLUDED.description,
      entry_date=EXCLUDED.entry_date,
      uom=EXCLUDED.uom,
      initial_qty=EXCLUDED.initial_qty,
      updated_at=NOW()
    `,
    [sku, description, lot, entry_date || null, uom, initial_qty],
  );
}

export async function listItems() {
  const { rows } = await db.query(`
    SELECT id, sku, description, lot, entry_date, uom, initial_qty, created_at
    FROM items
    ORDER BY created_at DESC
  `);
  return rows;
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
    await client.query(`DELETE FROM items`);
    await client.query(`DELETE FROM stock_reservations`);
    await client.query(`DELETE FROM bom_rows`);
    await client.query(`DELETE FROM bom_headers`);
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
      i.sku, i.description, i.lot, i.entry_date, i.uom, i.initial_qty,
        m.warehouse, m.location, m.bin,
        COALESCE(SUM(CASE WHEN m.type='IN' THEN m.qty END),0) AS qty_in,
        COALESCE(SUM(CASE WHEN m.type='OUT' THEN m.qty END),0) AS qty_out
      FROM items i
      LEFT JOIN movements m ON m.item_id = i.id
      ${where}
       GROUP BY i.id, i.sku, i.description, i.lot, i.entry_date, i.uom, i.initial_qty, m.warehouse, m.location, m.bin
    )
    SELECT
       item_id, sku, description, lot, entry_date, uom, initial_qty,
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
    SELECT h.equipment, h.created_at, h.updated_at, COUNT(r.id)::int AS rows_count
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
    `SELECT id, equipment, created_at, updated_at FROM bom_headers WHERE equipment=$1`,
    [eq],
  );
  const header = h.rows[0];
  if (!header) return null;

  const rows = await db.query(
    `
    SELECT sku, description, qty_required, qty_reserved, availability, reservation_note, updated_at
    FROM bom_rows
    WHERE bom_id=$1
    ORDER BY sku
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

  const compactRows = rows
    .map((r) => ({
      sku: String(r.sku || "").trim(),
      description: String(r.description || "").trim(),
      qty_required: Number(r.qty_required || 0),
    }))
    .filter(
      (r) => r.sku && Number.isFinite(r.qty_required) && r.qty_required > 0,
    );

  const aggregated = new Map();
  for (const r of compactRows) {
    const prev = aggregated.get(r.sku) || {
      sku: r.sku,
      description: r.description,
      qty_required: 0,
    };
    prev.qty_required += r.qty_required;
    if (!prev.description && r.description) prev.description = r.description;
    aggregated.set(r.sku, prev);
  }

  const bomRows = [...aggregated.values()];

  const client = await db.connect();
  try {
    await client.query("BEGIN");

    const created = await client.query(
      `
      INSERT INTO bom_headers (equipment)
      VALUES ($1)
      ON CONFLICT (equipment) DO UPDATE SET updated_at=NOW()
      RETURNING id, equipment
      `,
      [eq],
    );
    const bomId = created.rows[0].id;

    await client.query(`DELETE FROM stock_reservations WHERE equipment=$1`, [
      eq,
    ]);
    await client.query(`DELETE FROM bom_rows WHERE bom_id=$1`, [bomId]);

    const totals = await client.query(
      `
      WITH onhand AS (
        SELECT i.sku,
               SUM(i.initial_qty)
               + COALESCE(SUM(CASE WHEN m.type='IN' THEN m.qty END),0)
               - COALESCE(SUM(CASE WHEN m.type='OUT' THEN m.qty END),0) AS qty_onhand
        FROM items i
        LEFT JOIN movements m ON m.item_id=i.id
        GROUP BY i.sku
      )
      SELECT sku, qty_onhand FROM onhand
      `,
    );

    const reservedTotals = await client.query(
      `
      SELECT sku, SUM(qty_reserved) AS qty_reserved
      FROM stock_reservations
      GROUP BY sku
      `,
    );

    const onhandBySku = new Map(
      totals.rows.map((r) => [r.sku, Number(r.qty_onhand || 0)]),
    );
    const alreadyReservedBySku = new Map(
      reservedTotals.rows.map((r) => [r.sku, Number(r.qty_reserved || 0)]),
    );
    const assignedNowBySku = new Map();
    const uomRows = await client.query(
      `
      SELECT sku, COALESCE(NULLIF(MAX(TRIM(uom)), ''), 'PC') AS uom
      FROM items
      GROUP BY sku
      `,
    );
    const uomBySku = new Map(
      uomRows.rows.map((r) => [
        String(r.sku || "").toUpperCase(),
        r.uom || "PC",
      ]),
    );

    for (const row of bomRows) {
      const onhand = onhandBySku.get(row.sku) || 0;
      const reservedFromOthers = alreadyReservedBySku.get(row.sku) || 0;
      const assignedNow = assignedNowBySku.get(row.sku) || 0;
      const freeQty = Math.max(0, onhand - reservedFromOthers - assignedNow);
      const qtyReserved = Math.max(0, Math.min(row.qty_required, freeQty));

      assignedNowBySku.set(row.sku, assignedNow + qtyReserved);

      let availability = "MISSING";
      if (qtyReserved >= row.qty_required) availability = "OK";
      else if (qtyReserved > 0) availability = "PARTIAL";

      const uom = uomBySku.get(row.sku) || "PC";
      const qtyMissing = Math.max(0, row.qty_required - qtyReserved);

      const note =
        availability === "OK"
          ? ""
          : `ancora da acquistare: ${qtyMissing} ${uom}`;
      await client.query(
        `
        INSERT INTO bom_rows (bom_id, sku, description, qty_required, qty_reserved, availability, reservation_note, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
        `,
        [
          bomId,
          row.sku,
          row.description || "",
          row.qty_required,
          qtyReserved,
          availability,
          note,
        ],
      );

      if (qtyReserved > 0) {
        await client.query(
          `
          INSERT INTO stock_reservations (equipment, sku, qty_reserved, updated_at)
          VALUES ($1,$2,$3,NOW())
          `,
          [eq, row.sku, qtyReserved],
        );
      }
    }

    await client.query(`UPDATE bom_headers SET updated_at=NOW() WHERE id=$1`, [
      bomId,
    ]);
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
