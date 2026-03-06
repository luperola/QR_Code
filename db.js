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
      operator_user_id BIGINT REFERENCES users(id),
      ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      note TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_movements_item ON movements(item_id);
    CREATE INDEX IF NOT EXISTS idx_items_sku ON items(sku);
    CREATE INDEX IF NOT EXISTS idx_movements_wh ON movements(warehouse, location, bin);
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
    if (verifyPin(pin, r.pin_hash))
      return { id: r.id, name: r.name, role: r.role };
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

export async function getOnhandForItemAt(
  { item_id, warehouse, location, bin },
  client = db,
) {
  const { rows } = await client.query(
    `
    SELECT
      COALESCE(SUM(CASE WHEN type='IN' THEN qty END),0) -
      COALESCE(SUM(CASE WHEN type='OUT' THEN qty END),0) AS onhand
    FROM movements
    WHERE item_id=$1 AND warehouse=$2 AND location=$3 AND bin=$4
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

    await client.query(
      `
      INSERT INTO movements (item_id, type, qty, warehouse, location, bin, operator_user_id, note)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      `,
      [
        item_id,
        type,
        qty,
        warehouse,
        location,
        bin,
        operator_user_id || null,
        note || null,
      ],
    );

    await client.query("COMMIT");
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
    SELECT m.ts, m.type, m.qty, m.warehouse, m.location, m.bin, m.note,
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
        i.sku, i.description, i.lot, i.entry_date, i.uom, i.initial_qty,
        m.warehouse, m.location, m.bin,
        COALESCE(SUM(CASE WHEN m.type='IN' THEN m.qty END),0) AS qty_in,
        COALESCE(SUM(CASE WHEN m.type='OUT' THEN m.qty END),0) AS qty_out
      FROM items i
      LEFT JOIN movements m ON m.item_id = i.id
      ${where}
      GROUP BY i.sku, i.description, i.lot, i.entry_date, i.uom, i.initial_qty, m.warehouse, m.location, m.bin
    )
    SELECT
      sku, description, lot, entry_date, uom, initial_qty,
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
