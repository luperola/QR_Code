import Database from "better-sqlite3";
import path from "path";
import { fileURLToPath } from "url";
import fs from "fs";
import { randomBytes, pbkdf2Sync, timingSafeEqual } from "crypto";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const dataDir = path.join(__dirname, "data");
if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });

const dbPath = path.join(dataDir, "stock.sqlite");
export const db = new Database(dbPath);

// Simple migration helper: add column if missing
function ensureColumn(table, column, ddl) {
  const cols = db
    .prepare(`PRAGMA table_info(${table})`)
    .all()
    .map((r) => r.name);
  if (!cols.includes(column)) db.exec(`ALTER TABLE ${table} ADD COLUMN ${ddl}`);
}

export function initDb() {
  db.pragma("journal_mode = WAL");

  db.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      pin_hash TEXT NOT NULL,
      role TEXT NOT NULL DEFAULT 'operator',
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS items (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sku TEXT NOT NULL,
      description TEXT NOT NULL,
      lot TEXT NOT NULL,
      entry_date TEXT,
      uom TEXT NOT NULL DEFAULT 'PC',
      initial_qty REAL NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      UNIQUE(sku, lot)
    );

    CREATE TABLE IF NOT EXISTS movements (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      item_id INTEGER NOT NULL,
      type TEXT NOT NULL CHECK(type IN ('IN','OUT')),
      qty REAL NOT NULL CHECK(qty > 0),
      warehouse TEXT NOT NULL DEFAULT 'MAIN',
      location TEXT NOT NULL DEFAULT 'DEFAULT',
      bin TEXT NOT NULL DEFAULT 'DEFAULT',
      operator_user_id INTEGER,
      ts TEXT NOT NULL DEFAULT (datetime('now')),
      note TEXT,
      FOREIGN KEY(item_id) REFERENCES items(id),
      FOREIGN KEY(operator_user_id) REFERENCES users(id)
    );

    CREATE INDEX IF NOT EXISTS idx_movements_item ON movements(item_id);
    CREATE INDEX IF NOT EXISTS idx_items_sku ON items(sku);
    CREATE INDEX IF NOT EXISTS idx_movements_wh ON movements(warehouse, location, bin);
  `);

  // Backward compatible migrations (if DB from older version)
  ensureColumn("movements", "note", "note TEXT");
  ensureColumn(
    "users",
    "created_at",
    "created_at TEXT NOT NULL DEFAULT (datetime('now'))",
  );
  ensureColumn(
    "items",
    "created_at",
    "created_at TEXT NOT NULL DEFAULT (datetime('now'))",
  );
  ensureColumn(
    "items",
    "updated_at",
    "updated_at TEXT NOT NULL DEFAULT (datetime('now'))",
  );
  ensureColumn("items", "uom", "uom TEXT NOT NULL DEFAULT 'PC'");
  ensureColumn("items", "initial_qty", "initial_qty REAL NOT NULL DEFAULT 0");

  // Seed admin if none exists
  const n = db.prepare(`SELECT COUNT(*) AS n FROM users`).get().n;
  if (n === 0) {
    const defaultPin = "1234";
    const hash = hashPin(defaultPin);
    db.prepare(`INSERT INTO users (name, pin_hash, role) VALUES (?,?,?)`).run(
      "Admin",
      hash,
      "admin",
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

// ---- Users ----
export function listUsers() {
  return db
    .prepare(`SELECT id, name, role, created_at FROM users ORDER BY id`)
    .all();
}
export function getUserById(id) {
  return db.prepare(`SELECT id, name, role FROM users WHERE id=?`).get(id);
}
export function getUserByPin(pin) {
  const rows = db.prepare(`SELECT id, name, role, pin_hash FROM users`).all();
  for (const r of rows) {
    if (verifyPin(pin, r.pin_hash))
      return { id: r.id, name: r.name, role: r.role };
  }
  return null;
}
export function createUser({ name, pin, role }) {
  const hash = hashPin(pin);
  db.prepare(`INSERT INTO users (name, pin_hash, role) VALUES (?,?,?)`).run(
    name,
    hash,
    role || "operator",
  );
}
export function resetUserPin({ user_id, pin }) {
  const hash = hashPin(pin);
  db.prepare(`UPDATE users SET pin_hash=? WHERE id=?`).run(hash, user_id);
}
export function deleteUser({ user_id }) {
  db.prepare(`DELETE FROM users WHERE id=?`).run(user_id);
}

// ---- Items ----
export function upsertItem({
  sku,
  description,
  lot,
  entry_date,
  uom = "PC",
  initial_qty = 0,
}) {
  db.prepare(
    `
    INSERT INTO items (sku, description, lot, entry_date, uom, initial_qty)
    VALUES (@sku, @description, @lot, @entry_date, @uom, @initial_qty)
    ON CONFLICT(sku, lot) DO UPDATE SET
      description=excluded.description,
      entry_date=excluded.entry_date,
      uom=excluded.uom,
      initial_qty=excluded.initial_qty,
      updated_at=datetime('now')
  `,
  ).run({ sku, description, lot, entry_date, uom, initial_qty });
}
export function listItems() {
  return db
    .prepare(
      `
    SELECT sku, description, lot, entry_date, uom, initial_qty, created_at
    FROM items
    ORDER BY created_at DESC
  `,
    )
    .all();
}
export function getItemBySkuLot(sku, lot) {
  return db.prepare(`SELECT * FROM items WHERE sku=? AND lot=?`).get(sku, lot);
}

// ---- Movements & Stock ----
export function getOnhandForItemAt({ item_id, warehouse, location, bin }) {
  return db
    .prepare(
      `
    SELECT
      COALESCE(SUM(CASE WHEN type='IN' THEN qty END),0) -
      COALESCE(SUM(CASE WHEN type='OUT' THEN qty END),0) AS onhand
    FROM movements
    WHERE item_id=? AND warehouse=? AND location=? AND bin=?
  `,
    )
    .get(item_id, warehouse, location, bin).onhand;
}

export function addMovementChecked({
  item_id,
  type,
  qty,
  warehouse,
  location,
  bin,
  operator_user_id,
  note,
}) {
  const tx = db.transaction(() => {
    if (type === "OUT") {
      const onhand = getOnhandForItemAt({ item_id, warehouse, location, bin });
      if (onhand < qty) {
        const err = new Error(
          `Stock insufficiente: on-hand=${onhand}, richiesto OUT=${qty}`,
        );
        err.code = "INSUFFICIENT_STOCK";
        throw err;
      }
    }
    db.prepare(
      `
      INSERT INTO movements (item_id, type, qty, warehouse, location, bin, operator_user_id, note)
      VALUES (?,?,?,?,?,?,?,?)
    `,
    ).run(
      item_id,
      type,
      qty,
      warehouse,
      location,
      bin,
      operator_user_id || null,
      note || null,
    );
  });
  tx();
}

export function listMovements(limit = 200) {
  return db
    .prepare(
      `
    SELECT m.ts, m.type, m.qty, m.warehouse, m.location, m.bin, m.note,
           i.sku, i.lot, i.description,
           u.name AS operator
    FROM movements m
    JOIN items i ON i.id = m.item_id
    LEFT JOIN users u ON u.id = m.operator_user_id
    ORDER BY m.ts DESC, m.id DESC
    LIMIT ?
  `,
    )
    .all(limit);
}

export function getStockRows({ warehouse = null } = {}) {
  const where = warehouse ? `WHERE m.warehouse = @warehouse` : ``;

  return db
    .prepare(
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
      GROUP BY i.sku, i.lot, i.uom, i.initial_qty, m.warehouse, m.location, m.bin
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
    )
    .all({ warehouse });
}

export function listWarehouses() {
  return db
    .prepare(
      `
    SELECT DISTINCT warehouse FROM movements
    UNION
    SELECT 'MAIN' AS warehouse
    ORDER BY warehouse
  `,
    )
    .all()
    .map((r) => r.warehouse);
}

export function listLocations(warehouse) {
  return db
    .prepare(
      `
    SELECT DISTINCT location FROM movements WHERE warehouse=?
    UNION SELECT 'DEFAULT'
    ORDER BY location
  `,
    )
    .all(warehouse)
    .map((r) => r.location);
}

export function listBins(warehouse, location) {
  return db
    .prepare(
      `
    SELECT DISTINCT bin FROM movements WHERE warehouse=? AND location=?
    UNION SELECT 'DEFAULT'
    ORDER BY bin
  `,
    )
    .all(warehouse, location)
    .map((r) => r.bin);
}
