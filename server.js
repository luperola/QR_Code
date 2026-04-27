import "dotenv/config";
import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import QRCode from "qrcode";
import cookieSession from "cookie-session";
import multer from "multer";
import XLSX from "xlsx";
import ExcelJS from "exceljs";
import {
  initDb,
  upsertItem,
  listItems,
  getItemById,
  getItemBySkuLot,
  deleteItemById,
  addMovementChecked,
  getStockRows,
  listMovements,
  listUsers,
  getUserByPin,
  getUserById,
  createUser,
  resetUserPin,
  deleteUser,
  listWarehouses,
  listLocations,
  listBins,
  getOnhandForItemAt,
  listBomHeaders,
  getBomByEquipment,
  listOutMovementsByEquipmentAndSkus,
  deleteBomByEquipment,
  upsertBomFromRows,
  ensureBomHeader,
  listStockReservations,
  consumeBomReservation,
} from "./db.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(express.json({ limit: "1mb" }));
app.use(express.urlencoded({ extended: true }));

app.use(
  cookieSession({
    name: "qrstock_session",
    keys: [process.env.SESSION_KEY || "dev-session-key-change-me"],
    maxAge: 7 * 24 * 60 * 60 * 1000,
  }),
);

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 },
});

app.use("/static", express.static(path.join(__dirname, "public")));

// ---- Helpers ----
function getBaseUrl(req) {
  const forced = process.env.BASE_URL;
  if (forced) return forced.replace(/\/+$/, "");
  const proto = req.headers["x-forwarded-proto"] || req.protocol;
  const host = req.headers["x-forwarded-host"] || req.get("host");
  return `${proto}://${host}`;
}

function formatDate(dateValue) {
  if (!dateValue) return "";
  const date = new Date(dateValue);

  return date.toLocaleDateString("it-IT", {
    timeZone: "Europe/Rome",
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
  });
}

function formatDateTimeCET(dateValue) {
  if (!dateValue) return "";

  const date = new Date(dateValue);

  return date.toLocaleString("it-IT", {
    timeZone: "Europe/Rome",
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
}

function escapeHtml(s = "") {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function buildReservationViewBySku({ reservations = [], stockRows = [] }) {
  const bySku = new Map();
  const onhandBySku = new Map();

  for (const row of stockRows) {
    const sku = String(row.sku || "").trim();
    if (!sku) continue;
    const prev = onhandBySku.get(sku) || 0;
    onhandBySku.set(sku, prev + Number(row.qty_onhand || 0));
  }

  for (const r of reservations) {
    const sku = String(r.sku || "").trim();
    if (!sku) continue;
    const current = bySku.get(sku) || {
      qtyRequiredTotal: 0,
      qtyReservedTotal: 0,
      uom: String(r.uom || "").trim() || "PC",
      equipmentRows: [],
    };
    current.qtyRequiredTotal += Number(r.qty_required || 0);
    current.qtyReservedTotal += Number(r.qty_reserved || 0);
    current.equipmentRows.push({
      equipment: String(r.equipment || "").trim(),
      qtyRequired: Number(r.qty_required || 0),
      uom: String(r.uom || "").trim() || current.uom,
    });
    bySku.set(sku, current);
  }

  for (const [sku, entry] of bySku.entries()) {
    const onhand = onhandBySku.get(sku) || 0;
    entry.qtyToBuyTotal = Math.max(0, entry.qtyRequiredTotal - onhand);
    entry.warning =
      entry.qtyToBuyTotal > 0
        ? `ATTENZIONE - DA ACQUISTARE ${entry.qtyToBuyTotal} mt`
        : "";
  }

  return bySku;
}

async function requireAuth(req, res, next) {
  if (!req.session?.user_id) {
    return res.redirect(`/login?next=${encodeURIComponent(req.originalUrl)}`);
  }
  req.user = await getUserById(req.session.user_id);
  return next();
}

async function requireAdmin(req, res, next) {
  if (!req.session?.user_id) {
    return res.redirect(`/login?next=${encodeURIComponent(req.originalUrl)}`);
  }
  const u = await getUserById(req.session.user_id);
  if (!u || u.role !== "admin") {
    return res.status(403).send("Forbidden (admin only)");
  }
  req.user = u;
  return next();
}

function nav(req, active) {
  const u = req.user;
  const who = u
    ? `<span class="badge">👤 ${escapeHtml(u.name)} (${escapeHtml(u.role)})</span><a href="/logout">Logout</a>`
    : `<a href="/login">Login</a>`;

  return `
<header class="topbar">
  <div class="brand">QR Stock</div>
  <nav class="nav">
    <a href="/" class="${active === "stock" ? "active" : ""}">Stock</a>
    <a href="/items" class="${active === "items" ? "active" : ""}">Items</a>
    <a href="/labels" class="${active === "labels" ? "active" : ""}">Stampa QR</a>
    <a href="/movements" class="${active === "movements" ? "active" : ""}">Movimenti</a>
     <a href="/bom" class="${active === "bom" ? "active" : ""}">BOM</a>
    <a href="/admin" class="${active === "admin" ? "active" : ""}">Admin</a>
    ${who}
  </nav>
</header>`;
}

// ---- Auth ----
app.get("/login", (req, res) => {
  const nextUrl = req.query.next ? String(req.query.next) : "/";
  res.send(`<!doctype html>
<html lang="it"><head>
  <meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Login • QR Stock</title>
  <link rel="stylesheet" href="/static/css/style.css"/>
</head>
<body>
<main class="container">
  <div class="card pad">
    <h1>Login</h1>
    <p class="muted">Inserisci il PIN operatore. (Default admin: <span class="mono">1234</span>)</p>
    <form method="post" action="/login">
      <input type="hidden" name="next" value="${escapeHtml(nextUrl)}"/>
      <label>PIN
        <input name="pin" inputmode="numeric" pattern="[0-9]*" required />
      </label>
      <div class="row">
        <button class="btn" type="submit">Entra</button>
        <a class="btn secondary" href="/">Annulla</a>
      </div>
    </form>
  </div>
</main>
</body></html>`);
});

app.post("/login", async (req, res) => {
  const { pin, next } = req.body || {};
  const u = await getUserByPin(String(pin || ""));
  if (!u) {
    return res.status(401).send("PIN non valido. <a href='/login'>Riprova</a>");
  }
  req.session.user_id = u.id;
  res.redirect(next || "/");
});

app.get("/logout", (req, res) => {
  req.session = null;
  res.redirect("/login");
});
// ---- Pages ----
app.get("/", requireAuth, async (req, res) => {
  const stock = await getStockRows({ warehouse: null });
  const reservations = await listStockReservations();
  const reservationBySku = buildReservationViewBySku({
    reservations,
    stockRows: stock,
  });

  const rows = stock
    .map(
      (r) => `
    <tr>
      <td>${escapeHtml(r.sku)}</td>
      <td>${escapeHtml(r.description)}</td>
      <td>${escapeHtml(r.lot)}</td>
      <td>${escapeHtml(r.uom || "")}</td>
      <td style="text-align:right">${r.initial_qty ?? 0}</td>
                 <td style="text-align:right">${r.qty_in}</td>
      <td style="text-align:right">${r.qty_out}</td>
      <td style="text-align:right"><b>${r.qty_onhand}</b></td>
           <td>${escapeHtml(
             (reservationBySku.get(r.sku)?.equipmentRows || [])
               .map((e) => `${e.equipment}: ${e.qtyRequired} ${e.uom}`)
               .concat(
                 reservationBySku.get(r.sku)?.warning
                   ? [reservationBySku.get(r.sku).warning]
                   : [],
               )
               .join(" • "),
           )}</td>


    <td><a class="btn secondary" href="/q/${r.item_id}">IN / OUT</a></td>
      </tr>
  `,
    )
    .join("");

  res.send(`<!doctype html>
<html lang="it">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Stock • QR Stock</title>
  <link rel="stylesheet" href="/static/css/style.css" />
</head>
<body>
${nav(req, "stock")}

<main class="container">
  <h1>Stock (leggibile anche da iPhone e Android)</h1>
  <p class="muted">Apri dal telefono: <span class="mono">${escapeHtml(getBaseUrl(req))}</span></p>

  <div class="card pad">
    <div class="row" style="margin-top:0">
            <a class="btn" href="/export/stock.xlsx">Export Stock (XLSX)</a>
      <a class="btn secondary" href="/scan">Scanner (webcam)</a>
    </div>
  </div>

  <div class="card">
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
           <th>SKU</th><th>Descrizione</th><th>Lot</th><th>U.M.</th><th>Qty iniziale</th>
               <th>IN</th><th>OUT</th><th>On hand</th><th>Da usare per equipment</th><th>Azione</th>
          </tr>
        </thead>
        <tbody>
       ${rows || `<tr><td colspan="10" class="muted">Nessun dato. Vai su “Items” per aggiungere articoli.</td></tr>`}
        </tbody>
      </table>
    </div>
  </div>
</main>
</body>
</html>`);
});

app.get("/items", requireAuth, async (req, res) => {
  const items = await listItems();
  const canDeleteItems = req.user?.role === "admin";
  const imported = Number.parseInt(String(req.query.imported || ""), 10);
  const skipped = Number.parseInt(String(req.query.skipped || ""), 10);
  const importMessage =
    Number.isInteger(imported) && Number.isInteger(skipped)
      ? `<div class="flash ok">Import completato. OK=${imported}, Skipped=${skipped}.</div>`
      : "";
  const rows = items
    .map(
      (it) => `
    <tr>
      <td>${escapeHtml(it.sku)}</td>
      <td>${escapeHtml(it.description)}</td>
      <td>${escapeHtml(it.lot)}</td>
      <td>${escapeHtml(it.uom || "")}</td>
      <td style="text-align:right">${it.initial_qty ?? 0}</td>
      <td>${escapeHtml(formatDate(it.created_at))}</td>
     <td>
        ${
          canDeleteItems
            ? `<form method="post" action="/items/${it.id}/delete" onsubmit="return confirm('Confermi eliminazione item e relativi movimenti?');">
              <button class="btn danger" type="submit">Elimina</button>
            </form>`
            : `<span class="muted">Solo admin</span>`
        }
      </td>
      </tr>
  `,
    )
    .join("");

  res.send(`<!doctype html>
<html lang="it">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Items • QR Stock</title>
  <link rel="stylesheet" href="/static/css/style.css" />
</head>
<body>
${nav(req, "items")}

<main class="container">
  <h1>Items</h1>
${importMessage}

  <div class="card pad">
    <h2>Aggiungi / aggiorna item (SKU+Lot univoco)</h2>
    <form method="post" action="/items">
      <div class="form-grid">
        <label>SKU<input name="sku" required placeholder="es. DKW-12345" /></label>
        <label>Lot<input name="lot" required placeholder="es. LOT2026-01" /></label>
               <label>U.M.<input name="uom" placeholder="PC" /></label>
        <label>Quantità iniziale<input name="initial_qty" type="number" step="0.01" value="0" /></label>
        <label class="span2">Descrizione<input name="description" required placeholder="es. Tubo 1.4435 EP 1/2&quot;..." /></label>
      </div>
      <div class="row">
        <button class="btn" type="submit">Salva</button>
        <a class="btn secondary" href="/labels">Stampa QR</a>
               </div>
    </form>
  </div>
  <div class="card pad">
    <h2>Import items da Excel (.xlsx)</h2>
      <p class="muted">Header supportati: <span class="mono">SKU, Description/Descrizione, Lot</span>.</p>
    <form method="post" action="/items/import" enctype="multipart/form-data">
      <input type="file" name="file" accept=".xlsx,.xls" required />
      <div class="row">
        <button class="btn ok" type="submit">Importa</button>
        <a class="btn secondary" href="/export/items-template.xlsx">Scarica template</a>
      </div>
    </form>
  </div>
  <div class="card">
    <div class="table-wrap">
      <table>
        <thead><tr><th>SKU</th><th>Descrizione</th><th>Lot</th><th>U.M.</th><th>Qty iniziale</th><th>Creato</th><th>Azioni</th></tr></thead>
        <tbody>
            ${rows || `<tr><td colspan="7" class="muted">Nessun item.</td></tr>`}
        </tbody>
      </table>
    </div>
  </div>
</main>
</body>
</html>`);
});

app.post("/items/:id/delete", requireAdmin, async (req, res) => {
  const itemId = Number(req.params.id);
  if (!Number.isFinite(itemId) || itemId <= 0) {
    return res.status(400).send("Invalid item id");
  }

  await deleteItemById(itemId);
  return res.redirect("/items");
});

app.post("/items", requireAuth, async (req, res) => {
  const { sku, description, lot, entry_date, uom, initial_qty } =
    req.body || {};
  if (!sku || !description || !lot) {
    return res.status(400).send("Missing fields");
  }

  await upsertItem({
    sku: String(sku).trim(),
    description: String(description).trim(),
    lot: String(lot).trim(),
    entry_date: entry_date ? String(entry_date).trim() : null,
    uom: (uom ? String(uom).trim() : "PC") || "PC",
    initial_qty: Number(initial_qty || 0),
  });

  res.redirect("/items");
});

app.post(
  "/items/import",
  requireAuth,
  upload.single("file"),
  async (req, res) => {
    if (!req.file) return res.status(400).send("No file uploaded");

    const wb = XLSX.read(req.file.buffer, { type: "buffer", cellDates: true });
    const sheetName = wb.SheetNames[0];
    const ws = wb.Sheets[sheetName];
    const rows = XLSX.utils.sheet_to_json(ws, { defval: "", raw: true });

    const norm = (s) =>
      String(s || "")
        .trim()
        .toLowerCase();

    const date1904 = !!(
      wb.Workbook &&
      wb.Workbook.WBProps &&
      wb.Workbook.WBProps.date1904
    );

    function toIsoDate(d) {
      const yyyy = d.getFullYear();
      const mm = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      return `${yyyy}-${mm}-${dd}`;
    }

    function parseEntryDate(value) {
      if (value === null || value === undefined) return null;

      if (value instanceof Date && !Number.isNaN(value.getTime())) {
        return toIsoDate(value);
      }

      if (typeof value === "number" && Number.isFinite(value)) {
        const dc = XLSX.SSF.parse_date_code(value, { date1904 });
        if (dc && dc.y && dc.m && dc.d) {
          const d = new Date(dc.y, dc.m - 1, dc.d);
          return toIsoDate(d);
        }
        return null;
      }

      const s = String(value).trim();
      if (!s) return null;

      if (s.startsWith("=")) {
        const today = new Date();
        return toIsoDate(today);
      }

      const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
      if (iso) return s;

      const it = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
      if (it) {
        const dd = Number(it[1]);
        const mm = Number(it[2]);
        const yyyy = Number(it[3]);
        const d = new Date(yyyy, mm - 1, dd);
        if (!Number.isNaN(d.getTime())) return toIsoDate(d);
      }

      const t = Date.parse(s);
      if (!Number.isNaN(t)) return toIsoDate(new Date(t));

      return null;
    }

    let ok = 0;
    let skipped = 0;

    for (const r of rows) {
      const keys = Object.keys(r);

      const get = (...names) => {
        const wanted = names.map((n) => norm(n));

        for (const n of wanted) {
          const k = keys.find((k) => norm(k) === n);
          if (k) return r[k];
        }

        for (const n of wanted) {
          const k = keys.find((k) => norm(k).includes(n));
          if (k) return r[k];
        }

        return "";
      };

      const sku = String(get("SKU", "Sku")).trim();
      const description = String(get("Description", "Descrizione")).trim();
      const lot = String(get("Lot", "LOT")).trim();

      const entryRaw = get(
        "EntryDate",
        "DataIngresso",
        "Data ingresso",
        "Entry Date",
        "Data Ingresso",
      );
      const entry_date = parseEntryDate(entryRaw);

      const uomRaw = get(
        "UoM",
        "UOM",
        "UM",
        "U.M.",
        "Unit",
        "Unita",
        "Unità",
        "U.M",
      );
      const uom = String(uomRaw || "").trim() || "PC";

      const qtyRaw = get(
        "InitialQty",
        "Qty",
        "Quantita",
        "Quantità",
        "Qta",
        "Q.tà",
        "QTY",
      );
      const initial_qty =
        typeof qtyRaw === "number"
          ? qtyRaw
          : Number(String(qtyRaw || "").replace(",", "."));

      if (!sku || !description || !lot) {
        skipped++;
        continue;
      }

      await upsertItem({
        sku,
        description,
        lot,
        entry_date,
        uom,
        initial_qty: Number.isFinite(initial_qty) ? initial_qty : 0,
      });
      ok++;
    }

    return res.redirect(`/items?imported=${ok}&skipped=${skipped}`);
  },
);

app.get("/bom", requireAuth, async (req, res) => {
  const headers = await listBomHeaders();
  const activeHeaders = headers.filter((h) => Number(h.rows_count || 0) > 0);
  const equipmentOptions = headers
    .map(
      (h) =>
        `<option value="${escapeHtml(h.equipment)}">${escapeHtml(h.equipment)}</option>`,
    )
    .join("");
  const rows = headers
    .map(
      (h) => `
    <tr>
      <td><a class="mono" href="/bom/${encodeURIComponent(h.equipment)}">${escapeHtml(h.equipment)}</a></td>
      <td style="text-align:right">${h.rows_count}</td>
      <td>${escapeHtml(formatDateTimeCET(h.updated_at))}</td>
     <td>
        <div class="row">
          <a class="btn secondary" href="/export/bom/${encodeURIComponent(h.equipment)}.xlsx">Export</a>
          <form method="post" action="/bom/${encodeURIComponent(h.equipment)}/delete" onsubmit="return confirm('Eliminare il BOM di ${escapeHtml(h.equipment)} e ripristinare lo stock riservato?');">
            <button class="btn danger" type="submit">Elimina</button>
          </form>
        </div>
      </td>
    </tr>
  `,
    )
    .join("");

  res.send(`<!doctype html>
<html lang="it">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>BOM • QR Stock</title>
  <link rel="stylesheet" href="/static/css/style.css" />
</head>
<body>
${nav(req, "bom")}

<main class="container">
  <h1>BOM per equipment</h1>
  <div class="card pad">
    <h2>BOM attivi</h2>
    <label>Seleziona BOM equipment
      <select id="activeBomSelect">
        <option value="">-- Apri BOM --</option>
        ${activeHeaders
          .map(
            (h) =>
              `<option value="${escapeHtml(h.equipment)}">${escapeHtml(h.equipment)}</option>`,
          )
          .join("")}
      </select>
    </label>
  </div>
  <div class="card pad">
    <h2>Carica BOM Excel</h2>
    <p class="muted">Prima indica l'equipment (es. <span class="mono">DPROFAB4</span>), poi carica il file. Se l'equipment esiste già, il BOM verrà aggiornato.</p>
     <form method="post" action="/bom" enctype="multipart/form-data">
      <div class="form-grid">
        <label>Equipment
         <input name="equipment" required placeholder="es. DPROFAB4" list="equipment-list" style="text-transform:uppercase" oninput="this.value = this.value.toUpperCase()" />
          <datalist id="equipment-list">
            ${equipmentOptions}
          </datalist>
        </label>
        <label>File Excel BOM
          <input type="file" name="file" accept=".xlsx,.xls" required />
        </label>
      </div>
      <div class="row">
        <button class="btn ok" type="submit">Importa BOM</button>
      </div>
    </form>
    <p class="muted">Header supportati: <span class="mono">SKU, Description/Descrizione, Qty/Quantità</span>.</p>
  </div>

  <div class="card">
    <div class="table-wrap">
      <table>
        <thead>
          <tr><th>Equipment</th><th>Righe BOM</th><th>Ultimo aggiornamento</th><th>Azioni</th></tr>
        </thead>
        <tbody>
          ${rows || `<tr><td colspan="4" class="muted">Nessun BOM caricato.</td></tr>`}
        </tbody>
      </table>
    </div>
  </div>
</main>
<script>
document.getElementById("activeBomSelect")?.addEventListener("change", function(){
  if (!this.value) return;
  window.location.href = "/bom/" + encodeURIComponent(this.value);
});
</script>
</body>
</html>`);
});

const handleBomImport = async (req, res) => {
  if (!req.file) return res.status(400).send("No file uploaded");
  const equipment = String(req.body?.equipment || "").trim();
  if (!equipment) return res.status(400).send("Equipment mancante");

  const wb = XLSX.read(req.file.buffer, { type: "buffer", cellDates: true });
  const ws = wb.Sheets[wb.SheetNames[0]];
  const sourceRows = XLSX.utils.sheet_to_json(ws, { defval: "", raw: true });

  const norm = (s) =>
    String(s || "")
      .trim()
      .toLowerCase();

  const parsedRows = [];
  let skipped = 0;
  for (const row of sourceRows) {
    const keys = Object.keys(row);
    const get = (...names) => {
      const wanted = names.map((n) => norm(n));
      for (const n of wanted) {
        const exact = keys.find((k) => norm(k) === n);
        if (exact) return row[exact];
      }
      for (const n of wanted) {
        const partial = keys.find((k) => norm(k).includes(n));
        if (partial) return row[partial];
      }
      return "";
    };

    const sku = String(get("SKU", "Sku")).trim();
    const description = String(get("Description", "Descrizione")).trim();
    const qtyKeys = keys.filter((k) => {
      const nk = norm(k);
      return (
        (nk.includes("qty") ||
          nk.includes("quantita") ||
          nk.includes("quantità") ||
          nk.includes("qta")) &&
        !nk.includes("riserv") &&
        !nk.includes("reserved")
      );
    });
    const preferredQtyKey =
      qtyKeys.find((k) => {
        const nk = norm(k);
        return (
          nk === "qty" ||
          nk === "quantity" ||
          nk === "quantita" ||
          nk === "quantità" ||
          nk === "qta"
        );
      }) || qtyKeys[0];
    const qtyRaw = preferredQtyKey ? row[preferredQtyKey] : "";
    const qtyRequired =
      typeof qtyRaw === "number"
        ? qtyRaw
        : Number(String(qtyRaw || "").replace(",", "."));

    if (!sku || !Number.isFinite(qtyRequired) || qtyRequired <= 0) {
      skipped++;
      continue;
    }
    parsedRows.push({ sku, description, qty_required: qtyRequired });
  }

  const result = await upsertBomFromRows(equipment, parsedRows);
  return res.redirect(
    `/bom/${encodeURIComponent(result.equipment)}?imported=${result.rows_count}&skipped=${skipped}`,
  );
};

app.post("/bom", requireAuth, upload.single("file"), handleBomImport);
app.post("/bom/import", requireAuth, upload.single("file"), handleBomImport);

app.post("/bom/:equipment/delete", requireAuth, async (req, res) => {
  const equipment = String(req.params.equipment || "");
  const deleted = await deleteBomByEquipment(equipment);
  if (!deleted) {
    return res.status(404).send("BOM non trovato");
  }
  return res.redirect("/bom");
});

app.get("/bom/:equipment", requireAuth, async (req, res) => {
  const equipment = String(req.params.equipment || "");
  const bom = await getBomByEquipment(equipment);
  if (!bom) return res.status(404).send("BOM non trovato");
  const stockRows = await getStockRows({ warehouse: null });
  const onhandBySku = stockRows.reduce((acc, row) => {
    const sku = String(row.sku || "").trim();
    if (!sku) return acc;
    acc.set(sku, (acc.get(sku) || 0) + Number(row.qty_onhand || 0));
    return acc;
  }, new Map());
  const bomSkus = bom.rows
    .map((r) => String(r.sku || "").trim())
    .filter(Boolean);
  const outMovements = await listOutMovementsByEquipmentAndSkus(
    bom.equipment,
    bomSkus,
  );
  const outMovementsBySku = outMovements.reduce((acc, move) => {
    const sku = String(move.sku || "").trim();
    if (!acc.has(sku)) {
      acc.set(sku, []);
    }
    acc.get(sku).push(move);
    return acc;
  }, new Map());
  const imported = Number.parseInt(String(req.query.imported || ""), 10);
  const skipped = Number.parseInt(String(req.query.skipped || ""), 10);
  const importMessage =
    Number.isInteger(imported) && Number.isInteger(skipped)
      ? `<div class="flash ok">Import BOM completato. Righe valide=${imported}, righe ignorate=${skipped}.</div>`
      : "";
  const rows = bom.rows
    .map((r) => {
      const sku = String(r.sku || "").trim();
      const skuOutMovements = outMovementsBySku.get(sku) || [];
      const totalPickedQty = skuOutMovements.reduce(
        (sum, move) => sum + Number(move.qty || 0),
        0,
      );
      const originalQtyRequired = Number(r.qty_required || 0) + totalPickedQty;
      const currentOnhandQty = onhandBySku.get(sku) || 0;
      const qtyToBuy = Math.max(
        0,
        originalQtyRequired - totalPickedQty - currentOnhandQty,
      );
      const prelieviNotes = skuOutMovements.length
        ? skuOutMovements.map((m) => {
            const ts = new Date(m.ts);
            const day = ts.toLocaleDateString("it-IT", {
              timeZone: "Europe/Rome",
              day: "2-digit",
              month: "2-digit",
              year: "numeric",
            });
            const hour = ts.toLocaleTimeString("it-IT", {
              timeZone: "Europe/Rome",
              hour: "2-digit",
              minute: "2-digit",
              hour12: false,
            });
            return `Prelevati ${Number(m.qty)} mt, ${day}, ${hour}`;
          })
        : [];
      const uniquePrelieviNotes = Array.from(new Set(prelieviNotes));
      const notes = [
        qtyToBuy > 0 ? `ancora da acquistare: ${qtyToBuy} mt` : "",
        `On hand attuali : ${currentOnhandQty} mt`,
        ...uniquePrelieviNotes,
      ]

        .filter(Boolean)
        .join(" • ");
      return `
    <tr>
      <td>${escapeHtml(r.sku)}</td>
      <td>${escapeHtml(r.description || "")}</td>
       <td style="text-align:right">${originalQtyRequired}</td>
            <td>${escapeHtml(r.availability)}</td>    
   <td>${escapeHtml(notes)}</td>
    </tr>
  `;
    })
    .join("");

  res.send(`<!doctype html>
<html lang="it">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>BOM ${escapeHtml(bom.equipment)} • QR Stock</title>
  <link rel="stylesheet" href="/static/css/style.css" />
</head>
<body>
${nav(req, "bom")}
<main class="container">
  <h1>BOM equipment <span class="mono">${escapeHtml(bom.equipment)}</span></h1>
  <p class="muted">Ultimo aggiornamento: ${escapeHtml(formatDateTimeCET(bom.updated_at))}</p>
   ${importMessage}
  <div class="row">
    <a class="btn secondary" href="/bom">← Torna a BOM</a>
    <a class="btn" href="/export/bom/${encodeURIComponent(bom.equipment)}.xlsx">Export BOM (XLSX)</a>
  </div>

  <div class="card">
    <div class="table-wrap">
      <table>
        <thead>
           <tr><th>SKU</th><th>Descrizione</th><th>Qty richiesta</th><th>Stato stock</th><th>Note</th></tr>
        </thead>
        <tbody>
          ${rows || `<tr><td colspan="5" class="muted">Nessuna riga nel BOM.</td></tr>`}
        </tbody>
      </table>
    </div>
  </div>
</main>
</body>
</html>`);
});
app.get("/labels", requireAuth, async (req, res) => {
  const items = await listItems();
  const baseUrl = getBaseUrl(req);

  const cards = await Promise.all(
    items.map(async (it) => {
      const url = `${baseUrl}/q/${it.id}`;
      const dataUrl = await QRCode.toDataURL(url, {
        margin: 8,
        width: 520,
        errorCorrectionLevel: "H",
        color: {
          dark: "#000000",
          light: "#FFFFFF",
        },
      });

      return `
      <div class="label">
        <img class="qr" src="${dataUrl}" alt="QR" />
        <div class="label-text">
          <div class="mono sku">${escapeHtml(it.sku)}</div>
          <div class="mono lot">LOT: ${escapeHtml(it.lot)}</div>
        </div>
      </div>
      `;
    }),
  );

  res.send(`<!doctype html>
<html lang="it">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Stampa QR • QR Stock</title>
  <link rel="stylesheet" href="/static/css/style.css" />
  <style>
    @media print {
      header, .no-print { display: none !important; }
      body { background:#fff; }
      .container { max-width: none; padding: 0; }
      .label-sheet { gap: 6mm; }
      .label { break-inside: avoid; }
    }
  </style>
</head>
<body>
${nav(req, "labels")}

<main class="container">
  <h1>Stampa QR</h1>
  <p class="muted no-print">QR ottimizzato per lettura rapida su Android/iPhone (Lens, fotocamera, app scanner).</p>
  <div class="row no-print">
    <button class="btn" onclick="window.print()">Stampa</button>
    <a class="btn secondary" href="/items">Aggiungi items</a>
  </div>

  <div class="label-sheet">
    ${cards.join("") || `<div class="muted">Nessun item.</div>`}
  </div>
</main>
</body>
</html>`);
});

app.get("/q/:id", requireAuth, async (req, res) => {
  const id = Number(req.params.id);
  if (!Number.isFinite(id) || id <= 0) {
    return res.status(400).send("Invalid item id");
  }

  const item = await getItemById(id);
  if (!item) {
    return res.status(404).send("Item not found.");
  }
  const bomHeaders = await listBomHeaders();
  const stockRows = await getStockRows({ warehouse: null });
  const qtyOnHand = stockRows
    .filter((r) => r.sku === item.sku && r.lot === item.lot)
    .reduce((sum, r) => sum + Number(r.qty_onhand || 0), 0);
  const reservations = await listStockReservations();
  const reservationBySku = buildReservationViewBySku({
    reservations,
    stockRows,
  });
  const reservationsForSku = reservations
    .filter((r) => String(r.sku || "").trim() === String(item.sku || "").trim())
    .sort((a, b) =>
      String(a.equipment || "").localeCompare(String(b.equipment || "")),
    );

  const reservationByEquipment = new Map(
    reservationsForSku.map((r) => [
      String(r.equipment || "")
        .trim()
        .toUpperCase(),
      {
        qty_required: Number(r.qty_required || 0),
        uom: String(r.uom || "").trim() || "PC",
      },
    ]),
  );

  const bomOptions = bomHeaders
    .map((h) => {
      const equipment = String(h.equipment || "")
        .trim()
        .toUpperCase();
      const reservation = reservationByEquipment.get(equipment);
      const reservedLabel = reservation
        ? ` • richiesti ${reservation.qty_required} ${reservation.uom}`
        : "";
      return `<option value="${escapeHtml(equipment)}">${escapeHtml(equipment + reservedLabel)}</option>`;
    })
    .join("");
  const reservationSummary = reservationsForSku.length
    ? reservationsForSku
        .map((r) => {
          const uom = String(r.uom || "").trim() || "PC";
          const qtyRequired = Number(r.qty_required || 0);
          return `<li><span class="mono">${escapeHtml(r.equipment)}</span>: ${qtyRequired} ${escapeHtml(uom)}</li>`;
        })
        .concat(
          reservationBySku.get(item.sku)?.warning
            ? [
                `<li><b>${escapeHtml(reservationBySku.get(item.sku).warning)}</b></li>`,
              ]
            : [],
        )
        .join("")
    : `<li class="muted">Nessuna riserva BOM per SKU <span class="mono">${escapeHtml(item.sku)}</span>.</li>`;
  res.send(`<!doctype html>
<html lang="it">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Movimento • QR Stock</title>
  <link rel="stylesheet" href="/static/css/style.css" />
</head>
<body>
<main class="container">
  <div class="card pad">
    <h1>Movimento stock</h1>
    <p class="muted">Operatore: <b>${escapeHtml(req.user.name)}</b></p>
    <p class="muted">SKU <span class="mono">${escapeHtml(item.sku)}</span> • Lot <span class="mono">${escapeHtml(item.lot)}</span></p>
    <p><b>${escapeHtml(item.description)}</b></p>

    <form id="moveForm">
      <input type="hidden" name="sku" value="${escapeHtml(item.sku)}" />
      <input type="hidden" name="lot" value="${escapeHtml(item.lot)}" />

      <div class="form-grid">
        <label>Qty on hand
          <input value="${qtyOnHand}" readonly />
        </label>
        <label>Equipment (BOM)
          <select name="equipment" class="stock-equipment-select">
            <option value="__NONE__">-- Nessun equipment specifico --</option>
            ${bomOptions}
          </select>
        </label>
        <label>Nuovo equipment/BOM
          <input name="new_equipment" placeholder="Es. LINEA-02" />
        </label>
        <label>Quantità
          <input name="qty" type="number" min="0.01" step="0.01" value="1" required />
        </label>
        <label class="span2">Note (opzionale)
          <input name="note" placeholder="es. carico da fornitore / scarico produzione..." />
        </label>
      </div>
 <div class="card pad" style="margin-top:8px;">
        <p class="muted" style="margin:0 0 6px 0;">Riservato per SKU <span class="mono">${escapeHtml(item.sku)}</span> (per scegliere meglio il BOM):</p>
        <ul style="margin:0; padding-left:18px;">${reservationSummary}</ul>
      </div>
      <div class="row">
        <button class="btn ok" type="button" onclick="sendMove('IN')">IN</button>
        <button class="btn danger" type="button" onclick="sendMove('OUT')">OUT</button>
        <a class="btn secondary" href="/">Stock</a>
      </div>

      <div id="msg" class="flash" style="margin-top:10px; display:none;"></div>
    </form>

    <div class="hr"></div>
     <p class="muted">Tip: salva questa pagina in Home. L'equipment viene ricordato sul telefono.</p>
  <p class="muted">Se inserisci un nuovo equipment/BOM, verrà creato automaticamente anche nella pagina <span class="mono">/bom</span>.</p>
    </div>
</main>

<script>
(function restoreLoc(){
  const f = document.getElementById('moveForm');
  const get = (k) => localStorage.getItem('qrstock_' + k) || '';
 f.equipment.value = get('equipment') || '__NONE__';
})();

function saveLoc(payload){
  localStorage.setItem('qrstock_equipment', payload.equipment || '__NONE__');
}

function showMsg(text, ok){
  const el = document.getElementById('msg');
  el.style.display = 'block';
  el.className = 'flash ' + (ok ? 'ok' : 'err');
  el.textContent = text;
}

async function sendMove(type) {
  const form = document.getElementById('moveForm');
  const data = new FormData(form);
const selectedEquipment = String(data.get('equipment') || '').trim().toUpperCase();
  const newEquipment = String(data.get('new_equipment') || '').trim().toUpperCase();
  const normalizedSelectedEquipment = selectedEquipment === '__NONE__' ? '' : selectedEquipment;
  const finalEquipment = type === 'IN' ? '' : (newEquipment || normalizedSelectedEquipment);
  const payload = {
    sku: data.get('sku'),
    lot: data.get('lot'),
    qty: Number(data.get('qty') || 1),
    type,
    warehouse: 'MAIN',
    location: 'DEFAULT',
    bin: 'DEFAULT',
    equipment: finalEquipment,
    note: String(data.get('note') || '').trim()
  };

  if (type === 'OUT' && !newEquipment && !selectedEquipment) {
    showMsg("Per OUT seleziona un equipment, scegli 'nessun equipment specifico' o inserisci un nuovo nome BOM.", false);
    return;
  }

  saveLoc(payload);
  showMsg("Invio...", true);

  const res = await fetch('/api/move', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });

  const out = await res.json().catch(() => ({}));
  if (!res.ok) {
    showMsg("Errore: " + (out.error || res.statusText) + (out.onhand != null ? " | on-hand=" + out.onhand : ""), false);
    return;
  }

  const eqLabel = payload.equipment ? " | Equipment: " + payload.equipment : "";
 const createdLabel = out.bom_created ? " | Nuovo BOM registrato" : "";
  showMsg("OK ✓ Nuovo on-hand: " + out.onhand + eqLabel + createdLabel + " | Aggiornamento pagina...", true);
  form.new_equipment.value = '';
  setTimeout(() => window.location.reload(), 700);
}
</script>
</body>
</html>`);
});

app.get("/scanlink", requireAuth, async (req, res) => {
  const { sku, lot } = req.query;
  if (!sku || !lot) return res.status(400).send("Missing sku/lot");

  const item = await getItemBySkuLot(String(sku), String(lot));
  if (!item) {
    return res.status(404).send("Item not found. Create it in /items first.");
  }

  return res.redirect(`/q/${item.id}`);
});

app.get("/scan", requireAuth, (req, res) => {
  res.send(`<!doctype html>
<html lang="it">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Scanner • QR Stock</title>
  <link rel="stylesheet" href="/static/css/style.css" />
</head>
<body>
${nav(req, "")}

<main class="container">
  <h1>Scanner (webcam)</h1>
  <div class="card pad">
    <p class="muted">
      Questo scanner usa la camera live dentro il browser. Su iPhone funziona solo in <b>HTTPS</b> (o localhost).
      Se non hai HTTPS, usa la <b>Fotocamera</b> con le etichette.
    </p>
    <div id="reader" style="width: 100%;"></div>
    <div id="scanMsg" class="muted" style="margin-top:10px;"></div>
  </div>
</main>

<script src="https://unpkg.com/html5-qrcode"></script>
<script>
const msg = document.getElementById('scanMsg');

function handleDecodedText(decodedText) {
  try {
    const url = new URL(decodedText);

    if (url.pathname.startsWith('/q/')) {
      window.location.href = url.pathname;
      return;
    }

    if (url.pathname.endsWith('/scanlink')) {
      window.location.href = url.pathname + url.search;
      return;
    }
  } catch (e) {}

  msg.textContent = "QR letto ma non riconosciuto: " + decodedText;
}

const html5QrcodeScanner = new Html5QrcodeScanner(
  "reader",
  { fps: 10, qrbox: { width: 250, height: 250 } },
  false
);
html5QrcodeScanner.render(handleDecodedText);
</script>
</body>
</html>`);
});

app.get("/movements", requireAuth, async (req, res) => {
  const moves = await listMovements(500);
  const rows = moves
    .map(
      (m) => `
    <tr>
      <td>${escapeHtml(formatDateTimeCET(m.ts))}</td>
      <td>${escapeHtml(m.type)}</td>
      <td style="text-align:right">${m.qty}</td>
      <td>${escapeHtml(m.warehouse)}</td>
     <td>${escapeHtml(m.equipment || "")}</td>
      <td>${escapeHtml(m.sku)}</td>
      <td>${escapeHtml(m.lot)}</td>
      <td>${escapeHtml(m.description)}</td>
      <td>${escapeHtml(m.operator || "")}</td>
      <td>${escapeHtml(m.note || "")}</td>
    </tr>
  `,
    )
    .join("");

  res.send(`<!doctype html>
<html lang="it">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Movimenti • QR Stock</title>
  <link rel="stylesheet" href="/static/css/style.css" />
</head>
<body>
${nav(req, "movements")}

<main class="container">
  <h1>Ultimi movimenti</h1>
  <div class="card pad">
   <h2>Movimento da PC con codice</h2>
    <p class="muted">Inserisci SKU e Lot (il codice stampato nel QR) per aprire la pagina IN/OUT senza usare la fotocamera.</p>
    <form method="get" action="/scanlink">
      <div class="form-grid">
        <label>SKU<input name="sku" required placeholder="es. DKW-12345" /></label>
        <label>Lot<input name="lot" required placeholder="es. LOT2026-01" /></label>
      </div>
      <div class="row">
        <button class="btn ok" type="submit">Apri movimento IN / OUT</button>
      </div>
    </form>
    <div class="hr"></div>
  <div class="row" style="margin-top:0">
      <a class="btn" href="/export/movements.xlsx">Export Movimenti (XLSX)</a>
    </div>
  </div>
  <div class="card">
    <div class="table-wrap">
      <table>
        <thead><tr>
          <th>Data/ora</th><th>Tipo</th><th>Qty</th>
         <th>Warehouse</th><th>Equipment</th>
          <th>SKU</th><th>Lot</th><th>Descrizione</th><th>Operatore</th><th>Note</th>
        </tr></thead>
        <tbody>
          ${rows || `<tr><td colspan="10" class="muted">Nessun movimento.</td></tr>`}
        </tbody>
      </table>
    </div>
  </div>
</main>
</body>
</html>`);
});

// ---- Admin ----
app.get("/admin", requireAdmin, async (req, res) => {
  const users = await listUsers();
  const rows = users
    .map(
      (u) => `
    <tr>
      <td>${u.id}</td>
      <td>${escapeHtml(u.name)}</td>
      <td>${escapeHtml(u.role)}</td>
      <td>${escapeHtml(u.created_at)}</td>
      <td>
        <form method="post" action="/admin/users/reset" class="row" style="margin-top:0">
          <input type="hidden" name="user_id" value="${u.id}"/>
          <input name="pin" placeholder="Nuovo PIN" required />
          <button class="btn secondary" type="submit">Reset PIN</button>
        </form>
      </td>
      <td>
        ${
          u.role === "admin"
            ? `<span class="muted">-</span>`
            : `
          <form method="post" action="/admin/users/delete" onsubmit="return confirm('Eliminare utente?')" style="margin:0">
            <input type="hidden" name="user_id" value="${u.id}"/>
            <button class="btn danger" type="submit">Delete</button>
          </form>
        `
        }
      </td>
    </tr>
  `,
    )
    .join("");

  res.send(`<!doctype html>
<html lang="it">
<head>
  <meta charset="utf-8"/><meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Admin • QR Stock</title>
  <link rel="stylesheet" href="/static/css/style.css"/>
</head>
<body>
${nav(req, "admin")}
<main class="container">
  <h1>Admin</h1>

  <div class="card pad">
    <h2>Crea utente</h2>
    <form method="post" action="/admin/users/create">
      <div class="form-grid">
        <label>Nome<input name="name" required /></label>
        <label>PIN<input name="pin" required /></label>
        <label>Ruolo
          <select name="role">
            <option value="operator">operator</option>
            <option value="admin">admin</option>
          </select>
        </label>
      </div>
      <div class="row">
        <button class="btn ok" type="submit">Crea</button>
      </div>
      <p class="muted">Nota: il PIN viene salvato in hash (non reversibile).</p>
    </form>
  </div>

  <div class="card">
    <div class="table-wrap">
      <table>
        <thead><tr><th>ID</th><th>Nome</th><th>Ruolo</th><th>Creato</th><th>Reset PIN</th><th>Delete</th></tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  </div>
</main>
</body></html>`);
});

app.post("/admin/users/create", requireAdmin, async (req, res) => {
  const { name, pin, role } = req.body || {};
  if (!name || !pin) return res.status(400).send("Missing fields");

  await createUser({
    name: String(name).trim(),
    pin: String(pin).trim(),
    role: role === "admin" ? "admin" : "operator",
  });

  res.redirect("/admin");
});

app.post("/admin/users/reset", requireAdmin, async (req, res) => {
  const { user_id, pin } = req.body || {};
  if (!user_id || !pin) return res.status(400).send("Missing fields");

  await resetUserPin({ user_id: Number(user_id), pin: String(pin).trim() });
  res.redirect("/admin");
});

app.post("/admin/users/delete", requireAdmin, async (req, res) => {
  const { user_id } = req.body || {};
  if (!user_id) return res.status(400).send("Missing user_id");

  await deleteUser({ user_id: Number(user_id) });
  res.redirect("/admin");
});

// ---- API ----
app.get("/api/stock", requireAuth, async (req, res) => {
  const warehouse = req.query.warehouse ? String(req.query.warehouse) : null;
  res.json({ rows: await getStockRows({ warehouse }) });
});

app.get("/api/warehouses", requireAuth, async (req, res) => {
  res.json({ warehouses: await listWarehouses() });
});

app.get("/api/locations", requireAuth, async (req, res) => {
  const warehouse = String(req.query.warehouse || "MAIN");
  res.json({ locations: await listLocations(warehouse) });
});

app.get("/api/bins", requireAuth, async (req, res) => {
  const warehouse = String(req.query.warehouse || "MAIN");
  const location = String(req.query.location || "DEFAULT");
  res.json({ bins: await listBins(warehouse, location) });
});

app.post("/api/move", requireAuth, async (req, res) => {
  const { sku, lot, type, qty, warehouse, location, bin, equipment, note } =
    req.body || {};

  if (!sku || !lot || !type) {
    return res.status(400).json({ error: "Missing sku/lot/type" });
  }

  const q = Number(qty || 1);
  if (!Number.isFinite(q) || q <= 0) {
    return res.status(400).json({ error: "Invalid qty" });
  }

  if (type !== "IN" && type !== "OUT") {
    return res.status(400).json({ error: "Invalid type" });
  }

  const item = await getItemBySkuLot(String(sku), String(lot));
  if (!item) {
    return res.status(404).json({ error: "Item not found. Create it first." });
  }

  const wh = String(warehouse || "MAIN").trim() || "MAIN";
  const loc = String(location || "DEFAULT").trim() || "DEFAULT";
  const b = String(bin || "DEFAULT").trim() || "DEFAULT";
  const eq = String(equipment || "")
    .trim()
    .toUpperCase();

  let createdBom = false;
  if (eq) {
    const bom = await getBomByEquipment(eq);
    if (!bom) {
      await ensureBomHeader(eq);
      createdBom = true;
    }
  }

  try {
    const movement = await addMovementChecked({
      item_id: item.id,
      type,
      qty: q,
      warehouse: wh,
      location: loc,
      bin: b,
      equipment: eq,
      operator_user_id: req.user.id,
      note: note ? String(note).trim() : null,
    });
    if (type === "OUT" && eq) {
      await consumeBomReservation({
        equipment: eq,
        sku: item.sku,
        qty: q,
        consumedAt: movement?.ts || null,
      });
    }
  } catch (e) {
    if (e?.code === "INSUFFICIENT_STOCK") {
      const onhand = await getOnhandForItemAt({
        item_id: item.id,
        warehouse: wh,
        location: loc,
        bin: b,
      });
      return res.status(409).json({ error: `${e.message}`, onhand });
    }
    console.error("/api/move error", {
      message: e?.message,
      code: e?.code,
      detail: e?.detail,
      table: e?.table,
      column: e?.column,
    });
    return res.status(500).json({ error: "Server error" });
  }

  const row = (await getStockRows({ warehouse: null })).find(
    (r) =>
      r.sku === item.sku &&
      r.lot === item.lot &&
      r.warehouse === wh &&
      r.location === loc &&
      r.bin === b,
  );

  res.json({
    ok: true,
    onhand: row ? row.qty_onhand : null,
    bom_created: createdBom,
  });
});

// ---- Exports ----
app.get("/export/stock.xlsx", requireAuth, async (req, res) => {
  const rows = await getStockRows({ warehouse: null });
  const reservations = await listStockReservations();
  const reservationBySku = buildReservationViewBySku({
    reservations,
    stockRows: rows,
  });
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet("Stock");
  ws.columns = [
    { header: "SKU", key: "sku", width: 18 },
    { header: "Description", key: "description", width: 42 },
    { header: "Lot", key: "lot", width: 18 },
    { header: "UoM", key: "uom", width: 10 },
    { header: "InitialQty", key: "initial_qty", width: 12 },
    { header: "Warehouse", key: "warehouse", width: 14 },
    { header: "IN", key: "qty_in", width: 10 },
    { header: "OUT", key: "qty_out", width: 10 },
    { header: "OnHand", key: "qty_onhand", width: 10 },
    {
      header: "UseForEquipment",
      key: "use_for_equipment",
      width: 40,
    },
  ];
  ws.addRows(
    rows.map((r) => ({
      ...r,
      use_for_equipment: (reservationBySku.get(r.sku)?.equipmentRows || [])
        .map((e) => `${e.equipment}: ${e.qtyReserved} ${e.uom}`)
        .concat(
          reservationBySku.get(r.sku)?.warning
            ? [reservationBySku.get(r.sku).warning]
            : [],
        )
        .join(" • "),
    })),
  );
  ws.getRow(1).font = { bold: true };
  ws.autoFilter = "A1:J1";

  res.setHeader(
    "Content-Type",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  );
  res.setHeader("Content-Disposition", `attachment; filename="stock.xlsx"`);
  await wb.xlsx.write(res);
  res.end();
});

app.get("/export/bom/:equipment.xlsx", requireAuth, async (req, res) => {
  const equipment = String(req.params.equipment || "");
  const bom = await getBomByEquipment(equipment);
  if (!bom) return res.status(404).send("BOM non trovato");
  const bomSkus = bom.rows
    .map((r) => String(r.sku || "").trim())
    .filter(Boolean);
  const outMovements = await listOutMovementsByEquipmentAndSkus(
    bom.equipment,
    bomSkus,
  );
  const totalPickedBySku = outMovements.reduce((acc, move) => {
    const sku = String(move.sku || "").trim();
    acc.set(sku, (acc.get(sku) || 0) + Number(move.qty || 0));
    return acc;
  }, new Map());

  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet(`BOM-${bom.equipment}`.slice(0, 31));
  ws.columns = [
    { header: "Equipment", key: "equipment", width: 20 },
    { header: "SKU", key: "sku", width: 18 },
    { header: "Description", key: "description", width: 40 },
    { header: "QtyRequired", key: "qty_required", width: 14 },
    { header: "StockStatus", key: "availability", width: 14 },
    { header: "ReservationNote", key: "reservation_note", width: 42 },
  ];
  ws.addRows(
    bom.rows.map((r) => ({
      equipment: bom.equipment,
      ...r,
      qty_required:
        Number(r.qty_required || 0) +
        Number(totalPickedBySku.get(String(r.sku || "").trim()) || 0),
    })),
  );
  ws.getRow(1).font = { bold: true };
  ws.autoFilter = "A1:F1";

  res.setHeader(
    "Content-Type",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  );
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="BOM-${bom.equipment}.xlsx"`,
  );
  await wb.xlsx.write(res);
  res.end();
});
app.get("/export/movements.xlsx", requireAuth, async (req, res) => {
  const rows = await listMovements(5000);

  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet("Movements");
  ws.columns = [
    { header: "Timestamp", key: "ts", width: 20 },
    { header: "Type", key: "type", width: 8 },
    { header: "Qty", key: "qty", width: 8 },
    { header: "Warehouse", key: "warehouse", width: 14 },
    { header: "Equipment", key: "equipment", width: 18 },
    { header: "SKU", key: "sku", width: 18 },
    { header: "Lot", key: "lot", width: 18 },
    { header: "Description", key: "description", width: 42 },
    { header: "Operator", key: "operator", width: 18 },
    { header: "Note", key: "note", width: 24 },
  ];
  ws.addRows(rows);
  ws.getRow(1).font = { bold: true };
  ws.autoFilter = "A1:J1";

  res.setHeader(
    "Content-Type",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  );
  res.setHeader("Content-Disposition", `attachment; filename="movements.xlsx"`);
  await wb.xlsx.write(res);
  res.end();
});

app.get("/export/items-template.xlsx", requireAuth, async (req, res) => {
  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet("Items");
  ws.columns = [
    { header: "SKU", key: "sku", width: 18 },
    { header: "Description", key: "description", width: 42 },
    { header: "Lot", key: "lot", width: 18 },
    { header: "EntryDate", key: "entry_date", width: 14 },
  ];
  ws.addRow({
    sku: "DKW-12345",
    description: "Esempio descrizione",
    lot: "LOT-001",
    entry_date: "2026-02-23",
  });
  ws.getRow(1).font = { bold: true };
  ws.autoFilter = "A1:D1";

  res.setHeader(
    "Content-Type",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  );
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="items-template.xlsx"`,
  );
  await wb.xlsx.write(res);
  res.end();
});

app.get("/health", (req, res) => res.json({ ok: true }));

const PORT = process.env.PORT || 3000;

(async () => {
  await initDb();
  app.listen(PORT, "0.0.0.0", () => {
    console.log(`QR Stock running on http://localhost:${PORT}`);
    console.log(`For iPhone/Android on LAN: http://<PC_IP>:${PORT}`);
  });
})();
