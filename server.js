import "dotenv/config";
import express from "express";
import path from "path";
import { fileURLToPath } from "url";
import { readFile } from "fs/promises";
import QRCode from "qrcode";
import cookieSession from "cookie-session";
import multer from "multer";
import open from "open";
import XLSX from "xlsx";
import ExcelJS from "exceljs";
import {
  db,
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
  clearAllStockAndMovements,
  findItemByTechnicalKey,
  getBomRowById,
  listItemsByFamilyForBom,
  listBomSubfamiliesForFamily,
  matchBomRowToItem,
  markBomRowToBuy,
  finalizeBom,
  searchStockItemsForManual,
  addManualItemsToBom,
  matchBomRowToMultipleItems,
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

const INCH_TO_MM = {
  "1/4": "6.35",
  "3/8": "9.53",
  "1/2": "12.70",
  "3/4": "19.05",
  1: "25.40",
  '1"': "25.40",
};

function normText(s) {
  return String(s || "")
    .toUpperCase()
    .replace(",", ".")
    .replace(/"/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function detectBrand(text) {
  const s = normText(text);
  if (s.includes("ULTRON")) return "ULTRON";
  if (s.includes("TCC.1")) return "TCC.1";
  if (s.includes("TCC")) return "TCC";
  return "";
}

function detectFamily(text) {
  const s = normText(text);

  if (s.includes("COAX")) return "COAX-TUBE";
  if (s.includes("TUBO") || s.includes("TUBE")) return "TUBE";
  if (s.includes("ELBOW") || s.includes("CURVA")) return "ELBOW";
  if (s.includes("TEE")) return "TEE";
  if (s.includes("REDUC")) return "REDUCER";
  if (s.includes("VALVE")) return "VALVE";

  return "";
}

function extractOdMm(text) {
  const s = normText(text);

  for (const [inch, mm] of Object.entries(INCH_TO_MM)) {
    const cleanInch = inch.replace('"', "");
    if (s.includes(cleanInch)) return mm;
  }

  const m = s.match(/(\d{1,2}\.\d{2})\s*X\s*(\d{1,2}\.\d{2})/);
  if (m) return m[1];

  return "";
}

function extractThicknessMm(text) {
  const s = normText(text);
  const m = s.match(/(\d{1,2}\.\d{2})\s*X\s*(\d{1,2}\.\d{2})/);
  return m ? m[2] : "";
}

function buildTechnicalKey({ description, family, brand, size }) {
  const fullText = `${description || ""} ${family || ""} ${brand || ""} ${size || ""}`;

  const itemType = detectFamily(fullText);
  const itemBrand = brand || detectBrand(fullText);
  const od = extractOdMm(fullText);
  const th = extractThicknessMm(fullText);

  return [itemType, itemBrand, od, th].filter(Boolean).join("|");
}

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

function normalizeHeader(s) {
  return String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ");
}

function textAfterDash(value) {
  const s = String(value || "").trim();
  const parts = s.split(/\s*[-–—]\s*/);
  return parts.length > 1 ? parts.slice(1).join(" - ").trim() : s;
}

function textBeforeDash(value) {
  const s = String(value || "").trim();
  const parts = s.split(/\s*[-–—]\s*/);
  return parts[0].trim();
}

function roundExcel(value, decimals = 2) {
  const n = Number(value);
  if (!Number.isFinite(n)) return 0;
  const factor = Math.pow(10, decimals);
  // Arrotondamento come Excel ROUND/ARROTONDA: metà lontano dallo zero.
  return (
    (Math.sign(n) * Math.round(Math.abs(n) * factor + Number.EPSILON)) / factor
  );
}

function parseEuroNumber(value) {
  if (value === null || value === undefined || value === "") return 0;
  if (typeof value === "number") return Number.isFinite(value) ? value : 0;

  const s = String(value)
    .trim()
    .replace(/€/g, "")
    .replace(/\s+/g, "")
    // elimina i punti solo quando sono separatori migliaia: 1.234,56
    .replace(/\.(?=\d{3}(\D|$))/g, "")
    .replace(",", ".");

  const n = Number(s);
  return Number.isFinite(n) ? n : 0;
}

function formatEuro(value) {
  return `€ ${Number(value || 0).toLocaleString("it-IT", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function parseItemsFromWorksheet(workbook) {
  const sheetName = workbook.SheetNames[0];
  const ws = workbook.Sheets[sheetName];
  const rows = XLSX.utils.sheet_to_json(ws, { defval: "", raw: true });

  const date1904 = !!(
    workbook.Workbook &&
    workbook.Workbook.WBProps &&
    workbook.Workbook.WBProps.date1904
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

  const getValue = (row, ...aliases) => {
    const keys = Object.keys(row);
    const wanted = aliases.map((a) => normalizeHeader(a));

    for (const wantedName of wanted) {
      const exact = keys.find((k) => normalizeHeader(k) === wantedName);
      if (exact) return row[exact];
    }

    for (const wantedName of wanted) {
      const partial = keys.find((k) => normalizeHeader(k).includes(wantedName));
      if (partial) return row[partial];
    }

    return "";
  };

  return rows.map((row) => {
    const family = textAfterDash(getValue(row, "Famiglia", "Family"));
    const dimension_1 = String(
      getValue(
        row,
        "Dimensione_1",
        "Dimensione 1",
        "Dimension 1",
        "OD",
        "Diametro",
      ),
    ).trim();
    const dimension_2 = String(
      getValue(
        row,
        "Dimensione_2",
        "Dimensione 2",
        "Dimension 2",
        "OD2",
        "Diametro 2",
      ),
    ).trim();
    const subfamily = textAfterDash(
      getValue(row, "Serie", "Sottofamiglia", "Sotto famiglia", "Subfamily"),
    );
    const sku = String(
      getValue(row, "SKU", "Sku", "SKU Tecnico", "Codice SKU"),
    ).trim();
    const description = String(
      getValue(row, "Description", "Descrizione"),
    ).trim();
    const lot = String(getValue(row, "Lot", "LOT", "Nr Linde")).trim();
    const lotFallback = String(
      getValue(row, "ID interno", "ID", "Nr.", "Nr"),
    ).trim();

    const entryRaw = getValue(
      row,
      "EntryDate",
      "DataIngresso",
      "Data ingresso",
      "Entry Date",
      "Data Ingresso",
    );
    const entry_date = parseEntryDate(entryRaw);

    const uomRaw = getValue(
      row,
      "UoM",
      "UOM",
      "UM",
      "U.M.",
      "U.M",
      "u.m.",
      "Unit",
      "Unita",
      "Unità",
    );
    const uom = textBeforeDash(uomRaw) || "PC";

    const qtyRaw = getValue(
      row,
      "InitialQty",
      "Qty",
      "Quantita",
      "Quantità",
      "Qta",
      "Q.tà",
      "QTY",
      "Giacenza",
    );
    const initial_qty =
      typeof qtyRaw === "number"
        ? qtyRaw
        : Number(String(qtyRaw || "").replace(",", "."));
    const valueRaw = getValue(row, "Valore", "Value");
    const value_amount = roundExcel(parseEuroNumber(valueRaw), 2);

    const unitCostRaw = getValue(
      row,
      "Costo Unitario",
      "CostoUnitario",
      "Cost Unit",
      "Unit Cost",
    );
    const unit_cost = roundExcel(parseEuroNumber(unitCostRaw), 2);

    return {
      sku,
      description,
      family,
      subfamily,
      lot: lot || (lotFallback ? `ID-${lotFallback}` : "DEFAULT"),
      entry_date,
      uom,
      initial_qty: Number.isFinite(initial_qty) ? initial_qty : 0,
      value_amount: Number.isFinite(value_amount) ? value_amount : 0,
      unit_cost: Number.isFinite(unit_cost) ? unit_cost : 0,
      dimension_1,
      dimension_2,
    };
  });
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
    <tr data-search="${escapeHtml([r.sku, r.description, r.family, r.subfamily, r.dimension_1, r.dimension_2, r.lot, r.uom, r.initial_qty, r.qty_onhand, (reservationBySku.get(r.sku)?.equipmentRows || []).map((e) => `${e.equipment} ${e.qtyRequired} ${e.uom}`).join(" ")].join(" "))}">
      <td>${escapeHtml(r.sku)}</td>
      <td>${escapeHtml(r.description)}</td>
      <td>${escapeHtml(r.subfamily || "")}</td>
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
    <h2>Cerca nello stock</h2>
    <div class="row" style="margin-top:0; align-items:end">
      <label style="min-width:320px">Cerca item
        <input id="stockSearchInput" placeholder="SKU, descrizione, famiglia, dimensione, lotto..." />
      </label>
      <button class="btn" type="button" id="stockSearchBtn">Cerca</button>
      <button class="btn secondary" type="button" id="stockShowAllBtn">Mostra tutto</button>
      <span id="stockSearchCount" class="muted"></span>
    </div>
  </div>

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
            <th>SKU</th><th>Descrizione</th><th>Sottofamiglia</th><th>U.M.</th><th>Qty iniziale</th>
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
<script>
const stockSearchInput = document.getElementById("stockSearchInput");
const stockSearchBtn = document.getElementById("stockSearchBtn");
const stockShowAllBtn = document.getElementById("stockShowAllBtn");
const stockSearchCount = document.getElementById("stockSearchCount");
function applyStockSearch() {
  const q = String(stockSearchInput?.value || "").toLowerCase().trim();
  const stockRows = Array.from(document.querySelectorAll("tbody tr[data-search]"));
  let visible = 0;
  for (const row of stockRows) {
    const haystack = String(row.getAttribute("data-search") || "").toLowerCase();
    const ok = !q || haystack.includes(q);
    row.style.display = ok ? "" : "none";
    if (ok) visible++;
  }
  if (stockSearchCount) stockSearchCount.textContent = q ? (visible + " risultati") : (stockRows.length + " righe totali");
}
stockSearchBtn?.addEventListener("click", applyStockSearch);
stockSearchInput?.addEventListener("keydown", (e) => { if (e.key === "Enter") { e.preventDefault(); applyStockSearch(); } });
stockShowAllBtn?.addEventListener("click", () => { if (stockSearchInput) stockSearchInput.value = ""; applyStockSearch(); });
applyStockSearch();
</script>
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
  const cleared = String(req.query.cleared || "") === "1";
  const clearMessage = cleared
    ? `<div class="flash ok">Stock e movimenti cancellati.</div>`
    : "";
  const rows = items
    .map(
      (it) => `
      <tr data-family="${escapeHtml(it.family || "")}" data-subfamily="${escapeHtml(it.subfamily || "")}">
      <td>${escapeHtml(it.sku)}</td>
      <td>${escapeHtml(it.description)}</td>
      <td>${escapeHtml(it.family || "")}</td>
      <td>${escapeHtml(it.subfamily || "")}</td>
      <td style="text-align:right">${it.initial_qty ?? 0}</td>
      <td>${escapeHtml(it.uom || "")}</td>
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
${clearMessage}

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
    <p class="muted">Header supportati: <span class="mono">SKU, Descrizione, Famiglia, Serie, Giacenza, u.m.</span> (dal template originale: S, B, C, D, O, P).</p>
    <form method="post" action="/items/import" enctype="multipart/form-data">
      <input type="file" name="file" accept=".xlsx,.xls" required />
      <div class="row">
        <button class="btn ok" type="submit">Importa</button>
                 <a class="btn secondary" href="/export/items-template.xlsx">Scarica template</a>
      </div>
    </form>
    ${
      canDeleteItems
        ? `<form method="post" action="/items/reset-stock" onsubmit="return confirm('Confermi la cancellazione TEMPORANEA di stock, movimenti e BOM?');" style="margin-top:12px">
            <button class="btn danger" type="submit">TEMPORARY: elimina stock + movimenti</button>
          </form>`
        : ""
    }
  </div>
  <div class="card">
  <div class="pad">
      <label for="familyFilter">Filtra</label>
      <select id="familyFilter">
        <option value="">Tutte le serie</option>
        ${Array.from(
          new Set(
            items
              .map((it) => String(it.subfamily || "").trim())
              .filter(Boolean),
          ),
        )
          .sort((a, b) => a.localeCompare(b, "it"))
          .map(
            (sf) =>
              `<option value="${escapeHtml(sf)}">${escapeHtml(sf)}</option>`,
          )
          .join("")}
      </select>
    </div>
    <div class="table-wrap">
      <table id="itemsTable">
        <thead><tr><th>SKU</th><th>Descrizione</th><th>Famiglia</th><th>Serie</th><th>Giacenza</th><th>u.m.</th><th>Elimina</th></tr></thead>
        <tbody id="itemsTableBody">
            ${rows || `<tr><td colspan="7" class="muted">Nessun item.</td></tr>`}
        </tbody>
      </table>
    </div>
  </div>
</main>
<script>
document.getElementById("familyFilter")?.addEventListener("change", function () {
  const selectedSubfamily = this.value;
  const rows = document.querySelectorAll("#itemsTableBody tr[data-subfamily]");
  rows.forEach((row) => {
    const rowSubfamily = row.getAttribute("data-subfamily") || "";
    row.style.display = !selectedSubfamily || rowSubfamily === selectedSubfamily ? "" : "none";
  });
});
</script>
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
  const {
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
  } = req.body || {};
  if (!sku || !description || !lot) {
    return res.status(400).send("Missing fields");
  }

  await upsertItem({
    sku: String(sku).trim(),
    description: String(description).trim(),
    family: String(family).trim(),
    subfamily: String(subfamily).trim(),
    lot: String(lot).trim(),
    entry_date: entry_date ? String(entry_date).trim() : null,
    uom: (uom ? String(uom).trim() : "PC") || "PC",
    initial_qty: Number(initial_qty || 0),
    value_amount: roundExcel(parseEuroNumber(value_amount), 2),
    unit_cost: roundExcel(parseEuroNumber(unit_cost), 2),
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
    const parsedRows = parseItemsFromWorksheet(wb);

    let ok = 0;
    let skipped = 0;

    for (const parsed of parsedRows) {
      const {
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
      } = parsed;
      if (!sku || !description || !lot) {
        skipped++;
        continue;
      }

      await upsertItem({
        sku,
        description,
        family: family || "",
        subfamily: subfamily || "",
        lot,
        entry_date,
        uom,
        initial_qty,
        value_amount,
        unit_cost,
        dimension_1,
        dimension_2,
      });
      ok++;
    }

    return res.redirect(`/items?imported=${ok}&skipped=${skipped}`);
  },
);

app.post("/items/import/default-stock", requireAuth, async (req, res) => {
  const defaultStockPath = path.join(__dirname, "Stock_26042025_con_SKU.xlsx");
  const fileBuffer = await readFile(defaultStockPath);
  const wb = XLSX.read(fileBuffer, { type: "buffer", cellDates: true });
  const parsedRows = parseItemsFromWorksheet(wb);

  let ok = 0;
  let skipped = 0;

  for (const parsed of parsedRows) {
    const {
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
    } = parsed;
    if (!sku || !description || !lot) {
      skipped++;
      continue;
    }

    await upsertItem({
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
    });
    ok++;
  }

  return res.redirect(`/items?imported=${ok}&skipped=${skipped}`);
});

app.post("/items/reset-stock", requireAdmin, async (req, res) => {
  await clearAllStockAndMovements();
  return res.redirect("/items?cleared=1");
});

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
    <p class="muted">Header supportati: <span class="mono">DIAMETER, Family, Quantity DIMA / Quantity / Quantity Supplier</span>.</p>
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
  const sheetName = wb.SheetNames.includes("template1")
    ? "template1"
    : wb.SheetNames[0];
  const ws = wb.Sheets[sheetName];
  const matrix = XLSX.utils.sheet_to_json(ws, {
    header: 1,
    defval: "",
    raw: true,
  });

  const norm = (s) =>
    String(s || "")
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, " ")
      .replace(/\s+/g, " ");

  const headerRowIndex = matrix.findIndex((row) => {
    const normalized = row.map(norm);
    const joined = normalized.join("|");
    const hasDiameter = normalized.some(
      (h) => h === "diameter" || h.includes("diameter"),
    );
    const hasFamily = normalized.some(
      (h) => h === "family" || h === "famiglia",
    );
    const hasQuantity =
      normalized.some((h) => h === "quantity dima") ||
      normalized.some((h) => h === "quantity supplier") ||
      normalized.some((h) => h === "quantity") ||
      normalized.some((h) => h === "qty" || h === "q ty" || h === "qta") ||
      joined.includes("quantity dima") ||
      joined.includes("quantity supplier");
    return hasDiameter && hasFamily && hasQuantity;
  });

  if (headerRowIndex < 0) {
    return res
      .status(400)
      .send(
        "Header BOM non trovata: servono colonne 'DIAMETER', 'Family' e 'Quantity DIMA' / 'Quantity' / 'Quantity Supplier'.",
      );
  }

  const headers = matrix[headerRowIndex].map(norm);
  const col = (...names) => {
    const wanted = names.map(norm);
    for (const w of wanted) {
      const idx = headers.findIndex((h) => h === w);
      if (idx >= 0) return idx;
    }
    for (const w of wanted) {
      const idx = headers.findIndex((h) => h.includes(w));
      if (idx >= 0) return idx;
    }
    return -1;
  };

  const idx = {
    no: col("N°", "N", "No"),
    section: col("Section"),
    subsection: col("Subsection"),
    facilities: col("FACILITIES"),
    materials: col("MATERIALS"),
    brand: col("BRAND"),
    characteristics: col("CHARACTERISTICS"),
    diameter: col("DIAMETER"),
    qtySupplier: col(
      "Quantity DIMA",
      "Quantity Supplier",
      "Quantity",
      "Qty",
      "Q.ty",
      "Qta",
    ),
    unit: col("unit", "uom", "um", "u.m."),
    note: col("Note"),
    family: col("Family", "Famiglia"),
  };

  if (idx.qtySupplier < 0 || idx.family < 0 || idx.diameter < 0) {
    return res
      .status(400)
      .send(
        "Colonne obbligatorie mancanti: DIAMETER, Family e Quantity DIMA / Quantity / Quantity Supplier.",
      );
  }

  const numberValue = (value) => {
    if (typeof value === "number") return value;
    const raw = String(value || "").trim();
    if (!raw) return 0;
    const s = raw
      .replace(/\s+/g, "")
      .replace(/\.(?=\d{3}(\D|$))/g, "")
      .replace(",", ".");
    const n = Number(s);
    return Number.isFinite(n) ? n : 0;
  };

  const cell = (row, index) => (index >= 0 ? row[index] : "");
  const parsedRows = [];
  let skipped = 0;

  for (let r = headerRowIndex + 1; r < matrix.length; r++) {
    const row = matrix[r];
    const qtyRequired = numberValue(cell(row, idx.qtySupplier));
    const sourceFamily = String(cell(row, idx.family) || "").trim();
    // Regola richiesta: usare sempre la colonna H / header DIAMETER del BOM come riferimento dimensionale.
    const sourceDimension = String(cell(row, idx.diameter) || "").trim();

    if (!Number.isFinite(qtyRequired) || qtyRequired <= 0) {
      skipped++;
      continue;
    }
    if (!sourceFamily || sourceFamily.toUpperCase() === "SKIP") {
      skipped++;
      continue;
    }

    const descriptionParts = [
      cell(row, idx.section),
      cell(row, idx.subsection),
      cell(row, idx.facilities),
      cell(row, idx.materials),
      cell(row, idx.brand),
      cell(row, idx.characteristics),
      cell(row, idx.diameter),
    ]
      .map((v) => String(v || "").trim())
      .filter(Boolean);

    parsedRows.push({
      sku: "",
      description: descriptionParts.join(" | "),
      qty_required: qtyRequired,
      source_line_no: Number(cell(row, idx.no)) || r + 1,
      source_family: sourceFamily,
      source_dimension: sourceDimension,
      source_unit: String(cell(row, idx.unit) || "").trim(),
    });
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

app.get("/api/bom-row/:rowId/candidates", requireAuth, async (req, res) => {
  const rowId = Number(req.params.rowId);
  if (!Number.isFinite(rowId))
    return res.status(400).json({ error: "Invalid row id" });

  const row = await getBomRowById(rowId);
  if (!row) return res.status(404).json({ error: "Riga BOM non trovata" });

  const family = String(row.source_family || "")
    .trim()
    .toUpperCase();
  const familyTokens = family
    .split(/[;,/|+\s]+/)
    .map((v) => v.trim())
    .filter(Boolean);
  const needsSubfamilyChoice = familyTokens.some(
    (f) =>
      f.startsWith("FIT") ||
      f.startsWith("RED") ||
      f.startsWith("REG") ||
      f.startsWith("VAL") ||
      f.startsWith("MAN"),
  );
  const selectedSubfamilies = String(
    req.query.subfamilies || req.query.subfamily || "",
  )
    .split("||")
    .map((v) => v.trim())
    .filter(Boolean);
  const search = String(req.query.search || "").trim();

  const baseRow = {
    id: row.id,
    equipment: row.equipment,
    description: row.description,
    qty_required: Number(row.qty_required || 0),
    source_family: row.source_family,
    source_dimension: row.source_dimension,
    source_unit: row.source_unit,
  };

  if (needsSubfamilyChoice && selectedSubfamilies.length === 0 && !search) {
    const subfamilies = await listBomSubfamiliesForFamily({
      family: row.source_family,
    });
    return res.json({
      row: baseRow,
      mode: "SUBFAMILY_SELECT",
      subfamilies,
      candidates: [],
    });
  }

  const candidates = await listItemsByFamilyForBom({
    family: row.source_family,
    dimension: row.source_dimension,
    subfamilies: selectedSubfamilies,
    search,
  });

  res.json({
    row: baseRow,
    mode: "ITEM_SELECT",
    selected_subfamilies: selectedSubfamilies,
    search,
    candidates,
  });
});

app.post("/api/bom-row/:rowId/match", requireAuth, async (req, res) => {
  const rowId = Number(req.params.rowId);
  const itemId = Number(req.body?.item_id);
  if (!Number.isFinite(rowId) || !Number.isFinite(itemId)) {
    return res.status(400).json({ error: "Riga BOM o item non valido" });
  }

  try {
    const result = await matchBomRowToItem({
      bomRowId: rowId,
      itemId,
      appendWhenAlreadyMatched: true,
    });
    return res.json({ ok: true, result });
  } catch (error) {
    console.error("/api/bom-row match error", error);
    return res.status(500).json({ error: error.message || "Server error" });
  }
});

app.post("/api/bom-row/:rowId/to-buy", requireAuth, async (req, res) => {
  const rowId = Number(req.params.rowId);
  if (!Number.isFinite(rowId)) {
    return res.status(400).json({ error: "Riga BOM non valida" });
  }

  try {
    const result = await markBomRowToBuy({ bomRowId: rowId });
    return res.json({ ok: true, result });
  } catch (error) {
    console.error("/api/bom-row to-buy error", error);
    return res.status(500).json({ error: error.message || "Server error" });
  }
});

app.post("/bom/:equipment/finalize", requireAuth, async (req, res) => {
  const equipment = String(req.params.equipment || "");
  try {
    const result = await finalizeBom(equipment);
    if (!result.ok) {
      return res.redirect(
        `/bom/${encodeURIComponent(equipment)}?pending=${result.pending}`,
      );
    }
    return res.redirect(`/bom/${encodeURIComponent(equipment)}?finalized=1`);
  } catch (error) {
    console.error("/bom finalize error", error);
    return res.status(500).send(error.message || "Errore durante Termina BOM");
  }
});

app.get("/bom/:equipment/match/:rowId", requireAuth, async (req, res) => {
  const equipment = String(req.params.equipment || "");
  const rowId = Number(req.params.rowId);
  if (!Number.isFinite(rowId) || rowId <= 0)
    return res.status(400).send("Riga BOM non valida");

  const row = await getBomRowById(rowId);
  if (!row) return res.status(404).send("Riga BOM non trovata");

  const selectedSubfamilies = []
    .concat(req.query.subfamily || [])
    .concat(String(req.query.subfamilies || "").split("||"))
    .map((v) => String(v || "").trim())
    .filter(Boolean);
  const search = String(req.query.search || "").trim();
  const family = String(row.source_family || "")
    .trim()
    .toUpperCase();
  const familyTokens = family
    .split(/[;,/|+\s]+/)
    .map((v) => v.trim())
    .filter(Boolean);
  const needsSubfamilyChoice = familyTokens.some(
    (f) =>
      f.startsWith("FIT") ||
      f.startsWith("RED") ||
      f.startsWith("REG") ||
      f.startsWith("VAL") ||
      f.startsWith("MAN"),
  );

  const subfamilies = needsSubfamilyChoice
    ? await listBomSubfamiliesForFamily({ family: row.source_family })
    : [];
  const candidates =
    !needsSubfamilyChoice || selectedSubfamilies.length || search
      ? await listItemsByFamilyForBom({
          family: row.source_family,
          dimension: row.source_dimension,
          subfamilies: selectedSubfamilies,
          search,
        })
      : [];

  const subfamilyChoices = subfamilies
    .map((sf) => {
      const checked = selectedSubfamilies.includes(sf.subfamily)
        ? "checked"
        : "";
      return `<label class="subfamily-choice"><input type="checkbox" name="subfamily" value="${escapeHtml(sf.subfamily)}" ${checked}> <span><b>${escapeHtml(sf.subfamily)}</b> (${Number(sf.items_count || 0)} items)${sf.families ? `<br><small>${escapeHtml(sf.families)}</small>` : ""}</span></label>`;
    })
    .join("");

  const candidateRows = candidates
    .map(
      (c, idx) => `
    <tr>
      <td><input type="checkbox" name="item_id" value="${Number(c.item_id)}"></td>
      <td class="mono">${escapeHtml(c.sku)}</td>
      <td>${escapeHtml(c.description || "")}</td>
      <td>${escapeHtml(c.family || "")}</td>
      <td>${escapeHtml(c.subfamily || "")}</td>
      <td>${escapeHtml(c.dimension_1 || "")}</td>
      <td>${escapeHtml(c.dimension_2 || "")}</td>
      <td>${escapeHtml(c.lot || "")}</td>
      <td>${escapeHtml(c.uom || "")}</td>
      <td style="text-align:right"><b>${Number(c.qty_onhand || 0)}</b></td>
      <td><input name="qty_${Number(c.item_id)}" type="number" step="0.01" min="0" value="${idx === 0 ? Number(row.qty_required || 0) : 0}" style="width:90px"></td>
    </tr>`,
    )
    .join("");

  res.send(`<!doctype html>
<html lang="it">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Scegli item stock • QR Stock</title>
  <link rel="stylesheet" href="/static/css/style.css" />
  <style>
    .table-wrap table, .table-wrap tr, .table-wrap td { background:#1e1e1e; color:#e6e6e6; }
    .table-wrap th { background:#2c2c2c; color:#ffffff; }
    .subfamily-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap:8px; margin-top:10px; }
    .subfamily-choice { display:flex; gap:8px; align-items:flex-start; padding:10px; border:1px solid #374151; border-radius:10px; background:#0f172a; }
  </style>
</head>
<body>
${nav(req, "bom")}
<main class="container">
  <h1>Scegli item da stock</h1>
  <p><a class="btn secondary" href="/bom/${encodeURIComponent(equipment)}">Annulla e torna al BOM</a></p>
  <div class="card pad">
    <p><b>Family BOM:</b> <span class="mono">${escapeHtml(row.source_family || "")}</span></p>
    <p><b>Dimension:</b> <span class="mono">${escapeHtml(row.source_dimension || "")}</span></p>
    <p><b>Qty Supplier:</b> ${Number(row.qty_required || 0)} ${escapeHtml(row.source_unit || "")}</p>
    <p><b>Descrizione:</b> ${escapeHtml(row.description || "")}</p>
  </div>

  <div class="card pad">
    <form method="get" action="/bom/${encodeURIComponent(equipment)}/match/${rowId}">
      <label>Cerca nello stock
        <input name="search" value="${escapeHtml(search)}" placeholder="SKU, descrizione, dimensione, lotto..." />
      </label>
      ${needsSubfamilyChoice ? `<h3>Sottofamiglie</h3><p class="muted">Puoi selezionare una o più sottofamiglie.</p><div class="subfamily-grid">${subfamilyChoices || `<span class="muted">Nessuna sottofamiglia trovata.</span>`}</div>` : ""}
      <div class="row">
        <button class="btn ok" type="submit">Cerca / apri items</button>
        <a class="btn secondary" href="/bom/${encodeURIComponent(equipment)}/match/${rowId}">Pulisci</a>
      </div>
    </form>
  </div>


  <div class="card pad">
    <form method="post" action="/bom/${encodeURIComponent(equipment)}/match/${rowId}/to-buy" onsubmit="return confirm('Segnare questa riga BOM come TO BUY?');">
      <div class="row" style="margin-top:0; align-items:end">
        <label>Quantità TO BUY
          <input name="qty_to_buy" type="number" step="0.01" min="0.01" value="${Number(row.qty_required || 0)}" style="width:140px" />
        </label>
        <button class="btn danger" type="submit">TO BUY</button>
        <span class="muted">Usalo quando l’item non è disponibile a stock oppure devi acquistare solo una parte.</span>
      </div>
    </form>
  </div>
  <div class="card">
    <form method="post" action="/bom/${encodeURIComponent(equipment)}/match/${rowId}/multi" style="margin:0">
      <div class="pad row" style="margin-top:0; align-items:end">
        <button class="btn ok" type="submit">Conferma selezionati</button>
        <span class="muted">Seleziona uno o più item e indica la quantità per ciascuno.</span>
      </div>
      <div class="table-wrap">
        <table>
          <thead><tr><th>Sel.</th><th>SKU</th><th>Descrizione</th><th>Famiglia</th><th>Sottofamiglia</th><th>Dim.1</th><th>Dim.2</th><th>Lot</th><th>U.M.</th><th>Giacenza</th><th>Qty</th></tr></thead>
          <tbody>${candidateRows || `<tr><td colspan="11" class="muted">${needsSubfamilyChoice && !selectedSubfamilies.length && !search ? "Seleziona una sottofamiglia oppure cerca un testo." : "Nessun item trovato."}</td></tr>`}</tbody>
        </table>
      </div>
    </form>
  </div>
</main>
</body>
</html>`);
});

app.post("/bom/:equipment/match/:rowId", requireAuth, async (req, res) => {
  const equipment = String(req.params.equipment || "");
  const rowId = Number(req.params.rowId);
  const itemId = Number(req.body?.item_id);
  if (!Number.isFinite(rowId) || !Number.isFinite(itemId))
    return res.status(400).send("Riga BOM o item non valido");
  try {
    await matchBomRowToItem({
      bomRowId: rowId,
      itemId,
      appendWhenAlreadyMatched: true,
    });
    return res.redirect(`/bom/${encodeURIComponent(equipment)}`);
  } catch (error) {
    console.error("POST match page error", error);
    return res.status(500).send(error.message || "Errore durante il match");
  }
});

app.post(
  "/bom/:equipment/match/:rowId/to-buy",
  requireAuth,
  async (req, res) => {
    const equipment = String(req.params.equipment || "");
    const rowId = Number(req.params.rowId);
    const qtyToBuy = Number(
      String(req.body?.qty_to_buy || "0").replace(",", "."),
    );
    if (!Number.isFinite(rowId) || rowId <= 0)
      return res.status(400).send("Riga BOM non valida");
    try {
      await markBomRowToBuy({
        bomRowId: rowId,
        qtyToBuy:
          Number.isFinite(qtyToBuy) && qtyToBuy > 0 ? qtyToBuy : undefined,
      });
      return res.redirect(`/bom/${encodeURIComponent(equipment)}`);
    } catch (error) {
      console.error("POST match to-buy page error", error);
      return res.status(500).send(error.message || "Errore durante TO BUY");
    }
  },
);

app.post(
  "/bom/:equipment/match/:rowId/multi",
  requireAuth,
  async (req, res) => {
    const equipment = String(req.params.equipment || "");
    const rowId = Number(req.params.rowId);
    const rawIds = Array.isArray(req.body?.item_id)
      ? req.body.item_id
      : req.body?.item_id
        ? [req.body.item_id]
        : [];

    const items = rawIds
      .map((id) => {
        const itemId = Number(id);
        const qty = Number(
          String(req.body?.[`qty_${itemId}`] || "0").replace(",", "."),
        );
        return { item_id: itemId, qty_required: qty };
      })
      .filter(
        (x) =>
          Number.isFinite(x.item_id) &&
          x.item_id > 0 &&
          Number.isFinite(x.qty_required) &&
          x.qty_required > 0,
      );

    if (!Number.isFinite(rowId) || rowId <= 0 || items.length === 0) {
      return res
        .status(400)
        .send(
          "Seleziona almeno un item e indica una quantità maggiore di zero.",
        );
    }

    try {
      await matchBomRowToMultipleItems({ bomRowId: rowId, items });
      return res.redirect(`/bom/${encodeURIComponent(equipment)}`);
    } catch (error) {
      console.error("POST multi match page error", error);
      return res
        .status(500)
        .send(error.message || "Errore durante il multi match");
    }
  },
);

app.get("/bom/:equipment", requireAuth, async (req, res) => {
  const equipment = String(req.params.equipment || "");
  const bom = await getBomByEquipment(equipment);
  if (!bom) return res.status(404).send("BOM non trovato");

  const imported = Number.parseInt(String(req.query.imported || ""), 10);
  const skipped = Number.parseInt(String(req.query.skipped || ""), 10);
  const importMessage =
    Number.isInteger(imported) && Number.isInteger(skipped)
      ? `<div class="flash ok">Import BOM completato. Righe valide=${imported}, righe ignorate=${skipped}. Ora scegli gli item stock per ogni riga TO_MATCH.</div>`
      : "";
  const finalizedMessage =
    String(req.query.finalized || "") === "1"
      ? `<div class="flash ok">BOM terminato e congelato.</div>`
      : "";
  const pendingFinalize = Number.parseInt(String(req.query.pending || ""), 10);
  const finalizeError = Number.isInteger(pendingFinalize)
    ? `<div class="flash error">Non posso terminare il BOM: restano ${pendingFinalize} righe TO_MATCH da scegliere.</div>`
    : "";
  const statusBadge =
    bom.status === "FINALIZED"
      ? `<span class="badge ok">FINALIZED</span>`
      : `<span class="badge">DRAFT</span>`;

  const rows = bom.rows
    .map((r) => {
      const isPending =
        String(r.sku || "").startsWith("__PENDING__") ||
        r.availability === "TO_MATCH";
      const skuLabel = isPending ? "DA SCEGLIERE" : r.sku;
      const chooseButton =
        Number(r.qty_required || 0) > 0
          ? `<a class="btn secondary js-open-match" href="/bom/${encodeURIComponent(bom.equipment)}/match/${Number(r.id)}" data-row-id="${Number(r.id)}">Scegli da stock</a>`
          : "";
      const button = `<div class="row action-buttons">${chooseButton}</div>`;
      return `
    <tr class="${isPending ? "needs-match" : ""}">
      <td>${escapeHtml(r.source_family || "")}</td>
      <td>${escapeHtml(r.source_dimension || "")}</td>
      <td>${escapeHtml(r.description || "")}</td>
      <td style="text-align:right">${Number(r.qty_required || 0)}</td>
      <td>${escapeHtml(r.source_unit || "")}</td>
      <td class="mono">${escapeHtml(skuLabel)}</td>
      <td style="text-align:right">${Number(r.qty_reserved || 0)}</td>
      <td>${escapeHtml(r.availability || "")}</td>
      <td>${button}</td>
    </tr>`;
    })
    .join("");

  res.send(`<!doctype html>
<html lang="it">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>BOM ${escapeHtml(bom.equipment)} • QR Stock</title>
  <link rel="stylesheet" href="/static/css/style.css" />
  <style>
    tr.needs-match { background: #1e1e1e !important; }
    .table-wrap table, .table-wrap tr, .table-wrap td { background:#1e1e1e; color:#e6e6e6; }
    .table-wrap th { background:#2c2c2c; color:#ffffff; }
    dialog.match-dialog { width: min(1100px, 96vw); border: 0; border-radius: 14px; padding: 0; box-shadow: 0 20px 60px rgba(0,0,0,.25); background:#111827; color:#e6e6e6; }
    dialog.match-dialog::backdrop { background: rgba(15,23,42,.65); }
    dialog.match-dialog.force-open { display:block; position:fixed; inset:6vh auto auto 50%; transform:translateX(-50%); z-index:9999; }
    .modal-head { display:flex; align-items:center; justify-content:space-between; gap:12px; padding:14px 18px; border-bottom:1px solid #374151; }
    .modal-body { padding:16px 18px; max-height:70vh; overflow:auto; }
    .pill { display:inline-block; padding:3px 8px; border-radius:999px; background:#23314f; color:#e6e6e6; margin-right:6px; font-size:12px; }
    .bom-actions { align-items:stretch; gap:12px; }
    .bom-actions form { display:inline-flex; margin:0; }
    .bom-actions .btn { min-height:44px; display:inline-flex; align-items:center; justify-content:center; }
    .action-buttons { gap:8px; margin:0; }
    .action-buttons .btn { min-height:40px; }
    .subfamily-select { min-width:320px; background:#0f172a; color:#e6e6e6; border:1px solid #475569; border-radius:10px; padding:10px; }
    .modal-search { width:100%; background:#0f172a; color:#e6e6e6; border:1px solid #475569; border-radius:10px; padding:10px; margin-top:6px; }
    .subfamily-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap:8px; margin-top:10px; }
    .subfamily-choice { display:flex; gap:8px; align-items:flex-start; padding:10px; border:1px solid #374151; border-radius:10px; background:#0f172a; }
    .subfamily-choice input { margin-top:3px; }
  </style>
</head>
<body>
${nav(req, "bom")}
<main class="container">
  <h1>BOM equipment <span class="mono">${escapeHtml(bom.equipment)}</span></h1>
  <p class="muted">Ultimo aggiornamento: ${escapeHtml(formatDateTimeCET(bom.updated_at))}</p>
  ${importMessage}
  ${finalizedMessage}
  ${finalizeError}
  <p>${statusBadge}</p>
  <div class="row bom-actions">
    <a class="btn secondary" href="/bom">← Torna a BOM</a>
    <a class="btn" href="/export/bom/${encodeURIComponent(bom.equipment)}.xlsx">Export BOM (XLSX)</a>
    <button class="btn ok" type="button" onclick="openManualAddModal()">Aggiungi item</button>
    <form method="post" action="/bom/${encodeURIComponent(bom.equipment)}/finalize" style="display:inline" onsubmit="return confirm('Terminare il BOM? Dopo questa conferma il BOM risulta completato.');">
      <button class="btn ok" type="submit">Termina BOM</button>
    </form>
  </div>

  <div class="card">
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Family BOM</th><th>Dimension</th><th>Descrizione BOM</th><th>Qty Supplier</th><th>U.M.</th><th>SKU scelto</th><th>Qty riservata</th><th>Stato</th><th>Azione</th>
          </tr>
        </thead>
        <tbody>
          ${rows || `<tr><td colspan="9" class="muted">Nessuna riga nel BOM.</td></tr>`}
        </tbody>
      </table>
    </div>
  </div>
</main>



  <dialog id="manualAddDialog" style="width:min(1200px,96vw);max-height:90vh;overflow:auto;">
    <div class="row" style="justify-content:space-between;align-items:center">
      <h2>Aggiungi item da stock</h2>
      <button class="btn secondary" type="button" onclick="closeManualAddModal()">Chiudi</button>
    </div>
    <div class="row" style="align-items:end">
      <label style="min-width:320px">Cerca stock
        <input id="manualStockSearch" placeholder="SKU, descrizione, famiglia, dimensione, lotto..." />
      </label>
      <button class="btn" type="button" onclick="searchManualStock()">Cerca</button>
      <span id="manualStockCount" class="muted"></span>
    </div>
    <div id="manualStockResults" class="table-wrap" style="margin-top:12px"></div>
  </dialog>

<script>
const manualDialog = document.getElementById("manualAddDialog");
function openManualAddModal(){
  if (typeof manualDialog.showModal === "function") manualDialog.showModal();
  else manualDialog.setAttribute("open", "open");
  searchManualStock();
}
function closeManualAddModal(){
  if (manualDialog.open && typeof manualDialog.close === "function") manualDialog.close();
  else manualDialog.removeAttribute("open");
}
async function searchManualStock(){
  const q = document.getElementById("manualStockSearch")?.value || "";
  const res = await fetch("/api/stock/manual-search?q=" + encodeURIComponent(q));
  const data = await res.json();
  if (!res.ok || !data.ok) {
    document.getElementById("manualStockResults").innerHTML = htmlEscape(data.error || "Errore ricerca");
    return;
  }
  renderManualStock(data.items || []);
}
function renderManualStock(items){
  const count = document.getElementById("manualStockCount");
  if (count) count.textContent = items.length + " risultati";
  if (!items.length) {
    document.getElementById("manualStockResults").innerHTML = '<p class="muted">Nessun item trovato.</p>';
    return;
  }
  const body = items.map((it) =>
    '<tr>' +
      '<td><input type="checkbox" class="manual-check" data-item-id="' + Number(it.item_id) + '" /></td>' +
      '<td class="mono">' + htmlEscape(it.sku || "") + '</td>' +
      '<td>' + htmlEscape(it.description || "") + '</td>' +
      '<td>' + htmlEscape(it.family || "") + '</td>' +
      '<td>' + htmlEscape(it.subfamily || "") + '</td>' +
      '<td>' + htmlEscape(it.dimension_1 || "") + '</td>' +
      '<td>' + htmlEscape(it.dimension_2 || "") + '</td>' +
      '<td>' + htmlEscape(it.lot || "") + '</td>' +
      '<td>' + htmlEscape(it.uom || "") + '</td>' +
      '<td style="text-align:right"><b>' + Number(it.qty_onhand || 0) + '</b></td>' +
      '<td><input class="manual-qty" data-item-id="' + Number(it.item_id) + '" type="number" step="0.01" min="0" value="0" style="width:90px" /></td>' +
    '</tr>'
  ).join("");
  document.getElementById("manualStockResults").innerHTML =
    '<div class="row" style="margin-bottom:10px"><button class="btn ok" type="button" onclick="addSelectedManualItems()">Aggiungi selezionati al BOM</button></div>' +
    '<table><thead><tr><th>Sel.</th><th>SKU</th><th>Descrizione</th><th>Famiglia</th><th>Sottofamiglia</th><th>Dim.1</th><th>Dim.2</th><th>Lot</th><th>U.M.</th><th>Giacenza</th><th>Qty</th></tr></thead><tbody>' + body + '</tbody></table>';
}
async function addSelectedManualItems(){
  const selected = Array.from(document.querySelectorAll(".manual-check:checked")).map((check) => {
    const itemId = Number(check.dataset.itemId || 0);
    const qtyInput = document.querySelector('.manual-qty[data-item-id="' + itemId + '"]');
    return { item_id: itemId, qty_required: Number(qtyInput?.value || 0) };
  }).filter((x) => Number.isFinite(x.item_id) && x.item_id > 0 && Number.isFinite(x.qty_required) && x.qty_required > 0);
  if (!selected.length) {
    alert("Seleziona almeno un item e inserisci una quantità > 0.");
    return;
  }
  const res = await fetch("/api/bom/${encodeURIComponent(bom.equipment)}/manual-items", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ items: selected })
  });
  const data = await res.json();
  if (!res.ok || !data.ok) {
    alert(data.error || "Errore aggiunta item");
    return;
  }
  window.location.reload();
}
document.getElementById("manualStockSearch")?.addEventListener("keydown", (e) => { if (e.key === "Enter") { e.preventDefault(); searchManualStock(); } });
</script>

<dialog id="matchDialog" class="match-dialog">
  <div class="modal-head">
    <div>
      <h2 style="margin:0">Scegli item da stock</h2>
      <div id="matchMeta" class="muted"></div>
    </div>
    <button class="btn secondary" type="button" onclick="closeMatchModal()">Chiudi</button>
  </div>
  <div class="modal-body">
    <div id="matchCandidates" class="table-wrap muted">Caricamento...</div>
  </div>
</dialog>

<script>
const dialog = document.getElementById("matchDialog");
let activeBomRowId = null;
let activeManualFamilyMode = false;
let activeSelectedSubfamilies = [];
let activeModalSearch = "";

function htmlEscape(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

async function openMatchModal(rowId) {
  activeBomRowId = rowId;
  activeSelectedSubfamilies = [];
  activeModalSearch = "";
  document.getElementById("matchMeta").innerHTML = "";
  document.getElementById("matchCandidates").innerHTML = "Caricamento...";
  try {
    if (dialog && typeof dialog.showModal === "function" && !dialog.open) {
      dialog.showModal();
    } else if (dialog && !dialog.open) {
      dialog.setAttribute("open", "open");
      dialog.classList.add("force-open");
    }
  } catch (error) {
    console.error("Errore apertura modale", error);
    dialog.setAttribute("open", "open");
    dialog.classList.add("force-open");
  }

  const res = await fetch("/api/bom-row/" + rowId + "/candidates");
  const data = await res.json();
  if (!res.ok) {
    document.getElementById("matchCandidates").innerHTML = htmlEscape(data.error || "Errore");
    return;
  }

  const row = data.row;
  activeManualFamilyMode = /(^|[;,/|+\s])(FIT|RED|REG|VAL|MAN)/i.test(String(row.source_family || ""));
  document.getElementById("matchMeta").innerHTML =
    '<span class="pill">Family: ' + htmlEscape(row.source_family) + '</span>' +
    '<span class="pill">Dimension: ' + htmlEscape(row.source_dimension || "-") + '</span>' +
    '<span class="pill">Qty: ' + htmlEscape(row.qty_required) + ' ' + htmlEscape(row.source_unit || "") + '</span>';

  if (data.mode === "SUBFAMILY_SELECT") {
    const options = (data.subfamilies || []).map((sf, idx) =>
      '<label class="subfamily-choice">' +
        '<input type="checkbox" class="subfamilyCheck" value="' + htmlEscape(sf.subfamily) + '"> ' +
        '<span><b>' + htmlEscape(sf.subfamily) + '</b> (' + Number(sf.items_count || 0) + ' items)' +
        (sf.families ? '<br><small>' + htmlEscape(sf.families) + '</small>' : '') +
        '</span>' +
      '</label>'
    ).join("");

    document.getElementById("matchCandidates").innerHTML =
      '<p>Per questa famiglia puoi scegliere <b>una o più sottofamiglie</b>, oppure cercare direttamente tra gli item stock.</p>' +
      '<div class="row" style="align-items:end; gap:10px; margin-bottom:12px">' +
        '<label style="flex:1">Cerca negli item stock' +
          '<input id="modalSearch" class="modal-search" placeholder="SKU, descrizione, dimensione, lotto..." onkeydown="if(event.key===\'Enter\'){loadSubfamilyItems();}">' +
        '</label>' +
        '<button class="btn ok" type="button" onclick="loadSubfamilyItems()">Cerca / apri items</button>' +
        '<button class="btn secondary" type="button" onclick="toggleAllSubfamilies(true)">Seleziona tutte</button>' +
        '<button class="btn secondary" type="button" onclick="toggleAllSubfamilies(false)">Pulisci</button>' +
      '</div>' +
      '<div class="subfamily-grid">' + options + '</div>';
    setTimeout(() => document.getElementById("modalSearch")?.focus(), 50);
    return;
  }

  renderCandidateItems(data.candidates || []);
}

function toggleAllSubfamilies(checked) {
  document.querySelectorAll(".subfamilyCheck").forEach((el) => { el.checked = checked; });
}

async function loadSubfamilyItems() {
  const checkboxes = Array.from(document.querySelectorAll(".subfamilyCheck"));
  const selected = checkboxes.length
    ? checkboxes.filter((el) => el.checked).map((el) => el.value)
    : activeSelectedSubfamilies;
  const searchInput = document.getElementById("modalSearch");
  const search = searchInput ? (searchInput.value || "") : activeModalSearch;
  if (!selected.length && !search.trim()) {
    alert("Seleziona almeno una sottofamiglia oppure inserisci un testo nel campo Cerca");
    return;
  }
  activeSelectedSubfamilies = selected;
  activeModalSearch = search;
  document.getElementById("matchCandidates").innerHTML = "Caricamento items...";
  const params = new URLSearchParams();
  if (selected.length) params.set("subfamilies", selected.join("||"));
  if (search.trim()) params.set("search", search.trim());
  const res = await fetch("/api/bom-row/" + activeBomRowId + "/candidates?" + params.toString());
  const data = await res.json();
  if (!res.ok) {
    document.getElementById("matchCandidates").innerHTML = htmlEscape(data.error || "Errore");
    return;
  }
  renderCandidateItems(data.candidates || []);
}

function renderCandidateItems(candidates) {
  if (!candidates.length) {
    document.getElementById("matchCandidates").innerHTML =
      '<p>Nessun item trovato. Verifica Famiglia/Sottofamiglia nello stock oppure usa il campo cerca.</p>';
    return;
  }

  const defaultQty = Number((activeBomRow && activeBomRow.qty_required) || 0);
  const body = candidates.map((c, idx) =>
    '<tr>' +
      '<td><input type="checkbox" class="match-check" data-item-id="' + Number(c.item_id) + '" /></td>' +
      '<td class="mono">' + htmlEscape(c.sku) + '</td>' +
      '<td>' + htmlEscape(c.description) + '</td>' +
      '<td>' + htmlEscape(c.family || "") + '</td>' +
      '<td>' + htmlEscape(c.subfamily || "") + '</td>' +
      '<td>' + htmlEscape(c.dimension_1 || "") + '</td>' +
      '<td>' + htmlEscape(c.dimension_2 || "") + '</td>' +
      '<td>' + htmlEscape(c.dimension_match ? "SI" : "NO") + '</td>' +
      '<td>' + htmlEscape(c.lot || "") + '</td>' +
      '<td>' + htmlEscape(c.uom || "") + '</td>' +
      '<td style="text-align:right"><b>' + Number(c.qty_onhand || 0) + '</b></td>' +
      '<td><input class="match-qty" data-item-id="' + Number(c.item_id) + '" type="number" step="0.01" min="0" value="' + (idx === 0 ? defaultQty : 0) + '" style="width:90px" /></td>' +
    '</tr>'
  ).join("");

  document.getElementById("matchCandidates").innerHTML =
    '<div class="row" style="margin-bottom:10px"><button class="btn ok" type="button" onclick="confirmSelectedMatches()">Conferma selezionati</button></div>' +
    '<table>' +
      '<thead><tr><th>Sel.</th><th>SKU</th><th>Descrizione</th><th>Famiglia stock</th><th>Sottofamiglia</th><th>Dim.1</th><th>Dim.2</th><th>Dim. match</th><th>Lot</th><th>U.M.</th><th>Giacenza</th><th>Qty</th></tr></thead>' +
      '<tbody>' + body + '</tbody>' +
    '</table>';
}

function closeMatchModal() {
  if (!dialog) return;
  dialog.classList.remove("force-open");
  if (typeof dialog.close === "function" && dialog.open) dialog.close();
  else dialog.removeAttribute("open");
}


document.addEventListener("click", (event) => {
  const btn = event.target.closest(".js-open-match");
  if (!btn) return;
  event.preventDefault();
  const rowId = Number(btn.dataset.rowId || 0);
  if (!Number.isFinite(rowId) || rowId <= 0) {
    alert("ID riga BOM non valido");
    return;
  }
  openMatchModal(rowId);
});

</script>
</body>
</html>`);
});

app.get("/api/stock/manual-search", requireAuth, async (req, res) => {
  try {
    const search = String(req.query.q || "").trim();
    const items = await searchStockItemsForManual({ search, limit: 300 });
    return res.json({ ok: true, items });
  } catch (error) {
    console.error("/api/stock/manual-search error", error);
    return res
      .status(500)
      .json({ ok: false, error: error.message || "Errore ricerca stock" });
  }
});

app.post("/api/bom/:equipment/manual-items", requireAuth, async (req, res) => {
  const equipment = String(req.params.equipment || "").trim();
  try {
    const result = await addManualItemsToBom({
      equipment,
      items: Array.isArray(req.body?.items) ? req.body.items : [],
    });
    return res.json({ ok: true, result });
  } catch (error) {
    console.error("/api/bom manual-items error", error);
    return res
      .status(500)
      .json({ ok: false, error: error.message || "Errore aggiunta manuale" });
  }
});

app.post("/api/bom-row/:rowId/multi-match", requireAuth, async (req, res) => {
  const rowId = Number(req.params.rowId);
  const selected = (Array.isArray(req.body?.items) ? req.body.items : [])
    .map((x) => ({
      item_id: Number(x.item_id),
      qty_required: Number(x.qty_required || 0),
    }))
    .filter(
      (x) =>
        Number.isFinite(x.item_id) &&
        x.item_id > 0 &&
        Number.isFinite(x.qty_required) &&
        x.qty_required > 0,
    );

  if (!Number.isFinite(rowId) || rowId <= 0 || selected.length === 0) {
    return res.status(400).json({ ok: false, error: "Selezione non valida" });
  }

  try {
    await matchBomRowToMultipleItems({ bomRowId: rowId, items: selected });
    return res.json({ ok: true });
  } catch (error) {
    console.error("/api/bom-row multi-match error", error);
    return res
      .status(500)
      .json({ ok: false, error: error.message || "Errore multi match" });
  }
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
      <a class="btn secondary" href="/bom">Ritorna a BOM</a>      </div>
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
    { header: "Descrizione", key: "description", width: 42 },
    { header: "Sottofamiglia", key: "subfamily", width: 22 },
    { header: "U.M.", key: "uom", width: 10 },
    { header: "Qty iniziale", key: "initial_qty", width: 12 },
    { header: "IN", key: "qty_in", width: 10 },
    { header: "OUT", key: "qty_out", width: 10 },
    { header: "On hand", key: "qty_onhand", width: 10 },
    {
      header: "Da usare per equipment",
      key: "use_for_equipment",
      width: 40,
    },
    { header: "Azione", key: "action", width: 12 },
  ];
  ws.addRows(
    rows.map((r) => ({
      ...r,
      action: "IN / OUT",
      use_for_equipment: (reservationBySku.get(r.sku)?.equipmentRows || [])
        .map((e) => `${e.equipment}: ${e.qtyRequired} ${e.uom}`)
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
  const templatePath = path.join(__dirname, "Template_ORIGINAL.xlsx");
  const fileBuffer = await readFile(templatePath);

  res.setHeader(
    "Content-Type",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  );
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="Template_ORIGINAL.xlsx"`,
  );
  res.send(fileBuffer);
});

app.get("/health", (req, res) => res.json({ ok: true }));

const PORT = process.env.PORT || 3000;

(async () => {
  await initDb();

  app.listen(PORT, "0.0.0.0", async () => {
    const url = `http://localhost:${PORT}`;

    console.log(`QR Stock running on ${url}`);
    console.log(`For iPhone/Android on LAN: http://<PC_IP>:${PORT}`);

    if (process.env.NODE_ENV !== "production") {
      try {
        await open(url, { app: { name: "chrome" } });
      } catch (err) {
        await open(url);
      }
    }
  });
})();
