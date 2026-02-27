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
  getItemBySkuLot,
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

function escapeHtml(s = "") {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

async function requireAuth(req, res, next) {
  if (!req.session?.user_id)
    return res.redirect(`/login?next=${encodeURIComponent(req.originalUrl)}`);
  req.user = await getUserById(req.session.user_id);
  return next();
}

async function requireAdmin(req, res, next) {
  if (!req.session?.user_id)
    return res.redirect(`/login?next=${encodeURIComponent(req.originalUrl)}`);
  const u = await getUserById(req.session.user_id);
  if (!u || u.role !== "admin")
    return res.status(403).send("Forbidden (admin only)");
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
  if (!u)
    return res.status(401).send("PIN non valido. <a href='/login'>Riprova</a>");
  req.session.user_id = u.id;
  res.redirect(next || "/");
});

app.get("/logout", (req, res) => {
  req.session = null;
  res.redirect("/login");
});

// ---- Pages ----
app.get("/", requireAuth, async (req, res) => {
  const wh = req.query.warehouse ? String(req.query.warehouse) : "";
  const stock = await getStockRows({ warehouse: wh || null });
  const whList = await listWarehouses();

  const whOptions = whList
    .map(
      (x) =>
        `<option value="${escapeHtml(x)}" ${x === wh ? "selected" : ""}>${escapeHtml(x)}</option>`,
    )
    .join("");

  const rows = stock
    .map(
      (r) => `
    <tr>
      <td>${escapeHtml(r.sku)}</td>
      <td>${escapeHtml(r.description)}</td>
      <td>${escapeHtml(r.lot)}</td>
      <td>${escapeHtml(r.entry_date || "")}</td>
      <td>${escapeHtml(r.uom || "")}</td>
      <td style="text-align:right">${r.initial_qty ?? 0}</td>
      <td>${escapeHtml(r.warehouse)}</td>
      <td>${escapeHtml(r.location)}</td>
      <td>${escapeHtml(r.bin)}</td>
      <td style="text-align:right">${r.qty_in}</td>
      <td style="text-align:right">${r.qty_out}</td>
      <td style="text-align:right"><b>${r.qty_onhand}</b></td>
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
  <h1>Stock (leggibile anche da iPhone)</h1>
  <p class="muted">Apri dal telefono: <span class="mono">${escapeHtml(getBaseUrl(req))}</span></p>

  <div class="card pad">
    <div class="row" style="margin-top:0">
      <form method="get" action="/" class="row" style="margin-top:0">
        <label class="muted">Warehouse
          <select name="warehouse">
            <option value="">Tutti</option>
            ${whOptions}
          </select>
        </label>
        <button class="btn secondary" type="submit">Filtra</button>
      </form>

      <a class="btn" href="/export/stock.xlsx">Export Stock (XLSX)</a>
      <a class="btn secondary" href="/scan">Scanner (webcam)</a>
    </div>
  </div>

  <div class="card">
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>SKU</th><th>Descrizione</th><th>Lot</th><th>Data ingresso</th><th>U.M.</th><th>Qty iniziale</th>
            <th>Warehouse</th><th>Location</th><th>Bin</th>
            <th>IN</th><th>OUT</th><th>On hand</th>
          </tr>
        </thead>
        <tbody>
          ${rows || `<tr><td colspan="12" class="muted">Nessun dato. Vai su “Items” per aggiungere articoli.</td></tr>`}
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
  const rows = items
    .map(
      (it) => `
    <tr>
      <td>${escapeHtml(it.sku)}</td>
      <td>${escapeHtml(it.description)}</td>
      <td>${escapeHtml(it.lot)}</td>
      <td>${escapeHtml(it.entry_date || "")}</td>
      <td>${escapeHtml(it.uom || "")}</td>
      <td style="text-align:right">${it.initial_qty ?? 0}</td>
      <td>${escapeHtml(it.created_at)}</td>
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

  <div class="card pad">
    <h2>Aggiungi / aggiorna item (SKU+Lot univoco)</h2>
    <form method="post" action="/items">
      <div class="form-grid">
        <label>SKU<input name="sku" required placeholder="es. DKW-12345" /></label>
        <label>Lot<input name="lot" required placeholder="es. LOT2026-01" /></label>
        <label>Data ingresso
  <input name="entry_date" type="date" />
</label>
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
    <p class="muted">Header supportati: <span class="mono">SKU, Description/Descrizione, Lot, EntryDate/DataIngresso</span>.</p>
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
        <thead><tr><th>SKU</th><th>Descrizione</th><th>Lot</th><th>Data ingresso</th><th>U.M.</th><th>Qty iniziale</th><th>Creato</th></tr></thead>
        <tbody>
          ${rows || `<tr><td colspan="7" class="muted">Nessun item.</td></tr>`}
        </tbody>
      </table>
    </div>
  </div>
</main>
<script>
(function(){
  const el = document.querySelector('input[name="entry_date"]');
  if (!el) return;
  if (!el.value) el.value = new Date().toISOString().slice(0,10);
})();
</script>
</body>
</html>`);
});

app.post("/items", requireAuth, async (req, res) => {
  const { sku, description, lot, entry_date, uom, initial_qty } =
    req.body || {};
  if (!sku || !description || !lot)
    return res.status(400).send("Missing fields");
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

    // cellDates:true -> if the sheet stores dates as dates, xlsx will expose them as Date objects
    const wb = XLSX.read(req.file.buffer, { type: "buffer", cellDates: true });
    const sheetName = wb.SheetNames[0];
    const ws = wb.Sheets[sheetName];

    // raw:true keeps numbers/dates as-is (e.g., Excel serial numbers, Date objects)
    const rows = XLSX.utils.sheet_to_json(ws, { defval: "", raw: true });

    const norm = (s) =>
      String(s || "")
        .trim()
        .toLowerCase();

    // Determine 1904 date system if present
    const date1904 = !!(
      wb.Workbook &&
      wb.Workbook.WBProps &&
      wb.Workbook.WBProps.date1904
    );

    function toIsoDate(d) {
      // YYYY-MM-DD in local time (stable for inventory purposes)
      const yyyy = d.getFullYear();
      const mm = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      return `${yyyy}-${mm}-${dd}`;
    }

    function parseEntryDate(value) {
      if (value === null || value === undefined) return null;

      // 1) Date object
      if (value instanceof Date && !Number.isNaN(value.getTime())) {
        return toIsoDate(value);
      }

      // 2) Excel serial number (e.g., 45234)
      if (typeof value === "number" && Number.isFinite(value)) {
        // Use xlsx SSF helper
        const dc = XLSX.SSF.parse_date_code(value, { date1904 });
        if (dc && dc.y && dc.m && dc.d) {
          const d = new Date(dc.y, dc.m - 1, dc.d);
          return toIsoDate(d);
        }
        return null;
      }

      // 3) String: accept ISO (YYYY-MM-DD) or Italian (DD/MM/YYYY)
      const s = String(value).trim();
      if (!s) return null;

      // Formula like =TODAY() / =NOW() -> use today's date
      if (s.startsWith("=")) {
        const today = new Date();
        return toIsoDate(today);
      }

      // ISO
      const iso = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
      if (iso) return s;

      // Italian dd/mm/yyyy or dd-mm-yyyy
      const it = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})$/);
      if (it) {
        const dd = Number(it[1]);
        const mm = Number(it[2]);
        const yyyy = Number(it[3]);
        const d = new Date(yyyy, mm - 1, dd);
        if (!Number.isNaN(d.getTime())) return toIsoDate(d);
      }

      // Fallback: try Date.parse (e.g., "2026/02/24")
      const t = Date.parse(s);
      if (!Number.isNaN(t)) return toIsoDate(new Date(t));

      return null;
    }

    let ok = 0,
      skipped = 0;

    for (const r of rows) {
      const keys = Object.keys(r);

      const get = (...names) => {
        const wanted = names.map((n) => norm(n));
        // exact match
        for (const n of wanted) {
          const k = keys.find((k) => norm(k) === n);
          if (k) return r[k];
        }
        // contains match (handles headers like "Description/Descrizione")
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

    res.send(
      `Import completato. OK=${ok}, Skipped=${skipped}. <a href="/items">Torna a Items</a>`,
    );
  },
);
app.get("/labels", requireAuth, async (req, res) => {
  const items = await listItems();
  const baseUrl = getBaseUrl(req);

  // QR payload requirement: SKU + Lot + Description (embedded in URL so iPhone camera can open)
  const cards = await Promise.all(
    items.map(async (it) => {
      const url = `${baseUrl}/scanlink?sku=${encodeURIComponent(it.sku)}&lot=${encodeURIComponent(it.lot)}&d=${encodeURIComponent(it.description)}`;
      const dataUrl = await QRCode.toDataURL(url, { margin: 1, scale: 6 });
      return `
      <div class="label">
        <img class="qr" src="${dataUrl}" alt="QR" />
        <div class="label-text">
          <div class="mono"><b>${escapeHtml(it.sku)}</b></div>
          <div class="small">${escapeHtml(it.description)}</div>
          <div class="small">Lot: <span class="mono">${escapeHtml(it.lot)}</span></div>
          ${it.entry_date ? `<div class="small">Ingresso: <span class="mono">${escapeHtml(it.entry_date)}</span></div>` : ``}
          <div class="tiny muted">QR payload: SKU+Lot+Description</div>
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
  <p class="muted no-print">Inquadra con la <b>Fotocamera iPhone</b>: si apre la pagina per fare IN/OUT.</p>
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

app.get("/scanlink", requireAuth, async (req, res) => {
  const { sku, lot } = req.query;
  if (!sku || !lot) return res.status(400).send("Missing sku/lot");
  const item = await getItemBySkuLot(String(sku), String(lot));
  if (!item)
    return res.status(404).send("Item not found. Create it in /items first.");

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
        <label>Warehouse
          <input name="warehouse" placeholder="MAIN" />
        </label>
        <label>Location
          <input name="location" placeholder="DEFAULT" />
        </label>
        <label>Bin
          <input name="bin" placeholder="DEFAULT" />
        </label>
        <label>Quantità
          <input name="qty" type="number" min="0.01" step="0.01" value="1" required />
        </label>
        <label class="span2">Note (opzionale)
          <input name="note" placeholder="es. carico da fornitore / scarico produzione..." />
        </label>
      </div>

      <div class="row">
        <button class="btn ok" type="button" onclick="sendMove('IN')">IN</button>
        <button class="btn danger" type="button" onclick="sendMove('OUT')">OUT</button>
        <a class="btn secondary" href="/">Stock</a>
      </div>

      <div id="msg" class="flash" style="margin-top:10px; display:none;"></div>
    </form>

    <div class="hr"></div>
    <p class="muted">Tip: salva questa pagina in Home. Warehouse/Location/Bin vengono ricordati sul telefono.</p>
  </div>
</main>

<script>
(function restoreLoc(){
  const f = document.getElementById('moveForm');
  const get = (k) => localStorage.getItem('qrstock_' + k) || '';
  f.warehouse.value = get('warehouse') || 'MAIN';
  f.location.value = get('location') || 'DEFAULT';
  f.bin.value = get('bin') || 'DEFAULT';
})();
function saveLoc(payload){
  localStorage.setItem('qrstock_warehouse', payload.warehouse);
  localStorage.setItem('qrstock_location', payload.location);
  localStorage.setItem('qrstock_bin', payload.bin);
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
  const payload = {
    sku: data.get('sku'),
    lot: data.get('lot'),
    qty: Number(data.get('qty') || 1),
    type,
    warehouse: String(data.get('warehouse')||'MAIN').trim() || 'MAIN',
    location: String(data.get('location')||'DEFAULT').trim() || 'DEFAULT',
    bin: String(data.get('bin')||'DEFAULT').trim() || 'DEFAULT',
    note: String(data.get('note')||'').trim()
  };

  saveLoc(payload);
  showMsg("Invio...", true);

  const res = await fetch('/api/move', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });

  const out = await res.json().catch(() => ({}));
  if (!res.ok) {
    showMsg("Errore: " + (out.error || res.statusText) + (out.onhand!=null ? " | on-hand=" + out.onhand : ""), false);
    return;
  }
  showMsg("OK ✓ Nuovo on-hand ("+payload.warehouse+"/"+payload.location+"/"+payload.bin+"): " + out.onhand, true);
}
</script>
</body>
</html>`);
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
      Se non hai HTTPS, usa la <b>Fotocamera</b> con le etichette: aprirà <span class="mono">/scanlink</span>.
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
      <td>${escapeHtml(m.ts)}</td>
      <td>${escapeHtml(m.type)}</td>
      <td style="text-align:right">${m.qty}</td>
      <td>${escapeHtml(m.warehouse)}</td>
      <td>${escapeHtml(m.location)}</td>
      <td>${escapeHtml(m.bin)}</td>
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
    <div class="row" style="margin-top:0">
      <a class="btn" href="/export/movements.xlsx">Export Movimenti (XLSX)</a>
    </div>
  </div>
  <div class="card">
    <div class="table-wrap">
      <table>
        <thead><tr>
          <th>Data/ora</th><th>Tipo</th><th>Qty</th>
          <th>Warehouse</th><th>Location</th><th>Bin</th>
          <th>SKU</th><th>Lot</th><th>Descrizione</th><th>Operatore</th><th>Note</th>
        </tr></thead>
        <tbody>
          ${rows || `<tr><td colspan="11" class="muted">Nessun movimento.</td></tr>`}
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
  const { sku, lot, type, qty, warehouse, location, bin, note } =
    req.body || {};
  if (!sku || !lot || !type)
    return res.status(400).json({ error: "Missing sku/lot/type" });
  const q = Number(qty || 1);
  if (!Number.isFinite(q) || q <= 0)
    return res.status(400).json({ error: "Invalid qty" });
  if (type !== "IN" && type !== "OUT")
    return res.status(400).json({ error: "Invalid type" });

  const item = await getItemBySkuLot(String(sku), String(lot));
  if (!item)
    return res.status(404).json({ error: "Item not found. Create it first." });

  const wh = String(warehouse || "MAIN").trim() || "MAIN";
  const loc = String(location || "DEFAULT").trim() || "DEFAULT";
  const b = String(bin || "DEFAULT").trim() || "DEFAULT";

  try {
    await addMovementChecked({
      item_id: item.id,
      type,
      qty: q,
      warehouse: wh,
      location: loc,
      bin: b,
      operator_user_id: req.user.id,
      note: note ? String(note).trim() : null,
    });
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
  res.json({ ok: true, onhand: row ? row.qty_onhand : null });
});

// ---- Exports ----
app.get("/export/stock.xlsx", requireAuth, async (req, res) => {
  const rows = await getStockRows({ warehouse: null });

  const wb = new ExcelJS.Workbook();
  const ws = wb.addWorksheet("Stock");
  ws.columns = [
    { header: "SKU", key: "sku", width: 18 },
    { header: "Description", key: "description", width: 42 },
    { header: "Lot", key: "lot", width: 18 },
    { header: "EntryDate", key: "entry_date", width: 14 },
    { header: "UoM", key: "uom", width: 10 },
    { header: "InitialQty", key: "initial_qty", width: 12 },
    { header: "Warehouse", key: "warehouse", width: 14 },
    { header: "Location", key: "location", width: 14 },
    { header: "Bin", key: "bin", width: 14 },
    { header: "IN", key: "qty_in", width: 10 },
    { header: "OUT", key: "qty_out", width: 10 },
    { header: "OnHand", key: "qty_onhand", width: 10 },
  ];
  ws.addRows(rows);
  ws.getRow(1).font = { bold: true };
  ws.autoFilter = "A1:L1";

  res.setHeader(
    "Content-Type",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  );
  res.setHeader("Content-Disposition", `attachment; filename="stock.xlsx"`);
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
    { header: "Location", key: "location", width: 14 },
    { header: "Bin", key: "bin", width: 14 },
    { header: "SKU", key: "sku", width: 18 },
    { header: "Lot", key: "lot", width: 18 },
    { header: "Description", key: "description", width: 42 },
    { header: "Operator", key: "operator", width: 18 },
    { header: "Note", key: "note", width: 24 },
  ];
  ws.addRows(rows);
  ws.getRow(1).font = { bold: true };
  ws.autoFilter = "A1:K1";

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
    console.log(`For iPhone on LAN: http://<PC_IP>:${PORT}`);
  });
})();
