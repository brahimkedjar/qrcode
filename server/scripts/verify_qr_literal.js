/* eslint-disable no-console */
const path = require('node:path');
const odbc = require('odbc');
const fs = require('node:fs');
const dotenv = require('dotenv');

const envPath = path.resolve(__dirname, '..', '.env');
if (fs.existsSync(envPath)) {
  dotenv.config({ path: envPath });
}

function escapeLiteral(s) {
  return String(s).replace(/'/g, "''");
}

async function main() {
  const table = process.env.ACCESS_TABLE_PERMIS || 'Titres';
  const qr = (process.argv[2] || '').trim();
  if (!qr) {
    console.error('Usage: node scripts/verify_qr_literal.js <QrCode>');
    process.exit(2);
  }
  const dbq = process.env.ACCESS_DB_PATH;
  if (!dbq || !fs.existsSync(dbq)) {
    console.error('ACCESS_DB_PATH not found or file missing:', dbq);
    process.exit(2);
  }
  let connStr = (process.env.ACCESS_ODBC_CONN || '').trim();
  if (!connStr) {
    connStr = `Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=${dbq};Uid=Admin;Pwd=;`;
  } else {
    if (!/DBQ=/i.test(connStr)) connStr = connStr.replace(/;?\s*$/,'') + `;DBQ=${dbq}`;
    if (!/Uid=/i.test(connStr)) connStr += ';Uid=Admin';
    if (!/Pwd=/i.test(connStr)) connStr += ';Pwd=';
    if (!/Driver=/i.test(connStr)) connStr = `Driver={Microsoft Access Driver (*.mdb, *.accdb)};` + connStr;
  }

  console.log('[verify_qr_literal] connecting via ODBC...');
  const conn = await odbc.connect(connStr);
  try {
    const lit = `'${escapeLiteral(qr)}'`;
    const sql = `SELECT TOP 1 id, Code, QrCode, DateHeureSysteme, Qrinsererpar FROM ${table} WHERE [QrCode] = ${lit}`;
    console.log('[verify_qr_literal] SQL:', sql);
    const result = await conn.query(sql);
    const rows = Array.isArray(result) ? result : (result?.rows ?? []);
    console.log(JSON.stringify({ exists: rows.length > 0, rows }, null, 2));
  } finally {
    try { await conn.close(); } catch {}
  }
}

main().catch((e) => {
  console.error('[verify_qr_literal] error:', e && (e.message || e));
  if (e && (e.odbcErrors || e.errors)) console.error('odbcErrors:', e.odbcErrors || e.errors);
  process.exit(1);
});

