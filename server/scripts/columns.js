/* eslint-disable no-console */
const path = require('node:path');
const odbc = require('odbc');
const fs = require('node:fs');
const dotenv = require('dotenv');

const envPath = path.resolve(__dirname, '..', '.env');
if (fs.existsSync(envPath)) {
  dotenv.config({ path: envPath });
}

async function main() {
  const table = process.argv[2] || process.env.ACCESS_TABLE_PERMIS || 'Titres';
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

  console.log('[columns] connecting via ODBC...');
  const conn = await odbc.connect(connStr);
  try {
    const meta = await conn.columns(null, null, table, null);
    const rows = Array.isArray(meta) ? meta : (meta?.rows ?? []);
    const mapped = rows.map((r) => ({
      table: r.TABLE_NAME || r.tableName || table,
      column: r.COLUMN_NAME || r.columnName,
      typeName: r.TYPE_NAME || r.typeName,
      dataType: r.DATA_TYPE || r.dataType,
      columnSize: r.COLUMN_SIZE || r.columnSize,
      nullable: r.NULLABLE ?? r.nullable,
      remarks: r.REMARKS || r.remarks,
    }));
    console.log(JSON.stringify({ table, columns: mapped }, null, 2));
  } finally {
    try { await conn.close(); } catch {}
  }
}

main().catch((e) => {
  console.error('[columns] error:', e && (e.message || e));
  if (e && (e.odbcErrors || e.errors)) console.error('odbcErrors:', e.odbcErrors || e.errors);
  process.exit(1);
});

