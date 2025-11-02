import path from 'node:path';
import dotenv from 'dotenv';
import { AccessService } from '../src/permis/access.service';
import { PermisService } from '../src/permis/permis.service';

const baseDir = path.resolve(__dirname, '..');
const rootDir = path.resolve(baseDir, '..');

dotenv.config({ path: path.join(baseDir, '.env') });

const cmadonnees =
  process.env.ACCESS_DB_PATH ||
  path.join(process.env.USERPROFILE || rootDir, 'Desktop', 'CMADONNEES.mdb');
const cmasig =
  process.env.CMASIG_DB_PATH ||
  path.join(process.env.USERPROFILE || rootDir, 'Desktop', 'CMASIG.mdb');

process.env.ACCESS_DB_PATH = cmadonnees;
process.env.CMASIG_DB_PATH = cmasig;
process.env.ACCESS_MODE = process.env.ACCESS_MODE || 'odbc';
process.env.ACCESS_OLEDB_PROVIDER = process.env.ACCESS_OLEDB_PROVIDER || 'Microsoft.ACE.OLEDB.12.0';
process.env.ACCESS_X64 = process.env.ACCESS_X64 || '1';

async function run() {
  const access = new AccessService();
  const service = new PermisService(access as any);
  const id = process.argv[2] || '6300';
  const optionDate = process.argv[3] ? new Date(process.argv[3]) : undefined;
  try {
    const before = await access.runExternalQuery(cmasig, `SELECT TOP 1 [CodeType], [TypeTitre] FROM Titres WHERE [idTitre] = ${id}`);
    console.log('[Before CMASIG]', before);
  } catch (err) {
    console.warn('[Before CMASIG] lookup failed', err);
  }
  try {
    const result = await service.optPermisType(id, optionDate);
    console.log('[optPermisType] result =>', result);
  } catch (err) {
    console.error('[optPermisType] error', err);
  }
  try {
    const after = await access.runExternalQuery(cmasig, `SELECT TOP 1 [CodeType], [TypeTitre] FROM Titres WHERE [idTitre] = ${id}`);
    console.log('[After CMASIG]', after);
  } catch (err) {
    console.warn('[After CMASIG] lookup failed', err);
  }
  try {
    const permitRows = await access.query(`SELECT TOP 1 * FROM ${process.env.ACCESS_TABLE_PERMIS || 'Titres'} WHERE ${process.env.ACCESS_COL_PERMIS_ID || 'id'} = ${id}`);
    console.log('[CMADONNEES permit snapshot]', permitRows[0]);
  } catch (err) {
    console.warn('[CMADONNEES] lookup failed', err);
  }
  process.exit(0);
}

run();
