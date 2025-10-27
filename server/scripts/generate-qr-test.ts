import 'reflect-metadata';
import dotenv from 'dotenv';
// Load environment from current working directory (tm-app/server)
dotenv.config();

import { AccessService } from '../src/permis/access.service';
import { PermisService } from '../src/permis/permis.service';

async function main() {
  const access = new AccessService();
  const permis = new PermisService(access as any);

  // Try to pick one permis ID to test with. Prefer one without QR yet.
  let idRow: any | null = null;
  try {
    // QrCode column may not exist yet; get an ID without referencing it.
    const rows = await access.query('SELECT TOP 1 id FROM Titres ORDER BY id DESC');
    if (!rows || rows.length === 0) {
      console.error('No records found in Titres table.');
      process.exit(1);
    }
    idRow = rows[0];
  } catch (e) {
    console.error('Failed to read Titres table:', e);
    process.exit(1);
  }

  const id = String((idRow as any).id);
  console.log('Testing QR generation for permis id =', id);

  try {
    const result = await permis.generateAndSaveQrCode(id);
    console.log('generateAndSaveQrCode result:', result);
  } catch (e) {
    console.error('Error during QR generation:', e);
    process.exit(1);
  }

  // Verify columns updated
  try {
    const rs = await access.query(`SELECT DateHeureSysteme, QrCode, code_wilaya FROM Titres WHERE id = ${access.escapeValue(Number(id))}`);
    console.log('Record after update:', rs && rs[0]);
  } catch (e) {
    console.warn('Could not verify updated fields:', e);
  }
}

main().catch((e) => {
  console.error('Fatal error in test:', e);
  process.exit(1);
});
