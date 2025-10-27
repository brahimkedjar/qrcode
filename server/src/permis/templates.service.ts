import { Injectable } from '@nestjs/common';
import { AccessService } from './access.service';

@Injectable()
export class TemplatesService {
  constructor(private readonly access: AccessService) {}

  private ensured = false;
  private existingCols: Set<string> | null = null;

  private async getColumns(): Promise<Set<string>> {
    try {
      const info: any = await (this.access as any).getTableColumns('templates');
      const cols = new Set<string>();
      const arr = info?.columns || [];
      for (const c of arr) {
        const name = (c.column || c.COLUMN_NAME || '').toString();
        if (name) cols.add(name.toLowerCase());
      }
      return cols;
    } catch {
      return new Set<string>();
    }
  }

  private async ensureTableForRead() {
    if (this.ensured && this.existingCols) return;
    const cols = await this.getColumns();
    this.existingCols = cols;
    this.ensured = true;
  }

  private async ensureTableForWrite() {
    // Create if missing, then add any missing columns
    const required = [
      'id', 'permisid', 'permiscode', 'name', 'elements', 'createdby', 'createdat', 'updatedat'
    ];
    let cols = await this.getColumns();
    if (cols.size === 0) {
      // Try to create the table
      const createSql = `CREATE TABLE templates (
        id COUNTER PRIMARY KEY,
        permisId INTEGER,
        permisCode TEXT(50),
        name TEXT(200),
        elements MEMO,
        createdBy TEXT(100),
        createdAt DATETIME,
        updatedAt DATETIME
      )`;
      try { await this.access.query(createSql); } catch {}
      cols = await this.getColumns();
    }
    // Add only missing columns to avoid lock churn
    const tryAlter = async (sql: string) => { try { await this.access.query(sql); } catch {} };
    if (!cols.has('permisid')) await tryAlter('ALTER TABLE templates ADD COLUMN permisId INTEGER');
    if (!cols.has('permiscode')) await tryAlter('ALTER TABLE templates ADD COLUMN permisCode TEXT(50)');
    if (!cols.has('name')) await tryAlter('ALTER TABLE templates ADD COLUMN name TEXT(200)');
    if (!cols.has('elements')) await tryAlter('ALTER TABLE templates ADD COLUMN elements MEMO');
    if (!cols.has('createdby')) await tryAlter('ALTER TABLE templates ADD COLUMN createdBy TEXT(100)');
    if (!cols.has('createdat')) await tryAlter('ALTER TABLE templates ADD COLUMN createdAt DATETIME');
    if (!cols.has('updatedat')) await tryAlter('ALTER TABLE templates ADD COLUMN updatedAt DATETIME');
    this.existingCols = await this.getColumns();
    this.ensured = true;
  }

  async listByPermisId(id: string | number) {
    await this.ensureTableForRead();
    const cols = this.existingCols || new Set<string>();
    const selectCols: string[] = [];
    const add = (name: string) => { if (cols.has(name.toLowerCase())) selectCols.push(name); };
    add('id'); add('name'); add('elements'); add('createdBy'); add('createdAt'); add('updatedAt'); add('permisId'); add('permisCode');
    const proj = selectCols.length ? selectCols.join(', ') : 'id, name';
    const isNum = /^\d+$/.test(String(id));
    const where = `permisId = ${isNum ? id : this.access.escapeValue(String(id))}`;
    const canOrderDates = cols.has('createdat') || cols.has('updatedat');
    const order = canOrderDates ? 'ORDER BY IIF(updatedAt IS NULL, createdAt, updatedAt) DESC, id DESC' : 'ORDER BY id DESC';
    const sql = `SELECT ${proj} FROM templates WHERE ${where} ${order}`;
    try {
      const rows = await this.access.query(sql);
      return rows || [];
    } catch {
      return [];
    }
  }

  async listByPermisCode(code: string) {
    await this.ensureTableForRead();
    const cols = this.existingCols || new Set<string>();
    const selectCols: string[] = [];
    const add = (name: string) => { if (cols.has(name.toLowerCase())) selectCols.push(name); };
    add('id'); add('name'); add('elements'); add('createdBy'); add('createdAt'); add('updatedAt'); add('permisId'); add('permisCode');
    const proj = selectCols.length ? selectCols.join(', ') : 'id, name';
    const where = `permisCode = ${this.access.escapeValue(String(code))}`;
    const canOrderDates = cols.has('createdat') || cols.has('updatedat');
    const order = canOrderDates ? 'ORDER BY IIF(updatedAt IS NULL, createdAt, updatedAt) DESC, id DESC' : 'ORDER BY id DESC';
    const sql = `SELECT ${proj} FROM templates WHERE ${where} ${order}`;
    try {
      const rows = await this.access.query(sql);
      return rows || [];
    } catch {
      return [];
    }
  }

  async createTemplate(data: { name: string; elements: any[] | string; permisId?: number | string; permisCode?: string; createdBy?: string }) {
    await this.ensureTableForWrite();
    const name = this.access.escapeValue(String(data.name || ''));
    const elementsJson = typeof data.elements === 'string' ? data.elements : JSON.stringify(data.elements || []);
    const elementsLit = this.access.escapeValue(elementsJson);
    const createdBy = this.access.escapeValue(String(data.createdBy || ''));
    const now = new Date();
    const dt = this.access.escapeValue(`${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')} ${String(now.getHours()).padStart(2,'0')}:${String(now.getMinutes()).padStart(2,'0')}:${String(now.getSeconds()).padStart(2,'0')}`);
    const permisIdPart = data.permisId == null ? 'NULL' : (/^\d+$/.test(String(data.permisId)) ? String(data.permisId) : this.access.escapeValue(String(data.permisId)));
    const permisCodePart = data.permisCode == null ? 'NULL' : this.access.escapeValue(String(data.permisCode));
    const ins = `INSERT INTO templates (permisId, permisCode, name, elements, createdBy, createdAt, updatedAt)
                 VALUES (${permisIdPart}, ${permisCodePart}, ${name}, ${elementsLit}, ${createdBy}, ${dt}, ${dt})`;
    await this.access.query(ins);
    // Return last inserted by ordering DESC
    const sel = `SELECT TOP 1 id, name, elements, createdBy, createdAt, updatedAt, permisId, permisCode FROM templates
                 ORDER BY id DESC`;
    const rows = await this.access.query(sel);
    return rows && rows[0] ? rows[0] : { ok: true };
  }

  async updateTemplateMeta(templateId: number, meta: { name?: string; elements?: any[] | string }) {
    await this.ensureTableForWrite();
    const sets: string[] = [];
    if (meta.name != null) sets.push(`name = ${this.access.escapeValue(String(meta.name))}`);
    if (meta.elements != null) {
      const elJson = typeof meta.elements === 'string' ? meta.elements : JSON.stringify(meta.elements || []);
      sets.push(`elements = ${this.access.escapeValue(elJson)}`);
    }
    if (!sets.length) return;
    const now = new Date();
    const dt = this.access.escapeValue(`${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')} ${String(now.getHours()).padStart(2,'0')}:${String(now.getMinutes()).padStart(2,'0')}:${String(now.getSeconds()).padStart(2,'0')}`);
    sets.push(`updatedAt = ${dt}`);
    const sql = `UPDATE templates SET ${sets.join(', ')} WHERE id = ${Number(templateId)}`;
    await this.access.query(sql);
  }

  async deleteTemplate(templateId: number) {
    await this.ensureTableForWrite();
    const sql = `DELETE FROM templates WHERE id = ${Number(templateId)}`;
    await this.access.query(sql);
  }
}

