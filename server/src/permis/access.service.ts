import { Injectable } from '@nestjs/common';
import ADODB from 'node-adodb';
import odbc from 'odbc';
import path from 'node:path';
import fs from 'node:fs';

// Optional debug flag for node-adodb (not typed)
// @ts-ignore
(ADODB as any).debug = process.env.ADODB_DEBUG === '1';

@Injectable()
export class AccessService {
  private connection: any;
  private provider: string;
  private dbPath: string;
  private useX64: boolean;
  private lastOpenInfo: { provider: string; useX64: boolean } | null = null;
  private lastErrorMessage: string | null = null;
  private mode: 'adodb' | 'odbc' = 'adodb';
  private odbcConn: odbc.Connection | null = null;
  private odbcConnString: string | null = null;

  constructor() {
    const configuredPath = process.env.ACCESS_DB_PATH || '';
    const resolved = configuredPath
      ? path.isAbsolute(configuredPath)
        ? configuredPath
        : path.resolve(process.cwd(), configuredPath)
      : '';
    this.dbPath = resolved;

    if (!this.dbPath || !fs.existsSync(this.dbPath)) {
      // eslint-disable-next-line no-console
      console.warn('ACCESS_DB_PATH not set or file not found:', this.dbPath);
    }

    // Prefer ACE OLEDB provider (works for .mdb and .accdb if Access Database Engine is installed)
    this.provider = process.env.ACCESS_OLEDB_PROVIDER || 'Microsoft.ACE.OLEDB.12.0';
    this.useX64 = (process.env.ACCESS_X64 || '1') !== '0';
    this.mode = (process.env.ACCESS_MODE as any) === 'odbc' ? 'odbc' : 'adodb';
    if (this.mode === 'adodb') {
      this.openConnection(this.provider, this.useX64);
    } else {
      this.prepareOdbc();
    }
  }

  private openConnection(provider: string, useX64: boolean) {
    const connStr = `Provider=${provider};Data Source=${this.dbPath};Persist Security Info=False;`;
    // eslint-disable-next-line no-console
    console.log(`Opening ADODB connection: provider=${provider}, useX64=${useX64}, dbPath=${this.dbPath}`);
    this.connection = ADODB.open(connStr, useX64);
    this.provider = provider;
    this.useX64 = useX64;
    this.lastOpenInfo = { provider, useX64 };
    this.lastErrorMessage = null;
  }

  private prepareOdbc() {
    const explicit = (process.env.ACCESS_ODBC_CONN || '').trim();
    if (explicit) {
      let cs = explicit;
      const hasDbq = /(^|;)\s*DBQ\s*=/.test(cs);
      const hasUid = /(^|;)\s*Uid\s*=/.test(cs);
      const hasPwd = /(^|;)\s*Pwd\s*=/.test(cs);
      if (!hasDbq) cs = cs.replace(/;?\s*$/,'') + `;DBQ=${this.dbPath}`;
      if (!hasUid) cs += ';Uid=Admin';
      if (!hasPwd) cs += ';Pwd=';
      if (!/Driver=/.test(cs)) cs = `Driver={Microsoft Access Driver (*.mdb, *.accdb)};` + cs;
      this.odbcConnString = cs;
    } else {
      // Default Access ODBC driver; requires Access Database Engine ODBC driver installed (x64 for 64-bit Node)
      this.odbcConnString = `Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=${this.dbPath};Uid=Admin;Pwd=;`;
    }
    // eslint-disable-next-line no-console
    console.log(`Prepared ODBC connection string: ${this.odbcConnString}`);
  }

  isOdbcMode() {
    return this.mode === 'odbc';
  }

  escapeValue(val: string | number) {
    if (typeof val === 'number') return val;
    // Use single quotes and escape single-quote by doubling it for Access SQL
    return `'${String(val).replace(/'/g, "''")}'`;
  }

  async query(sql: string): Promise<any[]> {
    try { console.log('[ACCESS SQL]', sql); } catch {}
    if (this.mode === 'odbc') {
      return this.queryViaOdbc(sql);
    }
    try {
      return await this.connection.query(sql);
    } catch (origErr: any) {
      let lastErr = origErr;
      let lastMsg = `${origErr?.message || ''} ${origErr?.process ? JSON.stringify(origErr.process) : ''}`;
      this.lastErrorMessage = lastMsg;

      // 1) Spawn error: flip 32/64-bit engine and retry once
      if (lastMsg.includes('Spawn') && (lastMsg.includes('SysWOW64') || lastMsg.includes('System32'))) {
        // eslint-disable-next-line no-console
        console.warn('ADODB spawn error, toggling engine bitness and retrying once...');
        this.openConnection(this.provider, !this.useX64);
        try {
          return await this.connection.query(sql);
        } catch (err2: any) {
          lastErr = err2;
          lastMsg = `${err2?.message || ''} ${err2?.process ? JSON.stringify(err2.process) : ''}`;
          this.lastErrorMessage = lastMsg;
        }
      }

      // 2) Fallback to 32-bit JET for .mdb when ACE (or current provider) failed
      const isMdb = this.dbPath.toLowerCase().endsWith('.mdb');
      const usingJet = this.provider.toLowerCase().includes('jet');
      if (isMdb && !usingJet) {
        // eslint-disable-next-line no-console
        console.warn('ACE attempt failed; trying Microsoft.Jet.OLEDB.4.0 (x86) for .mdb...');
        this.openConnection('Microsoft.Jet.OLEDB.4.0', false);
        try {
          return await this.connection.query(sql);
        } catch (err3: any) {
          lastErr = err3;
          lastMsg = `${err3?.message || ''} ${err3?.process ? JSON.stringify(err3.process) : ''}`;
          this.lastErrorMessage = lastMsg;
        }
      }

      // 3) Switch to ODBC driver (no WSH) when ADODB path fails
      // eslint-disable-next-line no-console
      console.warn('Switching to ODBC driver for Access due to ADODB failures...');
      this.mode = 'odbc';
      this.prepareOdbc();
      try {
        return await this.queryViaOdbc(sql);
      } catch (err4: any) {
        lastErr = err4;
        lastMsg = `${err4?.message || ''}`;
        this.lastErrorMessage = lastMsg;
      }

      // 3) If provider-specific errors, rethrow with more context
      const providerProblem =
        lastMsg.toLowerCase().includes('provider cannot be found') ||
        lastMsg.toLowerCase().includes('class not registered') ||
        lastMsg.toLowerCase().includes("activex component can't create object") ||
        lastMsg.toLowerCase().includes('adodb.connection');
      if (providerProblem) {
        // eslint-disable-next-line no-console
        console.error('ADODB provider error after fallbacks:', lastMsg);
      }

      throw lastErr;
    }
  }

  async queryParam(sql: string, params: any[]): Promise<any[]> {
    if (this.mode === 'odbc') {
      try {
        const conn = await this.ensureOdbcConnected();
        const result: any = await conn.query(sql, params);
        const rows = Array.isArray(result) ? result : (result?.rows ?? []);
        return rows as any[];
      } catch (e: any) {
        const errors = (e && (e.odbcErrors || e.errors)) ? JSON.stringify(e.odbcErrors || e.errors) : '';
        this.lastErrorMessage = `[ODBC PARAM ERROR] ${e?.message || e} ${errors}`;
        // eslint-disable-next-line no-console
        console.error('ODBC param query error:', this.lastErrorMessage);
        throw e;
      }
    }
    // Fallback: inline parameters into SQL safely for ADODB
    let inlined = sql;
    for (const p of params) {
      const lit: string = typeof p === 'number' ? String(p) : (this.escapeValue(String(p)) as string);
      inlined = inlined.replace('?', lit);
    }
    return this.query(inlined);
  }

  private async ensureOdbcConnected(): Promise<odbc.Connection> {
    if (this.odbcConn && !(this.odbcConn as any).closed) return this.odbcConn;
    if (!this.odbcConnString) this.prepareOdbc();
    // eslint-disable-next-line no-console
    console.log('Connecting via ODBC...');
    try {
      this.odbcConn = await odbc.connect(this.odbcConnString!);
      return this.odbcConn;
    } catch (e: any) {
      const errors = (e && (e.odbcErrors || e.errors)) ? JSON.stringify(e.odbcErrors || e.errors) : '';
      this.lastErrorMessage = `[ODBC CONNECT ERROR] ${e?.message || e} ${errors}`;
      // eslint-disable-next-line no-console
      console.error('ODBC connect error:', this.lastErrorMessage);
      throw e;
    }
  }

  private async queryViaOdbc(sql: string): Promise<any[]> {
    try {
      const conn = await this.ensureOdbcConnected();
      const result: any = await conn.query(sql);
      const rows = Array.isArray(result) ? result : (result?.rows ?? []);
      return rows as any[];
    } catch (e: any) {
      const errors = (e && (e.odbcErrors || e.errors)) ? JSON.stringify(e.odbcErrors || e.errors) : '';
      this.lastErrorMessage = `[ODBC QUERY ERROR] ${e?.message || e} ${errors}`;
      // eslint-disable-next-line no-console
      console.error('ODBC query error:', this.lastErrorMessage);
      try { console.error('[ACCESS SQL FAILED]', sql); } catch {}
      throw e;
    }
  }

  private getEnginePath(useX64: boolean) {
    const sysroot = process.env['systemroot'] || process.env['windir'] || 'C:\\Windows';
    const isNodeX64 = process.arch === 'x64';
    const base = useX64 ? 'System32' : (isNodeX64 ? 'SysWOW64' : 'System32');
    return path.join(sysroot, base, 'cscript.exe');
  }

  async diagnostics() {
    const enginePath = this.getEnginePath(this.useX64);
    const info: any = {
      provider: this.provider,
      useX64: this.useX64,
      enginePath,
      dbPath: this.dbPath,
      lastOpenInfo: this.lastOpenInfo,
      lastErrorMessage: this.lastErrorMessage,
      mode: this.mode,
      odbcConnString: this.odbcConnString
    };
    try {
      info.odbcDrivers = await (odbc as any).drivers?.() ?? null;
    } catch {
      info.odbcDrivers = null;
    }
    try {
      if (this.mode === 'odbc') {
        const rows = await this.queryViaOdbc('SELECT 1 AS test');
        info.testQuery = { ok: true, rows };
      } else {
        const rows = await this.connection.query('SELECT 1 AS test');
        info.testQuery = { ok: true, rows };
      }
    } catch (e: any) {
      const errors = (e && (e.odbcErrors || e.errors)) ? JSON.stringify(e.odbcErrors || e.errors) : '';
      info.testQuery = { ok: false, error: `${e?.message || ''} ${e?.process ? JSON.stringify(e.process) : ''} ${errors}` };
      this.lastErrorMessage = info.testQuery.error;
    }
    return info;
  }

  // Return columns metadata for a given table using ODBC if possible
  async getTableColumns(table: string) {
    try {
      const conn = await this.ensureOdbcConnected();
      const meta: any = await (conn as any).columns(null, null, table, null);
      const rows = Array.isArray(meta) ? meta : (meta?.rows ?? []);
      // Normalize common fields from ODBC metadata
      const mapped = rows.map((r: any) => ({
        table: r.TABLE_NAME || r.tableName || table,
        column: r.COLUMN_NAME || r.columnName,
        typeName: r.TYPE_NAME || r.typeName,
        dataType: r.DATA_TYPE || r.dataType,
        columnSize: r.COLUMN_SIZE || r.columnSize,
        nullable: r.NULLABLE ?? r.nullable,
        remarks: r.REMARKS || r.remarks
      }));
      return { mode: this.mode, odbc: true, columns: mapped };
    } catch (e) {
      // eslint-disable-next-line no-console
      console.warn('ODBC columns() failed, falling back to SELECT 1=0', e);
      try {
        const rows = await this.query(`SELECT * FROM ${table} WHERE 1=0`);
        const cols = Array.isArray(rows) && rows[0] ? Object.keys(rows[0]) : [];
        return { mode: this.mode, odbc: false, columns: cols.map((c) => ({ column: c })) };
      } catch (e2) {
        return { mode: this.mode, odbc: false, error: String((e2 as any)?.message || e2) };
      }
    }
  }
}
