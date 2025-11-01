import { Controller, Get, Post, Query, Res } from '@nestjs/common';
import { Response } from 'express';
import { spawn } from 'child_process';
import path from 'path';
import fs from 'fs';
import odbc from 'odbc';

@Controller('sync')
export class SyncController {
  private resolveScriptPath(scriptParam?: string): string {
    if (scriptParam && fs.existsSync(scriptParam)) return scriptParam;
    const candidates = [
      // Prefer Python implementation if present
      path.resolve(__dirname, '..', 'sync_cma.py'),
      path.resolve(__dirname, '..', '..', 'sync_cma.py'),
      // PowerShell fallbacks
      path.resolve(process.cwd(), 'qrcode', 'Sync-CMADonnees.ps1'),
      path.resolve(process.cwd(), 'Sync-CMADonnees.ps1'),
      path.resolve(__dirname, '..', '..', 'Sync-CMADonnees.ps1'),
      path.resolve(__dirname, '..', 'Sync-CMADonnees.ps1'),
    ];
    for (const p of candidates) {
      if (fs.existsSync(p)) return p;
    }
    return candidates[0];
  }

  private buildProcess(scriptPath: string, source?: string, dest?: string, tables: string[] = [], resume?: string, statePathOverride?: string) {
    const env = {
      ...process.env,
      SYNC_TABLES: tables.join(','),
      SYNC_TABLES_JSON: JSON.stringify(tables),
      SYNC_SOURCE_DB: (source || '').trim(),
      SYNC_DEST_DB: (dest || '').trim(),
    } as NodeJS.ProcessEnv;

    const ext = path.extname(scriptPath).toLowerCase();
    if (ext === '.py') {
      const pythonCmd = process.env.PYTHON_CMD || 'python';
      const args: string[] = [scriptPath];
      if (source && source.trim()) { args.push('--source', source.trim()); }
      if (dest && dest.trim()) { args.push('--dest', dest.trim()); }
      if (tables.length) { args.push('--tables', tables.join(',')); }
      if (resume && /^(1|true|yes|on)$/i.test(resume.trim())) { args.push('--resume'); }
      const statePath = statePathOverride && statePathOverride.trim()
        ? statePathOverride.trim()
        : path.join(path.dirname(scriptPath), 'sync-state.json');
      args.push('--state', statePath);
      return { cmd: pythonCmd, args, env };
    }
    // Default to PowerShell
    const args: string[] = [
      '-NoProfile',
      '-ExecutionPolicy', 'Bypass',
      '-File', scriptPath,
    ];
    if (source && source.trim()) {
      args.push('-SourceDbPath', source.trim());
    }
    if (dest && dest.trim()) {
      args.push('-DestinationDbPath', dest.trim());
    }
    return { cmd: 'powershell.exe', args, env };
  }

  @Get('run')
  run(
    @Res() res: Response,
    @Query('script') script?: string,
    @Query('source') source?: string,
    @Query('dest') dest?: string,
    @Query('tables') tablesCsv?: string,
    @Query('resume') resume?: string,
    @Query('state') statePathOverride?: string,
    @Query('keys') keysJson?: string,
  ) {
    // Prepare SSE response
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    (res as any).flushHeaders?.();

    const send = (event: string, data: any) => {
      const payload = typeof data === 'string' ? data : JSON.stringify(data);
      res.write(`event: ${event}\n`);
      res.write(`data: ${payload}\n\n`);
    };

    const scriptPath = this.resolveScriptPath(script);
    if (!fs.existsSync(scriptPath)) {
      send('error', { message: `Script introuvable: ${scriptPath}` });
      res.end();
      return;
    }

    const tables = (tablesCsv || '')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);
    send('info', { message: `Démarrage de la synchronisation`, script: scriptPath, source, dest, tables });

    const build = this.buildProcess(scriptPath, source, dest, tables, resume, statePathOverride);
    const child = spawn(build.cmd, build.args, {
      windowsHide: true,
      env: build.env,
    });

    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');

    child.stdout.on('data', (chunk: string) => {
      for (const line of chunk.split(/\r?\n/)) {
        if (line.trim().length) send('log', line);
      }
    });
    child.stderr.on('data', (chunk: string) => {
      for (const line of chunk.split(/\r?\n/)) {
        if (line.trim().length) send('error', line);
      }
    });
    child.on('close', (code: number) => {
      send('done', { code });
      res.end();
    });
    child.on('error', (err) => {
      send('error', { message: err.message });
      send('done', { code: -1 });
      res.end();
    });

    // If the client disconnects, try to terminate the process
    const req: any = (res as any).req;
    if (req && typeof req.on === 'function') {
      req.on('close', () => { try { child.kill(); } catch {} });
    }
  }

  @Get('state')
  getState(@Res() res: Response, @Query('script') script?: string, @Query('state') state?: string) {
    try {
      const scriptPath = this.resolveScriptPath(script);
      const statePath = state && state.trim() ? state.trim() : path.join(path.dirname(scriptPath), 'sync-state.json');
      if (!fs.existsSync(statePath)) {
        res.json({ ok: true, statePath, state: {} });
        return;
      }
      const raw = fs.readFileSync(statePath, 'utf8');
      let json: any = null;
      try { json = JSON.parse(raw); } catch { json = null; }
      res.json({ ok: true, statePath, state: json || raw });
    } catch (e: any) {
      res.status(500).json({ ok: false, error: e?.message || String(e) });
    }
  }

  @Post('state/reset')
  resetState(@Res() res: Response, @Query('script') script?: string, @Query('state') state?: string) {
    try {
      const scriptPath = this.resolveScriptPath(script);
      const statePath = state && state.trim() ? state.trim() : path.join(path.dirname(scriptPath), 'sync-state.json');
      const existed = fs.existsSync(statePath);
      if (existed) fs.unlinkSync(statePath);
      res.json({ ok: true, statePath, removed: existed });
    } catch (e: any) {
      res.status(500).json({ ok: false, error: e?.message || String(e) });
    }
  }
}

@Controller('sync')
export class SyncTablesController {
  @Get('tables')
  async listTables(@Res() res: Response, @Query('db') db?: string) {
    const dbPath = (db && db.trim()) || (process.env.ACCESS_DB_PATH || '').trim();
    if (!dbPath || !fs.existsSync(dbPath)) {
      res.status(400).json({ ok: false, error: `Base introuvable: ${dbPath || '(non fournie)'}` });
      return;
    }
    const connStr = `Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=${dbPath};Uid=Admin;Pwd=;`;
    let conn: odbc.Connection | null = null;
    try {
      conn = await odbc.connect(connStr);
      const meta: any = await (conn as any).tables(null, null, null, null);
      const rows: any[] = Array.isArray(meta) ? meta : (meta?.rows ?? []);
      const names = rows
        .map((r: any) => ({
          name: r.TABLE_NAME || r.tableName,
          type: (r.TABLE_TYPE || r.tableType || '').toString().toUpperCase(),
        }))
        .filter((t) => t.name && !/^MSys|^USys/i.test(t.name))
        .filter((t) => t.type === 'TABLE' || t.type === 'VIEW')
        .map((t) => t.name)
        .sort((a, b) => a.localeCompare(b));
      res.json({ ok: true, dbPath, tables: names });
    } catch (e: any) {
      res.status(500).json({ ok: false, error: e?.message || String(e) });
    } finally {
      try { await (conn as any)?.close?.(); } catch {}
    }
  }
}
