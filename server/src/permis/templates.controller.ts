import { Body, Controller, Delete, Get, Param, Patch, Post, Query, Res } from '@nestjs/common';
import type { Response } from 'express';
import fs from 'node:fs';
import path from 'node:path';
import { TemplatesService } from './templates.service';

@Controller('permis')
export class TemplatesController {
  constructor(private readonly templates: TemplatesService) {}

  @Get(':id/templates')
  async listByPermisId(@Param('id') id: string) {
    return this.templates.listByPermisId(id);
  }

  @Get('templates')
  async listByPermisCode(@Query('permisId') code?: string) {
    if (!code) return [];
    return this.templates.listByPermisCode(code);
  }

  @Post('templates')
  async create(@Body() body: any) {
    const { name, elements, permisId, permisCode, createdBy } = body || {};
    if (!name || !elements) return { ok: false, message: 'Missing name or elements' };
    const rec = await this.templates.createTemplate({ name, elements, permisId, permisCode, createdBy });
    return rec;
  }

  @Patch('templates/:templateId')
  async rename(@Param('templateId') templateId: string, @Body() body: any) {
    const { name, elements } = body || {};
    await this.templates.updateTemplateMeta(Number(templateId), { name, elements });
    return { ok: true };
  }

  @Delete('templates/:templateId')
  async remove(@Param('templateId') templateId: string) {
    await this.templates.deleteTemplate(Number(templateId));
    return { ok: true };
  }

  // Save a generated PDF to disk in tm-app/client/permis_templates/(code type)
  @Post('templates/pdf/save')
  async savePdf(@Body() body: any) {
    const { code, typeCode, pdfBase64, fileName, templateName } = body || {};
    if (!code || !typeCode || !pdfBase64) return { ok: false, message: 'Missing code/typeCode/pdfBase64' };
    const base64 = String(pdfBase64);
    const commaIdx = base64.indexOf(',');
    const b64Data = commaIdx >= 0 ? base64.slice(commaIdx + 1) : base64;
    const buf = Buffer.from(b64Data, 'base64');
    const root = path.resolve(__dirname, '..', '..', '..', 'client', 'permis_templates');
    if (!fs.existsSync(root)) fs.mkdirSync(root, { recursive: true });
    const safe = (s: string) => s.replace(/[^\w\-\s\.\(\)]+/g, '').trim();
    const dirName = `(${safe(String(code))} ${safe(String(typeCode))})`;
    const dir = path.join(root, dirName);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    const now = new Date();
    const pad = (n: number) => String(n).padStart(2, '0');
    const ts = `${now.getFullYear()}${pad(now.getMonth()+1)}${pad(now.getDate())}_${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`;
    const base = safe(fileName || templateName || `permis-${code}`) || `permis-${code}`;
    const fname = `${base}_${ts}.pdf`;
    const fpath = path.join(dir, fname);
    fs.writeFileSync(fpath, buf);
    const url = `/static/permis_templates/${encodeURIComponent(dirName)}/${encodeURIComponent(fname)}`;
    return { ok: true, file: fname, dir: dirName, url };
  }

  // Search saved PDFs by query with pagination; supports flat or grouped results
  @Get('templates/pdf/search')
  async searchPdfs(
    @Query('q') q?: string,
    @Query('page') pageRaw?: string,
    @Query('size') sizeRaw?: string,
    @Query('mode') mode?: string,
    @Query('sort') sort?: string,
  ) {
    const query = String(q || '').trim().toLowerCase().replace(/\W+/g, '');
    const root = path.resolve(__dirname, '..', '..', '..', 'client', 'permis_templates');
    const page = Math.max(1, parseInt(String(pageRaw || '1'), 10) || 1);
    const sizeCap = 200;
    const size = Math.min(sizeCap, Math.max(1, parseInt(String(sizeRaw || '50'), 10) || 50));
    const flatMode = (mode || '').toLowerCase() === 'flat' || mode === '1';
    const sortKey = (sort || 'recent').toLowerCase();
    if (!fs.existsSync(root)) return flatMode ? { total: 0, page, size, items: [] } : { total: 0, page, size, items: [] };

    // Build entries list
    type Entry = { dir: string; name: string; url: string; mtime: number; size: number; code?: string; type?: string };
    const entries: Entry[] = [];
    const dirs = fs.readdirSync(root, { withFileTypes: true }).filter(d => d.isDirectory());
    for (const d of dirs) {
      const dirName = d.name;
      const dirPath = path.join(root, dirName);
      const files = fs.readdirSync(dirPath).filter(f => f.toLowerCase().endsWith('.pdf'));
      if (!files.length) continue;
      // Parse (code type)
      let code: string | undefined;
      let type: string | undefined;
      const m = dirName.match(/\(([^\)]+)\)/);
      if (m && m[1]) {
        const parts = m[1].split(/\s+/).map(s => s.trim()).filter(Boolean);
        if (parts.length >= 2) { code = parts[0]; type = parts[1]; }
      }
      const norm = dirName.toLowerCase().replace(/\W+/g, '');
      const rev = (type && code) ? (type + code).toLowerCase() : '';
      const dirMatch = !query || norm.includes(query) || (rev && rev.includes(query));
      // If directory doesn't match, still consider file name match
      for (const f of files) {
        const filePath = path.join(dirPath, f);
        const st = fs.statSync(filePath);
        const url = `/static/permis_templates/${encodeURIComponent(dirName)}/${encodeURIComponent(f)}`;
        const fileNorm = f.toLowerCase();
        const match = dirMatch || (query && fileNorm.includes(query));
        if (match) {
          entries.push({ dir: dirName, name: f, url, mtime: st.mtimeMs, size: st.size, code, type });
        }
      }
    }

    // Sort
    if (sortKey === 'name') {
      entries.sort((a, b) => a.name.localeCompare(b.name));
    } else {
      // recent first
      entries.sort((a, b) => b.mtime - a.mtime);
    }

    const total = entries.length;
    const start = (page - 1) * size;
    const slice = entries.slice(start, start + size);

    if (flatMode) {
      return { total, page, size, items: slice };
    }
    // Grouped by dir
    const groupsMap = new Map<string, { dir: string; files: any[] }>();
    for (const e of slice) {
      const g = groupsMap.get(e.dir) || { dir: e.dir, files: [] };
      g.files.push({ name: e.name, url: e.url, mtime: e.mtime, size: e.size });
      groupsMap.set(e.dir, g);
    }
    const items = Array.from(groupsMap.values());
    return { total, page, size, items };
  }

  // Download a saved PDF as attachment
  @Get('templates/pdf/download')
  async downloadPdf(@Query('dir') dir: string, @Query('file') file: string, @Res() res: Response) {
    const root = path.resolve(__dirname, '..', '..', '..', 'client', 'permis_templates');
    const safe = (s: string) => String(s || '').replace(/[\\/:*?"<>|]/g, '').trim();
    const dirName = safe(dir || '');
    const fileName = safe(file || '');
    if (!dirName || !fileName) {
      res.status(400).json({ ok: false, message: 'Missing dir/file' });
      return;
    }
    const fpath = path.join(root, dirName, fileName);
    if (!fs.existsSync(fpath)) {
      res.status(404).json({ ok: false, message: 'File not found' });
      return;
    }
    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="${encodeURIComponent(fileName)}"`);
    fs.createReadStream(fpath).pipe(res);
  }
}

