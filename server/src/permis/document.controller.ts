import { Controller, Get, Param, Query, Res } from '@nestjs/common';
import { Response } from 'express';
import fs from 'node:fs';
import path from 'node:path';
import PizZip from 'pizzip';
import Docxtemplater from 'docxtemplater';
import dayjs from 'dayjs';
import { PermisService } from './permis.service';

@Controller('permis')
export class DocumentController {
  private modelsDir: string;
  constructor(private readonly permis: PermisService) {
    this.modelsDir = path.resolve(process.cwd(), 'tm-app', 'models');
  }

  // List available Word templates in tm-app/models
  @Get('document/models')
  listModels() {
    if (!fs.existsSync(this.modelsDir)) return [];
    const files = fs.readdirSync(this.modelsDir).filter(f => f.toLowerCase().endsWith('.docx'));
    return files.map((f) => {
      const codeMatch = f.match(/[\s\-_]([A-Z]{2,})\b/);
      const key = codeMatch ? codeMatch[1] : path.basename(f, path.extname(f));
      return { key, name: f, path: f };
    });
  }

  // Generate a filled DOCX from a chosen model
  @Get(':id/document')
  async renderDocx(@Param('id') id: string, @Query('template') template: string, @Res() res: Response) {
    const models = this.listModels();
    const match = models.find(m => m.key.toLowerCase() === String(template || '').toLowerCase()) || models[0];
    if (!match) return res.status(404).json({ error: 'No templates found' });

    const filePath = path.join(this.modelsDir, match.path);
    if (!fs.existsSync(filePath)) return res.status(404).json({ error: 'Template not found' });

    const data = await this.permis.getPermisById(id);
    if (!data) return res.status(404).json({ error: 'Permis not found' });

    // Build a generic dataset. You may update placeholders in DOCX to match these keys.
    const payload = this.buildPayload(data);

    const content = fs.readFileSync(filePath);
    const zip = new PizZip(content);
    const doc = new Docxtemplater(zip, { paragraphLoop: true, linebreaks: true });
    try {
      doc.setData(payload);
      doc.render();
    } catch (e: any) {
      return res.status(500).json({ error: 'Render failed', details: e?.message || String(e) });
    }
    const buf = doc.getZip().generate({ type: 'nodebuffer' });
    const filename = `titre-${data.code_demande || data.codeDemande || id}-${match.key}.docx`;
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    return res.end(buf);
  }

  private buildPayload(data: any) {
    const fmt = (d: any) => (d ? dayjs(d).format('DD/MM/YYYY') : '');
    return {
      // Basics
      code: data.code_demande || data.codeDemande || '',
      id: data.id || data.id_demande || '',
      detenteur: data.detenteur || '',
      localisation: data.localisation || '',
      superficie: data.superficie || '',
      dateCreation: fmt(data.dateCreation),
      // Type
      type_lib: data?.typePermis?.lib_type || data?.typePermis?.nom || '',
      type_code: data?.typePermis?.code || '',
      duree_initiale: data?.typePermis?.duree_initiale || '',
      // Coordinates
      coordinates: Array.isArray(data.coordinates) ? data.coordinates.map((c: any, i: number) => ({
        n: i + 1,
        x: c.x,
        y: c.y,
        h: c.order || ''
      })) : []
    };
  }
}

