import { Controller, Get, Param, Query, Post, Body, Req } from '@nestjs/common';
import { PermisService } from './permis.service';

@Controller('permis')
export class PermisController {
  constructor(private readonly permisService: PermisService) {}

  // Verify first to avoid matching ':id' with 'verify'
  @Get('verify')
  async verifyByQr(@Query('code') code: string) {
    if (!code) return { exists: false };
    try {
      return await this.permisService.verifyByQrCode(code);
    } catch (e) {
      try { console.error('[verifyByQr] unexpected error:', (e as any)?.message || e); } catch {}
      return { exists: false };
    }
  }

  @Get(':id')
  async getPermis(@Param('id') id: string) {
    const data = await this.permisService.getPermisById(id);
    return data;
  }

  @Get(':id/coordinates')
  async getCoordinates(@Param('id') id: string) {
    return this.permisService.getCoordinatesByPermisId(id);
  }

  // Stub endpoint for templates to keep designer happy

  // Optional: raw query (secured by read-only intent). Keep off by default.
  @Get('query/raw')
  async runRaw(@Query('sql') sql?: string) {
    if (!sql) return { rows: [] };
    return this.permisService.runRaw(sql);
  }

  @Get('procedure/:procedureId/permis')
  async findByProcedure(@Param('procedureId') procedureId: string) {
    const result = await this.permisService.findPermisByProcedure(procedureId);
    return result;
  }

  @Post(':id/qrcode/generate')
  async generateQrForPermis(@Param('id') id: string, @Body() body?: any, @Req() req?: any) {
    let by = body?.by || body?.user || body?.username || '';
    if (!by && req?.headers) {
      by = req.headers['x-user-id'] || req.headers['x-user-name'] || '';
    }
    return this.permisService.generateAndSaveQrCode(id, String(by || ''));
  }
}
