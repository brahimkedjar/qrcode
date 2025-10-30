import { Controller, Get, Param, Query, Post, Body, Patch } from '@nestjs/common';
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

  // Unified search endpoint: accepts numeric id or combined type+code like "PEC8375" (case-insensitive, spaces allowed)
  // NOTE: must be declared BEFORE ':id' so it does not get captured as an id.
  @Get('search')
  async search(@Query('q') q?: string) {
    const query = String(q || '').trim();
    if (!query) return { exists: false };
    const data = await this.permisService.searchPermis(query);
    return data || { exists: false };
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
  async generateQrForPermis(@Param('id') id: string, @Body() body?: any) {
    const by = body?.by || body?.user || body?.username || '';
    return this.permisService.generateAndSaveQrCode(id, by);
  }

  @Patch(':id/signed')
  async setSigned(@Param('id') id: string, @Body() body?: any) {
    const val = !!(body?.isSigned ?? body?.is_signed ?? body?.value);
    return this.permisService.setSignedFlag(id, val);
  }

  @Patch(':id/collection')
  async setCollectionInfo(@Param('id') id: string, @Body() body?: any) {
    return this.permisService.setCollectionInfo(id, body ?? {});
  }
}
