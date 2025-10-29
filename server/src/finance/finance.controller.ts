import { Controller, Get, Query } from '@nestjs/common';
import { FinanceService } from './finance.service';

@Controller('finance')
export class FinanceController {
  constructor(private readonly finance: FinanceService) {}

  @Get('taxes-sup')
  async taxesSup(@Query('idTitre') idTitre?: string) {
    if (!idTitre || !/^\d+$/.test(idTitre)) {
      return { ok: false, message: 'idTitre manquant ou invalide' };
    }
    const id = Number(idTitre);
    const rows = await this.finance.getTaxesSupByIdTitre(id);
    return { ok: true, count: rows.length, rows };
  }

  @Get('dea')
  async dea(@Query('idTitre') idTitre?: string) {
    if (!idTitre || !/^\d+$/.test(idTitre)) {
      return { ok: false, message: 'idTitre manquant ou invalide' };
    }
    const id = Number(idTitre);
    const rows = await this.finance.getDeaByIdTitre(id);
    return { ok: true, count: rows.length, rows };
  }
}

