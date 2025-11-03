import { Controller, Get, Query } from '@nestjs/common';
import { FinanceService } from './finance.service';

@Controller('finance')
export class FinanceController {
  constructor(private readonly finance: FinanceService) {}

  @Get('receipts/batch')
  async batchReceipts(@Query() query: Record<string, any>) {
    const result = await this.finance.getReceiptsBatch({
      year: query.year ?? query.annee,
      from: query.from ?? query.start ?? query.dateFrom ?? query.fromDate,
      to: query.to ?? query.end ?? query.dateTo ?? query.toDate,
      wilaya: query.wilaya ?? query.codeWilaya ?? query.idWilaya,
      type: query.type ?? query.kind ?? query.receiptType,
      statusId: query.statusId ?? query.status ?? query.idStatutTitre,
    });
    return result;
  }

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

