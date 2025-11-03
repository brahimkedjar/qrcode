import { Injectable } from '@nestjs/common';
import { AccessService } from '../permis/access.service';

type ReceiptTypeQuery = 'TS' | 'DEA' | 'ALL';

type BatchFilterInput = {
  year?: number | string;
  from?: string;
  to?: string;
  wilaya?: string;
  type?: string;
  statusId?: number | string;
};

type NormalizedBatchFilters = {
  requestedType: ReceiptTypeQuery;
  includeTs: boolean;
  includeDea: boolean;
  statusId: number;
  year?: number;
  fromDate?: Date;
  toDate?: Date;
  wilayaRaw?: string;
  wilayaNumber?: number | null;
  wilayaLabel?: string | null;
};

type BatchPermitInfo = {
  id: number;
  code?: string | null;
  typeId?: number | null;
  typeCode?: string | null;
  typeName?: string | null;
  detenteurId?: number | null;
  detenteurName?: string | null;
  wilaya?: string | null;
  wilayaId?: number | null;
  lieuDit?: string | null;
  commune?: string | null;
  daira?: string | null;
  superficie?: string | number | null;
};

type BatchReceiptRow = {
  type: 'TS' | 'DEA';
  receiptId: number;
  titreId: number;
  orderNumber: string;
  amount: number;
  amountRaw?: any;
  date?: string | null;
  periodStart?: string | null;
  periodEnd?: string | null;
  payed?: boolean;
  permit: BatchPermitInfo;
  raw?: any;
};

@Injectable()
export class FinanceService {
  constructor(private readonly access: AccessService) {}

  private normalizeWilayaText(value?: string | null): string {
    if (!value && value !== '') return '';
    return String(value ?? '')
      .replace(/[\u061C\u200E\u200F\u202A-\u202E]/g, '')
      .trim()
      .toUpperCase();
  }

  private toBoolean(val: any): boolean {
    if (val === true) return true;
    if (val === false) return false;
    if (typeof val === 'number') return val !== 0; // Access ODBC often returns -1 for true
    const s = String(val ?? '').trim().toLowerCase();
    if (!s) return false;
    // Common truthy variants from Access/ODBC/localization
    if (['yes','oui','true','vrai','y','o','-1','1'].includes(s)) return true;
    if (['no','non','false','faux','0'].includes(s)) return false;
    return s === 't' || s === 'v';
  }

  private parseAmount(val: any): number {
    if (typeof val === 'number') return Number.isFinite(val) ? val : 0;
    const s = String(val ?? '').replace(/[^0-9,.\-]/g, '');
    if (!s) return 0;
    const normalized = s.includes(',') && !s.includes('.') ? s.replace(',', '.') : s.replace(/,/g, '');
    const parsed = parseFloat(normalized);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  private parseDateInput(value?: string | number | Date | null): Date | null {
    if (value === null || value === undefined || value === '') return null;

    if (value instanceof Date) {
      return Number.isFinite(value.getTime()) ? value : null;
    }
    if (typeof value === 'number') {
      const d = new Date(value);
      return Number.isFinite(d.getTime()) ? d : null;
    }
    const text = String(value).trim();
    if (!text) return null;
    const iso = text.match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$/);
    if (iso) {
      const [, y, m, d] = iso;
      const dt = new Date(Number(y), Number(m) - 1, Number(d));
      return Number.isFinite(dt.getTime()) ? dt : null;
    }
    const fr = text.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (fr) {
      const [, d, m, y] = fr;
      const dt = new Date(Number(y), Number(m) - 1, Number(d));
      return Number.isFinite(dt.getTime()) ? dt : null;
    }
    const parsed = new Date(text);
    return Number.isFinite(parsed.getTime()) ? parsed : null;
  }

  private formatDateLiteral(date?: Date | null): string | null {
    if (!date) return null;
    const mm = String(date.getMonth() + 1).padStart(2, '0');
    const dd = String(date.getDate()).padStart(2, '0');
    const yyyy = date.getFullYear();
    return `#${mm}/${dd}/${yyyy}#`;
  }

  private normalizeBatchFilters(input: BatchFilterInput): NormalizedBatchFilters {
    const typeRaw = String(input.type ?? '').trim().toUpperCase();
    let requestedType: ReceiptTypeQuery = 'ALL';
    if (typeRaw === 'TS' || typeRaw === 'DEA') requestedType = typeRaw;
    const includeTs = requestedType === 'ALL' || requestedType === 'TS';
    const includeDea = requestedType === 'ALL' || requestedType === 'DEA';

    let year: number | undefined;
    if (input.year !== undefined) {
      const yNum = Number(input.year);
      if (Number.isFinite(yNum)) {
        year = yNum;
      }
    }

    const fromDate = this.parseDateInput(input.from);
    const toDate = this.parseDateInput(input.to);
    const filters: NormalizedBatchFilters = {
      requestedType,
      includeTs,
      includeDea,
      statusId: Number.isFinite(Number(input.statusId)) ? Number(input.statusId) : 2,
      year,
      fromDate: fromDate ?? (year ? new Date(year, 0, 1) : undefined),
      toDate: toDate ?? (year ? new Date(year, 11, 31) : undefined),
    };

    const wilayaRaw = String(input.wilaya ?? '').trim();
    if (wilayaRaw) {
      filters.wilayaRaw = wilayaRaw;
      if (/^\d+$/.test(wilayaRaw)) {
        filters.wilayaNumber = Number(wilayaRaw);
      } else {
        filters.wilayaLabel = this.normalizeWilayaText(wilayaRaw);
      }
    }

    return filters;
  }

  private buildWilayaClause(filters: NormalizedBatchFilters, alias = 't'): string | null {
    if (!filters.wilayaRaw) return null;
    const clauses: string[] = [];
    if (typeof filters.wilayaNumber === 'number') {
      clauses.push(`${alias}.[idWilaya] = ${filters.wilayaNumber}`);
      clauses.push(`${alias}.[Wilaya] = ${this.access.escapeValue(filters.wilayaRaw)}`);
    }
    if (!clauses.length) return null;
    return `(${clauses.join(' OR ')})`;
  }

  private buildDateClauses(column: string, filters: NormalizedBatchFilters): string[] {
    const clauses: string[] = [];
    const fromLiteral = this.formatDateLiteral(filters.fromDate ?? undefined);
    const toLiteral = this.formatDateLiteral(filters.toDate ?? undefined);
    if (fromLiteral && toLiteral) {
      clauses.push(`${column} BETWEEN ${fromLiteral} AND ${toLiteral}`);
    } else if (fromLiteral) {
      clauses.push(`${column} >= ${fromLiteral}`);
    } else if (toLiteral) {
      clauses.push(`${column} <= ${toLiteral}`);
    }
    return clauses;
  }

  private mapTaxeRow(row: any): BatchReceiptRow {
    const permit: BatchPermitInfo = {
      id: Number(row?.TitreId ?? row?.idTitre ?? 0),
      code: row?.PermitCode ?? row?.Code ?? null,
      typeId: Number(row?.PermitTypeId ?? row?.idType ?? 0) || null,
      typeCode: row?.PermitTypeCode ?? row?.TypeCode ?? null,
      typeName: row?.PermitTypeName ?? row?.TypeName ?? null,
      detenteurId: Number(row?.DetenteurId ?? 0) || null,
      detenteurName: row?.DetenteurNom ?? row?.DetenteurName ?? null,
      wilaya: row?.Wilaya ?? null,
      wilayaId: Number(row?.WilayaId ?? 0) || null,
      lieuDit: row?.LieuDit ?? null,
      commune: row?.Commune ?? null,
      daira: row?.Daira ?? null,
      superficie: row?.Superficie ?? null,
    };
    return {
      type: 'TS',
      receiptId: Number(row?.ReceiptId ?? row?.id ?? 0),
      titreId: permit.id,
      orderNumber: String(row?.NumeroPerc ?? row?.OrderNumber ?? '').trim(),
      amount: this.parseAmount(row?.Taxe ?? row?.amount),
      amountRaw: row?.Taxe ?? row?.amount,
      date: row?.ReceiptDate ?? row?.Date ?? null,
      periodStart: row?.PeriodStart ?? row?.DatePerDebut ?? null,
      periodEnd: row?.PeriodEnd ?? row?.datePerFin ?? null,
      payed: this.toBoolean(row?.Paye ?? row?.paye),
      permit,
      raw: row,
    };
  }

  private mapDeaRow(row: any): BatchReceiptRow {
    const permit: BatchPermitInfo = {
      id: Number(row?.TitreId ?? row?.idTitre ?? 0),
      code: row?.PermitCode ?? row?.Code ?? null,
      typeId: Number(row?.PermitTypeId ?? row?.idType ?? 0) || null,
      typeCode: row?.PermitTypeCode ?? row?.TypeCode ?? null,
      typeName: row?.PermitTypeName ?? row?.TypeName ?? null,
      detenteurId: Number(row?.DetenteurId ?? 0) || null,
      detenteurName: row?.DetenteurNom ?? row?.DetenteurName ?? null,
      wilaya: row?.Wilaya ?? null,
      wilayaId: Number(row?.WilayaId ?? 0) || null,
      lieuDit: row?.LieuDit ?? null,
      commune: row?.Commune ?? null,
      daira: row?.Daira ?? null,
      superficie: row?.Superficie ?? null,
    };
    return {
      type: 'DEA',
      receiptId: Number(row?.ReceiptId ?? row?.id ?? 0),
      titreId: permit.id,
      orderNumber: String(row?.NumeroPerc ?? row?.OrderNumber ?? '').trim(),
      amount: this.parseAmount(row?.Droit ?? row?.amount),
      amountRaw: row?.Droit ?? row?.amount,
      date: row?.ReceiptDate ?? row?.date ?? null,
      payed: this.toBoolean(row?.paye ?? row?.Paye),
      permit,
      raw: row,
    };
  }

  private async fetchTaxesSupBatch(filters: NormalizedBatchFilters): Promise<BatchReceiptRow[]> {
    const conditions: string[] = [`t.[idStatutTitre] = ${filters.statusId}`];
    const dateClauses = this.buildDateClauses('ts.[Date]', filters);
    conditions.push(...dateClauses);
    const wilayaClause = this.buildWilayaClause(filters, 't');
    if (wilayaClause) conditions.push(wilayaClause);

    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
    const sql = `SELECT
        ts.[id] AS ReceiptId,
        ts.[idTitre] AS TitreId,
        ts.[NumeroPerc] AS NumeroPerc,
        ts.[Date] AS ReceiptDate,
        ts.[Taxe] AS Taxe,
        ts.[Surface] AS Surface,
        ts.[DatePerDebut] AS PeriodStart,
        ts.[datePerFin] AS PeriodEnd,
        ts.[Paye] AS Paye,
        ts.[num_quittance] AS NumQuittance,
        ts.[datepaiement] AS DatePaiement,
        ts.[dateremiseop] AS DateRemiseOp,
        t.[Code] AS PermitCode,
        t.[idType] AS PermitTypeId,
        t.[idDetenteur] AS DetenteurId,
        t.[Wilaya] AS Wilaya,
        t.[idWilaya] AS WilayaId,
        t.[LieuDit] AS LieuDit,
        t.[Commune] AS Commune,
        t.[Daira] AS Daira,
        t.[Superficie] AS Superficie,
        det.[Nom] AS DetenteurNom,
        types.[Code] AS PermitTypeCode,
        types.[Nom] AS PermitTypeName
      FROM (([TaxesSup] AS ts
        INNER JOIN [Titres] AS t ON ts.[idTitre] = t.[id])
        LEFT JOIN [Detenteur] AS det ON t.[idDetenteur] = det.[id])
        LEFT JOIN [TypesTitres] AS types ON t.[idType] = types.[id]
      ${where}
      ORDER BY ts.[Date] ASC, ts.[id] ASC`;
    const rows = await this.access.query(sql);
    const mapped = rows.map((row) => this.mapTaxeRow(row));
    if (filters.wilayaLabel && !filters.wilayaNumber) {
      const label = filters.wilayaLabel;
      return mapped.filter((row) => this.normalizeWilayaText(row.permit?.wilaya) === label);
    }
    return mapped;
  }

  private async fetchDeaBatch(filters: NormalizedBatchFilters): Promise<BatchReceiptRow[]> {
    const conditions: string[] = [`t.[idStatutTitre] = ${filters.statusId}`];
    const dateClauses = this.buildDateClauses('dea.[date]', filters);
    conditions.push(...dateClauses);
    const wilayaClause = this.buildWilayaClause(filters, 't');
    if (wilayaClause) conditions.push(wilayaClause);

    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
    const sql = `SELECT
        dea.[id] AS ReceiptId,
        dea.[idTitre] AS TitreId,
        dea.[NumeroPerc] AS NumeroPerc,
        dea.[date] AS ReceiptDate,
        dea.[droit] AS Droit,
        dea.[paye] AS Paye,
        dea.[num_quittance] AS NumQuittance,
        dea.[datepaiement] AS DatePaiement,
        dea.[dateremiseop] AS DateRemiseOp,
        t.[Code] AS PermitCode,
        t.[idType] AS PermitTypeId,
        t.[idDetenteur] AS DetenteurId,
        t.[Wilaya] AS Wilaya,
        t.[idWilaya] AS WilayaId,
        t.[LieuDit] AS LieuDit,
        t.[Commune] AS Commune,
        t.[Daira] AS Daira,
        t.[Superficie] AS Superficie,
        det.[Nom] AS DetenteurNom,
        types.[Code] AS PermitTypeCode,
        types.[Nom] AS PermitTypeName
      FROM (([DroitsEtabl] AS dea
        INNER JOIN [Titres] AS t ON dea.[idTitre] = t.[id])
        LEFT JOIN [Detenteur] AS det ON t.[idDetenteur] = det.[id])
        LEFT JOIN [TypesTitres] AS types ON t.[idType] = types.[id]
      ${where}
      ORDER BY dea.[date] ASC, dea.[id] ASC`;
    const rows = await this.access.query(sql);
    const mapped = rows.map((row) => this.mapDeaRow(row));
    if (filters.wilayaLabel && !filters.wilayaNumber) {
      const label = filters.wilayaLabel;
      return mapped.filter((row) => this.normalizeWilayaText(row.permit?.wilaya) === label);
    }
    return mapped;
  }

  async getTaxesSupByIdTitre(idTitre: number) {
    const sql = `SELECT id, idTitre, [NumeroPerc], [PAR], [Date], [Surface], [Taxe], [Paye], [Comment], [DatePerDebut], [datePerFin], [TS_SurfaceMin], [TS_SurfaceMax], [TS_DroitFixe], [TS_PerInit], [TS_PremierRen], [TS_DeuRen], [dateremiseop], [datepaiement], [num_quittance]
                 FROM TaxesSup
                 WHERE idTitre = ?
                 ORDER BY [Date] ASC`;
    const rows = await this.access.queryParam(sql, [idTitre]);
    return rows.map((r: any) => ({
      ...r,
      Paye: this.toBoolean(r?.Paye),
    }));
  }

  async getDeaByIdTitre(idTitre: number) {
    const sql = `SELECT id, idTitre, idTypeProcedure, idProcedure, [NumeroPerc], [date], [droit], [paye], [DUN], [PARLA], [Commentaire], [dateremiseop], [datepaiement], [num_quittance]
                 FROM DroitsEtabl
                 WHERE idTitre = ?
                 ORDER BY [date] ASC`;
    const rows = await this.access.queryParam(sql, [idTitre]);
    return rows.map((r: any) => ({
      ...r,
      paye: this.toBoolean(r?.paye),
    }));
  }

  async getReceiptsBatch(filters: BatchFilterInput) {
    const normalized = this.normalizeBatchFilters(filters);
    const receipts: BatchReceiptRow[] = [];
    let tsCount = 0;
    let deaCount = 0;

    if (normalized.includeTs) {
      const tsRows = await this.fetchTaxesSupBatch(normalized);
      receipts.push(...tsRows);
      tsCount = tsRows.length;
    }

    if (normalized.includeDea) {
      const deaRows = await this.fetchDeaBatch(normalized);
      receipts.push(...deaRows);
      deaCount = deaRows.length;
    }

    receipts.sort((a, b) => {
      const dateA = this.parseDateInput(a.date)?.getTime() ?? 0;
      const dateB = this.parseDateInput(b.date)?.getTime() ?? 0;
      if (dateA !== dateB) return dateA - dateB;
      return a.receiptId - b.receiptId;
    });

    return {
      ok: true,
      count: receipts.length,
      tsCount,
      deaCount,
      filters: {
        requestedType: normalized.requestedType,
        statusId: normalized.statusId,
        year: normalized.year,
        from: normalized.fromDate?.toISOString().slice(0, 10),
        to: normalized.toDate?.toISOString().slice(0, 10),
        wilaya: normalized.wilayaRaw ?? null,
      },
      rows: receipts,
    };
  }
}
