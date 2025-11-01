import { Injectable, Logger } from '@nestjs/common';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { AccessService } from './access.service';

// Adjust these defaults to match your Access schema
const DEFAULT_TABLES = {
  permis: process.env.ACCESS_TABLE_PERMIS || 'Titres',
  coordinates: process.env.ACCESS_TABLE_COORDINATES || 'coordonees',
  types: process.env.ACCESS_TABLE_TYPES || 'TypesTitres',
  detenteur: process.env.ACCESS_TABLE_DETENTEUR || 'Detenteur',
  statutjuridique: process.env.ACCESS_TABLE_STATUTJURIDIQUE || 'statutjuridique',
  procedures: process.env.ACCESS_TABLE_PROCEDURES || 'Procedures',
  taxesSup: process.env.ACCESS_TABLE_TAXES_SUP || 'TaxesSup',
  droitsEtabl: process.env.ACCESS_TABLE_DROITS_ETABL || 'DroitsEtabl',
  typesProcedures: process.env.ACCESS_TABLE_TYPES_PROCEDURES || 'TypesProcedures'
};

// Map Access column names to API fields expected by the client
const DEFAULT_COLUMNS = {
  permis: {
    id: process.env.ACCESS_COL_PERMIS_ID || 'id',
    typePermis: process.env.ACCESS_COL_TYPE || 'idType',
    codeDemande: process.env.ACCESS_COL_CODE || 'Code',
    detenteur: process.env.ACCESS_COL_DETENTEUR || 'idDetenteur',
    superficie: process.env.ACCESS_COL_SUPERFICIE || 'Superficie',
    duree: process.env.ACCESS_COL_DUREE || '',
    // Use Wilaya as localisation (was LieuDit before)
    localisation: process.env.ACCESS_COL_LOCALISATION || 'Wilaya',
    dateCreation: process.env.ACCESS_COL_DATE || 'DateDemande',
    // Added for duration calculation
    dateDebut: process.env.ACCESS_COL_DATE_DEBUT || 'DateOctroi',
    dateFin: process.env.ACCESS_COL_DATE_FIN || 'DateExpiration',
    signed: process.env.ACCESS_COL_PERMIS_SIGNED || 'is_signed',
    takenDate: process.env.ACCESS_COL_PERMIS_TAKEN_DATE || 'date_remise_titre',
    takenBy: process.env.ACCESS_COL_PERMIS_TAKEN_BY || 'nom_remise_titre'
  },
  coordinates: {
    permisId: process.env.ACCESS_COL_COORD_PERMIS_ID || 'idTitre',
    id: process.env.ACCESS_COL_COORD_ID || 'id',
    x: process.env.ACCESS_COL_COORD_X || 'x',
    y: process.env.ACCESS_COL_COORD_Y || 'y',
    zone: process.env.ACCESS_COL_COORD_ZONE || 'h',
    order: process.env.ACCESS_COL_COORD_ORDER || ''
  },
  types: {
    id: process.env.ACCESS_COL_TYPES_ID || 'id',
    nom: process.env.ACCESS_COL_TYPES_NOM || 'Nom',
    code: process.env.ACCESS_COL_TYPES_CODE || 'Code',
    validiteMaximale: process.env.ACCESS_COL_TYPES_VALIDITE_MAX || 'ValiditeMaximale',
    renouvellementsPossibles: process.env.ACCESS_COL_TYPES_RENOUV_POSS || 'RenouvellementsPossibles',
    validiteRenouvellement: process.env.ACCESS_COL_TYPES_VALIDITE_RENOUV || 'ValiditeRenouvellement',
    delaiMaximalDemandeRenouvellement: process.env.ACCESS_COL_TYPES_DELAI_RENOUV || 'DelaiMaximalDemandeRenouvellement',
    validiteMaximaleTotale: process.env.ACCESS_COL_TYPES_VALIDITE_TOTALE || 'ValiditeMaximaleTotale',
    surfaceMaximale: process.env.ACCESS_COL_TYPES_SURFACE_MAX || 'SurfaceMaximale'
  },
  procedures: {
    id: process.env.ACCESS_COL_PROC_ID || 'id',
    titreId: process.env.ACCESS_COL_PROC_TITRE_ID || 'idTitre',
    typeId: process.env.ACCESS_COL_PROC_TYPE_ID || 'idTypeTitre',
    label: process.env.ACCESS_COL_PROC_LABEL || 'Procedure',
    dateOption: process.env.ACCESS_COL_PROC_DATE_OPTION || 'date_option'
  },
  taxesSup: {
    id: process.env.ACCESS_COL_TAXES_ID || 'id',
    titreId: process.env.ACCESS_COL_TAXES_TITRE_ID || 'idTitre',
    numeroPerc: process.env.ACCESS_COL_TAXES_NUMERO_PERC || 'NumeroPerc',
    par: process.env.ACCESS_COL_TAXES_PAR || 'PAR',
    date: process.env.ACCESS_COL_TAXES_DATE || 'Date'
  },
  droitsEtabl: {
    id: process.env.ACCESS_COL_DROITS_ID || 'id',
    titreId: process.env.ACCESS_COL_DROITS_TITRE_ID || 'idTitre',
    numeroPerc: process.env.ACCESS_COL_DROITS_NUMERO_PERC || 'NumeroPerc'
  },
  typesProcedures: {
    id: process.env.ACCESS_COL_TPROC_ID || 'id',
    nom: process.env.ACCESS_COL_TPROC_NOM || 'Nom'
  }
};

const TYPE_OPTION_MAP: Record<string, string> = {
  PPM: 'APM',
  PEM: 'TEM',
  PEC: 'TEC',
  PXM: 'TXM',
  PXC: 'TXC',
  ARM: 'AAM',
  ARC: 'AAC'
};

@Injectable()
export class PermisService {
  private readonly logger = new Logger(PermisService.name);

  constructor(private readonly access: AccessService) {}

  private quote(name: string): string {
    if (!name) return '';
    return name.startsWith('[') ? name : `[${name}]`;
  }

  async getPermisById(id: string) {
    const t = DEFAULT_TABLES.permis;
    const c = DEFAULT_COLUMNS.permis;
    const isNumericId = /^\d+$/.test(id);
    const sql = `SELECT * FROM ${t} WHERE ${c.id} = ${isNumericId ? id : this.access.escapeValue(id)}`;
    const rows = await this.access.query(sql);
    if (!rows.length) return null;
    const r = rows[0] as Record<string, any>;
    const toStr = (v: any) => (v == null ? '' : String(v));
    const toNum = (v: any) => (v == null || v === '' ? null : Number(v));
    // fetch type details from TypesTitres
    const typeId = r[c.typePermis];
    const typeData = await this.getTypeById(typeId).catch(() => null);
    // fetch detenteur details for Arabic name
    const detId = r[c.detenteur];
    const detData = await this.getDetenteurById(detId).catch(() => null);

    // helpers for dates
    const parseAccessDate = (v: any): Date | null => {
      if (!v && v !== 0) return null;
      if (v instanceof Date) return isNaN(+v) ? null : v;
      const s = String(v).trim();
      // dd/MM/yyyy
      const m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
      if (m) {
        const d = new Date(Number(m[3]), Number(m[2]) - 1, Number(m[1]));
        return isNaN(+d) ? null : d;
      }
      const d2 = new Date(s);
      return isNaN(+d2) ? null : d2;
    };
    const fmtFr = (d: Date | null) => d ? `${d.getFullYear()}/${String(d.getMonth()+1).padStart(2,'0')}/${String(d.getDate()).padStart(2,'0')}` : '';
    const diffYears = (a: Date, b: Date) => {
      let years = b.getFullYear() - a.getFullYear();
      const m = b.getMonth() - a.getMonth();
      if (m < 0 || (m === 0 && b.getDate() < a.getDate())) years -= 1;
      return years < 0 ? 0 : years;
    };

    const dDebut = parseAccessDate((r as any)[(DEFAULT_COLUMNS.permis as any).dateDebut]);
    const dFin = parseAccessDate((r as any)[(DEFAULT_COLUMNS.permis as any).dateFin]);
    let dureeDisplayAr = '';
    if (dDebut && dFin && dFin.getFullYear() > 1900) {
      const y = diffYears(dDebut, dFin);
      const y2 = String(y).padStart(2, '0');
      const words: Record<number, string> = { 1: 'Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½', 2: 'Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½', 3: 'Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½', 4: 'Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½', 5: 'Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½', 6: 'Ã¯Â¿Â½Ã¯Â¿Â½', 7: 'Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½', 8: 'Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½', 9: 'Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½', 10: 'Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½' };
      const word = words[y] ? (y <= 2 ? words[y] : `${words[y]} (${y2}) Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½`) : `(${y2}) Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½`;
      // Compose full text with tatweel in Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½/Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½ as per example
      // Example: (Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½: Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½ (04) Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½ (Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½ 18/12/2025 Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½ 18/12/2029))
      if (y <= 2) {
        // normalize singular/dual
        const noun = y === 1 ? 'Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½' : 'Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½';
        dureeDisplayAr = `${word} (Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½ ${fmtFr(dDebut)} Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½ ${fmtFr(dFin)})`;
      } else {
        dureeDisplayAr = `${word} (Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½ ${fmtFr(dDebut)} Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½Ã¯Â¿Â½ ${fmtFr(dFin)})`;
      }
    }

    // Normalize detenteur payload to include Arabic/Latin names if available
    // Resolve statut juridique (Arabic/FR) for detenteur if id is available
    let statutNorm: any = null;
    try {
      const sjId = (detData as any)?.id_statutJuridique || (detData as any)?.idStatutJuridique || (detData as any)?.id_statut || (detData as any)?.statutJuridiqueId;
      if (sjId != null && sjId !== '') {
        const sj = await this.getStatutJuridiqueById(sjId).catch(() => null);
        if (sj) {
          statutNorm = {
            id: (sj as any).id_statutJuridique ?? (sj as any).id ?? null,
            Code: (sj as any).code_statut ?? (sj as any).Code ?? undefined,
            Statut: (sj as any).statut_fr ?? (sj as any).Statut ?? (sj as any).statut ?? undefined,
            StatutArab: (sj as any).statut_ar ?? (sj as any).StatutArab ?? (sj as any).statut_arabe ?? undefined,
          };
        }
      }
    } catch {}

    const detNorm = detData ? {
      id: detData.id ?? detData.Id ?? detData.ID ?? toStr(r[c.detenteur]),
      Nom: detData.Nom ?? detData.nom ?? detData.RaisonSociale ?? detData.raison_sociale ?? detData['Raison Sociale'] ?? detData.raisonSociale ?? detData.denomination ?? detData.nom_societe ?? detData.nom_societeFR ?? '',
      NomArab: detData.NomArab ?? detData.NomAR ?? detData.nomAR ?? detData.nom_ar ?? detData.nom_societeAR ?? '',
      nom_societeFR: detData.nom_societeFR ?? detData.Nom ?? detData.nom ?? detData.RaisonSociale ?? '',
      nom_societe: detData.nom_societe ?? detData.Nom ?? detData.nom ?? '',
      nom_ar: detData.nom_ar ?? detData.NomArab ?? detData.NomAR ?? '',
      raison_sociale: detData.raison_sociale ?? detData.RaisonSociale ?? detData['Raison Sociale'] ?? '',
      // Attach statut juridique if found; also project top-level fields for convenience
      StatutJuridique: statutNorm || undefined,
      StatutArab: (statutNorm && statutNorm.StatutArab) || undefined,
    } : null;

    // Resolve substance (Arabic) with broad fallbacks; if Arabic missing, use FR/Latin to avoid blanks
    const substanceResolved = toStr(
      (r as any).SubstancesArabe ?? (r as any).substances_arabe ?? (r as any).SubstancesAR ??
      (r as any).Substances ?? (r as any).SubstancesFR ?? (r as any).substance ?? (r as any).substance_fr ?? ''
    );

    // Extract location details
    const wilayaVal = toStr(
      (r as any)[c.localisation] ??
      (r as any).Wilaya ?? (r as any).wilaya ?? (r as any).idWilaya ?? ''
    );
    const communeVal = toStr((r as any).Commune ?? (r as any).commune ?? (r as any).idCommune ?? '');
    const dairaVal = toStr((r as any).Daira ?? (r as any)['DaÃƒÂ¯ra'] ?? (r as any).daira ?? '');
    const lieuditVal = toStr((r as any).LieuDit ?? (r as any).lieudit ?? '');

    const signedCol = c.signed || 'is_signed';
    const takenDateCol = c.takenDate || 'date_remise_titre';
    const takenByCol = c.takenBy || 'nom_remise_titre';

    const val: any = {
      id: r[c.id],
      typePermis: typeData || toStr(r[c.typePermis]),
      codeDemande: toStr(r[c.codeDemande]),
      detenteur: detNorm || toStr(r[c.detenteur]),
      superficie: toNum(r[c.superficie]),
      duree: c.duree ? toStr(r[c.duree]) : '',
      // Localisation now prefers Wilaya; keep broad fallbacks for older schemas
      localisation: toStr(
        (r as any)[c.localisation] ??
        (r as any).Wilaya ?? (r as any).wilaya ?? (r as any).idWilaya ??
        (r as any).LieuDit ?? (r as any).lieudit ?? ''
      ),
      dateCreation: r[c.dateCreation] ? new Date(r[c.dateCreation]).toISOString() : null,
      coordinates: await this.getCoordinatesByPermisId(String(r[c.id])).catch(() => []),
      duree_display_ar: dureeDisplayAr,
      date_octroi_fr: fmtFr(dDebut),
      date_expiration_fr: fmtFr(dFin),
      detenteur_ar: (detNorm && (detNorm.NomArab || detNorm.nom_ar)) || detData?.NomArab || detData?.nom_ar || '',
      substance_ar: substanceResolved,
      wilaya: wilayaVal,
      commune: communeVal,
      daira: dairaVal,
      lieudit: lieuditVal,
      is_signed: (() => {
        const v = (r as any)[signedCol];
        if (v === true) return true;
        if (v === false) return false;
        const s = String(v ?? '').trim().toLowerCase();
        return s === 'true' || s === 'yes' || s === '-1' || s === '1';
      })(),
      takenDate: (() => {
        const raw = (r as any)[takenDateCol];
        const parsed = parseAccessDate(raw);
        if (!parsed) return '';
        const y = parsed.getFullYear();
        const m = String(parsed.getMonth() + 1).padStart(2, '0');
        const d2 = String(parsed.getDate()).padStart(2, '0');
        return `${y}-${m}-${d2}`;
      })(),
      takenBy: toStr((r as any)[takenByCol])
    };
    // Add compatibility fields expected by designer
    val.code_demande = val.codeDemande;
    val.id_demande = val.id;
    val.taken_date = val.takenDate;
    val.taken_by = val.takenBy;
    const procTable = DEFAULT_TABLES.procedures;
    if (procTable) {
      const pc: any = DEFAULT_COLUMNS.procedures;
      const dateCol = this.quote(pc.dateOption || 'date_option');
      const titreCol = this.quote(pc.titreId || 'idTitre');
      const labelPick = await this.pickExistingColumn(procTable, [pc.label, 'Nom', 'Procedure', 'Libelle', 'Label']);
      const labelCol = labelPick ? labelPick.quoted : this.quote(pc.label || 'Procedure');
      const whereId = isNumericId ? String(id) : String(this.access.escapeValue(String(id)));
      try {
        const sqlOpt = `SELECT TOP 1 ${dateCol} AS opt_date FROM ${procTable} WHERE ${titreCol} = ${whereId} AND ${labelCol} LIKE 'Opt%' ORDER BY ${dateCol} DESC`;
        const optRows = await this.access.query(sqlOpt);
        if (optRows && optRows.length) {
          const optDateParsed = parseAccessDate(optRows[0]?.opt_date);
          if (optDateParsed) {
            const y = optDateParsed.getFullYear();
            const m = String(optDateParsed.getMonth() + 1).padStart(2, '0');
            const d2 = String(optDateParsed.getDate()).padStart(2, '0');
            val.optionDate = `${y}-${m}-${d2}`;
            val.date_option = val.optionDate;
          }
        }
      } catch {}
    }
    if (!val.typePermis || typeof val.typePermis === 'string') {
      val.typePermis = { lib_type: String(r[c.typePermis] ?? ''), duree_initiale: null };
    } else {
      val.typePermis.lib_type = String(val.typePermis.nom ?? val.typePermis.Nom ?? '');
      val.typePermis.duree_initiale = Number(val.typePermis.validiteMaximale ?? val.typePermis.ValiditeMaximale ?? 0) || null;
    }
    return val;
  }

  async getCoordinatesByPermisId(id: string) {
    const t = DEFAULT_TABLES.coordinates;
    const c: any = DEFAULT_COLUMNS.coordinates;
    const isNumericId = /^\d+$/.test(id);
    const idCol = c.id || 'id';
    const zoneCol = c.zone || 'h';
    const sql = `SELECT ${c.x} AS cx, ${c.y} AS cy, ${zoneCol} AS zone, ${idCol} AS coord_id FROM ${t} WHERE ${c.permisId} = ${isNumericId ? id : this.access.escapeValue(id)} ORDER BY ${idCol}`;
    let rows: any[] = [];
    try {
      rows = await this.access.query(sql);
    } catch {
      return [];
    }
    const parseFr = (val: any): number => {
      if (typeof val === 'number') return val;
      const s = String(val || '').replace(/\s+/g, '').replace('\u00A0', '').replace(',', '.');
      const n = parseFloat(s);
      return isNaN(n) ? 0 : n;
    };
    return rows.map((r: any) => ({ x: parseFr(r.cx ?? r[c.x] ?? r.x), y: parseFr(r.cy ?? r[c.y] ?? r.y), order: Number(r.coord_id || r[c.id] || 0), zone: r.zone }));
  }

  private async ensureColumnExists(table: string, column: string, attempts: string[]) {
    const hasColumn = async () => {
      try {
        await this.access.query(`SELECT ${this.quote(column)} FROM ${table} WHERE 1 = 0`);
        return true;
      } catch {
        return false;
      }
    };

    if (await hasColumn()) return;

    const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
    // Try each ALTER, and if the table is locked, backoff and retry a few times.
    for (const sql of attempts) {
      let applied = false;
      let lastErr: any = null;
      for (let i = 0; i < 3; i++) {
        try {
          await this.access.query(sql);
          applied = true;
          break;
        } catch (err) {
          lastErr = err;
          const msg = (err as any)?.message ? String((err as any).message) : '';
          const locked = /Could not lock table/i.test(msg) || /database is locked/i.test(msg);
          try { this.logger.warn(`[ensureColumnExists] attempt failed (${sql}) [try ${i+1}/3]: ${msg}`); } catch {}
          if (locked) {
            await sleep(800 + i * 400);
            continue;
          }
          break; // non-lock error; bail to next ALTER variant
        }
      }
      if (applied) break;
    }

    if (!(await hasColumn())) {
      throw new Error(`Unable to add column ${column} to table ${table}`);
    }
  }

  // Detect if a column exists in a table by attempting a zero-row select
  private async columnExists(table: string, column: string): Promise<boolean> {
    try {
      await this.access.query(`SELECT ${this.quote(column)} FROM ${table} WHERE 1 = 0`);
      return true;
    } catch {
      return false;
    }
  }

  // Return the first existing column among candidates, with raw and quoted forms
  private async pickExistingColumn(table: string, candidates: (string | undefined)[]): Promise<{ name: string; quoted: string } | null> {
    for (const cand of candidates) {
      const col = (cand || '').trim();
      if (!col) continue;
      if (await this.columnExists(table, col)) return { name: col, quoted: this.quote(col) };
    }
    return null;
  }

  // Find a suitable TypesProcedures.id to satisfy FK when inserting an option row
  private async resolveProcedureTypeIdForOption(typeTitreIdLiteral?: string | null): Promise<string | null> {
    const table = (DEFAULT_TABLES as any).typesProcedures;
    if (!table) return null;
    const cols: any = (DEFAULT_COLUMNS as any).typesProcedures || { id: 'id', nom: 'Nom' };
    const idCol = cols.id || 'id';
    // Pick the label/name column that exists in TypesProcedures (Nom vs Procedure vs Libelle)
    let labelColQuoted = this.quote(cols.nom || 'Nom');
    try {
      const pick = await this.pickExistingColumn(table, [cols.nom, 'Nom', 'Procedure', 'Libelle', 'Label']);
      if (pick) labelColQuoted = pick.quoted;
    } catch {}
    const idColQuoted = this.quote(idCol);
    // Optionally filter by the TypesProcedures.idTypeTitre if present and provided
    let typeTitreFilter = '';
    let idTypeTitreColumn: { name: string; quoted: string } | null = null;
    try {
      idTypeTitreColumn = await this.pickExistingColumn(table, ['idTypeTitre', 'IdTypeTitre', 'id_type_titre']);
      if (idTypeTitreColumn && typeTitreIdLiteral) {
        const raw = String(typeTitreIdLiteral).trim();
        const unquoted = raw.replace(/^'(.+)'$/, '$1');
        if (/^\d+$/.test(unquoted)) {
          const asNum = unquoted;
          const asTxt = String(this.access.escapeValue(unquoted));
          typeTitreFilter = ` AND (${idTypeTitreColumn.quoted} = ${asNum} OR ${idTypeTitreColumn.quoted} = ${asTxt})`;
        } else {
          typeTitreFilter = ` AND ${idTypeTitreColumn.quoted} = ${raw}`;
        }
      }
    } catch {}
    const queries: string[] = [
      `SELECT TOP 1 ${idColQuoted} AS tid FROM ${table} WHERE ${labelColQuoted} LIKE 'Opt%'${typeTitreFilter} ORDER BY ${idColQuoted}`,
      `SELECT TOP 1 ${idColQuoted} AS tid FROM ${table} WHERE UCase(${labelColQuoted}) LIKE 'OPT%'${typeTitreFilter} ORDER BY ${idColQuoted}`,
      `SELECT TOP 1 ${idColQuoted} AS tid FROM ${table} WHERE UCase(${labelColQuoted}) LIKE 'DEMANDE%'${typeTitreFilter} ORDER BY ${idColQuoted}`,
      `SELECT TOP 1 ${idColQuoted} AS tid FROM ${table} WHERE 1=1${typeTitreFilter} ORDER BY ${idColQuoted}`
    ];
    for (const sql of queries) {
      try {
        const rows = await this.access.query(sql);
        if (rows && rows.length) {
          const v = rows[0]?.tid ?? rows[0]?.[idCol];
          if (v != null && v !== '') {
            const s = String(v);
            return /^\d+$/.test(s) ? s : this.access.escapeValue(s) as any as string;
          }
        }
      } catch {}
    }
    // Retry without idTypeTitre filter in case of type mismatch issues
    const queriesNoFilter: string[] = [
      `SELECT TOP 1 ${idColQuoted} AS tid FROM ${table} WHERE ${labelColQuoted} LIKE 'Opt%' ORDER BY ${idColQuoted}`,
      `SELECT TOP 1 ${idColQuoted} AS tid FROM ${table} WHERE UCase(${labelColQuoted}) LIKE 'OPT%' ORDER BY ${idColQuoted}`,
      `SELECT TOP 1 ${idColQuoted} AS tid FROM ${table} WHERE UCase(${labelColQuoted}) LIKE 'DEMANDE%' ORDER BY ${idColQuoted}`,
      `SELECT TOP 1 ${idColQuoted} AS tid FROM ${table} ORDER BY ${idColQuoted}`
    ];
    for (const sql of queriesNoFilter) {
      try {
        const rows = await this.access.query(sql);
        if (rows && rows.length) {
          const v = rows[0]?.tid ?? rows[0]?.[idCol];
          if (v != null && v !== '') {
            const s = String(v);
            return /^\d+$/.test(s) ? s : this.access.escapeValue(s) as any as string;
          }
        }
      } catch {}
    }
    return null;
  }

  // Insert a robust "opté à titre" row into Procedures, adapting to schema differences
  private async logOptionProcedureRow(
    table: string,
    titreIdLiteral: string,
    targetCode: string,
    typeTitreIdLiteral: string | null,
    optionDateLiteral: string | null
  ) {
    const pc: any = DEFAULT_COLUMNS.procedures;
    // Ensure date_option exists
    const dateColName = pc.dateOption || 'date_option';
    await this.ensureColumnExists(table, dateColName, [
      `ALTER TABLE ${table} ADD COLUMN ${dateColName} DATETIME`,
      `ALTER TABLE ${table} ADD COLUMN ${dateColName} DATE`,
      `ALTER TABLE ${table} ADD COLUMN ${dateColName} TEXT(50)`
    ]);
    const dateCol = this.quote(dateColName);

    // Resolve actual column names
    const titreColPick = await this.pickExistingColumn(table, [pc.titreId, 'idTitre', 'IdTitre', 'titre_id', 'TitreId']);
    const typeTitreColPick = await this.pickExistingColumn(table, [pc.typeId, 'idTypeTitre', 'IdTypeTitre', 'id_type_titre']);
    const typeProcedureColPick = await this.pickExistingColumn(table, ['idTypeProcedure', 'IdTypeProcedure', 'id_type_procedure']);
    const labelColPick = await this.pickExistingColumn(table, [pc.label, 'Procedure', 'Nom', 'Libelle', 'Label']);

    const columns: string[] = [];
    const values: string[] = [];
    if (titreColPick) { columns.push(titreColPick.quoted); values.push(titreIdLiteral); }
    if (typeTitreColPick && typeTitreIdLiteral) { columns.push(typeTitreColPick.quoted); values.push(typeTitreIdLiteral); }
    if (typeProcedureColPick) {
      const procTypeId = await this.resolveProcedureTypeIdForOption(typeTitreIdLiteral);
      if (procTypeId) { columns.push(typeProcedureColPick.quoted); values.push(procTypeId); }
    }
    if (labelColPick) {
      const label = `Opté à titre (${targetCode})`;
      columns.push(labelColPick.quoted);
      values.push(String(this.access.escapeValue(label)));
    }
    columns.push(dateCol);
    values.push(optionDateLiteral ?? 'NULL');

    const insertSql = `INSERT INTO ${table} (${columns.join(', ')}) VALUES (${values.join(', ')})`;
    await this.access.query(insertSql);
  }

  async runRaw(sql: string) {
    const rows = await this.access.query(sql);
    return { rows };
  }

  async findPermisByProcedure(procedureId: string) {
    const t = DEFAULT_TABLES.permis;
    const c = DEFAULT_COLUMNS.permis;
    const isNumericId = /^\d+$/.test(procedureId);
    // Avoid using alias 'code' which may trigger circular alias error in Access
    const sql = `SELECT ${c.id} AS perm_id, ${c.codeDemande} AS perm_code FROM [${t}] WHERE ${c.id} = ${isNumericId ? procedureId : this.access.escapeValue(procedureId)}`;
    const rows = await this.access.query(sql);
    if (!rows.length) return { exists: false };
    const r = rows[0] as any;
    return { exists: true, permisId: Number(r.perm_id), permisCode: String(r.perm_code) };
  }

  // Flexible search: accepts numeric id OR combined type+code like "PEC8375" or "pec 8375"
  async searchPermis(query: string) {
    const raw = String(query || '').trim();
    if (!raw) return null;
    // If purely numeric, treat as ID
    if (/^\d+$/.test(raw)) {
      return this.getPermisById(raw);
    }
    // Normalize combined input: remove spaces, uppercase
    const flat = raw.replace(/\s+/g, '').toUpperCase();
    const m = flat.match(/^([A-Z]+)(\d+)$/);
    if (!m) return null;
    const typeCode = m[1];
    const codeNum = m[2];
    const typesT = DEFAULT_TABLES.types;
    const tc: any = DEFAULT_COLUMNS.types;
    // Find type by code (case-insensitive)
    const safeCodeExact = this.access.escapeValue(typeCode);
    const safeCodeUpper = this.access.escapeValue(typeCode.toUpperCase());
    let typeRows: any[] = [];
    // Try exact match first
    try { typeRows = await this.access.query(`SELECT TOP 1 * FROM ${typesT} WHERE ${tc.code} = ${safeCodeExact}`); } catch {}
    // Try case-insensitive via UCase (Access SQL function)
    if (!typeRows || !typeRows.length) {
      try { typeRows = await this.access.query(`SELECT TOP 1 * FROM ${typesT} WHERE UCase(${tc.code}) = ${safeCodeUpper}`); } catch {}
    }
    if (!typeRows || !typeRows.length) return null;
    const typeId = typeRows[0]?.[tc.id] ?? typeRows[0]?.id;
    if (typeId == null) return null;
    const t = DEFAULT_TABLES.permis;
    const c: any = DEFAULT_COLUMNS.permis;
    const pad5 = codeNum.padStart(5, '0');
    const safePad = this.access.escapeValue(pad5);
    const safeNum = this.access.escapeValue(codeNum);
    const fullCode = `${typeCode}${codeNum}`;
    const safeFull = this.access.escapeValue(fullCode);
    const safeFullUpper = this.access.escapeValue(fullCode.toUpperCase());
    const likeSuffix = this.access.escapeValue(`%${codeNum}`);
    const likePadSuffix = this.access.escapeValue(`%${pad5}`);
    const numN = Number(codeNum);
    const typeCond = /^\d+$/.test(String(typeId)) ? String(typeId) : this.access.escapeValue(String(typeId));
    // Try with both padded and raw numeric/text comparisons
    let rows: any[] = [];
    // Attempt 1: treat Code as numeric (no quotes)
    if (!isNaN(numN)) {
      const sqlNum = `SELECT TOP 1 * FROM ${t} WHERE ${c.typePermis} = ${typeCond} AND ${c.codeDemande} = ${String(numN)}`;
      try { rows = await this.access.query(sqlNum); } catch {}
    }
    // Attempt 2: treat Code as text (with quotes, include padded and raw)
    if (!rows || !rows.length) {
      const whereText = `(${c.codeDemande} = ${safePad} OR ${c.codeDemande} = ${safeNum} OR ${c.codeDemande} = ${safeFull})`;
      const sqlTxt = `SELECT TOP 1 * FROM ${t} WHERE ${c.typePermis} = ${typeCond} AND ${whereText}`;
      try { rows = await this.access.query(sqlTxt); } catch {}
    }
    // Attempt 3: match uppercase / combined code without spacing
    if (!rows || !rows.length) {
      const sqlFull = `SELECT TOP 1 * FROM ${t} WHERE ${c.typePermis} = ${typeCond} AND (UCase(${c.codeDemande}) = ${safeFullUpper})`;
      try { rows = await this.access.query(sqlFull); } catch {}
    }
    // Attempt 4: match codes that end with the numeric part (e.g., PEC236 -> 7236)
    if (!rows || !rows.length) {
      const sqlLike = `SELECT TOP 1 * FROM ${t} WHERE ${c.typePermis} = ${typeCond} AND (${c.codeDemande} LIKE ${likeSuffix} OR ${c.codeDemande} LIKE ${likePadSuffix}) ORDER BY ${c.codeDemande}`;
      try { rows = await this.access.query(sqlLike); } catch {}
    }
    if (!rows || !rows.length) return null;
    const idVal = rows[0]?.[c.id] ?? rows[0]?.id;
    if (idVal == null) return null;
    return this.getPermisById(String(idVal));
  }

  private async getTypeById(typeId: any) {
    if (typeId == null || typeId === '') return null;
    const t = DEFAULT_TABLES.types;
    const c = DEFAULT_COLUMNS.types as any;
    const isNumeric = /^\d+$/.test(String(typeId));
    const sql = `SELECT * FROM ${t} WHERE ${c.id} = ${isNumeric ? typeId : this.access.escapeValue(String(typeId))}`;
    const rows = await this.access.query(sql);
    if (!rows.length) return null;
    const r = rows[0] as any;
    return {
      id: r[c.id],
      nom: r[c.nom],
      code: r[c.code],
      validiteMaximale: r[c.validiteMaximale],
      renouvellementsPossibles: r[c.renouvellementsPossibles],
      validiteRenouvellement: r[c.validiteRenouvellement],
      delaiMaximalDemandeRenouvellement: r[c.delaiMaximalDemandeRenouvellement],
      validiteMaximaleTotale: r[c.validiteMaximaleTotale],
      surfaceMaximale: r[c.surfaceMaximale]
    };
  }

  private async getTypeByCode(code: string) {
    const normalized = String(code ?? '').trim();
    if (!normalized) return null;
    const t = DEFAULT_TABLES.types;
    const c = DEFAULT_COLUMNS.types as any;
    const literal = this.access.escapeValue(normalized);
    const literalUpper = this.access.escapeValue(normalized.toUpperCase());
    let rows: any[] = [];
    try { rows = await this.access.query(`SELECT TOP 1 * FROM ${t} WHERE ${c.code} = ${literal}`); } catch {}
    if (!rows || !rows.length) {
      try { rows = await this.access.query(`SELECT TOP 1 * FROM ${t} WHERE UCase(${c.code}) = ${literalUpper}`); } catch {}
    }
    if (!rows || !rows.length) return null;
    const r = rows[0] as any;
    return {
      id: r[c.id],
      nom: r[c.nom],
      code: r[c.code],
      validiteMaximale: r[c.validiteMaximale],
      renouvellementsPossibles: r[c.renouvellementsPossibles],
      validiteRenouvellement: r[c.validiteRenouvellement],
      delaiMaximalDemandeRenouvellement: r[c.delaiMaximalDemandeRenouvellement],
      validiteMaximaleTotale: r[c.validiteMaximaleTotale],
      surfaceMaximale: r[c.surfaceMaximale]
    };
  }

  private async getDetenteurById(detId: any) {
    if (detId == null || detId === '') return null;
    const t = DEFAULT_TABLES.detenteur;
    const isNumeric = /^\d+$/.test(String(detId));
    const sql = `SELECT * FROM ${t} WHERE id = ${isNumeric ? detId : this.access.escapeValue(String(detId))}`;
    const rows = await this.access.query(sql);
    if (!rows.length) return null;
    return rows[0] as any;
  }

  private async getStatutJuridiqueById(statutId: any) {
    if (statutId == null || statutId === '') return null;
    const t = (DEFAULT_TABLES as any).statutjuridique;
    const idVal = String(statutId);
    const isNumeric = /^\d+$/.test(idVal);
    // Try common id columns: id_statutJuridique, id
    const candidates = [
      `SELECT TOP 1 * FROM ${t} WHERE id_statutJuridique = ${isNumeric ? idVal : this.access.escapeValue(idVal)}`,
      `SELECT TOP 1 * FROM ${t} WHERE id = ${isNumeric ? idVal : this.access.escapeValue(idVal)}`,
    ];
    for (const sql of candidates) {
      try {
        const rows = await this.access.query(sql);
        if (rows && rows.length) return rows[0] as any;
      } catch {}
    }
    return null;
  }

  private async ensureQrColumns() {
    const t = (DEFAULT_TABLES as any).permis;
    const tryAlter = async (sql: string) => { try { await this.access.query(sql); } catch {} };
    await tryAlter(`ALTER TABLE ${t} ADD COLUMN DateHeureSysteme TEXT(50)`);
    await tryAlter(`ALTER TABLE ${t} ADD COLUMN QrCode TEXT(50)`);
    await tryAlter(`ALTER TABLE ${t} ADD COLUMN code_wilaya TEXT(5)`);
    await tryAlter(`ALTER TABLE ${t} ADD COLUMN Qrinsererpar TEXT(100)`);
  }

  private async ensureSignedColumn() {
    const t = (DEFAULT_TABLES as any).permis;
    const c: any = DEFAULT_COLUMNS.permis;
    const column = c.signed || 'is_signed';
    await this.ensureColumnExists(t, column, [
      `ALTER TABLE ${t} ADD COLUMN ${column} YESNO`,
      `ALTER TABLE ${t} ADD COLUMN ${column} BIT`,
      `ALTER TABLE ${t} ADD COLUMN ${column} BOOLEAN`,
      `ALTER TABLE ${t} ADD COLUMN ${column} INTEGER DEFAULT 0`,
      `ALTER TABLE ${t} ADD COLUMN ${column} BYTE DEFAULT 0`
    ]);

    try {
      await this.access.query(`UPDATE ${t} SET ${column} = 0 WHERE ${column} IS NULL`);
    } catch {}
  }

  private async ensureCollectionColumns() {
    const t = (DEFAULT_TABLES as any).permis;
    const c: any = DEFAULT_COLUMNS.permis;
    const dateCol = c.takenDate || 'date_remise_titre';
    const nameCol = c.takenBy || 'nom_remise_titre';

    await this.ensureColumnExists(t, dateCol, [
      `ALTER TABLE ${t} ADD COLUMN ${dateCol} DATETIME`,
      `ALTER TABLE ${t} ADD COLUMN ${dateCol} DATE`,
      `ALTER TABLE ${t} ADD COLUMN ${dateCol} TEXT(50)`
    ]);

    await this.ensureColumnExists(t, nameCol, [
      `ALTER TABLE ${t} ADD COLUMN ${nameCol} TEXT(100)`,
      `ALTER TABLE ${t} ADD COLUMN ${nameCol} VARCHAR(100)`,
      `ALTER TABLE ${t} ADD COLUMN ${nameCol} CHAR(100)`
    ]);
  }

  private formatAccessDateLiteral(input: any): string | null {
    if (input == null || input === '') return null;
    const parseInput = (val: any): Date | null => {
      if (val instanceof Date && !isNaN(+val)) return val;
      const s = String(val).trim();
      if (!s) return null;
      const iso = s.match(/^(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})$/);
      if (iso) {
        const d = new Date(Number(iso[1]), Number(iso[2]) - 1, Number(iso[3]));
        return isNaN(+d) ? null : d;
      }
      const fr = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
      if (fr) {
        const d = new Date(Number(fr[3]), Number(fr[2]) - 1, Number(fr[1]));
        return isNaN(+d) ? null : d;
      }
      const d = new Date(s);
      return isNaN(+d) ? null : d;
    };

    const date = parseInput(input);
    if (!date) return null;
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `#${year}-${month}-${day}#`;
  }

  private extractYear(input: any): number | null {
    if (input == null || input === '') return null;
    if (input instanceof Date && !isNaN(+input)) {
      return input.getFullYear();
    }
    const s = String(input).trim();
    if (!s) return null;
    const iso = s.match(/^(\d{4})[-\/]/);
    if (iso) return Number(iso[1]) || null;
    const fr = s.match(/[-\/](\d{4})$/);
    if (fr) return Number(fr[1]) || null;
    const any = s.match(/(\d{4})(?!.*\d)/);
    if (any) return Number(any[1]) || null;
    const d = new Date(s);
    return isNaN(+d) ? null : d.getFullYear();
  }

  private async updateTaxesSupForOption(titreId: string, oldCode: string, newCode: string, newTypeName: string, optionYear: number | null) {
    const table = DEFAULT_TABLES.taxesSup;
    if (!table) return;
    const cols: any = DEFAULT_COLUMNS.taxesSup;
    const idCol = this.quote(cols.id || 'id');
    const titreCol = this.quote(cols.titreId || 'idTitre');
    const numeroCol = this.quote(cols.numeroPerc || 'NumeroPerc');
    const parCol = this.quote(cols.par || 'PAR');
    const dateCol = cols.date ? this.quote(cols.date) : null;
    const isNumericTitre = /^\d+$/.test(String(titreId));
    const titreLiteral = isNumericTitre ? String(titreId) : this.access.escapeValue(String(titreId));
    const selectSql = `SELECT ${idCol} AS rec_id, ${numeroCol} AS rec_numero, ${parCol} AS rec_par${dateCol ? `, ${dateCol} AS rec_date` : ''} FROM ${table} WHERE ${titreCol} = ${titreLiteral}`;
    let rows: any[] = [];
    try {
      rows = await this.access.query(selectSql);
    } catch (err) {
      try { this.logger.warn(`[updateTaxesSupForOption] select failed: ${(err as any)?.message || err}`); } catch {}
      return;
    }
    const upperOld = (oldCode || '').toUpperCase();
    const replacement = newCode || '';
    const normalizedName = String(newTypeName ?? '').trim();
    for (const row of rows) {
      const recordId = row.rec_id;
      const recordIdLiteral = typeof recordId === 'number' ? String(recordId) : this.access.escapeValue(String(recordId));
      const numeroRaw = row.rec_numero ?? '';
      const numeroStr = String(numeroRaw || '');
      if (!numeroStr || !numeroStr.toUpperCase().includes(upperOld)) continue;
      let recordYear = this.extractYear(row.rec_date);
      if (recordYear == null) recordYear = this.extractYear(numeroStr);
      if (optionYear != null && recordYear != null && recordYear < optionYear) continue;
      const replaced = numeroStr.replace(new RegExp(oldCode, 'gi'), replacement);
      const sets: string[] = [];
      if (replaced !== numeroStr) {
        sets.push(`${numeroCol} = ${this.access.escapeValue(replaced)}`);
      }
      if (normalizedName && String(row.rec_par || '').trim() !== normalizedName) {
        sets.push(`${parCol} = ${this.access.escapeValue(normalizedName)}`);
      }
      if (!sets.length) continue;
      const updateSql = `UPDATE ${table} SET ${sets.join(', ')} WHERE ${idCol} = ${recordIdLiteral}`;
      try {
        await this.access.query(updateSql);
      } catch (err) {
        try { this.logger.warn(`[updateTaxesSupForOption] update failed: ${(err as any)?.message || err}`); } catch {}
      }
    }
  }

  private async updateDroitsEtablForOption(titreId: string, oldCode: string, newCode: string, optionYear: number | null) {
    const table = DEFAULT_TABLES.droitsEtabl;
    if (!table) return;
    const cols: any = DEFAULT_COLUMNS.droitsEtabl;
    const idCol = this.quote(cols.id || 'id');
    const titreCol = this.quote(cols.titreId || 'idTitre');
    const numeroCol = this.quote(cols.numeroPerc || 'NumeroPerc');
    const isNumericTitre = /^\d+$/.test(String(titreId));
    const titreLiteral = isNumericTitre ? String(titreId) : this.access.escapeValue(String(titreId));
    const selectSql = `SELECT ${idCol} AS rec_id, ${numeroCol} AS rec_numero FROM ${table} WHERE ${titreCol} = ${titreLiteral}`;
    let rows: any[] = [];
    try {
      rows = await this.access.query(selectSql);
    } catch (err) {
      try { this.logger.warn(`[updateDroitsEtablForOption] select failed: ${(err as any)?.message || err}`); } catch {}
      return;
    }
    const replacement = newCode || '';
    const upperOld = (oldCode || '').toUpperCase();
    for (const row of rows) {
      const numeroRaw = row.rec_numero ?? '';
      const numeroStr = String(numeroRaw || '');
      if (!numeroStr || !numeroStr.toUpperCase().includes(upperOld)) continue;
      let recordYear = this.extractYear(numeroStr);
      if (optionYear != null && recordYear != null && recordYear < optionYear) continue;
      const replaced = numeroStr.replace(new RegExp(oldCode, 'gi'), replacement);
      if (replaced === numeroStr) continue;
      const recordId = row.rec_id;
      const recordIdLiteral = typeof recordId === 'number' ? String(recordId) : this.access.escapeValue(String(recordId));
      const updateSql = `UPDATE ${table} SET ${numeroCol} = ${this.access.escapeValue(replaced)} WHERE ${idCol} = ${recordIdLiteral}`;
      try {
        await this.access.query(updateSql);
      } catch (err) {
        try { this.logger.warn(`[updateDroitsEtablForOption] update failed: ${(err as any)?.message || err}`); } catch {}
      }
    }
  }

  async setSignedFlag(id: string, value: boolean) {
    await this.ensureSignedColumn();
    const t: any = (DEFAULT_TABLES as any).permis;
    const c: any = (DEFAULT_COLUMNS as any).permis;
    const isNumericId = /^\d+$/.test(String(id));
    const lit = value ? 1 : 0;
    const signedCol = c.signed || 'is_signed';
    const sql = `UPDATE ${t} SET ${signedCol} = ${lit} WHERE ${c.id} = ${isNumericId ? id : this.access.escapeValue(String(id))}`;
    try {
      await this.access.query(sql);
    } catch (err) {
      const message = (err as any)?.message ? String((err as any).message) : '';
      if (message && new RegExp(signedCol, 'i').test(message)) {
        await this.ensureSignedColumn();
        await this.access.query(sql);
      } else {
        throw err;
      }
    }
    return { ok: true, is_signed: !!value };
  }

  async setCollectionInfo(id: string, payload: any) {
    const t: any = (DEFAULT_TABLES as any).permis;
    const c: any = (DEFAULT_COLUMNS as any).permis;
    const dateCol = c.takenDate || 'date_remise_titre';
    const nameCol = c.takenBy || 'nom_remise_titre';
    const isNumericId = /^\d+$/.test(String(id));
    // Ensure columns exist; if table is locked, proceed best-effort with existing columns only
    try {
      await this.ensureCollectionColumns();
    } catch (e) {
      try { this.logger.warn(`[setCollectionInfo] ensureCollectionColumns skipped due to: ${(e as any)?.message || e}`); } catch {}
    }

    const takenDateRaw = payload?.takenDate ?? payload?.date ?? payload?.dateTaken ?? payload?.taken_date ?? null;
    const takenByRaw = payload?.takenBy ?? payload?.name ?? payload?.taken_by ?? payload?.recipient ?? null;

    const assignments: string[] = [];
    const dateLiteral = this.formatAccessDateLiteral(takenDateRaw);
    const nameVal = String(takenByRaw ?? '').trim();

    // Only include columns that actually exist to avoid "Too few parameters" when table is locked
    const dateColExists = await this.columnExists(t, dateCol).catch(() => false);
    const nameColExists = await this.columnExists(t, nameCol).catch(() => false);
    if (dateColExists) assignments.push(dateLiteral ? `${dateCol} = ${dateLiteral}` : `${dateCol} = NULL`);
    if (nameColExists) assignments.push(nameVal ? `${nameCol} = ${this.access.escapeValue(nameVal)}` : `${nameCol} = NULL`);

    const whereId = isNumericId ? id : this.access.escapeValue(String(id));
    if (assignments.length) {
      const sql = `UPDATE ${t} SET ${assignments.join(', ')} WHERE ${c.id} = ${whereId}`;
      await this.access.query(sql).catch((err) => {
        const msg = (err as any)?.message ? String((err as any).message) : '';
        try { this.logger.warn(`[setCollectionInfo] update failed: ${msg}`); } catch {}
        throw err;
      });
    }
    const isoMatch = dateLiteral?.match(/#(\d{4})-(\d{2})-(\d{2})#/);
    const isoDate = isoMatch ? `${isoMatch[1]}-${isoMatch[2]}-${isoMatch[3]}` : null;

    return {
      ok: assignments.length > 0,
      message: assignments.length ? undefined : 'Columns unavailable (table locked); please close Access and retry',
      takenDate: isoDate,
      takenBy: nameVal
    };
  }

  async optPermisType(id: string, optionDate?: any) {
    const t = DEFAULT_TABLES.permis;
    const c = DEFAULT_COLUMNS.permis;
    const isNumericId = /^\d+$/.test(String(id));
    const whereId = isNumericId ? String(id) : String(this.access.escapeValue(String(id)));
    const rows = await this.access.query(`SELECT * FROM ${t} WHERE ${c.id} = ${whereId}`);
    if (!rows.length) {
      throw new Error('Permis introuvable');
    }
    const permit = rows[0] as any;
    const currentTypeId = permit[c.typePermis];
    const currentType = await this.getTypeById(currentTypeId);
    if (!currentType || !currentType.code) {
      throw new Error('Type actuel introuvable');
    }
    const sourceCode = String(currentType.code).trim().toUpperCase();
    const targetCode = TYPE_OPTION_MAP[sourceCode];
    if (!targetCode) {
      throw new Error(`Aucun type cible configuré pour ${sourceCode}`);
    }
    const targetType = await this.getTypeByCode(targetCode);
    if (!targetType || targetType.id == null) {
      throw new Error(`Type cible ${targetCode} introuvable`);
    }

    const typeLiteral: string = /^\\d+$/.test(String(targetType.id)) ? String(targetType.id) : String(this.access.escapeValue(String(targetType.id)));
    await this.access.query(`UPDATE ${t} SET ${c.typePermis} = ${typeLiteral} WHERE ${c.id} = ${whereId}`);

    const proceduresTable = DEFAULT_TABLES.procedures;
    const optionLiteral = this.formatAccessDateLiteral(optionDate) || this.formatAccessDateLiteral(new Date());
    const optionYear = this.extractYear(optionDate) ?? new Date().getFullYear();

    if (proceduresTable) {
      const pc: any = DEFAULT_COLUMNS.procedures;
      const titreCol = this.quote(pc.titreId || 'idTitre');
      const typeCol = this.quote(pc.typeId || 'idTypeTitre');
      const labelCol = this.quote(pc.label || 'Procedure');
      const dateColName = pc.dateOption || 'date_option';
      await this.ensureColumnExists(proceduresTable, dateColName, [
        `ALTER TABLE ${proceduresTable} ADD COLUMN ${dateColName} DATETIME`,
        `ALTER TABLE ${proceduresTable} ADD COLUMN ${dateColName} DATE`,
        `ALTER TABLE ${proceduresTable} ADD COLUMN ${dateColName} TEXT(50)`
      ]);
      const dateCol = this.quote(dateColName);
      const label = `Opté en titre (${targetCode})`;
      const columns: string[] = [titreCol, typeCol, labelCol, dateCol];
      const values: string[] = [String(whereId), String(typeLiteral), String(this.access.escapeValue(label)), optionLiteral ?? 'NULL'];
      const insertSql = `INSERT INTO ${proceduresTable} (${columns.join(', ')}) VALUES (${values.join(', ')})`;
      try {
        await this.access.query(insertSql);
      } catch (err) {
        try { this.logger.warn(`[optPermisType] insertion procédure échouée: ${(err as any)?.message || err}`); } catch {}
      }
    }
    // Fallback: ensure we still create a Procedures row using adaptive schema detection
    if (proceduresTable) {
      try {
        // Skip if an Opt row already exists for this titre (best-effort)
        let shouldInsert = true;
        try {
          const pc: any = DEFAULT_COLUMNS.procedures;
          const titreColPick = await this.pickExistingColumn(proceduresTable, [pc.titreId, 'idTitre', 'IdTitre', 'titre_id', 'TitreId']);
          const labelColPick = await this.pickExistingColumn(proceduresTable, [pc.label, 'Procedure', 'Nom', 'Libelle', 'Label']);
          const dateColName = pc.dateOption || 'date_option';
          const dateCol = this.quote(dateColName);
          if (titreColPick && labelColPick) {
            const where = `${titreColPick.quoted} = ${whereId} AND ${labelColPick.quoted} LIKE 'Opt%'` + (optionLiteral ? ` AND ${dateCol} = ${optionLiteral}` : '');
            const chkSql = `SELECT TOP 1 * FROM ${proceduresTable} WHERE ${where}`;
            const chk = await this.access.query(chkSql).catch(() => []);
            if (chk && chk.length) shouldInsert = false;
          }
        } catch {}

        if (shouldInsert) {
          await this.logOptionProcedureRow(proceduresTable, String(whereId), targetCode, String(typeLiteral), optionLiteral);
        }
      } catch (e) {
        try { this.logger.warn(`[optPermisType] fallback insert warning: ${(e as any)?.message || e}`); } catch {}
      }
    }

    try {
      await this.updateTaxesSupForOption(String(id), sourceCode, targetType.code || targetCode, targetType.nom || targetType.code || targetCode, optionYear);
    } catch (err) {
      try { this.logger.warn(`[optPermisType] update TaxesSup warning: ${(err as any)?.message || err}`); } catch {}
    }
    try {
      await this.updateDroitsEtablForOption(String(id), sourceCode, targetType.code || targetCode, optionYear);
    } catch (err) {
      try { this.logger.warn(`[optPermisType] update DroitsEtabl warning: ${(err as any)?.message || err}`); } catch {}
    }

    return {
      ok: true,
      optionDate: optionLiteral ? optionLiteral.replace(/#/g, '') : null,
      oldType: { id: currentType.id, code: sourceCode, nom: currentType.nom },
      newType: { id: targetType.id, code: targetType.code, nom: targetType.nom }
    };
  }

  private loadWilayaCodeMap(): Record<string, string> {
    try {
      const csvPath = path.resolve(process.cwd(), 'tm-app/df_wilaya.csv');
      const content = fs.readFileSync(csvPath, 'utf8');
      const lines = content.split(/\r?\n/).filter(Boolean);
      const map: Record<string, string> = {};
      for (let i = 1; i < lines.length; i++) {
        const parts = lines[i].split(';');
        if (parts.length < 3) continue;
        const idWil = String(parts[0] || '').trim();
        const codeWil = String(parts[2] || '').trim();
        if (idWil) map[idWil] = codeWil;
      }
      return map;
    } catch { return {}; }
  }

  private generateUniqueQr(codePermis: string, typeCode: string, dateDemandeRaw: any, codeWilaya: string, nomSociete: string) {
    const date_systeme = new Date();
    const pad = (n: number) => String(n).padStart(2, '0');
    const date_heure_systeme = `${date_systeme.getFullYear()}-${pad(date_systeme.getMonth()+1)}-${pad(date_systeme.getDate())}T${pad(date_systeme.getHours())}:${pad(date_systeme.getMinutes())}:${pad(date_systeme.getSeconds())}`;
    const horodatage_hash = date_heure_systeme.replace(/[-:TZ.]/g, '');
    const date_demande = String(dateDemandeRaw || '').replace(/[^0-9]/g, '');
    const combined = `${codePermis}${typeCode}${date_demande}${codeWilaya}${nomSociete}${horodatage_hash}`;
    const components = { codePermis, typeCode, date_demande, codeWilaya, nomSociete, horodatage_hash };
    this.logger.log(`[QR GENERATE] components => ${JSON.stringify(components)}`);
    this.logger.log(`[QR GENERATE] raw combined string => ${combined}`);
    const hash = crypto.createHash('sha256').update(combined).digest('hex').toUpperCase();
    const base = hash.substring(0, 20);
    const code_unique = (base.match(/.{1,5}/g) || [base]).join('-');
    this.logger.log(`[QR GENERATE] hash result => ${JSON.stringify({ hash, code_unique, date_heure_systeme })}`);
    return {
      code_unique,
      date_heure_systeme,
      debug: {
        ...components,
        combined,
        hash,
        code_unique,
        date_heure_systeme
      }
    };
  }

  async generateAndSaveQrCode(id: string, insertedBy?: string) {
    await this.ensureQrColumns();
    const t: any = (DEFAULT_TABLES as any).permis;
    const c: any = (DEFAULT_COLUMNS as any).permis;
    const isNumericId = /^\d+$/.test(String(id));
    const sql = `SELECT TOP 1 * FROM ${t} WHERE ${c.id} = ${isNumericId ? id : this.access.escapeValue(String(id))}`;
    const rows = await this.access.query(sql);
    if (!rows.length) return { ok: false, message: 'Permis not found' };
    const r: any = rows[0];
    const typeId = r[c.typePermis];
    const type = await this.getTypeById(typeId).catch(() => null);
    const detId = r[c.detenteur];
    const det = await this.getDetenteurById(detId).catch(() => null);
    const wilayaId = r.idWilaya || r.id_wilaya || r.idwilaya || r.Wilaya || r.wilaya;
    const map = this.loadWilayaCodeMap();
    const codeWilaya = (map[String(wilayaId)] || String(wilayaId || '')).toString().padStart(2, '0');
    const codePermis = String(r[c.codeDemande] || '').padStart(5, '0');
    const typeCode = String((type as any)?.code || (type as any)?.Code || '').trim();
    const nomSociete = String((det as any)?.Nom || (det as any)?.nom || '').trim();
    const requestSnapshot = {
      id: String(id),
      codePermis,
      typeCode,
      typeName: (type as any)?.nom || (type as any)?.Nom || undefined,
      detenteurName: (det as any)?.Nom || (det as any)?.nom || undefined,
      wilayaId,
      codeWilaya,
      localisation: (r as any)[c.localisation],
      superficie: (r as any)[c.superficie],
      dateCreation: (r as any)[c.dateCreation],
      insertedBy: insertedBy || ''
    };
    this.logger.log(`[QR GENERATE] request payload => ${JSON.stringify(requestSnapshot)}`);

    const { code_unique, date_heure_systeme, debug } = this.generateUniqueQr(codePermis, typeCode, r[c.dateCreation], codeWilaya, nomSociete);

    const combinedData = {
      ...requestSnapshot,
      dateHeureSysteme: date_heure_systeme,
      qrCode: code_unique,
    };
    this.logger.log(`[QR GENERATE] combined data => ${JSON.stringify(combinedData)}`);

    const up = `UPDATE ${t} SET DateHeureSysteme = ${this.access.escapeValue(date_heure_systeme)}, QrCode = ${this.access.escapeValue(code_unique)}, code_wilaya = ${this.access.escapeValue(codeWilaya)}, Qrinsererpar = ${this.access.escapeValue(String(insertedBy || ''))} WHERE ${c.id} = ${isNumericId ? id : this.access.escapeValue(String(id))}`;
    await this.access.query(up).catch(() => {});
    this.logger.log(`[QR GENERATE] persisted => ${JSON.stringify({ id: String(id), code_unique, date_heure_systeme, codeWilaya, insertedBy: insertedBy || '' })}`);
    return { ok: true, QrCode: code_unique, DateHeureSysteme: date_heure_systeme, code_wilaya: codeWilaya, insertedBy: insertedBy || '', request: combinedData, debug };
  }

  async verifyByQrCode(code: string) {
    const t = (DEFAULT_TABLES as any).permis;
    const c: any = (DEFAULT_COLUMNS as any).permis;
    const lookup = String(code ?? '').trim();
    if (!lookup) return { exists: false };
    const literal = `'${lookup.replace(/'/g, "''")}'`;
    const sql = `SELECT TOP 1 * FROM [${t}] WHERE [QrCode] = ${literal}`;
    let rows: any[] = [];
    try {
      try { console.log('[verifyByQrCode] mode=', (this.access as any)?.isOdbcMode?.() ? 'odbc' : 'adodb', 'code=', lookup); } catch {}
      rows = await this.access.query(sql);
    } catch (e) {
      try { console.error('[verifyByQrCode] query failed, sql=', sql, (e as any)?.message || e); } catch {}
      return { exists: false };
    }
    if (!rows || rows.length === 0) return { exists: false };
    const r: any = rows[0];
    // Minimal normalized info, similar to getPermisById
    const typeId = r[c.typePermis];
    const type = await this.getTypeById(typeId).catch(() => null);
    const detId = r[c.detenteur];
    const det = await this.getDetenteurById(detId).catch(() => null);
    return {
      exists: true,
      permis: {
        id: r[c.id],
        codeDemande: r[c.codeDemande],
        typePermis: { code: (type as any)?.code || (type as any)?.Code, nom: (type as any)?.nom || (type as any)?.Nom },
        detenteur: { nom: (det as any)?.Nom || (det as any)?.nom },
        localisation: r[c.localisation],
        superficie: r[c.superficie],
        QrCode: r.QrCode,
        Qrinsererpar: r.Qrinsererpar,
        DateHeureSysteme: r.DateHeureSysteme,
      }
    };
  }
}












