import { Injectable } from '@nestjs/common';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { AccessService } from './access.service';

// Adjust these defaults to match your Access schema
const DEFAULT_TABLES = {
  permis: process.env.ACCESS_TABLE_PERMIS || 'Titres',
  coordinates: process.env.ACCESS_TABLE_COORDINATES || 'coordonees',
  types: process.env.ACCESS_TABLE_TYPES || 'TypesTitres',
  detenteur: process.env.ACCESS_TABLE_DETENTEUR || 'Detenteur'
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
    dateFin: process.env.ACCESS_COL_DATE_FIN || 'DateExpiration'
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
  }
};

@Injectable()
export class PermisService {
  constructor(private readonly access: AccessService) {}

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
    const fmtFr = (d: Date | null) => d ? `${String(d.getDate()).padStart(2,'0')}/${String(d.getMonth()+1).padStart(2,'0')}/${d.getFullYear()}` : '';
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
      const words: Record<number, string> = { 1: '���', 2: '�����', 3: '����', 4: '����', 5: '���', 6: '��', 7: '���', 8: '����', 9: '���', 10: '���' };
      const word = words[y] ? (y <= 2 ? words[y] : `${words[y]} (${y2}) �����`) : `(${y2}) �����`;
      // Compose full text with tatweel in ���/��� as per example
      // Example: (�������: ����� (04) ����� (���� 18/12/2025 ����� 18/12/2029))
      if (y <= 2) {
        // normalize singular/dual
        const noun = y === 1 ? '���' : '�����';
        dureeDisplayAr = `${word} (���� ${fmtFr(dDebut)} ����� ${fmtFr(dFin)})`;
      } else {
        dureeDisplayAr = `${word} (���� ${fmtFr(dDebut)} ����� ${fmtFr(dFin)})`;
      }
    }

    // Normalize detenteur payload to include Arabic/Latin names if available
    const detNorm = detData ? {
      id: detData.id ?? detData.Id ?? detData.ID ?? toStr(r[c.detenteur]),
      Nom: detData.Nom ?? detData.nom ?? detData.RaisonSociale ?? detData.raison_sociale ?? detData['Raison Sociale'] ?? detData.raisonSociale ?? detData.denomination ?? detData.nom_societe ?? detData.nom_societeFR ?? '',
      NomArab: detData.NomArab ?? detData.NomAR ?? detData.nomAR ?? detData.nom_ar ?? detData.nom_societeAR ?? '',
      nom_societeFR: detData.nom_societeFR ?? detData.Nom ?? detData.nom ?? detData.RaisonSociale ?? '',
      nom_societe: detData.nom_societe ?? detData.Nom ?? detData.nom ?? '',
      nom_ar: detData.nom_ar ?? detData.NomArab ?? detData.NomAR ?? '',
      raison_sociale: detData.raison_sociale ?? detData.RaisonSociale ?? detData['Raison Sociale'] ?? '',
      // Include legal status verbatim when present
      StatutJuridique: detData.StatutJuridique ?? detData.statutJuridique ?? detData.statut ?? ''
    } : null;

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
      detenteur_ar: (detNorm && (detNorm.NomArab || detNorm.nom_ar)) || detData?.NomArab || detData?.nom_ar || ''
    };
    // Add compatibility fields expected by designer
    val.code_demande = val.codeDemande;
    val.id_demande = val.id;
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

  private async getDetenteurById(detId: any) {
    if (detId == null || detId === '') return null;
    const t = DEFAULT_TABLES.detenteur;
    const isNumeric = /^\d+$/.test(String(detId));
    const sql = `SELECT * FROM ${t} WHERE id = ${isNumeric ? detId : this.access.escapeValue(String(detId))}`;
    const rows = await this.access.query(sql);
    if (!rows.length) return null;
    return rows[0] as any;
  }

  private async ensureQrColumns() {
    const t = (DEFAULT_TABLES as any).permis;
    const tryAlter = async (sql: string) => { try { await this.access.query(sql); } catch {} };
    await tryAlter(`ALTER TABLE ${t} ADD COLUMN DateHeureSysteme TEXT(50)`);
    await tryAlter(`ALTER TABLE ${t} ADD COLUMN QrCode TEXT(50)`);
    await tryAlter(`ALTER TABLE ${t} ADD COLUMN code_wilaya TEXT(5)`);
    await tryAlter(`ALTER TABLE ${t} ADD COLUMN Qrinsererpar TEXT(100)`);
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
    try {
      // Log the exact combined string used to generate the QR code hash
      // (codePermis + typeCode + date_demande + codeWilaya + nomSociete + horodatage_hash)
      // eslint-disable-next-line no-console
      console.log('[QR GENERATE] raw combined string:', combined);
    } catch {}
    const hash = crypto.createHash('sha256').update(combined).digest('hex').toUpperCase();
    const base = hash.substring(0, 20);
    const code_unique = (base.match(/.{1,5}/g) || [base]).join('-');
    return { code_unique, date_heure_systeme };
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
    const { code_unique, date_heure_systeme } = this.generateUniqueQr(codePermis, typeCode, r[c.dateCreation], codeWilaya, nomSociete);

    // Log all relevant attributes to the backend console for traceability
    try {
      const combinedData = {
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
        dateHeureSysteme: date_heure_systeme,
        insertedBy: insertedBy || '',
        qrCode: code_unique,
      };
      // eslint-disable-next-line no-console
      console.log('[QR GENERATE] combined data =>', combinedData);
    } catch {}
    const up = `UPDATE ${t} SET DateHeureSysteme = ${this.access.escapeValue(date_heure_systeme)}, QrCode = ${this.access.escapeValue(code_unique)}, code_wilaya = ${this.access.escapeValue(codeWilaya)}, Qrinsererpar = ${this.access.escapeValue(String(insertedBy || ''))} WHERE ${c.id} = ${isNumericId ? id : this.access.escapeValue(String(id))}`;
    await this.access.query(up).catch(() => {});
    return { ok: true, QrCode: code_unique, DateHeureSysteme: date_heure_systeme, code_wilaya: codeWilaya, insertedBy: insertedBy || '' };
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
