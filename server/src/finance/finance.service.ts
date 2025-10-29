import { Injectable } from '@nestjs/common';
import { AccessService } from '../permis/access.service';

@Injectable()
export class FinanceService {
  constructor(private readonly access: AccessService) {}

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
}
