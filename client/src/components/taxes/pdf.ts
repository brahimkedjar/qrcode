import jsPDF from 'jspdf';

export type TaxeSupRow = {
  id: number;
  idTitre: number;
  NumeroPerc?: string;
  PAR?: string;
  Date?: string;
  Surface?: string | number;
  Taxe?: string | number;
  Paye?: boolean;
  Comment?: string;
  DatePerDebut?: string;
  datePerFin?: string;
  TS_SurfaceMin?: string | number;
  TS_SurfaceMax?: string | number;
  TS_DroitFixe?: string | number;
  TS_PerInit?: string | number;
  TS_PremierRen?: string | number;
  TS_DeuRen?: string | number;
  dateremiseop?: string;
  datepaiement?: string;
  num_quittance?: string;
};

export type DeaRow = {
  id: number;
  idTitre: number;
  idTypeProcedure?: number;
  idProcedure?: number;
  NumeroPerc?: string;
  date?: string;
  droit?: string | number;
  paye?: boolean;
  DUN?: string;
  PARLA?: string;
  Commentaire?: string;
  dateremiseop?: string;
  datepaiement?: string;
  num_quittance?: string;
};

const header = (doc: jsPDF, title: string) => {
  doc.setDrawColor(20);
  doc.setFillColor(248, 250, 252);
  doc.roundedRect(20, 18, 555, 40, 4, 4, 'F');
  doc.setFont('helvetica', 'bold');
  doc.setFontSize(16);
  doc.text(title, 30, 44);
};

const footer = (doc: jsPDF) => {
  const w = doc.internal.pageSize.getWidth();
  const h = doc.internal.pageSize.getHeight();
  doc.setDrawColor(220);
  doc.line(20, h - 30, w - 20, h - 30);
  doc.setFontSize(9);
  doc.setTextColor(120);
  doc.text(`Généré le ${new Date().toLocaleString()}`, 22, h - 18);
};

export function generateTaxeSupPdf(row: TaxeSupRow): Blob {
  const doc = new jsPDF('p', 'pt', 'a4');
  header(doc, 'Taxe Superficiaire');
  doc.setFont('helvetica', 'normal');
  doc.setFontSize(12);

  let y = 90;
  const add = (label: string, value?: any) => {
    doc.setTextColor(90);
    doc.text(label, 30, y);
    doc.setTextColor(20);
    const v = value == null ? '' : String(value);
    doc.text(v, 220, y);
    y += 22;
  };

  add('ID Titre', row.idTitre);
  add('Numéro perception', row.NumeroPerc);
  add('PAR', row.PAR);
  add('Période', [row.DatePerDebut, row.datePerFin].filter(Boolean).join(' → '));
  add('Date enregistrement', row.Date);
  add('Surface (ha)', row.Surface);
  add('Taxe', row.Taxe);
  add('Payé', row.Paye ? 'Oui' : 'Non');
  add('Quittance', row.num_quittance);
  add('Date paiement', row.datepaiement);
  add('Remise OP', row.dateremiseop);
  add('Tranches (ha)', [row.TS_SurfaceMin, row.TS_SurfaceMax].filter(Boolean).join(' – '));
  add('Droit fixe', row.TS_DroitFixe);
  add('Périodes', [row.TS_PerInit, row.TS_PremierRen, row.TS_DeuRen].filter(Boolean).join(' / '));
  if (row.Comment) add('Commentaire', row.Comment);

  footer(doc);
  return doc.output('blob');
}

export function generateDeaPdf(row: DeaRow): Blob {
  const doc = new jsPDF('p', 'pt', 'a4');
  header(doc, "Droit d'établissement d'acte (DEA)");
  doc.setFont('helvetica', 'normal');
  doc.setFontSize(12);

  let y = 90;
  const add = (label: string, value?: any) => {
    doc.setTextColor(90);
    doc.text(label, 30, y);
    doc.setTextColor(20);
    const v = value == null ? '' : String(value);
    doc.text(v, 220, y);
    y += 22;
  };

  add('ID Titre', row.idTitre);
  add('Numéro perception', row.NumeroPerc);
  add('Date', row.date);
  add('Droit', row.droit);
  add('Payé', row.paye ? 'Oui' : 'Non');
  add('Quittance', row.num_quittance);
  add('Date paiement', row.datepaiement);
  add('Remise OP', row.dateremiseop);
  add('PAR/LA', row.PARLA);
  add('DUN', row.DUN);
  if (row.Commentaire) add('Commentaire', row.Commentaire);

  footer(doc);
  return doc.output('blob');
}

