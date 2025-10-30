// services/pdfGenerator.ts
import jsPDF from 'jspdf';

interface OrderData {
  companyName: string;
  permitType: string;
  permitCode: string;
  location: string;
  amount: number;
  orderNumber: string;
  date: Date;
  taxReceiver: string;
  taxReceiverAddress: string;
  period?: string; 
  signatureName?: string;
  detenteurId?: number; 
  obligationId?: number; 
  president?: string;
}

// Helper function to generate unique order number
export const generateUniqueOrderNumber = (
  type: 'DEA' | 'TS' | 'PRODUIT_ATTRIBUTION',
  detenteurId: number,
  obligationId: number,
  permisId: number,
  year: number = new Date().getFullYear()
): string => {
  // Create a hash from detenteurId and obligationId for uniqueness
  const hash = Math.abs((detenteurId * 31 + obligationId * 17) % 10000)
    .toString()
    .padStart(4, '0');
  
  const typeCode = type === 'DEA' ? 'DEA' : type === 'TS' ? 'TS' : 'PA';
  
  return `${typeCode}-${permisId}-${hash}-${year}`;
};

// Helper function to format date as in the examples
const formatDate = (date: Date): string => {
  const day = date.getDate();
  const month = date.toLocaleString('fr-FR', { month: 'short' }).toUpperCase();
  const year = date.getFullYear();
  return `${day} ${month}. ${year}`;
};

// Helper function to split text into lines that fit within specified width
const splitTextIntoLines = (doc: jsPDF, text: string, maxWidth: number): string[] => {
  const lines = doc.splitTextToSize(text, maxWidth);
  return lines;
};

// Render a paragraph to an offscreen canvas using browser font shaping (Arabic-friendly)
// and embed it as an image in the PDF to keep wrapping inside the width.
const PX_PER_MM = 96 / 25.4; // 96dpi
const PX_PER_PT = 96 / 72;
function addParagraphImage(
  doc: jsPDF,
  text: string,
  xMm: number,
  yMm: number,
  maxWidthMm: number,
  opts?: { fontPt?: number; lineHeight?: number; color?: string; fontFamily?: string }
): number {
  const fontPt = Math.max(8, Math.floor((opts?.fontPt ?? 10)));
  const fontPx = Math.max(10, Math.round(fontPt * PX_PER_PT));
  const lineH = (opts?.lineHeight ?? 1.35) * fontPx;
  const color = opts?.color ?? '#000';
  const fam = opts?.fontFamily ?? 'Tahoma, Arial, "Segoe UI", sans-serif';
  const maxWidthPx = Math.max(40, Math.floor(maxWidthMm * PX_PER_MM));
  // Create context
  const cnv = document.createElement('canvas');
  const ctx = cnv.getContext('2d');
  if (!ctx) {
    const fallback = doc.splitTextToSize(text, maxWidthMm);
    fallback.forEach((ln: string | string[]) => doc.text(ln, xMm, yMm));
    return (fallback.length * (fontPt * 1.35)) * (25.4 / 72); // approx mm
  }
  ctx.font = `${fontPx}px ${fam}`;
  ctx.fillStyle = color;
  ctx.textBaseline = 'top';
  // Basic wrap preserving explicit newlines
  const inputLines = String(text || '').split(/\r?\n/);
  const measure = (s: string) => ctx.measureText(s).width;
  const outLines: string[] = [];
  for (const raw of inputLines) {
    const words = raw.split(/\s+/);
    let current = '';
    for (const w of words) {
      const test = current ? current + ' ' + w : w;
      if (measure(test) <= maxWidthPx) {
        current = test;
      } else {
        if (current) outLines.push(current);
        // if a single word is too long, hard-break
        if (measure(w) > maxWidthPx) {
          let piece = '';
          for (const ch of w) {
            const t2 = piece + ch;
            if (measure(t2) > maxWidthPx && piece) {
              outLines.push(piece);
              piece = ch;
            } else {
              piece = t2;
            }
          }
          if (piece) outLines.push(piece);
          current = '';
        } else {
          current = w;
        }
      }
    }
    if (current) outLines.push(current);
  }
  const heightPx = Math.ceil(outLines.length * lineH + fontPx * 0.2);
  cnv.width = maxWidthPx;
  cnv.height = Math.max(1, heightPx);
  const ctx2 = cnv.getContext('2d')!;
  ctx2.font = `${fontPx}px ${fam}`;
  ctx2.fillStyle = color;
  ctx2.textBaseline = 'top';
  let y = 0;
  for (const ln of outLines) {
    ctx2.fillText(ln, 0, y);
    y += lineH;
  }
  const dataUrl = cnv.toDataURL('image/png');
  const heightMm = cnv.height / PX_PER_MM;
  doc.addImage(dataUrl, 'PNG', xMm, yMm, maxWidthMm, heightMm);
  return heightMm;
}

// Custom function to format amount with dot as thousand separator and comma as decimal separator
const formatAmount = (amount: number): string => {
  // Convert to string and split integer and decimal parts
  const amountStr = amount.toFixed(2);
  const parts = amountStr.split('.');
  const integerPart = parts[0];
  const decimalPart = parts[1] || '00';
  
  // Format integer part with dots as thousand separators
  const formattedInteger = integerPart.replace(/\B(?=(\d{3})+(?!\d))/g, '.');
  
  return `${formattedInteger},${decimalPart}`;
};

export const generatePDFForPreview = async (type: 'DEA' | 'TS' | 'PRODUIT_ATTRIBUTION', data: OrderData): Promise<string> => {
  const doc = new jsPDF();
  const pageWidth = doc.internal.pageSize.getWidth();
  const margin = 20;
  const contentWidth = pageWidth - (margin * 2);
  let yPosition = margin;
  
  // Set font styles
  const setNormalFont = () => {
    doc.setFont("helvetica", "normal");
    doc.setFontSize(10);
  };
  
  const setBoldFont = () => {
    doc.setFont("helvetica", "bold");
    doc.setFontSize(10);
  };
  
  const setTitleFont = () => {
    doc.setFont("helvetica", "bold");
    doc.setFontSize(14);
  };
  
  // Format the amount using our custom function
  const formattedAmount = formatAmount(data.amount);
  
  if (type === 'DEA') {
    // DEA Order content
    setTitleFont();
    doc.text("REPUBLIQUE ALGERIENNE DEMOCRATIQUE ET POPULAIRE", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 7;
    doc.text("MINISTERE DE l'ENERGIE, DES MINES ET DES ENERGIES RENOUVELABLES", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 10;
    
    doc.setFontSize(12);
    doc.text("Agence Nationale des Activités Minières", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 7;
    doc.text("Siège central", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 15;
    
    // Legal references
    setNormalFont();
    const legalReferences = [
      "Vu la loi n°14-05 du 24 Rabie Ethani 1435 correspondant au 24 Février 2014 portant loi minière, notamment ses articles 131, 132, 133 et 192 ;",
      "Vu la loi n°15-18 du 30 décembre 2015 portant loi de finances pour 2016, notamment son article 53 ;",
      "Vu la loi n°16-14 du 28 décembre 2016 portant loi de finances pour 2017, notamment son article 132 ;",
      "Vu l'instruction n°02 du 16 janvier 2020 et n°03 du 19 janvier 2020 relatives à la clôture du compte d'affectation spéciale n°302-105 ; « Fonds du Patrimoine Public minier » ;"
    ];
    
    legalReferences.forEach(ref => {
      const lines = splitTextIntoLines(doc, ref, contentWidth);
      lines.forEach(line => {
        doc.text(line, margin, yPosition);
        yPosition += 5;
      });
      yPosition += 2;
    });
    
    yPosition += 5;
    
    // Order number
    setBoldFont();
    doc.text(`Ordre de perception n° :`, margin, yPosition);
    doc.text(data.orderNumber, margin + 50, yPosition);
    yPosition += 10;
    
    // Main content - using formattedAmount instead of data.amount.toLocaleString()
    setNormalFont();
    const mainText = `Un ordre de perception est émis par l'Agence nationale des Activités Minières (siège central) d'un montant de ${formattedAmount} DA au profit du ${data.taxReceiver} sise au 17 rue Arezki Hammani, 3ème étage –Alger au titre de paiement des droits d'établissement d'acte d'un ${data.permitType} ${data.permitCode} par la ${data.companyName} à ${data.location}.`;
    
    const usedMm = addParagraphImage(doc, mainText, margin, yPosition, Math.max(10, contentWidth - 2), { fontPt: 10, lineHeight: 1.4 });
    yPosition += usedMm;
    
    yPosition += 5;
    
    const secondText = `Le montant de l'ordre de perception mentionné ci-dessus est inscrit par le receveur des impôts au crédit du compte « Produits divers du budget n°201-007 ».`;
    const secondLines = splitTextIntoLines(doc, secondText, contentWidth);
    secondLines.forEach(line => {
      doc.text(line, margin, yPosition);
      yPosition += 5;
    });
    
    yPosition += 15;
    
    // Date and signature
    doc.text(`Fait à Alger, le ${formatDate(data.date)}`, margin, yPosition);
    yPosition += 10;
    doc.text(data.president ||"P/Le Président du Comité de Direction", margin, yPosition);
    yPosition += 7;
    doc.text(data.signatureName || "Seddik BENABBES", margin, yPosition);
    
  } else if (type === 'TS') {
    // TS Order content
    setTitleFont();
    doc.text("REPUBLIQUE ALGERIENNE DEMOCRATIQUE ET POPULAIRE", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 7;
    doc.text("MINISTERE DE l'ENERGIE, DES MINES ET DES ENERGIES RENOUVELABLES", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 10;
    
    doc.setFontSize(12);
    doc.text("Agence Nationale des Activités Minières", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 7;
    doc.text("Siège central", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 15;
    
    // Legal references
    setNormalFont();
    const legalReferences = [
      "Vu la loi n°14-05 du 24 Rabie Ethani 1435 correspondant au 24 Février 2014 portant loi minière, notamment ses articles 131, 132, 133 et 192 ;",
      "Vu la loi n°15-18 du 30 décembre 2015 portant loi de finances pour 2016, notamment son article 53 ;",
      "Vu la loi n°16-14 du 28 décembre 2016 portant loi de finances pour 2017, notamment son article 132 ;",
      "Vu l'instruction n°02 du 16 janvier 2020 et n°03 du 19 janvier 2020 relatives à la clôture du compte d'affectation spéciale n°302-105 ; « Fonds du Patrimoine Public minier » ;",
      "Vu l'ordonnance n°15-01, correspondant au 23 juillet 2015 portant loi de finance complémentaire pour 2015, notamment son article 70 ;"
    ];
    
    legalReferences.forEach(ref => {
      const lines = splitTextIntoLines(doc, ref, contentWidth);
      lines.forEach(line => {
        doc.text(line, margin, yPosition);
        yPosition += 5;
      });
      yPosition += 2;
    });
    
    yPosition += 5;
    
    // Order number
    setBoldFont();
    doc.text(`Ordre de perception n° :`, margin, yPosition);
    doc.text(data.orderNumber, margin + 50, yPosition);
    yPosition += 10;
    
    // Main content - using formattedAmount instead of data.amount.toLocaleString()
    setNormalFont();
    const mainText = `Un ordre de perception est émis par l'Agence nationale des Activités Minières (siège central) d'un montant de ${formattedAmount} DA au profit du ${data.taxReceiver} sise au 17 rue Arezki Hammani, 3ème étage –Alger au titre de paiement de la taxe superficiaire par ${data.companyName} pour le ${data.permitType} ${data.permitCode} situé à ${data.location} pour la période du ${data.period}.`;
    
    const usedMm = addParagraphImage(doc, mainText, margin, yPosition, Math.max(10, contentWidth - 2), { fontPt: 10, lineHeight: 1.4 });
    yPosition += usedMm;
    
    yPosition += 5;
    
    const secondText = `La quote-part de la taxe superficiaire à verser au compte « Produit divers du budget » est fixée à cinquante pour cent (50%). Les cinquante pour cent (50%) restants sont à verser à la Caisse de Solidarité et de Garantie des Collectivités Locales.`;
    const secondLines = splitTextIntoLines(doc, secondText, contentWidth);
    secondLines.forEach(line => {
      doc.text(line, margin, yPosition);
      yPosition += 5;
    });
    
    yPosition += 15;
    
    // Date and signature
    doc.text(`Fait à Alger, le ${formatDate(data.date)}`, margin, yPosition);
    yPosition += 10;
    doc.text(data.president || "P/Le Président du Comité de Direction", margin, yPosition);
    yPosition += 7;
    doc.text(data.signatureName || "Seddik BENABBES", margin, yPosition);
    
  } else {
    // Produit d'Attribution Order content
    setTitleFont();
    doc.text("REPUBLIQUE ALGERIENNE DEMOCRATIQUE ET POPULAIRE", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 7;
    doc.text("MINISTERE DE l'ENERGIE, DES MINES ET DES ENERGIES RENOUVELABLES", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 10;
    
    doc.setFontSize(12);
    doc.text("Agence Nationale des Activités Minières", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 15;
    
    // Legal references
    setNormalFont();
    const legalReferences = [
      "Vu la loi n°14-05 du 24 Rabie Ethani 1435 correspondant au 24 Février 2014 portant loi minière, notamment ses articles 131, 132, 133 et 192 ;",
      "Vu le décret exécutif n°18-202 du 23 Dhou El Kaada 1439 correspondant au 05 Aout 2018 fixing les modalités et procédures d'attribution des permis miniers notamment son article n°05 qui prévoit que l'octroi du permis minier est assorti du paiement d'un produit d'attribution conformément à la législation, auprès des receveurs des impôts et versé au compte « produit divers du budget »",
      "Vu l'instruction n°02 du 16 Janvier 2020 et n°03 du 19 Janvier 2020 relatives à la clôture du compte d'affectation spéciale n°302-105 ; « Fonds du Patrimoine Public Minier »",
      "Vu la résolution du comité de direction du 27/04/2023, fixant le montant du produit d'attribution."
    ];
    
    legalReferences.forEach(ref => {
      const lines = splitTextIntoLines(doc, ref, contentWidth);
      lines.forEach(line => {
        doc.text(line, margin, yPosition);
        yPosition += 5;
      });
      yPosition += 2;
    });
    
    yPosition += 5;
    
    // Order number
    setBoldFont();
    doc.text(`Ordre de Paiement n° :`, margin, yPosition);
    doc.text(`N° ${data.orderNumber}`, margin + 50, yPosition);
    yPosition += 10;
    
    // Main content - using formattedAmount instead of data.amount.toLocaleString()
    setNormalFont();
    const mainText = `Un ordre de paiement est émis par l'Agence Nationale des Activités Minières (siège central) d'un montant de ${formattedAmount} DA au profit du ${data.taxReceiver} sise au 17 rue Arezki Hammani, 3ème étage –Alger. Au titre de paiement des droits du produit d'attribution du ${data.permitType} ${data.permitCode} attribué à la ${data.companyName} pour le site de ${data.location}.`;
    
    const usedMm = addParagraphImage(doc, mainText, margin, yPosition, Math.max(10, contentWidth - 2), { fontPt: 10, lineHeight: 1.4 });
    yPosition += usedMm;
    
    yPosition += 5;
    
    const secondText = `Le montant de l'ordre de perception mentionné ci-dessus est inscrit par le receveur des impôts au crédit du compte « Produits divers du budget n°201007 ».`;
    const secondLines = splitTextIntoLines(doc, secondText, contentWidth);
    secondLines.forEach(line => {
      doc.text(line, margin, yPosition);
      yPosition += 5;
    });
    
    yPosition += 15;
    
    // Date and signature
    doc.text(`Fait à Alger, le ${formatDate(data.date)}`, margin, yPosition);
    yPosition += 10;
    doc.text("P/ Le Président du Comité de Direction", margin, yPosition);
    yPosition += 7;
    doc.text(data.signatureName || "Seddik BÉNABBES", margin, yPosition);
  }
  
  return doc.output('datauristring');
};
