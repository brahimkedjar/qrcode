// services/pdfGenerator.ts
import jsPDF from 'jspdf';

export interface OrderData {
  companyName: string;
  permitType: string;
  permitCode: string;
  permitDisplay?: string;
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
  place?: string;
  showDate?: boolean;
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
  // fr-FR short month usually includes a trailing dot (ex: "oct.").
  // Remove any trailing dot and do NOT add an extra one to avoid "OCT..".
  const rawMonth = date.toLocaleString('fr-FR', { month: 'short' }).toUpperCase();
  const month = rawMonth.replace(/\.$/, '');
  const year = date.getFullYear();
  return `${day} ${month} ${year}`;
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
  const fontPt = Math.max(8, Math.round(opts?.fontPt ?? 11));
  const fontPx = Math.max(10, Math.round(fontPt * PX_PER_PT));
  const lineH = (opts?.lineHeight ?? 1.37) * fontPx;
  const color = opts?.color ?? '#000';
  const fam = opts?.fontFamily ?? 'Arial, Helvetica, sans-serif';
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

const formatPeriodForDisplay = (period?: string): string => {
  if (!period) return '';
  const pad = (value: string | number) => String(value).padStart(2, '0');
  const swapIso = (input: string) =>
    input.replace(/\b(\d{4})[-\/](\d{1,2})[-\/](\d{1,2})\b/g, (_, y: string, m: string, d: string) => `${pad(d)}-${pad(m)}-${y}`);
  const swapEuropean = (input: string) =>
    input.replace(/\b(\d{1,2})\/(\d{1,2})\/(\d{4})\b/g, (_, d: string, m: string, y: string) => `${pad(d)}-${pad(m)}-${y}`);
  return swapEuropean(swapIso(period));
};

const renderOrderToDoc = (doc: jsPDF, type: 'DEA' | 'TS' | 'PRODUIT_ATTRIBUTION', data: OrderData) => {
  const pageWidth = doc.internal.pageSize.getWidth();
  const margin = 20;
  const contentWidth = pageWidth - (margin * 2);
  let yPosition = margin;
  
  // Set font styles
  const applyFont = (style: 'normal' | 'bold', size: number) => {
    try {
      doc.setFont('Arial', style);
    } catch {
      doc.setFont('helvetica', style);
    }
    doc.setFontSize(size);
  };

  const setNormalFont = () => applyFont('normal', 13);
  const setBoldFont = () => applyFont('bold', 16);
  const setTitleFont = () => setBoldFont();
  const setNormalFont1 = ()  => applyFont('normal', 14);
  const setNormalFont2 = () => applyFont('normal', 17);

  const paragraphFontSize = 13;
  const paragraphLineHeight = 6;

  const renderStyledParagraph = (
    segments: { text: string; style?: 'normal' | 'bold' }[],
    x: number,
    y: number,
    maxWidth: number,
    lineHeight: number = paragraphLineHeight,
    fontSize: number = paragraphFontSize,
    align: 'left' | 'justify' = 'left'
  ): number => {
    if (!segments.length) {
      return 0;
    }

    type StyledToken = {
      text: string;
      style: 'normal' | 'bold';
      isWhitespace: boolean;
      width: number;
    };

    const tokens: { text: string; style: 'normal' | 'bold'; isWhitespace: boolean }[] = [];

    const ensureFont = (style: 'normal' | 'bold' = 'normal', size: number = fontSize) => {
      applyFont(style, size);
    };

    const measureText = (text: string, style: 'normal' | 'bold'): number => {
      ensureFont(style, fontSize);
      return doc.getTextWidth(text);
    };

    const pushToken = (text: string, style: 'normal' | 'bold' = 'normal') => {
      if (!text) {
        return;
      }
      const parts = text.split(/(\r?\n)/);
      parts.forEach(part => {
        if (!part) {
          return;
        }
        if (part === '\n' || part === '\r\n') {
          tokens.push({ text: '\n', style, isWhitespace: false });
          return;
        }
        const subParts = part.split(/(\s+)/);
        subParts.forEach(sub => {
          if (!sub) {
            return;
          }
          tokens.push({
            text: sub,
            style,
            isWhitespace: /^\s+$/.test(sub)
          });
        });
      });
    };

    segments.forEach(segment => {
      pushToken(segment.text ?? '', segment.style ?? 'normal');
    });

    const breakLongToken = (
      tokenText: string,
      style: 'normal' | 'bold'
    ): { text: string; style: 'normal' | 'bold'; isWhitespace: boolean }[] => {
      if (!tokenText) {
        return [];
      }
      const fragments: { text: string; style: 'normal' | 'bold'; isWhitespace: boolean }[] = [];
      let buffer = '';
      for (const char of tokenText) {
        const tentative = buffer + char;
        const width = measureText(tentative, style);
        if (width > maxWidth && buffer) {
          fragments.push({ text: buffer, style, isWhitespace: false });
          buffer = char;
        } else if (width > maxWidth) {
          fragments.push({ text: char, style, isWhitespace: false });
          buffer = '';
        } else {
          buffer = tentative;
        }
      }
      if (buffer) {
        fragments.push({ text: buffer, style, isWhitespace: false });
      }
      return fragments;
    };

    const lines: { tokens: StyledToken[]; width: number; spaces: number }[] = [];
    let currentTokens: StyledToken[] = [];
    let currentWidth = 0;

    const finalizeLine = () => {
      while (currentTokens.length && currentTokens[currentTokens.length - 1].isWhitespace) {
        const last = currentTokens.pop();
        if (last) {
          currentWidth -= last.width;
        }
      }
      if (currentTokens.length) {
        const spaceCount = currentTokens.filter(token => token.isWhitespace).length;
        lines.push({ tokens: currentTokens, width: currentWidth, spaces: spaceCount });
      }
      currentTokens = [];
      currentWidth = 0;
    };

    let index = 0;
    while (index < tokens.length) {
      const originalToken = tokens[index];

      if (originalToken.text === '\n') {
        finalizeLine();
        index += 1;
        continue;
      }

      const style = originalToken.style;
      const isWhitespace = originalToken.isWhitespace;
      const text = originalToken.text;

      if (!isWhitespace) {
        const width = measureText(text, style);
        if (width > maxWidth) {
          const fragments = breakLongToken(text, style);
          if (fragments.length > 1) {
            tokens.splice(index, 1, ...fragments);
            continue;
          }
        }
      }

      const width = measureText(text, style);
      const availableWidth = maxWidth - currentWidth;

      if (width > availableWidth && currentTokens.length) {
        finalizeLine();
        continue;
      }

      if (isWhitespace && !currentTokens.length) {
        index += 1;
        continue;
      }

      currentTokens.push({
        text,
        style,
        isWhitespace,
        width
      });
      currentWidth += width;
      index += 1;
    }

    finalizeLine();

    if (!lines.length) {
      return 0;
    }

    let currentY = y;
    lines.forEach((line, lineIndex) => {
      const isLastLine = lineIndex === lines.length - 1;
      const remainingWidth = maxWidth - line.width;
      const justify =
        align === 'justify' && !isLastLine && line.spaces > 0 && remainingWidth > 0;
      const extraSpace = justify ? remainingWidth / line.spaces : 0;
      let currentX = x;

      line.tokens.forEach(token => {
        ensureFont(token.style, fontSize);
        if (token.isWhitespace) {
          const baseWidth = token.width;
          const offset = justify ? baseWidth + extraSpace : baseWidth;
          currentX += offset;
        } else {
          doc.text(token.text, currentX, currentY);
          currentX += token.width;
        }
      });

      currentY += lineHeight;
    });

    ensureFont('normal', fontSize);
    return lines.length * lineHeight;
  };

  // Format the amount using our custom function
  const formattedAmount = formatAmount(data.amount);
  const permitLabel =
    (
      [data.permitType].filter(Boolean).join(' ').trim()) ||
    '-';
  const periodText = formatPeriodForDisplay(data.period);
  
  if (type === 'DEA') {
    // DEA Order content
    setTitleFont();
    doc.text("REPUBLIQUE ALGERIENNE DEMOCRATIQUE ET POPULAIRE", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 7;
    doc.text("MINISTÈRE DES HYDROCARBURES ET DES MINES ", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 10;
    setBoldFont();
    doc.text("Agence Nationale des Activités Minières", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 15;
    setBoldFont();
    doc.text("Siège central", margin, yPosition);
    yPosition += 10;
    
    // Legal references
    setNormalFont1();
    const legalReferences = [
      "Vu la loi n° 25-12 du 9 Safar 1447 correspondant au 3 août 2025 portant loi régissant les activités minières, notamment son article 213 ;",
      "Vu la loi n°15-18 du 30 décembre 2015 portant loi de finances pour 2016, notamment son article 53;",
      "Vu la loi n°16-14 du 28 décembre 2016 portant loi de finances pour 2017, notamment son article 132 ;",
      "Vu l’instruction n°02 du 16 janvier 2020 et n°03 du 19 janvier 2020 relatives à la clôture du compte d’affectation spéciale n°302-105 ; « Fonds du Patrimoine Public minier »;"
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
    doc.text(data.orderNumber, margin + 63, yPosition);
    yPosition += 10;
    
    // Main content - using formattedAmount instead of data.amount.toLocaleString()
    setNormalFont2();
    const mainParagraph = [
      {
        text: `Un ordre de perception est émis par l'Agence nationale des Activités Minières (siège central) d'un montant de ${formattedAmount} DA au profit du ${data.taxReceiver} de ${data.taxReceiverAddress} au titre de paiement des droits d'établissement d'acte d'un `,
        style: 'normal' as const
      },
      {
        text: `${permitLabel} par ${data.companyName}`,
        style: 'bold' as const
      }
    ];

    const usedMm = renderStyledParagraph(mainParagraph, margin, yPosition, contentWidth, 7, 17, 'justify');
    yPosition += usedMm;
    setNormalFont2();
    
    yPosition += 5;
    const secondText = `Le montant de l'ordre de perception mentionné ci-dessus est inscrit par le receveur des impôts au crédit du compte « Produits divers du budget n°201-007 ».`;
    const secondParagraph = [{ text: secondText, style: 'normal' as const }];
    const secondUsedMm = renderStyledParagraph(secondParagraph, margin, yPosition, contentWidth, 7, 15, 'justify');
    yPosition += secondUsedMm;
    
    yPosition += 15;
    
    // Date and signature
    const showDate = data.showDate !== false;
    const placeText = 'Alger';
    const dateSuffix = showDate ? ` ${formatDate(data.date)}` : '';
    setBoldFont();
    doc.text(`Fait à Alger, le${dateSuffix}`, margin, yPosition);
    yPosition += 10;
    setBoldFont();
    doc.text(data.president || "P/ Le Président du Comité de Direction", margin, yPosition);
    setBoldFont();
    yPosition += 10;
    doc.text(data.signatureName || "Seddik BENABBES", margin, yPosition);
    
  } else if (type === 'TS') {
    // TS Order content
    setTitleFont();
    doc.text("REPUBLIQUE ALGERIENNE DEMOCRATIQUE ET POPULAIRE", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 7;
    doc.text("MINISTÈRE DES HYDROCARBURES ET DES MINES ", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 10;
    setBoldFont();
    doc.text("Agence Nationale des Activités Minières", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 15;
    setBoldFont();
    doc.text("Siège central", margin, yPosition);
    yPosition += 10;
    
    // Legal references
    setNormalFont1();
    const legalReferences = [
      "Vu la loi n° 25-12 du 9 Safar 1447 correspondant au 3 août 2025 portant loi régissant les activités minières, notamment son article 213 ;",
      "Vu la loi n°15-18 du 30 décembre 2015 portant loi de finances pour 2016, notamment son article 53;",
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
    const orderLabel = `Ordre de perception n° : TS -`;
    doc.text(orderLabel, margin, yPosition);
    doc.text(data.orderNumber, margin + doc.getTextWidth(`${orderLabel} `), yPosition);
    yPosition += 10;
    
    // Main content - using formattedAmount instead of data.amount.toLocaleString()
    setNormalFont2();
    const mainParagraph = [
      {
        text: `Un ordre de perception est émis par l'Agence Nationale des Activités Minières (siège central) d'un montant de ${formattedAmount} DA au profit du ${data.taxReceiver} sise au ${data.taxReceiverAddress} au titre de paiement de la taxe superficiaire par `,
        style: 'normal' as const
      },
      {
        text: `${data.companyName} pour la période du ${periodText}.`,
        style: 'bold' as const
      }
    ];

    const usedMm = renderStyledParagraph(mainParagraph, margin, yPosition, contentWidth, 7, 17, 'justify');
    yPosition += usedMm;
    
    yPosition += 5;
    setNormalFont2();
    const secondText = `La quote-part de la taxe superficiaire à verser au compte « Produit divers du budget » est fixée à cinquante pour cent (50%). Les cinquante pour cent (50%) restants sont à verser à la Caisse de Solidarité et de Garantie des Collectivités Locales.`;
    const secondParagraph = [{ text: secondText, style: 'normal' as const }];
    const secondUsedMm = renderStyledParagraph(secondParagraph, margin, yPosition, contentWidth, 7, 15, 'justify');
    yPosition += secondUsedMm;
    
    yPosition += 15;
    
    // Date and signature
    const showDate = data.showDate !== false;
    const placeText = data.place || 'Alger';
    const dateSuffix = showDate ? ` ${formatDate(data.date)}` : '';
    setBoldFont();
    doc.text(`Fait à Alger, le${dateSuffix}`, margin, yPosition);
    yPosition += 10;
    setBoldFont();
    doc.text(data.president || "P/ Le Président du Comité de Direction", margin, yPosition);
    setBoldFont();
    yPosition += 10;
    doc.text(data.signatureName || "Seddik BENABBES", margin, yPosition);
    
  } else {
    // Produit d'Attribution Order content
    setTitleFont();
    doc.text("REPUBLIQUE ALGERIENNE DEMOCRATIQUE ET POPULAIRE", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 7;
    doc.text("MINISTERE DE l'ENERGIE, DES MINES ET DES ENERGIES RENOUVELABLES", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 10;
    setBoldFont();
    doc.text("Agence Nationale des Activités Minières", pageWidth / 2, yPosition, { align: "center" });
    yPosition += 10;
    
    // Legal references
    setNormalFont1();
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
    setNormalFont2();
    const mainText = `Un ordre de paiement est émis par l'Agence Nationale des Activités Minières (siège central) d'un montant de ${formattedAmount} DA au profit du ${data.taxReceiver} sise au ${data.taxReceiverAddress}. Au titre de paiement des droits du produit d'attribution du ${permitLabel} attribué à la ${data.companyName} pour le site de ${data.location}.`;

    const usedMm = addParagraphImage(doc, mainText, margin, yPosition, Math.max(10, contentWidth - 2), { fontPt: 11, lineHeight: 1.4 });
    yPosition += usedMm;
    
    yPosition += 5;
    setNormalFont();
    const secondText = `Le montant de l'ordre de perception mentionné ci-dessus est inscrit par le receveur des impôts au crédit du compte « Produits divers du budget n°201007 ».`;
    const secondLines = splitTextIntoLines(doc, secondText, contentWidth);
    secondLines.forEach(line => {
      doc.text(line, margin, yPosition);
      yPosition += 5;
    });
    
    yPosition += 15;
    
    const showDate = data.showDate !== false;
    const placeText = data.place || 'Alger';
    const dateSuffix = showDate ? ` ${formatDate(data.date)}` : '';
    setNormalFont();
    doc.text(`Fait à Alger, le${dateSuffix}`, margin, yPosition);
    yPosition += 10;
    setBoldFont();
    doc.text(data.president || "P/ Le Président du Comité de Direction", margin, yPosition);
    setNormalFont();
    yPosition += 10;
    doc.text(data.signatureName || "Seddik BÉNABBES", margin, yPosition);
  }
};

export const createOrderPdfDoc = (type: 'DEA' | 'TS' | 'PRODUIT_ATTRIBUTION', data: OrderData): jsPDF => {
  const doc = new jsPDF();
  renderOrderToDoc(doc, type, data);
  return doc;
};

export const buildBatchPdf = (
  entries: { type: 'DEA' | 'TS' | 'PRODUIT_ATTRIBUTION'; data: OrderData }[]
): jsPDF => {
  if (!entries.length) {
    throw new Error('No entries provided');
  }
  const doc = new jsPDF();
  entries.forEach((entry, index) => {
    if (index > 0) {
      doc.addPage();
    }
    doc.setPage(doc.getNumberOfPages());
    renderOrderToDoc(doc, entry.type, entry.data);
  });
  return doc;
};

export const generatePDFForPreview = async (type: 'DEA' | 'TS' | 'PRODUIT_ATTRIBUTION', data: OrderData): Promise<string> => {
  const doc = createOrderPdfDoc(type, data);
  return doc.output('datauristring');
};











