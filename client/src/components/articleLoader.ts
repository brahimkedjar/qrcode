// =============================================================/// File: components/articleLoader.ts
// Loads predefined Arabic articles for a permit and converts
// them into positioned canvas elements
// =============================================================
import { v4 as uuidv4 } from 'uuid';
import type { ArticleItem, PermisElement } from './types';
import PXC5419 from './articles-txm.json';

export async function loadArticlesForPermit(initialData: any): Promise<ArticleItem[]> {
  try {
    if (
      String(initialData?.code_demande || '').toLowerCase().includes('5419') ||
      String(initialData?.code_demande || '').toLowerCase().includes('pxc')
    ) {
      return (PXC5419.articles || []) as ArticleItem[];
    }
  } catch {}
  return (PXC5419.articles || []) as ArticleItem[];
}

// Build positioned elements from (title, content) pairs, inline on one line.
export const toArticleElements = (options: {
  articleIds: string[];
  articles: ArticleItem[];
  yStart: number;
  x: number;
  width: number;
  fontFamily: string;
  textAlign: string;
  direction: string;
  fontSize: number;
  lineHeight: number;
  padding: number;
  spacing: number;
}): PermisElement[] => {
  const elements: PermisElement[] = [];
  let currentY = options.yStart;

  const buildCombined = (title?: string, content?: string): { text: string; titleLen: number } => {
    const tRaw = (title || '').trim();
    // Replace the normal space between Arabic word and Latin/number with NBSP so underline is continuous
    const t = tRaw.replace(/([\u0600-\u06FF]+)\s+([0-9A-Za-z]+)/g, '$1\u00A0$2');
    const c = (content || '').trim();
    if (!t && !c) return { text: '', titleLen: 0 };
    if (!t) return { text: c, titleLen: 0 };
    if (!c) return { text: t, titleLen: t.length };
    const hasColon = /[:ï¼š]$/.test(t);
    const titlePrefix = hasColon ? t : `${t} :`;
    return { text: `${titlePrefix} ${c}`, titleLen: titlePrefix.length };
  };

  options.articleIds.forEach(articleId => {
    const article = options.articles.find(a => a.id === articleId);
    if (!article) return;

    const { text: textCombined, titleLen } = buildCombined(article.title, article.content);
    const blockHeight = calculateTextHeight(
      textCombined,
      options.width,
      options.fontSize,
      options.lineHeight
    );

    // Keep title bold, but avoid text-decoration underline to prevent gaps between words.
    // A single continuous rule is drawn separately in the designer.
    const styledRanges = (titleLen > 0) ? [{ start: 0, end: titleLen, fontWeight: 'bold', underline: true }] : undefined as any;
    elements.push({
      id: uuidv4(),
      type: 'text',
      x: options.x,
      y: currentY,
      width: options.width,
      text: textCombined,
      language: 'ar',
      direction: 'rtl',
      fontSize: options.fontSize,
      fontFamily: options.fontFamily,
      color: '#000000',
      draggable: true,
      textAlign: 'right',
      opacity: 1,
      rotation: 0,
      wrap: 'word',
      lineHeight: options.lineHeight,
      styledRanges
    });

    currentY += blockHeight + Math.max(2, options.spacing);
  });

  return elements;
};

// Height estimator for simple pagination safety (RTL-friendly)
const calculateTextHeight = (
  text: string,
  width: number,
  fontSize: number,
  lineHeight: number
): number => {
  // Use a slightly larger average char width to avoid over-estimating lines for Arabic text
  const avgCharWidth = fontSize * 0.52;
  const charsPerLine = Math.max(1, Math.floor(width / avgCharWidth));
  const lines = Math.ceil(text.length / charsPerLine);
  return Math.ceil(lines * fontSize * lineHeight);
};

  


