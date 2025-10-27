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

  const joinTitleContent = (title?: string, content?: string) => {
    const t = (title || '').trim();
    const c = (content || '').trim();
    if (!t && !c) return '';
    if (!t) return c;
    if (!c) return t;
    const hasColon = /[:ï¼š]$/.test(t);
    return `${t}${hasColon ? '' : ' :'} ${c}`;
  };

  options.articleIds.forEach(articleId => {
    const article = options.articles.find(a => a.id === articleId);
    if (!article) return;

    const textCombined = joinTitleContent(article.title, article.content);
    const blockHeight = calculateTextHeight(
      textCombined,
      options.width,
      options.fontSize,
      options.lineHeight
    );

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
      lineHeight: options.lineHeight
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

  


