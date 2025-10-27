'use client';
import React, { useEffect, useMemo, useRef, useState, useCallback } from 'react';
import { Stage, Layer, Transformer, Rect } from 'react-konva';
import {
  FiDownload, FiSave, FiEdit2, FiMove, FiType,
  FiTrash2, FiCopy, FiLayers, FiChevronLeft, FiChevronRight, FiGrid,
  FiChevronUp, FiChevronDown, FiRefreshCw, FiCheckCircle, FiAlertCircle,
  FiUpload
} from 'react-icons/fi';
import { BsTextParagraph, BsImage, BsBorderWidth } from 'react-icons/bs';
import axios from 'axios';
import { v4 as uuidv4 } from 'uuid';
import { useHotkeys } from 'react-hotkeys-hook';
import { toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { ElementRenderer } from './ElementRenderer';
import type { PermisElement, PermisDesignerProps, TextEditOverlay, ArticleItem } from './types';
import { ArticlesPanel } from './ArticlesPanel';
import { toArticleElements } from './articleLoader';
import { listArticleSets, getArticlesForSet, saveArticleSet, exportArticleSet, importArticleSet, inferDefaultArticleSetKey, type ArticleSetMeta } from './articleSets';
import { FONT_FAMILIES, ARABIC_FONTS, DEFAULT_CANVAS } from './constants';
import { gridBackground, clamp } from './layout';
import { AiOutlineQrcode } from "react-icons/ai";
import styles from './PermisDesigner.module.css';
import jsPDF from 'jspdf';
import { Upload } from "antd";
import { FileUp } from "lucide-react";

const PAGES = {
  PERMIS_DETAILS: 0,
  ARTICLES: 1
} as const;

type TransformerBoundingBox = {
  x: number;
  y: number;
  width: number;
  height: number;
};

interface QRCodeData {
  typePermis: string;
  codeDemande: string;
  detenteur: string;
  superficie: number;
  duree: string;
  localisation: string;
  dateCreation: string;
  coordinates?: any[];
}

type PermisPages = PermisElement[][];

const PermisDesigner: React.FC<PermisDesignerProps> = ({
  initialData,
  onSave,
  onGeneratePdf,
  onSavePermis,
  procedureId
}) => {
  const [pages, setPages] = useState<PermisPages>([[], [], []]);
  const [currentPage, setCurrentPage] = useState<number>(PAGES.PERMIS_DETAILS);
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [tool, setTool] = useState<'select' | 'text' | 'rectangle' | 'image' | 'line' | 'qrcode' | 'table'>('select');
  const stageRef = useRef<any>(null);
  const transformerRef = useRef<any>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [templates, setTemplates] = useState<any[]>([]);
  const [activeTemplate, setActiveTemplate] = useState<number | null>(null);
  const [zoom, setZoom] = useState(1);
  const [showGrid, setShowGrid] = useState(false);
  const [history, setHistory] = useState<PermisPages[]>([]);
  const [historyIndex, setHistoryIndex] = useState(-1);
  const [articles, setArticles] = useState<ArticleItem[]>([]);
  const [articleSets, setArticleSets] = useState<ArticleSetMeta[]>([]);
  const [selectedArticleSet, setSelectedArticleSet] = useState<string | null>(null);
  const importInputRef = useRef<HTMLInputElement | null>(null);
  const [selectedArticleIds, setSelectedArticleIds] = useState<string[]>([]);
  const [textOverlay, setTextOverlay] = useState<TextEditOverlay | null>(null);
  // PDF export quality control
  const [pdfQuality, setPdfQuality] = useState<'high' | 'balanced' | 'small'>(() => {
    try {
      const v = localStorage.getItem('pdf_export_quality');
      return (v === 'high' || v === 'balanced' || v === 'small') ? (v as any) : 'high';
    } catch { return 'high'; }
  });
  const apiURL = process.env.NEXT_PUBLIC_API_URL;
  const [canvasSizes, setCanvasSizes] = useState<{width: number, height: number}[]>([
    {width: DEFAULT_CANVAS.width, height: DEFAULT_CANVAS.height},
    {width: DEFAULT_CANVAS.width, height: DEFAULT_CANVAS.height},
    // Articles page: single-column portrait (no double width)
    {width: DEFAULT_CANVAS.width, height: DEFAULT_CANVAS.height}
  ]);
  const [savedPermisId, setSavedPermisId] = useState<number | null>(null);
  const [savingPermis, setSavingPermis] = useState(false);
  const [savingTemplate, setSavingTemplate] = useState(false);
  const [permisCode, setPermisCode] = useState<string | null>(null);
  const [loadingTemplates, setLoadingTemplates] = useState(false);
  // Table layout controls
  const [rowsPerColSetting, setRowsPerColSetting] = useState<number>(() => {
    const v = typeof window !== 'undefined' ? localStorage.getItem('rowsPerColSetting') : null;
    const n = v ? parseInt(v, 10) : 10;
    return isNaN(n) ? 10 : Math.max(4, Math.min(30, n));
  });
  const [dragNormalized, setDragNormalized] = useState(false);
  // Marquee selection state
  const [isSelecting, setIsSelecting] = useState(false);
  const [selectionRect, setSelectionRect] = useState<{ x: number; y: number; width: number; height: number } | null>(null);
  const selectionStartRef = useRef<{ x: number; y: number } | null>(null);
  const initialSelectionIdsRef = useRef<string[]>([]);
  // Table editing helpers (for selected table elements)
  const [colEditIndex, setColEditIndex] = useState(0);
  const [cellEditRow, setCellEditRow] = useState(0);
  const [cellEditKey, setCellEditKey] = useState<string>('x');
  const [cellEditValue, setCellEditValue] = useState<string>('');

  const currentCanvasSize = canvasSizes[currentPage] || canvasSizes[0] || { width: DEFAULT_CANVAS.width, height: DEFAULT_CANVAS.height };
  // Ensure color inputs always receive #rrggbb to avoid browser warnings
  const normalizeHexColor = (val: any, fallback = '#000000') => {
    try {
      const s = String(val || '').trim();
      if (/^#[0-9a-fA-F]{6}$/.test(s)) return s;
      const m3 = s.match(/^#([0-9a-fA-F])([0-9a-fA-F])([0-9a-fA-F])$/);
      if (m3) return `#${m3[1]}${m3[1]}${m3[2]}${m3[2]}${m3[3]}${m3[3]}`;
    } catch {}
    return fallback;
  };

  useEffect(() => {
    try { localStorage.setItem('rowsPerColSetting', String(rowsPerColSetting)); } catch {}
  }, [rowsPerColSetting]);

  const handleRefreshTemplates = useCallback(async () => {
    if (!savedPermisId) {
      toast.error('No permis found to refresh templates');
      return;
    }
    setLoadingTemplates(true);
    try {
      const templatesResponse = await axios.get(`${apiURL}/api/permis/${savedPermisId}/templates`);
      const templatesData = Array.isArray(templatesResponse.data) ? templatesResponse.data : [];
      const parsedTemplates = templatesData.map(template => ({
        ...template,
        elements: typeof template.elements === 'string'
          ? JSON.parse(template.elements)
          : template.elements || []
      }));
      setTemplates(parsedTemplates);
      toast.success('Templates refreshed successfully');
    } catch (error) {
      console.error('Error refreshing templates:', error);
      toast.error('Failed to refresh templates');
    } finally {
      setLoadingTemplates(false);
    }
  }, [savedPermisId, apiURL]);

  // Removed duplicate initialization effect to avoid pages reset during edits

  function createPermisDetailsPage(data: any): PermisElement[] {
    const borderElements = createPageBorder(DEFAULT_CANVAS.width, DEFAULT_CANVAS.height);
    const contentElements = createGeneralPageModel(data);
    return [...borderElements, ...contentElements];
  }

  // function createCoordinatesPage(data: any): PermisElement[] {
  //   const borderElements = createPageBorder(DEFAULT_CANVAS.width, DEFAULT_CANVAS.height);
  //   const contentElements = createCoordsPageModel(data);
  //   return [...borderElements, ...contentElements];
  // }

  function createArticlesPage(data: any, size?: { width: number; height: number }): PermisElement[] {
    // Use standard page width for articles (single-column per page)
    const width = size?.width ?? DEFAULT_CANVAS.width;
    const height = size?.height ?? DEFAULT_CANVAS.height;
    const borderElements = createPageBorder(width, height);
    const contentElements = createArticlesPageHeader(width, height);
    return [...borderElements, ...contentElements];
  }

  function filterElementsByPage(elements: PermisElement[], pageIndex: number): PermisElement[] {
    if (!elements || !elements.length) return [];
    return elements.filter(el => el.meta?.pageIndex === pageIndex);
  }

  // Keep a stable history updater to avoid effect loops
  const historyIndexRef = useRef(historyIndex);
  useEffect(() => { historyIndexRef.current = historyIndex; }, [historyIndex]);
  const pushHistory = useCallback((snap: PermisPages) => {
    setHistory(prev => {
      const trimmed = prev.slice(0, historyIndexRef.current + 1);
      const next = [...trimmed, JSON.parse(JSON.stringify(snap)) as PermisPages];
      setHistoryIndex(next.length - 1);
      return next;
    });
  }, []);

  const handleResizeCanvas = useCallback((direction: 'width' | 'height', amount: number) => {
    setCanvasSizes(prev => {
      const newSizes = [...prev];
      const currentSize = newSizes[currentPage];
      const newSize = {
        width: direction === 'width' ? Math.max(currentSize.width + amount, 300) : currentSize.width,
        height: direction === 'height' ? Math.max(currentSize.height + amount, 300) : currentSize.height
      };
      newSizes[currentPage] = newSize;
      setPages(prevP => {
        let newP = [...prevP] as PermisPages;
        // Clamp elements
        newP[currentPage] = newP[currentPage].map(el => ({
          ...el,
          x: Math.min(el.x, newSize.width - (el.width || 50)),
          y: Math.min(el.y, newSize.height - (el.height || 50))
        }));
        // Update borders
        newP[currentPage] = newP[currentPage].filter(el => !el.meta?.isBorder);
        const newBorders = createPageBorder(newSize.width, newSize.height);
        newP[currentPage] = [...newP[currentPage], ...newBorders];
        pushHistory(newP);
        return newP;
      });
      return newSizes;
    });
  }, [currentPage, pushHistory]);

  const handleResetCanvasSize = useCallback(() => {
    setCanvasSizes(prev => {
      const newSizes = [...prev];
      newSizes[currentPage] = {width: DEFAULT_CANVAS.width, height: DEFAULT_CANVAS.height};
      setPages(prevP => {
        let newP = [...prevP] as PermisPages;
        newP[currentPage] = newP[currentPage].filter(el => !el.meta?.isBorder);
        const newBorders = createPageBorder(newSizes[currentPage].width, newSizes[currentPage].height);
        newP[currentPage] = [...newP[currentPage], ...newBorders];
        pushHistory(newP);
        return newP;
      });
      return newSizes;
    });
  }, [currentPage, pushHistory]);

  // Fixed createPageBorder function
function createPageBorder(width: number, height: number): PermisElement[] {
  const borderColor = '#1a5276';
  const borderWidth = 2;
  const dashPattern = [0.8, 0.4];
  const borders = [
    {
      id: uuidv4(),
      type: 'rectangle' as const,  // Added 'as const' for literal type inference
      x: 10,
      y: 10,
      width: width - 15,
      height: height - 15,
      stroke: borderColor,
      strokeWidth: borderWidth,
      fill: 'transparent',
      dash: dashPattern,
      draggable: false,
      meta: { isBorder: true }
    },
    {
      id: uuidv4(),
      type: 'rectangle' as const,  // Added 'as const' for literal type inference
      x: 20,
      y: 20,
      width: width - 35,
      height: height - 35,
      stroke: borderColor,
        strokeWidth: 2,
      fill: 'transparent',
      draggable: false,
      opacity: 0.6,
      meta: { isBorder: true }
    },
    ...createCornerDecorations(borderColor, width, height)
  ];
  return borders;
}

// Fixed createCornerDecorations function
function createCornerDecorations(color: string, width: number, height: number): PermisElement[] {
  const size = 30;
  return [
    {
      id: uuidv4(),
      type: 'line' as const,  // Added 'as const' for literal type inference
      x: 40,
      y: 40,
      points: [0, 0, size, size],
      stroke: color,
      strokeWidth: 2,
      dash: [5, 3],
      draggable: false,
      meta: { isBorder: true }
    },
    {
      id: uuidv4(),
      type: 'line' as const,  // Added 'as const' for literal type inference
      x: width - 40,
      y: 40,
      points: [0, 0, -size, size],
      stroke: color,
      strokeWidth: 2,
      dash: [5, 3],
      draggable: false,
      meta: { isBorder: true }
    },
    {
      id: uuidv4(),
      type: 'line' as const,  // Added 'as const' for literal type inference
      x: 40,
      y: height - 40,
      points: [0, 0, size, -size],
      stroke: color,
      strokeWidth: 2,
      dash: [5, 3],
      draggable: false,
      meta: { isBorder: true }
    },
    {
      id: uuidv4(),
      type: 'line' as const,  // Added 'as const' for literal type inference
      x: width - 40,
      y: height - 40,
      points: [0, 0, -size, -size],
      stroke: color,
      strokeWidth: 2,
      dash: [5, 3],
      draggable: false,
      meta: { isBorder: true }
    }
  ];
}

  const handleRenameTemplate = useCallback(async (templateId: number, newName: string) => {
    try {
      const template = templates.find(t => t.id === templateId);
      if (!template) return;
      await axios.patch(`${apiURL}/api/permis/templates/${templateId}`, {
        name: newName
      });
      if (savedPermisId) {
        const templatesResponse = await axios.get(`${apiURL}/api/permis/${savedPermisId}/templates`);
        setTemplates(Array.isArray(templatesResponse.data) ? templatesResponse.data : []);
      }
      toast.success('Template renamed successfully');
    } catch (error) {
      console.error('Failed to rename template:', error);
      toast.error('Failed to rename template');
    }
  }, [templates, savedPermisId, apiURL]);

 
 const insertArticlesAsElements = useCallback((articleIds: string[], source: ArticleItem[]) => {
  // Single-column layout with automatic pagination
  const isTXC = (selectedArticleSet || '').toLowerCase().includes('txc');
  let marginX = isTXC ? 24 : 40;
  const padding = 1;
  let defaultArticleGap = isTXC ? 1 : 2; 
  let decisionGap = isTXC ? 6 : 10;
  let startY = isTXC ? 24 : 40;
  const bottomMargin = 20;

  // Helper to get size for a given absolute page index
  const getSize = (absIndex: number) =>
    canvasSizes[absIndex] ||
    canvasSizes[PAGES.ARTICLES] ||
    { width: DEFAULT_CANVAS.width, height: DEFAULT_CANVAS.height };

  // Collect blocks per article page (relative index 0 => page 3 visual)
  const perPageBlocks: PermisElement[][] = [];
  const currentYByPage: number[] = [];

  const ensurePage = (relPage: number) => {
    if (!perPageBlocks[relPage]) perPageBlocks[relPage] = [];
    if (currentYByPage[relPage] == null) currentYByPage[relPage] = startY;
  };

  let relPage = 0; // 0 => absolute pageIndex 2
  ensurePage(relPage);

  // Prepare holder name for placeholder substitution in article texts
  const pickFirstNonEmpty = (...vals: any[]) => {
    for (const v of vals) {
      if (v == null) continue;
      const s = String(v).trim();
      if (s) return s;
    }
    return '';
  };
  const holderArabic = pickFirstNonEmpty(
    (initialData as any)?.detenteur_ar,
    (initialData?.detenteur as any)?.NomArab,
    (initialData?.detenteur as any)?.nom_ar,
    (initialData?.detenteur as any)?.nom_societeAR,
  );
  const holderLatin = pickFirstNonEmpty(
    (initialData?.detenteur as any)?.Nom,
    (initialData?.detenteur as any)?.nom,
    (initialData?.detenteur as any)?.nom_societe,
    (initialData?.detenteur as any)?.nom_societeFR,
    typeof initialData?.detenteur === 'string' ? (initialData as any)?.detenteur : ''
  );
  const holderDisplay = pickFirstNonEmpty(holderArabic, holderLatin);
 
  const replaceHolderDots = (text?: string) => {
    const t = String(text || '');
    if (!t) return t;
    if (!holderDisplay) return t;
    return t
      // السيد/السيدة .......
      .replace(/(السيد)\s*\.{2,}/g, `$1 ${holderDisplay}`)
      .replace(/(السيدة)\s*\.{2,}/g, `$1 ${holderDisplay}`)
      // المقدم من طرف .......
      .replace(/(المقدم\s+من\s+طرف)\s*\.{2,}/g, `$1 ${holderDisplay}`)
      // ....... المقدم من طرف
      .replace(/\.{2,}\s*(?=المقدم\s+من\s+طرف)/g, `${holderDisplay} `);
  };
  const statutJuridique = pickFirstNonEmpty(
    (initialData?.detenteur as any)?.StatutArab,
    (initialData?.detenteur as any)?.StatutJuridique?.StatutArab,
    (initialData?.detenteur as any)?.StatutJuridique,
    (initialData?.detenteur as any)?.statutJuridique,
    (initialData?.detenteur as any)?.statut,
  );
  const replaceStatut = (text?: string) => {
    const t = String(text || '');
    if (!t) return t;
    if (!statutJuridique) return t;
    // Replace default placeholder with actual legal status
    return t.replace(/الشركة\s+ذات\s+المسؤولية\s+المحدودة/g, statutJuridique);
  };
  const replacedSource: ArticleItem[] = source.map(a => ({
    ...a,
    title: replaceStatut(replaceHolderDots((a as any).title)),
    content: replaceStatut(replaceHolderDots((a as any).content)),
  }));

  articleIds.forEach((articleId) => {
    const article = source.find(a => a.id === articleId);
    if (!article) return;

    const size = getSize(PAGES.ARTICLES + relPage);
    const contentWidth = size.width - marginX * 2;

    const articleElements = toArticleElements({
      articleIds: [articleId],
      articles: replacedSource,
      yStart: 0,
      x: 0,
      width: contentWidth - padding * 2,
      fontFamily: ARABIC_FONTS[0],
      textAlign: 'right',
      direction: 'rtl',
      fontSize: isTXC ? 18 : 24,
      lineHeight: isTXC ? 1.4 : 1.8,
      padding: padding,
      spacing: 0,
    });
    if (articleElements.length === 0) return;

    // Inject an underline under titles like "المادة 1:" within each article block
    const estimateWidth = (text: string, fontSize: number) => {
      const s = String(text || '');
      if (!s) return 0;
      const factor = 0.48;
      return Math.ceil(s.length * fontSize * factor);
    };
    const withUnderlines: PermisElement[] = [];
    articleElements.forEach(el => {
      withUnderlines.push(el);
      if (el.type === 'text') {
        const txt = String((el as any).text || '');
        const ci = txt.indexOf(':');
        // Only underline if looks like a title (starts with المادة ...:)
        if (ci > 0 && /\u0627\u0644\u0645\u0627\u062F\u0629/.test(txt.slice(0, ci))) {
          const titlePart = txt.slice(0, ci + 1);
          const fz = (el as any).fontSize || 24;
          const lh = (el as any).lineHeight || 1.8;
          // Underline width at ~73% of title visual width (bounded by column)
          const titleVisualW = estimateWidth(titlePart, fz);
          const uW = Math.min(
            (el.width as number) || (contentWidth - padding * 2),
            Math.max(40, Math.floor(titleVisualW * 0.59))
          );
          withUnderlines.push({
            id: uuidv4(),
            type: 'line',
            x: 0, // will be right-aligned during positioning
            y: Math.ceil(fz * lh) - 7,
            width: uW,
            height: 2,
            rotation: 0,
            stroke: '#000',
            strokeWidth: 1,
            opacity: 1,
            draggable: true,
            meta: { articleGroup: `${articleId}_underline` }
          } as any);
        }
      }
    });

    // Estimate article block height
    let articleHeight = 0;
        const heightsByGroup = new Map<string, number>();
    withUnderlines.forEach(el => {
      let h = 0;
      if (el.type === 'text') {
        const fontSize = (el.fontSize || 20);
        const lineH = (el.lineHeight || 1.8);
        const text = String(el.text || '');
        const boxW = Math.max(10, el.width || (contentWidth - padding *2));
        // Slightly smaller average character width to avoid overestimating text height
        const avgCharsPerLine = Math.max(1, Math.floor(boxW / (fontSize * 0.45)));
        const parts = text.split(/\r?\n/);
        let totalLines = 0;
        parts.forEach(part => {
          const len = Math.max(1, part.trim().length);
          totalLines += Math.ceil(len / avgCharsPerLine);
        });
        h = Math.max(fontSize * lineH, totalLines * fontSize * lineH);
      } else {
        h = el.height || 2;
      }
      const gid = (el as any).meta?.articleGroup || articleId;
      const prev = heightsByGroup.get(gid) || 0;
      heightsByGroup.set(gid, Math.max(prev, h));
    });
    // Sum heights of all groups for this article (typically just one)
    heightsByGroup.forEach(v => { articleHeight += v; });
    articleHeight += (article && article.id === "decision" ? decisionGap : defaultArticleGap);
    // If this article is the DECISION header, gently pull it up a bit to remove visual gap
    if (article && article.id === 'decision') {
      currentYByPage[relPage] = Math.max(startY, currentYByPage[relPage] - 12);
    }

    // If it doesn't fit on current page, move to a new page
    const bottom = size.height - bottomMargin;
    if ((currentYByPage[relPage] + articleHeight) > bottom) {
      relPage += 1;
      ensurePage(relPage);
    }

    const positioned = withUnderlines.map(el => {
      const elWidth = el.width || (contentWidth - padding * 2);
      // Right-align within the single column for RTL text
      const elementX = marginX + (contentWidth - elWidth) - padding;
      return {
        ...el,
        x: elementX,
        y: currentYByPage[relPage] + (el.y || 0),
        isArticle: true,
        direction: 'rtl',
        textAlign: 'right',
        language: 'ar',
        wrap: 'word',
        padding: padding,
        width: elWidth,
        meta: { ...(el.meta || {}), pageIndex: PAGES.ARTICLES + relPage, isArticle: true }
      } as PermisElement;
    });

    perPageBlocks[relPage].push(...positioned);
    currentYByPage[relPage] += articleHeight;
  });

  const neededArticlePages = Math.max(1, perPageBlocks.length);

  // Commit to state: ensure page arrays and canvas sizes, then place content
  setPages(prev => {
    let next = [...prev] as PermisPages;
    // Ensure enough pages
    while (next.length < PAGES.ARTICLES + neededArticlePages) {
      const base = canvasSizes[PAGES.ARTICLES] || { width: DEFAULT_CANVAS.width, height: DEFAULT_CANVAS.height };
      next.push(createArticlesPage(initialData, base));
    }
    // Trim extra article pages if any
    if (next.length > PAGES.ARTICLES + neededArticlePages) {
      next = next.slice(0, PAGES.ARTICLES + neededArticlePages);
    }
    // For each article page, keep non-article elements and add blocks
    for (let rp = 0; rp < neededArticlePages; rp++) {
      const absIdx = PAGES.ARTICLES + rp;
      const keep = (next[absIdx] || []).filter(el => !el.isArticle && !el.meta?.isArticle && !el.meta?.isFooter);
      next[absIdx] = [...keep, ...(perPageBlocks[rp] || [])];
    }
    // Add footer labels to the last page (middle bottom + right bottom)
    const lastAbs = PAGES.ARTICLES + neededArticlePages - 1;
    if (lastAbs >= 0 && next[lastAbs]) {
      const centerLabelY = (DEFAULT_CANVAS.height - 70);
      const centerWidth = 600;
      const centerX = (DEFAULT_CANVAS.width - centerWidth) / 2;
      const rightWidth = 220;
      const rightY = (DEFAULT_CANVAS.height - 70);
      const rightX = DEFAULT_CANVAS.width - rightWidth - 40;
      const footerEls: PermisElement[] = [
        {
          id: uuidv4(),
          type: 'text',
          x:rightX-150,
          y: centerLabelY-50 ,
          width: centerWidth,
          text: 'رئـيس اللجنة المديرة',
          language: 'ar',
          direction: 'rtl',
          fontSize: 24,
          fontFamily: ARABIC_FONTS[0],
          color: '#000',
          draggable: true,
          textAlign: 'center',
          opacity: 1,
          wrap: 'none',
          lineHeight: 1.2,
          meta: { isFooter: true, pageIndex: lastAbs }
        } as any,
        {
          id: uuidv4(),
          type: 'text',
          x: centerX-150,
          y: centerLabelY-30,
          width: centerWidth,
          text: 'حـرر بالـجزائـر في:',
          language: 'ar',
          direction: 'rtl',
          fontSize: 24,
          fontFamily: ARABIC_FONTS[0],
          color: '#000',
          draggable: true,
          textAlign: 'center',
          opacity: 1,
          wrap: 'none',
          lineHeight: 1.2,
          meta: { isFooter: true, pageIndex: lastAbs }
        } as any,
        {
          id: uuidv4(),
          type: 'text',
          x: rightX,
          y: rightY,
          width: rightWidth,
          text: 'مراد حنيفي',
          language: 'ar',
          direction: 'rtl',
          fontSize: 24,
          fontFamily: ARABIC_FONTS[0],
          color: '#000',
          draggable: true,
          textAlign: 'right',
          opacity: 1,
          wrap: 'none',
          lineHeight: 1.2,
          meta: { isFooter: true, pageIndex: lastAbs }
        } as any,
      ];
      next[lastAbs] = [...next[lastAbs], ...footerEls];
    }
    pushHistory(JSON.parse(JSON.stringify(next)) as PermisPages);
    return next;
  });

  // Keep canvasSizes in sync
  setCanvasSizes(prev => {
    let sizes = [...prev];
    while (sizes.length < PAGES.ARTICLES + neededArticlePages) {
      const base = prev[PAGES.ARTICLES] || { width: DEFAULT_CANVAS.width, height: DEFAULT_CANVAS.height };
      sizes.push({ width: base.width, height: base.height });
    }
    if (sizes.length > PAGES.ARTICLES + neededArticlePages) {
      sizes = sizes.slice(0, PAGES.ARTICLES + neededArticlePages);
    }
    return sizes;
  });
}, [pushHistory, canvasSizes, initialData]);

  useEffect(() => {
    let mounted = true;
    const load = async () => {
      if (!initialData) return;
      try {
        const permisResponse = await axios.get(`${apiURL}/api/permis/procedure/${procedureId}/permis`);
        if (permisResponse.data.exists) {
          const { permisId, permisCode } = permisResponse.data;
          if (!mounted) return;
          setSavedPermisId(permisId);
          setPermisCode(permisCode);
          const resp = await axios.get(`${apiURL}/api/permis/${permisId}/templates`);
          const serverTemplates = Array.isArray(resp.data) ? resp.data : resp.data?.data ?? [];
          if (!mounted) return;
          setTemplates(serverTemplates);
          const lastUsedTemplateId = localStorage.getItem(`lastTemplate_${permisCode}`);
          const numericTemplateId = lastUsedTemplateId ? parseInt(lastUsedTemplateId, 10) : null;
          const defaultTemplate = serverTemplates.find((t: { id: number }) => t.id === numericTemplateId) || serverTemplates[0];
          setActiveTemplate(defaultTemplate?.id ?? null);
          const buildPages = (template?: typeof defaultTemplate): PermisPages => {
            if (!template?.elements?.length) {
              return [
                createPermisDetailsPage(initialData),
               // createCoordinatesPage(initialData),
                createArticlesPage(initialData)
              ];
            }
            let templateElements: PermisElement[] = [];
            if (typeof template.elements === 'string') {
              templateElements = JSON.parse(template.elements);
            } else if (Array.isArray(template.elements)) {
              templateElements = template.elements;
            }
            return [
              filterElementsByPage(templateElements, 0) || createPermisDetailsPage(initialData),
           //   filterElementsByPage(templateElements, 1) || createCoordinatesPage(initialData),
              filterElementsByPage(templateElements, 2) || createArticlesPage(initialData)
            ];
          };
          const initialPages = buildPages(defaultTemplate);
          let sets = listArticleSets();
          try {
            const serverSets = await (await import('./articleSets')).listServerArticleSets();
            const merged = [...sets];
            serverSets.forEach(s => { if (!merged.some(m => m.key === s.key)) merged.push(s); });
            sets = merged;
          } catch {}
          setArticleSets(sets);
          const defKey = inferDefaultArticleSetKey(initialData, sets);
          setSelectedArticleSet(defKey);
          const loadedArticles = await getArticlesForSet(defKey);
          if (!mounted) return;
          setArticles(loadedArticles);
          const preselected = loadedArticles.filter(a => (a as any).preselected).map(a => a.id);
          setSelectedArticleIds(preselected);
          if (preselected.length) {
            insertArticlesAsElements(preselected, loadedArticles);
          }
          setPages(JSON.parse(JSON.stringify(initialPages)));
          pushHistory(JSON.parse(JSON.stringify(initialPages)));
        } else {
          if (!mounted) return;
          setTemplates([]);
          setActiveTemplate(null);
          const fallback: PermisPages = [
            createPermisDetailsPage(initialData),
           // createCoordinatesPage(initialData),
            createArticlesPage(initialData)
          ];
          setPages(fallback);
          pushHistory(fallback);
          let sets = listArticleSets();
          try { const serverSets = await (await import('./articleSets')).listServerArticleSets();
            const merged = [...sets]; serverSets.forEach(s => { if (!merged.some(m => m.key === s.key)) merged.push(s); }); sets = merged; } catch {}
          setArticleSets(sets);
          const defKey = inferDefaultArticleSetKey(initialData, sets);
          getArticlesForSet(defKey).then(a => {
            if (!mounted) return;
            setArticles(a);
          }).catch(() => {});
        }
      } catch (err) {
        console.error('Init error', err);
        if (!mounted) return;
        setTemplates([]);
        setActiveTemplate(null);
        const fallback: PermisPages = [
          createPermisDetailsPage(initialData),
         // createCoordinatesPage(initialData),
          createArticlesPage(initialData)
        ];
        setPages(fallback);
        pushHistory(fallback);
        let sets = listArticleSets();
        try { const serverSets = await (await import('./articleSets')).listServerArticleSets();
          const merged = [...sets]; serverSets.forEach(s => { if (!merged.some(m => m.key === s.key)) merged.push(s); }); sets = merged; } catch {}
        setArticleSets(sets);
        const defKey = inferDefaultArticleSetKey(initialData, sets);
        getArticlesForSet(defKey).then(a => {
          if (!mounted) return;
          setArticles(a);
        }).catch(() => {});
      }
    };
    load();
    return () => { mounted = false; };
  }, [initialData, procedureId, apiURL]);

  // Sync the main title with the selected article set name after pages are built
  useEffect(() => {
    const selectedSetName = (articleSets || []).find(s => s.key === selectedArticleSet)?.name;
    if (!selectedSetName) return;
    const idxAr = selectedSetName.search(/[\u0600-\u06FF]/);
    const cleaned = idxAr >= 0 ? selectedSetName.slice(idxAr).trim() : selectedSetName;
    setPages(prev => {
      const next = [...prev] as PermisPages;
      if (!next[0]) return prev;
      let idx = next[0].findIndex(el => el.type === 'text' && (el as any).meta?.isMainTitle);
      if (idx === -1) {
        // Fallback: guess the title text element by position/size
        idx = next[0].findIndex(el => el.type === 'text' && (el as any).fontSize >= 24 && (el as any).y >= 120 && (el as any).y <= 220 && ((el as any).textAlign === 'center'));
        if (idx === -1) return prev;
      }
      const page0 = [...next[0]];
      if ((page0[idx] as any).text === cleaned) return prev;
      page0[idx] = { ...page0[idx], text: cleaned } as any;
      next[0] = page0;
      pushHistory(next);
      return next;
    });
  }, [articleSets, selectedArticleSet]);

  const changeArticleSet = useCallback(async (key: string) => {
    setSelectedArticleSet(key);
    try {
      const a = await getArticlesForSet(key);
      setArticles(a);
      const ids = a.filter(x => (x as any).preselected).map(x => x.id);
      setSelectedArticleIds(ids);
      insertArticlesAsElements(ids, a);
    } catch (e) {
      console.error('Failed to load article set', e);
    }
  }, [insertArticlesAsElements]);

  useEffect(() => {
    if (!transformerRef.current) return;
    if (selectedIds.length > 0) {
      const nodes = selectedIds.map(id => stageRef.current?.findOne(`#${id}`)).filter(Boolean);
      transformerRef.current.nodes(nodes);
      transformerRef.current.getLayer()?.batchDraw();
    } else {
      transformerRef.current.nodes([]);
      transformerRef.current.getLayer()?.batchDraw();
    }
  }, [selectedIds, currentPage, pages]);

  useHotkeys('delete', () => handleDeleteSelected(), [pages, currentPage, selectedIds]);
  useHotkeys('ctrl+z, cmd+z', () => undo(), [history, historyIndex]);
  useHotkeys('ctrl+y, cmd+y', () => redo(), [history, historyIndex]);
  useHotkeys('ctrl+c, cmd+c', () => handleCopySelected(), [pages, currentPage, selectedIds]);
  useHotkeys('ctrl+v, cmd+v', () => handlePasteSelected(), [pages, currentPage]);
  useHotkeys('esc', () => setTextOverlay(null), []);

  const undo = useCallback(() => {
    setHistoryIndex(idx => {
      if (idx > 0) {
        setPages(history[idx - 1]);
        setSelectedIds([]);
        return idx - 1;
      }
      return idx;
    });
  }, [history]);

  const redo = useCallback(() => {
    setHistoryIndex(idx => {
      if (idx < history.length - 1) {
        setPages(history[idx + 1]);
        setSelectedIds([]);
        return idx + 1;
      }
      return idx;
    });
  }, [history]);

  const generateQRCodeData = (data: any, codeOverride?: string): string => {
    const baseUrl = 'https://sig.anam.dz/';
    const code = (codeOverride && String(codeOverride)) || String(data?.code_demande || data?.codeDemande || '');
    // Keep URL short; include just a code param and type
    const params = new URLSearchParams({
      code,
    });
    return `${baseUrl}?${params.toString()}`;
  };

  const createQRCodeElement = (data: any, x: number, y: number): PermisElement => {
    return {
      id: uuidv4(),
      type: 'qrcode',
      x,
      y,
      width: 200,
      height: 200,
      qrData: generateQRCodeData(data),
      draggable: true,
      opacity: 1,
      rotation: 0,
      scaleX: 1,
      scaleY: 1
    };
  };

  // New general page layout modeled after official ANAM models
  function createGeneralPageModel(data: any): PermisElement[] {
    const typeCode = data?.typePermis?.code || '';
    const code = data?.code_demande || data?.codeDemande || '';
    const superficie = data?.superficie ? `${data.superficie}` : '';
    const localisation = data?.localisation || '';
    const detenteur = (data?.detenteur && (data.detenteur.nom_societeFR || data.detenteur.nom_societe || data.detenteur.nom)) || (data?.detenteur || '');
    const detenteurArabicRaw = (data as any)?.detenteur_ar || ((data?.detenteur as any)?.NomArab) || ((data?.detenteur as any)?.nom_ar) || '';
    // Holder display: Arabic if present, else Latin name, else id
    const pickFirstNonEmpty = (...vals: any[]) => {
      for (const v of vals) {
        if (v == null) continue;
        const s = String(v).trim();
        if (s) return s;
      }
      return '';
    };
    const holderArabic = pickFirstNonEmpty(
      (data as any)?.detenteur_ar,
      (data?.detenteur as any)?.NomArab,
      (data?.detenteur as any)?.nom_ar,
      (data?.detenteur as any)?.nom_societeAR,
    );
    const holderLatin = pickFirstNonEmpty(
      (data?.detenteur as any)?.Nom,
      (data?.detenteur as any)?.nom,
      (data?.detenteur as any)?.nom_societe,
      (data?.detenteur as any)?.nom_societeFR,
      typeof data?.detenteur === 'string' ? (data as any)?.detenteur : ''
    );
    const holderId = (() => {
      const id1 = (data?.detenteur as any)?.id;
      const id2 = (data as any)?.detenteur_id;
      const id3 = typeof data?.detenteur === 'number' ? data.detenteur : undefined;
      const id = id1 ?? id2 ?? id3;
      return id != null ? String(id) : '';
    })();
    const holderDisplay = pickFirstNonEmpty(holderArabic, holderLatin, holderId);
    console.log('sssssssssssssssssssssss',data)
    const els: PermisElement[] = [];

    // Logo (ANAM) on the top-left
    els.push({ id: uuidv4(), type: 'image', x: 38, y: 34, width: 110, height: 90, draggable: false, src: '/logo.png' } as any);

    // Bilingual header centered horizontally with padding between lines
    const headerWidth = 480;
    const headerX = (DEFAULT_CANVAS.width - headerWidth) / 2;
    const headerY1 = 28;
    const headerPad = 36; // vertical spacing between header lines
    els.push({ id: uuidv4(), type: 'text', x: headerX, y: headerY1, width: headerWidth, text: 'الجمهورية الجزائرية الديمقراطية الشعبية', language: 'ar', direction: 'rtl', fontSize: 31, fontFamily: ARABIC_FONTS[0], color: '#000', draggable: true, textAlign: 'center', opacity: 1 });
    els.push({ id: uuidv4(), type: 'text', x: headerX, y: headerY1 + headerPad, width: headerWidth, text: "People's Democratic Republic of Algeria", fontSize: 22, fontFamily: 'Arial', color: '#000', draggable: true, textAlign: 'center', opacity: 1 });
    els.push({ id: uuidv4(), type: 'text', x: headerX, y: headerY1 + headerPad * 2, width: headerWidth, text:' وزارة المحــروقــات والمنــاجــم', language: 'ar', direction: 'rtl', fontSize: 26, fontFamily: ARABIC_FONTS[0], color: '#000', draggable: true, textAlign: 'center' });
    els.push({ id: uuidv4(), type: 'text', x: headerX, y: headerY1 + headerPad * 3, width: headerWidth, text: 'الوكالة الوطنية للنشاطات المنجمية', language: 'ar', direction: 'rtl', fontSize: 26, fontFamily: ARABIC_FONTS[0], color: '#000', draggable: true, textAlign: 'center' });
    // Thin rule
    //els.push({ id: uuidv4(), type: 'line', x: 60, y: 134, width: 680, height: 0, stroke: '#000', strokeWidth: 1, draggable: false });

    // Main Arabic title (inside a framed box like the model)
    const permitTitle = ((articleSets || []).find(s => s.key === selectedArticleSet)?.name)
      || (data?.typePermis?.lib_type_ar)
      
      || 'رخصة منجمية';
    // Framed box
    const boxX = 110; 
    const boxY = 174; 
    const boxW = 580; 
    const boxH = 170; // increased to fit 150x150 QR
    const rectLeft = boxX - 90; 
    const rectWidth = 865;
    els.push({ id: uuidv4(), type: 'line', x: rectLeft, y: boxY, width: rectWidth, height: 0, stroke: '#000', strokeWidth: 1, draggable: true } as any);
    els.push({ id: uuidv4(), type: 'line', x: rectLeft, y: boxY + boxH, width: rectWidth, height: 0, stroke: '#000', strokeWidth: 1, draggable: true } as any);
    // Place a QR code (150x150) inside the right part of the rectangle
    const qrSize = 150;
    const qrX = rectLeft + rectWidth - 8 - qrSize; // right aligned inside rect
    const qrY = boxY + Math.max(3, Math.floor((boxH - qrSize) / 2));
    // Title text restricted to left side of the box so it doesn't overlap the QR
    const titleLeftX = rectLeft + 10;
    const titleAvailW = rectWidth - (qrSize + 28); // leave margin near QR
    // Compute a cleaned set-name-only Arabic title if available (strip codes like "TEM - ")
    const __rawTitleSetName = (articleSets || []).find(s => s.key === selectedArticleSet)?.name || '';
    const __arabicIdx = __rawTitleSetName.search(/[\u0600-\u06FF]/);
    const __cleanTitleFromSet = __arabicIdx >= 0 ? __rawTitleSetName.slice(__arabicIdx).trim() : __rawTitleSetName;
    // Center title inside the header band (no change to Arabic text)
    const centerBandW = Math.min(DEFAULT_CANVAS.width - 120, rectWidth - 40);
    const centerBandX = (DEFAULT_CANVAS.width - centerBandW) / 2;
    els.push({ id: uuidv4(), type: 'text', x: centerBandX, y: boxY + 34, width: centerBandW, text: (__cleanTitleFromSet || permitTitle), language: 'ar', direction: 'rtl', fontSize: 42, fontFamily: ARABIC_FONTS[0], color: '#000', draggable: true, textAlign: 'center', fontWeight: 'bold', meta: { isMainTitle: true } });

    // Number line centered
    const LRM = '\u200e';
    const numberLine = `رقم: ${LRM}${code} ${LRM}${typeCode}`.trim();
    // Center number line in the same band (no change to Arabic text)
    els.push({ id: uuidv4(), type: 'text', x: centerBandX, y: boxY + Math.min(120, Math.max(70, Math.floor(boxH/2) + 30))-20, width: centerBandW, text: numberLine, language: 'ar', direction: 'rtl', fontSize: 38, fontFamily: ARABIC_FONTS[0], color: '#000', draggable: true, textAlign: 'center' });

     const parseAccessDate = (v: any): Date | null => {
      if (!v && v !== 0) return null;
      if (v instanceof Date) return isNaN(+v) ? null : v;
      const s = String(v).trim();
      const m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
      if (m) {
        const d = new Date(Number(m[3]), Number(m[2]) - 1, Number(m[1]));
        return isNaN(+d) ? null : d;
      }
      const d2 = new Date(s);
      return isNaN(+d2) ? null : d2;
    };

    // For permits, expiry is often the day before anniversary; still count full years
    const diffPermitYears = (a: Date, b: Date) => {
      const naive = b.getFullYear() - a.getFullYear();
      if (naive <= 0) return 0;
      const anniversary = new Date(a.getTime());
      anniversary.setFullYear(a.getFullYear() + naive);
      // If end is the same day or up to 1 day before anniversary, count the full naive years
      const oneDayMs = 24 * 60 * 60 * 1000;
      if (b.getTime() + oneDayMs >= anniversary.getTime()) return naive;
      return Math.max(0, naive - 1);
    };

        const formatFr = (d: Date | null) => d ? `${String(d.getDate()).padStart(2,'0')}/${String(d.getMonth()+1).padStart(2,'0')}/${d.getFullYear()}` : '';

     // Robust Arabic duration composer with Unicode escapes to avoid encoding issues
    const computeDureeAr2 = (): string => {
      const serverVal: any = (data as any)?.duree_display_ar;
      // Try to extract dates from server string even if encoding is corrupted
      let svStart: Date | null = null;
      let svEnd: Date | null = null;
      if (serverVal) {
        const m = String(serverVal).match(/(\d{1,2}\/\d{1,2}\/\d{4}).*?(\d{1,2}\/\d{1,2}\/\d{4})/);
        if (m) {
          svStart = parseAccessDate(m[1]);
          svEnd = parseAccessDate(m[2]);
        }
      }
      if (svStart && svEnd) {
        // Use the two dates seen in the server string, recomposed cleanly
        const y = diffPermitYears(svStart, svEnd);
        let phrase = '';
        if (y === 1) {
          phrase = `1 \u0633\u0646\u0629`;
        } else {
          phrase = `${y} \u0633\u0646\u0648\u0627\u062A`;
        }
        const minWord = '\u0645\u0640\u0640\u0646';
        const ilaWord = '\u0625\u0644\u0640\u0640\u0649';
        return `${phrase} (${minWord} ${formatFr(svStart)} ${ilaWord} ${formatFr(svEnd)})`;
      }
      if (serverVal && !/\uFFFD/.test(String(serverVal)) && /[\u0600-\u06FF]/.test(String(serverVal))) {
        return String(serverVal).trim();
      }
      const dCreation = parseAccessDate((data as any)?.dateCreation || (data as any)?.DateDemande || (data as any)?.date_demande);
      let dStart = parseAccessDate((data as any)?.DateOctroi
        || (data as any)?.dateDebut
        || (data as any)?.date_debut
        || (data as any)?.date_octroi
      );
      let dEnd = parseAccessDate((data as any)?.DateExpiration
        || (data as any)?.dateFin
        || (data as any)?.date_fin
        || (data as any)?.date_expiration
      );
      if ((!dStart && dCreation) || (dStart && dCreation && dStart.getTime() < dCreation.getTime() - 24*3600*1000)) {
        dStart = dCreation;
      }
      let years = Number(
        (data as any)?.typePermis?.duree_initiale
        ?? (data as any)?.typePermis?.validiteMaximale
        ?? (data as any)?.typePermis?.ValiditeMaximale
        ?? (data as any)?.duree
        ?? 0
      ) || 0;
      if (!dEnd && dStart && years > 0) {
        const tmp = new Date(dStart.getTime());
        tmp.setFullYear(tmp.getFullYear() + years);
        dEnd = tmp;
      }
      if (!dStart && dEnd && years > 0) {
        const tmp = new Date(dEnd.getTime());
        tmp.setFullYear(tmp.getFullYear() - years);
        dStart = tmp;
      }
      if (!dStart || !dEnd || dEnd.getFullYear() <= 1900) return '';
      const y = diffPermitYears(dStart, dEnd);
      let phrase = '';
      if (y === 1) {
        phrase = `1 \u0633\u0646\u0629`;
      } else {
        phrase = `${y} \u0633\u0646\u0648\u0627\u062A`;
      }
      const minWord = '\u0645\u0640\u0640\u0646';
      const ilaWord = '\u0625\u0644\u0640\u0640\u0649';
      return `${phrase} (${minWord} ${formatFr(dStart)} ${ilaWord} ${formatFr(dEnd)})`;
    };
   

     const computeDureeAr = (): string => { return computeDureeAr2();
    };

    // Details block (RTL on the right side of the page)
    // Dynamic details block sizing based on content (keeps inside page borders)
    const gap = 10;
    const marginRight = 40;
    const leftMargin = 40;
    let y = boxY + boxH + 20;
    const lineH = 30;
    // Text measurement helpers: measure visual width to size blocks accurately
    let __measureCtx: CanvasRenderingContext2D | null = null;
    const ensureMeasureCtx = () => {
      if (typeof document === 'undefined') return null;
      if (__measureCtx) return __measureCtx;
      try {
        const cnv = document.createElement('canvas');
        const ctx = cnv.getContext('2d');
        if (ctx) __measureCtx = ctx;
      } catch {}
      return __measureCtx;
    };
    const measureTextWidth = (text: string, fontSize: number, fontFamily?: string): number => {
      const s = String(text || '');
      if (!s) return 0;
      const ctx = ensureMeasureCtx();
      if (ctx) {
        try {
          const fam = fontFamily || ARABIC_FONTS[0] || 'Arial';
          // Quote family to handle spaces; use normal weight/style to match Konva default
          ctx.font = `${Math.max(10, fontSize)}px "${fam}"`;
          const m = ctx.measureText(s);
          // Use measured width only to avoid double-counting bounding boxes
          const w = m.width || 0;
          return Math.ceil(w);
        } catch {}
      }
      // Fallback heuristic (slightly narrower to avoid overestimation)
      const isArabic = /[\u0600-\u06FF]/.test(s);
      const factor = isArabic ? 0.48 : 0.42;
      return Math.ceil(s.length * fontSize * factor);
    };
    const statutJuridique = pickFirstNonEmpty(
      (initialData?.detenteur as any)?.StatutJuridique?.StatutArab,
      (initialData?.detenteur as any)?.StatutArab,
      (initialData?.detenteur as any)?.statut?.StatutArab,
      (initialData?.detenteur as any)?.statut?.statut_ar,
    );
    const statutArabic = pickFirstNonEmpty(
      (data as any)?.statut_ar_label,
      (data?.detenteur as any)?.StatutJuridique?.StatutArab,
      (data?.detenteur as any)?.StatutArab,
      (data?.detenteur as any)?.statut?.statut_ar,
      (initialData?.detenteur as any)?.StatutJuridique?.StatutArab,
      (initialData?.detenteur as any)?.StatutArab,
      (initialData?.detenteur as any)?.statut?.StatutArab,
      (initialData?.detenteur as any)?.statut?.statut_ar,
    );
    const beneficiaryDisplay = [statutArabic, holderDisplay].filter(Boolean).join(' ').trim();
    const detenteurArabic = beneficiaryDisplay || holderDisplay;



    const estimateWidth = (text: string, fontSize: number) => measureTextWidth(text, fontSize, ARABIC_FONTS[0]) + 6;
    const labelGrant = 'يُمنـــح إلى:';
    const value1Display = [statutArabic, (holderDisplay || '')].filter(Boolean).join(' ');
   // const label1 = 'يُمنـــح إلى: الشركة ذات المسؤولية المحدودة';
   // const value1 = detenteurArabic || detenteur || '';
    const label2 = 'الموقع: ولايــة'; const value2 = localisation || '';
    const label3 = 'المساحة:'; const value3 = superficie ? `${superficie} هكتار` : '';
    const label4 = 'المــدة:'; const value4 = '';
    const sepRowLabelW = estimateWidth(labelGrant, 26);
    const sepRowValueW = estimateWidth(value1Display, 26);
    const inlineW2 = estimateWidth(`${label2} ${value2}`, 24);
    const inlineW3 = estimateWidth(`${label3} ${value3}`, 24);
    const inlineW4 = estimateWidth(`${label4} 00/00/0000 إلى 00/00/0000`, 24);
    const desiredW = Math.max(sepRowLabelW + gap + sepRowValueW, inlineW2, inlineW3, inlineW4);
    const maxBlockW = DEFAULT_CANVAS.width - (leftMargin + marginRight);
    const detailsBlockW = Math.min(Math.max(300, desiredW), maxBlockW);
    // Allocate label/value widths within the available space (keep alignment)
    const minValueW = 220;
    let valueW = Math.max(minValueW, Math.min(sepRowValueW, detailsBlockW - gap - 160));
    let labelW = Math.max(160, detailsBlockW - gap - valueW);
    // Ensure first label fits on a single line
    if (labelW < sepRowLabelW) {
      labelW = sepRowLabelW;
      valueW = Math.max(120, detailsBlockW - gap - labelW);
    }
    // startX anchors the whole block to the right margin
    const startX = DEFAULT_CANVAS.width - marginRight - detailsBlockW;
    const estimateHeight = (text: string, widthPx: number, fontSize: number, lineHeight: number): number => {
      const s = String(text || '');
      if (!s) return fontSize * lineHeight;
      const avgCharW = fontSize * 0.42;
      const perLine = Math.max(1, Math.floor((widthPx - 2) / Math.max(1, avgCharW)));
      const parts = s.split(/\r?\n/);
      let lines = 0;
      parts.forEach(p => { lines += Math.ceil(Math.max(1, p.trim().length) / perLine); });
      return Math.max(fontSize * lineHeight, lines * fontSize * lineHeight);
    };
    const addRow = (value: string) => {
      const fz = 26; const lh = 1.35;
      // Unified details renderer in use; disable legacy row rendering
      return;
      // if (labelAr === label1) {
      //   // Compute exact adjacency: label flush-right, value ends at (label start - gap)
      //   const blockRight = startX + detailsBlockW;
      //   const lblW = Math.max(1, measureTextWidth(labelAr, fz, ARABIC_FONTS[0]));
      //   const lblX = blockRight - lblW;
      //   const valW = Math.max(120, lblX - gap - startX);
      //   // Let value wrap within its column starting at startX; label stays single-line
      //   const hVal = estimateHeight(value || '....................', valW, fz, lh);
      //   const rowH = Math.max(hVal, fz * lh);
      //   els.push({ id: uuidv4(), type: 'text', x: startX, y, width: valW, text: value || '....................', language: 'ar', direction: 'rtl', fontSize: fz, fontFamily: ARABIC_FONTS[0], color: '#000', draggable: true, textAlign: 'right', wrap: 'word', lineHeight: lh });
      //   els.push({ id: uuidv4(), type: 'text', x: lblX, y, width: lblW, text: labelAr, language: 'ar', direction: 'rtl', fontSize: fz, fontFamily: ARABIC_FONTS[0], color: '#000', draggable: true, textAlign: 'right', wrap: 'none', lineHeight: lh });
      //   y += Math.ceil(rowH) + 6;
      // } else {
      //   const hVal = estimateHeight(value || '....................', valueW, fz, lh);
      //   const hLbl = estimateHeight(labelAr, labelW, fz, lh);
      //   const rowH = Math.max(hVal, hLbl);
      //   els.push({ id: uuidv4(), type: 'text', x: startX, y, width: valueW, text: value || '....................', language: 'ar', direction: 'rtl', fontSize: fz, fontFamily: ARABIC_FONTS[0], color: '#000', draggable: true, textAlign: 'right', wrap: 'word', lineHeight: lh });
      //   els.push({ id: uuidv4(), type: 'text', x: startX + valueW + gap, y, width: labelW, text: labelAr, language: 'ar', direction: 'rtl', fontSize: fz, fontFamily: ARABIC_FONTS[0], color: '#000', draggable: true, textAlign: 'right', wrap: 'word', lineHeight: lh });
       
      // }
    };
    // Inline row: label and value as one contiguous text (no gap after ':')
    const addInlineRow = (labelAr: string, value: string) => {
      const full = `${labelAr} ${value || '....................'}`.trim();
      const fz = 24; const lh = 1.35;
      // Unified details renderer in use; disable legacy inline row rendering
      return;
      const h = estimateHeight(full, detailsBlockW, fz, lh);
      // Allow wrapping within details block width
      els.push({ id: uuidv4(), type: 'text', x: startX, y, width: detailsBlockW, text: full, language: 'ar', direction: 'rtl', fontSize: fz, fontFamily: ARABIC_FONTS[0], color: '#000', draggable: true, textAlign: 'right', wrap: 'word', lineHeight: lh });
      y += Math.ceil(h) + 6;
    };
    // Details: unified block for labels/values (single multi-line text)
const detailsFontSize = 26;
    const detailsLineHeight = 1.55;
    const dotPlaceholder = '....................';
    // Substance (Arabic) from server data
    const substanceAr = pickFirstNonEmpty(
      (data as any)?.substance_ar,
      (data as any)?.SubstancesArabe,
      (data as any)?.substances_arabe,
      (data as any)?.SubstancesAR,
      (initialData as any)?.substance_ar,
      (initialData as any)?.SubstancesArabe,
      (initialData as any)?.substances_arabe,
      (initialData as any)?.SubstancesAR,
    );
   // const label1Unified = 'يُمنـــح إلى: الشركة ذات المسؤولية المحدودة';
    const label2Unified = 'الموقع: ولايــة';
    const label3Unified = 'المساحة:';
    const labelSubUnified = 'المــادة المرخّصة:';
    const label4Unified = 'المــدة:';
    const linesUnified = [
      `${labelGrant} ${value1Display || dotPlaceholder}`.trim(),
      `${label2Unified} ${localisation || dotPlaceholder}`.trim(),
      `${label3Unified} ${superficie ? `${superficie} هكتار` : dotPlaceholder}`.trim(),
      `${labelSubUnified} ${substanceAr || dotPlaceholder}`.trim(),
      `${label4Unified} ${computeDureeAr() || dotPlaceholder}`.trim(),
    ];
    const maxBlockW2 = DEFAULT_CANVAS.width - (leftMargin + marginRight);
    const maxLineW2 = Math.max(0, ...linesUnified.map(l => measureTextWidth(l, detailsFontSize, ARABIC_FONTS[0])));
    const detailsBlockW2 = Math.min(Math.max(300, maxLineW2 + 12), maxBlockW2);
    const startX2 = DEFAULT_CANVAS.width - marginRight - detailsBlockW2;
    const detailsTextUnified = linesUnified.join('\n');
    const blockH2 = estimateHeight(detailsTextUnified, detailsBlockW2, detailsFontSize, detailsLineHeight);
    els.push({ id: uuidv4(), type: 'text', x: startX2, y, width: detailsBlockW2, text: detailsTextUnified, language: 'ar', direction: 'rtl', fontSize: detailsFontSize, fontFamily: ARABIC_FONTS[0], color: '#000', draggable: true, textAlign: 'right', wrap: 'word', lineHeight: detailsLineHeight });
    y += Math.ceil(blockH2) + 6;
    // Beneficiary: first the legal form, then the actual holder on a separate line
    addRow( detenteurArabic||detenteur);
    addInlineRow('الموقع:', localisation);
    addInlineRow('المساحة:', superficie ? `${superficie} هكتار` : '');
    // Duration row: compute from DateOctroi -> DateExpiration when available, else use server-provided display   
    addInlineRow('المــدة:', computeDureeAr());
    try {
      const MIN_ROWS = rowsPerColSetting;
      const coords: any[] = Array.isArray(data?.coordinates) ? data.coordinates : [];
      const desiredCols = Math.max(1, Math.ceil(Math.max(coords.length, MIN_ROWS) / MIN_ROWS));
      const blockCols = Math.min(3, desiredCols);
      // Columns (left to right): ع (X), س (Y), النقطة (Point)
      const baseColWidths = [90, 90, 60];
      // Exact rows per column to match dataset size (no extra rows)
      const rowsPerColExact = Math.max(1, Math.min(60, Math.ceil((coords.length || 1) / blockCols)));
      const headerText = 'إحـداثيات قمم المحيط المنجمي حسب نظام UTM شمال الصحراء';
      const headerH = 48;
      const rowH = 34;
      const blockW = baseColWidths.reduce((a, b) => a + b, 0);
      const tableWAvail = DEFAULT_CANVAS.width - 80;
      const tableW = Math.min(tableWAvail, blockW * Math.max(blockCols, 2));
      const headerY = y + 40;
      const tableH = headerH + rowH * (rowsPerColExact + 1);
      const tableEl: PermisElement = {
        id: uuidv4(),
        type: 'table',
        x: (DEFAULT_CANVAS.width - tableW) / 2,
        y: headerY,
        width: tableW,
        height: tableH,
        draggable: true,
        stroke: '#000',
        strokeWidth: 1.2,
        headerText,
        headerHeight: headerH,
        headerFill: '#f5f5f5',
        altRowFill: '#f8f8f8',
        rowHeight: rowH,
        rowsPerCol: rowsPerColExact,
        blockCols,
        colWidths: baseColWidths,
        tableFontFamily: ARABIC_FONTS[0],
        tableFontSize: 14,
        tableTextAlign: 'right',
        headerTextAlign: 'center',
        showCellBorders: true,
        tableGridColor: '#4F4040',
        tableGridWidth: 1,
        outerBorderColor: '#000',
        outerBorderWidth: 1.2,
        cellPadding: 8,
        tableColumns: [
          { key: 'x', title: 'ع', width: baseColWidths[0], align: 'right' },
          { key: 'y', title: 'س', width: baseColWidths[1], align: 'right' },
          { key: 'point', title: 'النقطة', width: baseColWidths[2], align: 'right' },
        ],
        tableData: coords.map((c: any, idx: number) => ({ x: String(c.x ?? ''), y: String(c.y ?? ''), point: String(idx + 1) })),
      } as any;
      els.push(tableEl as any);
      // Add bottom title inside double lines just below the table
      try {
        // Use a centered band that is 90% of the page width
        const bandW = Math.floor(DEFAULT_CANVAS.width * 0.8);
        const bandX = Math.floor((DEFAULT_CANVAS.width - bandW) / 2);
        const baseY = (tableEl as any).y + (tableEl as any).height + 8;
        const dblGap = 3;
        const titleFontSize = 32;
        const titlePadding = 8;
        const topY1 = baseY;
        const topY2 = topY1 + dblGap;
        const textY = topY2 + titlePadding;
        const bottomY1 = textY + titleFontSize + 6;
        const bottomY2 = bottomY1 + dblGap;
        const bottomNotice = `سند منجمي مسجل في السجل المنجمي تحت رقم : ${LRM}${code} ${LRM}${typeCode}`;
        // Double top lines
        els.push({ id: uuidv4(), type: 'line', x: bandX, y: topY1, width: bandW, height: 0, stroke: '#000', strokeWidth: 1.2, draggable: true } as any);
        els.push({ id: uuidv4(), type: 'line', x: bandX, y: topY2, width: bandW, height: 0, stroke: '#000', strokeWidth: 1.2, draggable: true } as any);
        // Title text centered between doubles
        const noticeText = `سند منجمي مسجل في السجل المنجمي تحت رقم : ${LRM}${code} ${LRM}${typeCode}`;
        const noticeFontSize = 32;
    els.push({ id: uuidv4(), type: 'text', x: bandX, y: textY, width: bandW, text: noticeText, language: 'ar', direction: 'rtl', fontSize: noticeFontSize, fontFamily: ARABIC_FONTS[0], color: '#000', draggable: true, textAlign: 'center' } as any);
        
        // Dynamic diagonal line starting from the left edge of the page band under the sentence to the bottom-left corner
        try {
          // Start at the beginning X of the band (page content start) just beneath the text baseline
          const xStartDiag = 830; // left boundary of the centered text band
          const yStartDiag = textY + noticeFontSize * 2; // approximate baseline
          const xDestDiag = 30; // inside left page border
          const yDestDiag = DEFAULT_CANVAS.height - 20; // inside bottom page border
          const dxDiag = xDestDiag - xStartDiag;
          const dyDiag = yDestDiag - yStartDiag;
          const angleDiag = Math.atan2(dyDiag, dxDiag) * 180 / Math.PI;
          const lenDiag = Math.hypot(dxDiag, dyDiag);
          els.push({
            id: uuidv4(),
            type: 'line',
            x: xStartDiag,
            y: yStartDiag,
            width: lenDiag,
            height: 0,
            rotation: angleDiag,
            stroke: '#999999',
            strokeWidth: 2,
            opacity: 0.9,
            draggable: true
          } as any);
        } catch {}
        // Double bottom lines
        els.push({ id: uuidv4(), type: 'line', x: bandX, y: bottomY1, width: bandW, height: 0, stroke: '#000', strokeWidth: 1.2, draggable: true } as any);
        els.push({ id: uuidv4(), type: 'line', x: bandX, y: bottomY2, width: bandW, height: 0, stroke: '#000', strokeWidth: 1.2, draggable: true } as any);
      } catch {}
      return els;
    } catch {}

    // Multi-column coordinates table (colors/separators like the model)
    let tableX = 40;
    // Columns (left to right): ع (X), س (Y), النقطة (Point)
    let colPtW = 90, colXW = 90, colYW = 60;
    let blockW = colPtW + colXW + colYW; // 240
    const baseBlockW = blockW;
    const MIN_ROWS = rowsPerColSetting;
    const coords: any[] = Array.isArray(data?.coordinates) ? data.coordinates : [];
    // Compute columns: add a new column every 10 rows, cap to 3 to fit width
    const desiredCols = Math.max(1, Math.ceil(Math.max(coords.length, MIN_ROWS) / MIN_ROWS));
    const blockCols = Math.min(3, desiredCols);
    // Make the table fill the page width proportionally when there are few coordinates
    const tableMarginX = 40;
    const tableWAvail = DEFAULT_CANVAS.width - tableMarginX * 2;
    const perBlockWidth = tableWAvail / blockCols;
    const scaleW = perBlockWidth / baseBlockW;
    const scaleClamped = Math.max(1, Math.min(1.35, scaleW));
    colPtW = Math.round(colPtW * scaleClamped);
    colXW = Math.round(colXW * scaleClamped);
    colYW = Math.round(colYW * scaleClamped);
    blockW = colPtW + colXW + colYW;
    const tableW = blockW * blockCols;
    tableX = (DEFAULT_CANVAS.width - tableW) / 2;
    const headerH = 48;
    const headerY = y + 40;
    els.push({ id: uuidv4(), type: 'rectangle', x: tableX, y: headerY, width: tableW, height: headerH, stroke: '#000', strokeWidth: 1.2, fill: '#f5f5f5' } as any);
    els.push({ id: uuidv4(), type: 'text', x: tableX + 6, y: headerY + 4, width: tableW - 12, text: 'إحـداثيات قمم المحيط المنجمي حسب نظام UTM شمال الصحراء — Fuseau 32', language: 'ar', direction: 'rtl', fontSize: 20, fontFamily: ARABIC_FONTS[0], color: '#000', draggable: true, textAlign: 'center' } as any);

    // Multi-column coordinates table (colors/separators like the model)
    const tableStartY = headerY + headerH;
    const rowH = Math.round(34 * scaleClamped);
    const rowsPerCol = Math.max(MIN_ROWS, Math.min(40, Math.ceil((coords.length || MIN_ROWS) / blockCols)));

    // Draw outer border for each block and header background; alternate row shading
    for (let b = 0; b < blockCols; b++) {
      const bx = tableX + b * blockW;
      // Header background
      els.push({ id: uuidv4(), type: 'rectangle', x: bx, y: tableStartY, width: blockW, height: rowH, stroke: '#000', strokeWidth: 1.2, fill: '#eeeeee', meta: { nonInteractive: true, isGrid: true } } as any);
      // Internal vertical separators (use rotation=90 because our Line renderer ignores height)
      const vHeight = rowH * (rowsPerCol + 1);
      els.push({ id: uuidv4(), type: 'line', x: bx + colPtW, y: tableStartY, width: vHeight, height: 0, rotation: 90, stroke: '#4F4040', strokeWidth: 1, meta: { nonInteractive: true, isGrid: true } } as any);
      els.push({ id: uuidv4(), type: 'line', x: bx + colPtW + colXW, y: tableStartY, width: vHeight, height: 0, rotation: 90, stroke: '#4F4040', strokeWidth: 1, meta: { nonInteractive: true, isGrid: true } } as any);
      // Column headers
      els.push({ id: uuidv4(), type: 'text', x: bx + 8, y: tableStartY + 4, width: colPtW - 16, text: 'ع', fontSize: 14, fontFamily: ARABIC_FONTS[0], color: '#000', draggable: true, textAlign: 'right' } as any);
      els.push({ id: uuidv4(), type: 'text', x: bx + colPtW + 8, y: tableStartY + 4, width: colXW - 16, text: 'س', fontSize: 14, fontFamily: ARABIC_FONTS[0], color: '#000', draggable: true, textAlign: 'right' } as any);
      els.push({ id: uuidv4(), type: 'text', x: bx + colPtW + colXW + 8, y: tableStartY + 4, width: colYW - 16, text: 'النقطة', fontSize: 14, fontFamily: ARABIC_FONTS[0], color: '#000', draggable: true, textAlign: 'right' } as any);

      // Rows
      for (let r = 0; r < rowsPerCol; r++) {
        const idx = b * rowsPerCol + r;
        const ry = tableStartY + rowH * (r + 1);
        // Alternate background
        const fill = r % 2 === 0 ? '#f8f8f8' : '#ffffff';
        els.push({ id: uuidv4(), type: 'rectangle', x: bx, y: ry, width: blockW, height: rowH, stroke: '#4F4040', strokeWidth: 0.5, fill, meta: { nonInteractive: true, isGrid: true } } as any);
        // Draw a thin border for each cell so every value is inside a bounded box
        let cxCell = bx;
        const cellWidths = [colPtW, colXW, colYW];
        for (let c = 0; c < cellWidths.length; c++) {
          const cw = cellWidths[c];
          els.push({ id: uuidv4(), type: 'rectangle', x: cxCell, y: ry, width: cw, height: rowH, stroke: '#4F4040', strokeWidth: 0.8, fill: 'transparent', meta: { nonInteractive: true, isGrid: true } } as any);
          cxCell += cw;
        }
        if (idx < coords.length) {
          const item = coords[idx];
          const vPoint = String(idx + 1);
          const vX = String(item.x ?? '');
          const vY = String(item.y ?? '');
          // Left to right columns: ع (X), س (Y), النقطة (Point)
          els.push({ id: uuidv4(), type: 'text', x: bx + 8, y: ry + 4, width: colPtW - 16, text: vX, fontSize: 14, fontFamily: 'Arial', color: '#000', draggable: true, textAlign: 'right' } as any);
          els.push({ id: uuidv4(), type: 'text', x: bx + colPtW + 8, y: ry + 4, width: colXW - 16, text: vY, fontSize: 14, fontFamily: 'Arial', color: '#000', draggable: true, textAlign: 'right' } as any);
          els.push({ id: uuidv4(), type: 'text', x: bx + colPtW + colXW + 8, y: ry + 4, width: colYW - 16, text: vPoint, fontSize: 14, fontFamily: 'Arial', color: '#000', draggable: true, textAlign: 'right' } as any);
        }
        // Horizontal subtle grid line
        els.push({ id: uuidv4(), type: 'line', x: bx, y: ry, width: blockW, height: 0, stroke: '#4F4040', strokeWidth: 1, meta: { nonInteractive: true, isGrid: true } } as any);
      }
      // Outer border box
      els.push({ id: uuidv4(), type: 'rectangle', x: bx, y: tableStartY, width: blockW, height: rowH * (rowsPerCol + 1), stroke: '#000', strokeWidth: 1.2, fill: 'transparent', meta: { nonInteractive: true, isGrid: true } } as any);
    }
    
    
    // Compute bottom of table and optionally add a 45° line if space remains
    const tableBottom = tableStartY + rowH * (rowsPerCol + 1);
    const remainingBelow = DEFAULT_CANVAS.height - tableBottom - 70;
    let diagY: number | null = null;
    
    if (remainingBelow > 80) {
      const diagLen = Math.min(DEFAULT_CANVAS.width - 160, remainingBelow + 40);
      diagY = tableBottom + 30;
      els.push({ id: uuidv4(), type: 'line', x: DEFAULT_CANVAS.width - 80, y: diagY +30, width: 600, height: 0, rotation: 135, stroke: '#999999', strokeWidth: 2, opacity: 0.8, draggable: true } as any);
    }

    // Bottom notice, centered near page bottom
    try {
      const typeCode = data?.typePermis?.code || '';
      const code = data?.code_demande || data?.codeDemande || '';
      const bottomNotice = `سند منجمي مسجل في السجل المنجمي تحت رقم : ${LRM}${code} ${LRM}${typeCode}`;
      // Bottom notice band is always 90% of page width
      const noticeWidth = Math.floor(DEFAULT_CANVAS.width * 0.9);
      const noticeX = (DEFAULT_CANVAS.width - noticeWidth) / 2;
      const noticeY = DEFAULT_CANVAS.height - 60;
      els.push({
        id: uuidv4(),
        type: 'text',
        x: noticeX,
        y: noticeY,
        width: noticeWidth,
        text: bottomNotice,
        language: 'ar',
        direction: 'rtl',
        fontSize: 32,
        fontFamily: ARABIC_FONTS[0],
        color: '#000',
        draggable: true,
        textAlign: 'center',
        opacity: 1,
        wrap: 'none',
        lineHeight: 1.2,
        meta: { isFooter: true }
      } as any);

      // Dynamic diagonal line starting from the end of the bottom notice text to the bottom-left corner
      try {
        const noticeFontSize = 32;
        const centerX = noticeX + noticeWidth / 2;
        const textW = Math.max(1, measureTextWidth(bottomNotice, noticeFontSize, ARABIC_FONTS[0]));
        // Arabic text logical end sits on the left-most edge when centered
        const xStart = centerX - textW / 2 - 4; // small visual padding
        const yStart = noticeY + noticeFontSize * 0.7; // approximate baseline
        const xDest = 20; // inside left border
        const yDest = DEFAULT_CANVAS.height - 20; // inside bottom border
        const dx = xDest - xStart;
        const dy = yDest - yStart;
        const angle = Math.atan2(dy, dx) * 180 / Math.PI;
        const len = Math.hypot(dx, dy);
        els.push({
          id: uuidv4(),
          type: 'line',
          x: xStart,
          y: yStart,
          width: len,
          height: 0,
          rotation: angle,
          stroke: '#999999',
          strokeWidth: 2,
          opacity: 0.9,
          draggable: true,
          meta: { isFooter: true }
        } as any);
      } catch {}
    } catch {}

    // QR is already placed inside the title box above
  
    return els;
  }

  function createArticlesPageHeader(width: number, height: number): PermisElement[] {
    const contentElements: PermisElement[] = [
     
      
    ];
    return [...contentElements];
  }

  const elements = pages[currentPage];
  const setElementsForCurrent = useCallback((updater: (prev: PermisElement[]) => PermisElement[]) => {
    setPages(prev => {
      const next = [...prev] as PermisPages;
      next[currentPage] = updater(prev[currentPage]);
      pushHistory(next);
      return next;
    });
  }, [currentPage, pushHistory]);

  const handleStageClick = useCallback((e: any) => {
    if (isSelecting) return;
    // Click empty area: clear selection
    if (e.target === e.currentTarget) {
      setSelectedIds([]);
      setTool('select');
      return;
    }
    // Find nearest ancestor with an element id
    let node = e.target;
    let foundId: string | null = null;
    while (node && node !== e.currentTarget) {
      const nid = node.getAttr?.('id');
      if (nid && elements.some(el => el.id === nid)) {
        foundId = nid;
        break;
      }
      node = node.getParent ? node.getParent() : null;
    }
    if (!foundId) return;
    const el = elements.find(x => x.id === foundId);
    // Do not allow selecting decorative borders
    if (el?.meta?.isBorder) return;
    if (e.evt?.shiftKey) setSelectedIds(prev => (prev.includes(foundId!) ? prev : [...prev, foundId!]));
    else setSelectedIds([foundId!]);
  }, [elements, isSelecting]);

  // Helpers for marquee selection
  const getPointerWorldPos = useCallback(() => {
    const stage = stageRef.current;
    if (!stage) return { x: 0, y: 0 } as { x: number; y: number };
    const p = stage.getPointerPosition() || { x: 0, y: 0 };
    const sx = stage.scaleX?.() || zoom || 1;
    const sy = stage.scaleY?.() || zoom || 1;
    return { x: p.x / sx, y: p.y / sy };
  }, [zoom]);

  const rectFromPoints = (a: { x: number; y: number }, b: { x: number; y: number }) => {
    const x = Math.min(a.x, b.x);
    const y = Math.min(a.y, b.y);
    const width = Math.abs(a.x - b.x);
    const height = Math.abs(a.y - b.y);
    return { x, y, width, height };
  };

  const intersects = (r: { x: number; y: number; width: number; height: number }, s: { x: number; y: number; width: number; height: number }) => {
    return !(r.x + r.width < s.x || s.x + s.width < r.x || r.y + r.height < s.y || s.y + s.height < r.y);
  };

  const computeSelectedInRect = useCallback((sel: { x: number; y: number; width: number; height: number }) => {
    const stage = stageRef.current;
    if (!stage) return [] as string[];
    const ids: string[] = [];
    const current = pages[currentPage] || [];
    current.forEach(el => {
      if ((el as any).meta?.isBorder || (el as any).meta?.nonInteractive || (el as any).meta?.isGrid) return;
      const node = stage.findOne(`#${el.id}`);
      if (!node || !node.getClientRect) return;
      try {
        const rect = node.getClientRect({ skipShadow: true, skipStroke: true });
        const sX = stage.scaleX?.() || 1;
        const sY = stage.scaleY?.() || 1;
        const nr = { x: rect.x / sX, y: rect.y / sY, width: rect.width / sX, height: rect.height / sY };
        if (intersects(sel, nr)) ids.push(el.id);
      } catch {}
    });
    return ids;
  }, [pages, currentPage]);

  const handleStageMouseDown = useCallback((e: any) => {
    const stage = stageRef.current;
    if (!stage) return;
    if (e.target !== e.currentTarget) return;
    const start = getPointerWorldPos();
    selectionStartRef.current = start;
    setIsSelecting(true);
    setSelectionRect({ x: start.x, y: start.y, width: 0, height: 0 });
    if (e.evt?.shiftKey) {
      initialSelectionIdsRef.current = selectedIds;
    } else {
      initialSelectionIdsRef.current = [];
      setSelectedIds([]);
    }
  }, [getPointerWorldPos, selectedIds]);

  const handleStageMouseMove = useCallback(() => {
    if (!isSelecting || !selectionStartRef.current) return;
    const pos = getPointerWorldPos();
    const rect = rectFromPoints(selectionStartRef.current, pos);
    setSelectionRect(rect);
    const ids = computeSelectedInRect(rect);
    const combined = Array.from(new Set([...(initialSelectionIdsRef.current || []), ...ids]));
    setSelectedIds(combined);
  }, [isSelecting, getPointerWorldPos, computeSelectedInRect]);

  const handleStageMouseUp = useCallback(() => {
    if (!isSelecting) return;
    if (selectionStartRef.current) {
      const pos = getPointerWorldPos();
      const rect = rectFromPoints(selectionStartRef.current, pos);
      const ids = computeSelectedInRect(rect);
      const combined = Array.from(new Set([...(initialSelectionIdsRef.current || []), ...ids]));
      setSelectedIds(combined);
    }
    selectionStartRef.current = null;
    setIsSelecting(false);
    setSelectionRect(null);
  }, [isSelecting, getPointerWorldPos, computeSelectedInRect]);

  // Direct element click handler (used by element nodes)
  const handleElementClick = useCallback((e: any) => {
    const nid = e?.currentTarget?.getAttr?.('id') || e?.target?.getAttr?.('id');
    if (!nid) return;
    const el = elements.find(x => x.id === nid);
    if (el?.meta?.isBorder) return;
    setSelectedIds([nid]);
  }, [elements]);

  const handleDragEnd = useCallback((e: any) => {
    const id = e.target.getAttr('id');
    if (!id) return;
    const node = e.target;
    const nx = node.x();
    const ny = node.y();
    setElementsForCurrent(prev => prev.map(el => (el.id === id ? { ...el, x: nx, y: ny } : el)));
  }, [setElementsForCurrent]);

  const handleTransformEnd = useCallback(() => {
    const nodes = transformerRef.current?.nodes?.() || [];
    setElementsForCurrent(prev => {
      const updated = [...prev];
      nodes.forEach((node: any) => {
        const id = node.getAttr('id');
        const i = updated.findIndex(el => el.id === id);
        if (i >= 0) {
          const className = node.getClassName ? node.getClassName() : '';
          if (className === 'Line') {
            try {
              // compute new length for lines in world space then map back to element width
              const pts = node.points ? node.points() : [0, 0, node.width() || 0, 0];
              const x1 = pts[0] || 0, y1 = pts[1] || 0, x2 = pts[2] || 0, y2 = pts[3] || 0;
              const t = node.getAbsoluteTransform ? node.getAbsoluteTransform() : null;
              let newLen = (node.width() || 0) * (node.scaleX() || 1);
              if (t && t.point) {
                const p1 = t.point({ x: x1, y: y1 });
                const p2 = t.point({ x: x2, y: y2 });
                newLen = Math.hypot(p2.x - p1.x, p2.y - p1.y);
              }
              updated[i] = {
                ...updated[i],
                x: node.x(),
                y: node.y(),
                width: newLen,
                // preserve existing height for lines (stroke thickness is controlled via strokeWidth)
                scaleX: 1,
                scaleY: 1,
                rotation: node.rotation(),
              } as PermisElement;
            } catch {
              updated[i] = {
                ...updated[i],
                x: node.x(),
                y: node.y(),
                width: (node.width() || 0) * (node.scaleX() || 1),
                scaleX: 1,
                scaleY: 1,
                rotation: node.rotation(),
              } as PermisElement;
            }
          } else {
            // Prefer reliable geometry from client rect, which accounts for Groups and scaled nodes
            let newW = 0;
            let newH = 0;
            try {
              if (node.getClientRect) {
                const rect = node.getClientRect({ skipShadow: true, skipStroke: true });
                const sX = stageRef.current?.scaleX?.() || 1;
                const sY = stageRef.current?.scaleY?.() || 1;
                newW = Math.max(1, rect.width / sX);
                newH = Math.max(1, rect.height / sY);
              }
            } catch {}
            // Fallback to width/height * scale if needed
            if (!newW || !newH) {
              newW = (node.width?.() || 0) * (node.scaleX?.() || 1) || newW;
              newH = (node.height?.() || 0) * (node.scaleY?.() || 1) || newH;
            }
            updated[i] = {
              ...updated[i],
              x: node.x(),
              y: node.y(),
              width: newW || updated[i].width,
              height: newH || updated[i].height,
              scaleX: 1,
              scaleY: 1,
              rotation: node.rotation(),
            } as PermisElement;
          }
        }
        if (node.scaleX) node.scaleX(1);
        if (node.scaleY) node.scaleY(1);
      });
      return updated;
    });
  }, [setElementsForCurrent]);

  // Live update sizes/positions during transform so properties reflect immediately
  const handleTransform = useCallback(() => {
    const nodes = transformerRef.current?.nodes?.() || [];
    setPages(prev => {
      const next = [...prev] as PermisPages;
      const arr = [...next[currentPage]];
      nodes.forEach((node: any) => {
        const id = node.getAttr('id');
        const idx = arr.findIndex(el => el.id === id);
        if (idx < 0) return;
        const className = node.getClassName ? node.getClassName() : '';
        if (className === 'Line') {
          try {
            const pts = node.points ? node.points() : [0, 0, node.width() || 0, 0];
            const x1 = pts[0] || 0, y1 = pts[1] || 0, x2 = pts[2] || 0, y2 = pts[3] || 0;
            const t = node.getAbsoluteTransform ? node.getAbsoluteTransform() : null;
            let newLen = (node.width() || 0) * (node.scaleX() || 1);
            if (t && t.point) {
              const p1 = t.point({ x: x1, y: y1 });
              const p2 = t.point({ x: x2, y: y2 });
              newLen = Math.hypot(p2.x - p1.x, p2.y - p1.y);
            }
            arr[idx] = {
              ...arr[idx],
              x: node.x(),
              y: node.y(),
              width: Math.max(1, newLen),
              rotation: node.rotation(),
            } as PermisElement;
            // Reset scale during live transform to bake the change into width
            if (node.scaleX) node.scaleX(1);
            if (node.scaleY) node.scaleY(1);
          } catch {}
        } else {
          let newW = 0;
          let newH = 0;
          try {
            if (node.getClientRect) {
              const rect = node.getClientRect({ skipShadow: true, skipStroke: true });
              const sX = stageRef.current?.scaleX?.() || 1;
              const sY = stageRef.current?.scaleY?.() || 1;
              newW = Math.max(1, rect.width / sX);
              newH = Math.max(1, rect.height / sY);
            }
          } catch {}
          if (!newW || !newH) {
            newW = (node.width?.() || 0) * (node.scaleX?.() || 1) || newW;
            newH = (node.height?.() || 0) * (node.scaleY?.() || 1) || newH;
          }
          arr[idx] = {
            ...arr[idx],
            x: node.x(),
            y: node.y(),
            width: newW || arr[idx].width,
            height: newH || arr[idx].height,
            rotation: node.rotation(),
          } as PermisElement;
          if (node.scaleX) node.scaleX(1);
          if (node.scaleY) node.scaleY(1);
        }
      });
      next[currentPage] = arr;
      return next;
    });
  }, [currentPage]);

  const handleTextChange = useCallback((id: string, newText: string) => {
    setElementsForCurrent(prev => prev.map(el => (el.id === id && el.type === 'text' ? { ...el, text: newText } : el)));
  }, [setElementsForCurrent]);

  const handleAddElement = useCallback((type: 'text' | 'rectangle' | 'image' | 'line') => {
    const newElement: PermisElement = {
      id: uuidv4(),
      pageIndex: currentPage,
      type,
      x: 100 / zoom,
      y: 100 / zoom,
      width: type === 'text' ? 240 : type === 'line' ? 150 : 120,
      height: type === 'text' ? 36 : type === 'line' ? 2 : 60,
      text: type === 'text' ? (currentPage === PAGES.ARTICLES ? '?†?µ ?…?§?¯?©' : '?†?µ ?¬?¯???¯') : undefined,
      language: type === 'text' ? (currentPage === PAGES.ARTICLES ? 'ar' : 'fr') : undefined,
      direction: type === 'text' ? (currentPage === PAGES.ARTICLES ? 'rtl' : 'ltr') : undefined,
      fontSize: 20,
      fontFamily: type === 'text' ? (currentPage === PAGES.ARTICLES ? ARABIC_FONTS[0] : FONT_FAMILIES[0]) : FONT_FAMILIES[0],
      color: '#101822',
      draggable: true,
      fill: type === 'rectangle' ? '#ffffff' : undefined,
      stroke: type === 'rectangle' || type === 'line' ? '#000000' : undefined,
      strokeWidth: type === 'rectangle' || type === 'line' ? 1 : undefined,
      opacity: 1,
      rotation: 0,
      wrap: type === 'text' ? 'word' : undefined,
      lineHeight: type === 'text' ? 1.3 : undefined,
      textAlign: type === 'text' ? (currentPage === PAGES.ARTICLES ? 'right' : 'left') : undefined,
    };
    setElementsForCurrent(prev => [...prev, newElement]);
    setSelectedIds([newElement.id]);
    setTool('select');
  }, [zoom, currentPage, setElementsForCurrent]);

  const handleDeleteSelected = useCallback(() => {
    setElementsForCurrent(prev => prev.filter(el => !selectedIds.includes(el.id)));
    if (selectedIds.length > 0) toast.success('?‰l?©ments supprim?©s');
    setSelectedIds([]);
  }, [selectedIds, setElementsForCurrent]);

  const handleCopySelected = useCallback(() => {
    if (selectedIds.length === 0) return;
    const selectedElements = elements.filter(el => selectedIds.includes(el.id));
    localStorage.setItem('copiedElements', JSON.stringify(selectedElements));
    toast.success('?‰l?©ments copi?©s');
  }, [selectedIds, elements]);

  const handlePasteSelected = useCallback(() => {
    const copied = localStorage.getItem('copiedElements');
    if (!copied) return;
    try {
      const parsed: PermisElement[] = JSON.parse(copied);
      const newElements = parsed.map(el => ({ ...el, id: uuidv4(), x: (el.x + 20) / zoom, y: (el.y + 20) / zoom }));
      setElementsForCurrent(prev => [...prev, ...newElements]);
      setSelectedIds(newElements.map(el => el.id));
      toast.success('?‰l?©ments coll?©s');
    } catch (err) {
      console.error('Failed to paste elements', err);
    }
  }, [zoom, setElementsForCurrent]);

  const flattenWithPageIndex = useCallback((ps: PermisPages): PermisElement[] => {
    const out: PermisElement[] = [];
    ps.forEach((arr, pageIndex) => {
      arr.forEach(el => out.push({
        ...el,
        meta: {
          ...(el as any).meta,
          pageIndex
        }
      } as any));
    });
    return out;
  }, []);

  const handleSavePermisOnly = useCallback(async () => {
    if (savedPermisId) {
      toast.info('Permis already exists for this procedure');
      return;
    }
    setSavingPermis(true);
    try {
      const { id, code_permis } = await onSavePermis({
        id_demande: initialData.id_demande
      });
      setSavedPermisId(id);
      setPermisCode(code_permis);
      setLoadingTemplates(true);
      try {
        const templatesResponse = await axios.get(`${apiURL}/api/permis/${id}/templates`);
        setTemplates(Array.isArray(templatesResponse.data) ? templatesResponse.data : []);
      } catch (templateError) {
        console.error('Error loading templates after permis creation:', templateError);
      } finally {
        setLoadingTemplates(false);
      }
      toast.success('Permis saved successfully');
    } catch (error) {
      console.error('Failed to save permis', error);
      toast.error("?‰chec de l'enregistrement du permis");
    } finally {
      setSavingPermis(false);
    }
  }, [initialData, onSavePermis, savedPermisId, apiURL]);

  const handleSaveTemplateOnly = useCallback(async () => {
    let targetPermisId = savedPermisId;
    let targetPermisCode = permisCode;
    if (!targetPermisId && procedureId) {
      try {
        const response = await axios.get(`${apiURL}/api/permis/procedure/${procedureId}/permis`);
        if (response.data.exists) {
          targetPermisId = response.data.permisId;
          targetPermisCode = response.data.permisCode;
          setSavedPermisId(targetPermisId);
          setPermisCode(targetPermisCode);
        }
      } catch (error) {
        console.error('Error checking permis:', error);
      }
    }
    if (!targetPermisId) {
      toast.error('Please save the permis first before saving the template');
      return;
    }
    setSavingTemplate(true);
    try {
      const flat = flattenWithPageIndex(pages);
      const templateName = prompt('Enter template name:', `Template for ${targetPermisCode || initialData.code_demande}`);
      if (!templateName) {
        setSavingTemplate(false);
        return;
      }
      await onSave({
        elements: flat,
        permisId: targetPermisId,
        name: templateName
      });
      try {
        const templatesResponse = await axios.get(`${apiURL}/api/permis/${targetPermisId}/templates`);
        setTemplates(Array.isArray(templatesResponse.data) ? templatesResponse.data : []);
      } catch (refreshError) {
        console.error('Error refreshing templates:', refreshError);
      }
      toast.success('Template saved successfully');
    } catch (error) {
      console.error('Failed to save template', error);
      toast.error("?‰chec de l'enregistrement du template");
    } finally {
      setSavingTemplate(false);
    }
  }, [savedPermisId, permisCode, procedureId, pages, flattenWithPageIndex, initialData, onSave, apiURL]);

  const handleSaveDesign = useCallback(async () => {
    setIsLoading(true);
    try {
      const name = prompt('Enter template name:',
        templates.find(t => t.id === activeTemplate)?.name || `Template ${new Date().toLocaleDateString()}`);
      if (!name) {
        setIsLoading(false);
        return;
      }
      const flat = flattenWithPageIndex(pages);
      await onSave({
        elements: flat,
        permisId: savedPermisId || initialData.id_demande,
        templateId: activeTemplate,
        name
      });
      toast.success('Design sauvegardé avec succ?¨s');
      try {
        if (savedPermisId) {
          const templatesResponse = await axios.get(`${apiURL}/api/permis/${savedPermisId}/templates`);
          setTemplates(Array.isArray(templatesResponse.data) ? templatesResponse.data : []);
        } else {
          const resp = await axios.get(`${apiURL}/api/permis/templates?permisId=${initialData.code_demande}`);
          setTemplates(Array.isArray(resp.data) ? resp.data : resp.data?.data ?? []);
        }
      } catch (refreshError) {
        console.error('Error refreshing templates:', refreshError);
      }
    } catch (error) {
      console.error('Failed to save design', error);
      toast.error('?‰chec de la sauvegarde du design');
    } finally {
      setIsLoading(false);
    }
  }, [onSave, pages, flattenWithPageIndex, initialData, activeTemplate, templates, savedPermisId, apiURL]);

  const handleCreateNewTemplate = useCallback(async () => {
    if (!savedPermisId) {
      toast.error('Please save the permis first before creating a template');
      return;
    }
    const templateName = prompt('Enter a name for the new template:');
    if (!templateName) return;
    setSavingTemplate(true);
    try {
      const flat = flattenWithPageIndex(pages);
      await onSave({
        elements: flat,
        permisId: savedPermisId,
        name: templateName
      });
      const templatesResponse = await axios.get(`${apiURL}/api/permis/${savedPermisId}/templates`);
      setTemplates(Array.isArray(templatesResponse.data) ? templatesResponse.data : []);
      toast.success('New template created successfully');
    } catch (error) {
      console.error('Failed to create template:', error);
      toast.error('Failed to create template');
    } finally {
      setSavingTemplate(false);
    }
  }, [savedPermisId, pages, flattenWithPageIndex, onSave, apiURL]);

  const handleDeleteTemplate = useCallback(async (templateId: number) => {
    if (!confirm('Are you sure you want to delete this template?')) return;
    try {
      await axios.delete(`${apiURL}/api/permis/templates/${templateId}`);
      toast.success('Template deleted');
      const resp = await axios.get(`${apiURL}/api/permis/templates?permisId=${initialData.code_demande}`);
      setTemplates(Array.isArray(resp.data) ? resp.data : resp.data?.data ?? []);
      const numericTemplateId = templateId;
      if (activeTemplate === numericTemplateId) {
        setActiveTemplate(null);
        const defaultPages: PermisPages = [
          createPermisDetailsPage(initialData),
        //  createCoordinatesPage(initialData),
          createArticlesPage(initialData)
        ];
        setPages(defaultPages);
        pushHistory(defaultPages);
      }
    } catch (error) {
      console.error('Failed to delete template', error);
      toast.error('Failed to delete template');
    }
  }, [initialData.code_demande, activeTemplate, apiURL]);

  const handleGeneratePDF = useCallback(async () => {
    if (!stageRef.current) return;
    setIsLoading(true);
    try {
      const pdf = new jsPDF('p', 'pt', 'a7');
      let a4Width = pdf.internal.pageSize.getWidth();
      let a4Height = pdf.internal.pageSize.getHeight();
      for (let pageIndex = 0; pageIndex < pages.length; pageIndex++) {
        setCurrentPage(pageIndex);
        await new Promise((r) => setTimeout(r, 300));
        const stage = stageRef.current;
        const currentSize = canvasSizes[pageIndex];
        // Export at scale 1 to avoid zoom distortion
        const prevScale = { x: stage.scaleX(), y: stage.scaleY() };
        stage.scale({ x: 1, y: 1 });
        // Export stage as PNG first (preserves transparency correctly)
        // Then, for JPEG profiles, composite onto a white background to avoid black pages
        const exportAsPng = pdfQuality === 'high';
        const pixelRatio = exportAsPng ? 2 : (pdfQuality === 'balanced' ? 1.5 : 1);
        const jpegQuality = pdfQuality === 'balanced' ? 0.85 : (pdfQuality === 'small' ? 0.72 : 0.92);

        const pngData = stage.toDataURL({
          pixelRatio,
          width: currentSize.width,
          height: currentSize.height,
          mimeType: 'image/png'
        });
        stage.scale(prevScale);
        if (pageIndex > 0) {
          const portrait = currentSize.height >= currentSize.width;
          pdf.addPage('a7', portrait ? 'portrait' : 'landscape');
          a4Width = pdf.internal.pageSize.getWidth();
          a4Height = pdf.internal.pageSize.getHeight();
        }
        const scale = Math.min(a4Width / currentSize.width, a4Height / currentSize.height);
        const imgWidth = currentSize.width * scale;
        const imgHeight = currentSize.height * scale;
        const x = (a4Width - imgWidth) / 2;
        const y = (a4Height - imgHeight) / 2;
        if (exportAsPng) {
          pdf.addImage(pngData, 'PNG', x, y, imgWidth, imgHeight);
        } else {
          // Convert PNG (with alpha) to JPEG over white background to avoid black fills
          const toJpegOnWhite = async (srcDataUrl: string, w: number, h: number, quality: number) => {
            const img = new Image();
            await new Promise<void>((resolve, reject) => {
              img.onload = () => resolve();
              img.onerror = (e) => reject(e);
              img.src = srcDataUrl;
            });
            const canvas = document.createElement('canvas');
            canvas.width = Math.round(w * pixelRatio);
            canvas.height = Math.round(h * pixelRatio);
            const ctx = canvas.getContext('2d');
            if (ctx) {
              ctx.fillStyle = '#ffffff';
              ctx.fillRect(0, 0, canvas.width, canvas.height);
              ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
            }
            return canvas.toDataURL('image/jpeg', quality);
          };
          const jpegData = await toJpegOnWhite(pngData, currentSize.width, currentSize.height, jpegQuality);
          const comp = pdfQuality === 'small' ? 'FAST' : 'SLOW';
          pdf.addImage(jpegData, 'JPEG', x, y, imgWidth, imgHeight, undefined, comp as any);
        }
      }
      const defaultName = `permis-${initialData.code_demande}`;
      try {
        const dataUri = pdf.output('datauristring');
        const typeCode = (initialData?.typePermis?.code || initialData?.typePermis?.Code || '').toString();
        await axios.post(`${apiURL}/api/permis/templates/pdf/save`, {
          code: String(initialData?.code_demande ?? initialData?.codeDemande ?? ''),
          typeCode,
          pdfBase64: dataUri,
          fileName: defaultName,
          templateName: templates.find(t => t.id === activeTemplate)?.name || undefined,
        });
      } catch (e) {
        console.warn('Failed to upload PDF copy to server', e);
      }
      pdf.save(`${defaultName}.pdf`);
      toast.success("PDF généré avec succés");
    } catch (error) {
      console.error("Failed to generate PDF", error);
      toast.error("?‰chec de la génération du PDF");
    } finally {
      setIsLoading(false);
    }
  }, [pages, initialData, canvasSizes, apiURL, templates, activeTemplate, pdfQuality]);

  const handleSavePermis = useCallback(async () => {
    setIsLoading(true);
    try {
      const flat = flattenWithPageIndex(pages);
      const { id } = await onSavePermis({ pages, elements: flat, data: initialData, id_demande: initialData.id_demande });
      await onSave({ pages, elements: flat, permisId: id, name: `Template for ${initialData.code_demande}` });
      toast.success('Permis and template saved successfully');
    } catch (error) {
      console.error('Failed to save permis', error);
      toast.error("?‰chec de l'enregistrement du permis");
    } finally {
      setIsLoading(false);
    }
  }, [pages, flattenWithPageIndex, initialData, onSavePermis, onSave]);

  const handleApplyTemplate = useCallback(async (templateId: string) => {
    if (loadingTemplates) return;
    try {
      const numericTemplateId = parseInt(templateId, 10);
      const template = templates.find(t => t.id === numericTemplateId);
      if (!template) {
        toast.error('Template not found');
        return;
      }
      let templateElements: PermisElement[] = [];
      if (typeof template.elements === 'string') {
        templateElements = JSON.parse(template.elements);
      } else if (Array.isArray(template.elements)) {
        templateElements = template.elements;
      }
      const elementsWithIds = templateElements.map(el => ({
        ...el,
        id: el.id || uuidv4(),
        meta: {
          ...el.meta,
          pageIndex: el.meta?.pageIndex || 0
        }
      }));
      const page0Elements = elementsWithIds.filter(el => el.meta?.pageIndex === 0);
      //const page1Elements = elementsWithIds.filter(el => el.meta?.pageIndex === 1);
      const page2Elements = elementsWithIds.filter(el => el.meta?.pageIndex === 1);
      const newPages: PermisPages = [
        page0Elements.length > 0 ? page0Elements : createPermisDetailsPage(initialData),
      //  page1Elements.length > 0 ? page1Elements : createCoordinatesPage(initialData),
        page2Elements.length > 0 ? page2Elements : createArticlesPage(initialData)
      ];
      setPages(newPages);
      setActiveTemplate(numericTemplateId);
      setSelectedIds([]);
      pushHistory(newPages);
      toast.success(`Template "${template.name}" applied successfully`);
    } catch (error) {
      console.error('Error applying template:', error);
      toast.error('Failed to apply template');
    }
  }, [templates, loadingTemplates, initialData, pushHistory]);

  const handlePropertyChange = useCallback((property: keyof PermisElement, value: any) => {
    if (selectedIds.length === 0) return;
    setElementsForCurrent(prev => prev.map(el => (selectedIds.includes(el.id) ? { ...el, [property]: value } as PermisElement : el)));
    // If inline text editor is open for the selected text, reflect style changes immediately
    if (textOverlay && selectedIds.includes(textOverlay.id)) {
      const styleProps = new Set(['fontFamily','fontSize','color','lineHeight','direction','textAlign']);
      if (styleProps.has(property as string)) {
        setTextOverlay(prev => prev ? {
          ...prev,
          fontFamily: property === 'fontFamily' ? String(value) : prev.fontFamily,
          fontSize: property === 'fontSize' ? Number(value) * zoom : prev.fontSize,
          color: property === 'color' ? String(value) : prev.color,
          lineHeight: property === 'lineHeight' ? Number(value) : prev.lineHeight,
          direction: property === 'direction' ? String(value) as any : prev.direction,
          textAlign: property === 'textAlign' ? String(value) as any : prev.textAlign,
        } : prev);
      }
    }
    // Proactively ensure the requested font is loaded before final draw (for Konva on canvas)
    if (property === 'fontFamily') {
      try {
        const fam = String(value);
        const quoted = /\s/.test(fam) ? `"${fam}"` : fam;
        const fontsObj: any = (document as any).fonts;
        if (fontsObj?.load) {
          fontsObj.load(`16px ${quoted}`).then(() => {
            try { (stageRef.current as any)?.getLayer?.()?.batchDraw?.(); } catch {}
          }).catch(() => {});
        }
      } catch {}
    }
  }, [selectedIds, setElementsForCurrent, textOverlay, zoom]);


  const handleZoom = useCallback((direction: 'in' | 'out') => {
    const newZoom = direction === 'in' ? zoom * 1.2 : zoom / 1.2;
    setZoom(clamp(newZoom, 0.5, 3));
  }, [zoom]);

  const selectedElementsList = useMemo(() => elements.filter(el => selectedIds.includes(el.id)), [elements, selectedIds]);
  const firstSelected = selectedElementsList[0];
  const onlyLineSelected = useMemo(() => (
    selectedIds.length > 0 && selectedIds.every(id => elements.find(e => e?.id === id)?.type === 'line')
  ), [selectedIds, elements]);
  // Keep rotation available for all shapes, including lines.

  // Normalize draggable for common elements to make them movable smoothly
  useEffect(() => {
    if (dragNormalized) return;
    setPages(prev => {
      let changed = false;
      const mapped = prev.map(page => page.map(el => {
        if ((el.type === 'text' || el.type === 'qrcode') && !(el as any).meta?.isBorder) {
          if (!el.draggable) { changed = true; return { ...el, draggable: true } as PermisElement; }
        }
        return el;
      }));
      if (changed) {
        setDragNormalized(true);
        pushHistory(JSON.parse(JSON.stringify(mapped)) as PermisPages);
        return mapped as PermisPages;
      }
      return prev;
    });
  }, [pages, dragNormalized, pushHistory]);

  const onToggleArticle = useCallback((id: string, checked: boolean) => {
    setSelectedArticleIds(prev => {
      const next = checked ? Array.from(new Set([...prev, id])) : prev.filter(x => x !== id);
      insertArticlesAsElements(next, articles);
      return next;
    });
  }, [articles, insertArticlesAsElements]);

  const onAddCustomArticle = useCallback((article: ArticleItem) => {
    setArticles(prev => [...prev, article]);
    setSelectedArticleIds(prev => [...prev, article.id]);
    insertArticlesAsElements([...selectedArticleIds, article.id], [...articles, article]);
  }, [articles, selectedArticleIds, insertArticlesAsElements]);

  const onRemoveArticle = useCallback((id: string) => {
    setArticles(prev => prev.filter(a => a.id !== id));
    const filteredIds = selectedArticleIds.filter(x => x !== id);
    setSelectedArticleIds(filteredIds);
    insertArticlesAsElements(filteredIds, articles.filter(a => a.id !== id));
  }, [articles, selectedArticleIds, insertArticlesAsElements]);

  const onUpdateArticle = useCallback((a: ArticleItem) => {
    setArticles(prev => prev.map(x => (x.id === a.id ? a : x)));
    if (selectedArticleIds.includes(a.id)) {
      insertArticlesAsElements(selectedArticleIds, [...articles.filter(x => x.id !== a.id), a]);
    }
  }, [articles, selectedArticleIds, insertArticlesAsElements]);

  const openTextEditor = useCallback((element: PermisElement) => {
    if (!containerRef.current) return;
    const containerRect = containerRef.current.getBoundingClientRect();
    const canvasPad = 20;
    const left = element.x * zoom + containerRect.left + canvasPad;
    const top = element.y * zoom + containerRect.top + canvasPad;
    const width = (element.width || 200) * zoom;
    // Add extra height to make editing more comfortable
    const height = Math.max(120, (element.height || 40) * zoom + 80);
    setTextOverlay({
      id: element.id,
      value: element.text || '',
      left,
      top,
      width,
      height,
      fontSize: (element.fontSize || 20) * zoom,
      fontFamily: element.fontFamily || ((element.language === 'ar') ? ARABIC_FONTS[0] : FONT_FAMILIES[0]),
      color: element.color || '#000',
      direction: element.direction || 'ltr',
      textAlign: (element as any).textAlign || 'left',
      lineHeight: (element as any).lineHeight || 1.2,
      selectionStart: 0,
      selectionEnd: 0
    });
  }, [zoom]);

  const commitTextEditor = useCallback((save: boolean) => {
    if (!textOverlay) return;
    if (save) handleTextChange(textOverlay.id, textOverlay.value);
    setTextOverlay(null);
  }, [textOverlay, handleTextChange]);

  const onTextAreaKeyDown = useCallback((e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      commitTextEditor(true);
    } else if (e.key === 'Escape') {
      e.preventDefault();
      commitTextEditor(false);
    }
  }, [commitTextEditor]);

  const canPrev = currentPage > 0;
  const canNext = currentPage < pages.length - 1;

  const gotoPrev = useCallback(() => {
    if (!canPrev) return;
    setSelectedIds([]);
    setCurrentPage(p => p - 1);
  }, [canPrev]);

  const gotoNext = useCallback(() => {
    if (!canNext) return;
    setSelectedIds([]);
    setCurrentPage(p => p + 1);
  }, [canNext]);

const pageLabel = (idx: number) => {
  if (idx === PAGES.PERMIS_DETAILS) return 'Page 1';
 // if (idx === PAGES.COORDINATES) return 'Page 2';
  if (idx >= PAGES.ARTICLES) return `Articles ${idx - PAGES.ARTICLES + 1}`;
  return `Page ${idx + 1}`;
};

  return (
    <div className={styles.container}>
      <div className={styles.header}>
        <div className={styles.toolbar}>
          <div className={styles.tools}>
            <button className={`${styles.toolButton} ${tool === 'text' ? styles.active : ''}`} onClick={() => { setTool('text'); handleAddElement('text'); }} title="Add Text (T)"><FiType /></button>
            <button className={`${styles.toolButton} ${tool === 'rectangle' ? styles.active : ''}`} onClick={() => { setTool('rectangle'); handleAddElement('rectangle'); }} title="Add Rectangle (R)"><BsTextParagraph /></button>
            <button className={`${styles.toolButton} ${tool === 'line' ? styles.active : ''}`} onClick={() => { setTool('line'); handleAddElement('line'); }} title="Add Line (L)"><BsBorderWidth /></button>
          </div>
          <div className={styles.pager}>
            <button className={styles.iconBtn} onClick={gotoPrev} disabled={!canPrev}>
              <FiChevronLeft />
            </button>
            <div className={styles.pageLabel}>
              {pageLabel(currentPage)}
              <span className={styles.pageCount}> ({currentPage + 1}/{pages.length})</span>
            </div>
            <button className={styles.iconBtn} onClick={gotoNext} disabled={!canNext}>
              <FiChevronRight />
            </button>
        </div>
        <div className={styles.rightTools}>
            {/* Articles source selection */}
            <div className={styles.templateSection}>
              <select
                className={styles.templateSelect}
                value={selectedArticleSet || ''}
                onChange={(e) => changeArticleSet(e.target.value)}
              >
                <option value="">Select Articles Set ({articleSets.length})</option>
                {articleSets.map(s => (
                  <option key={s.key} value={s.key}>{s.name} {s.source === 'custom' ? '(custom)' : ''}</option>
                ))}
              </select>
              <button
                className={styles.iconBtn}
                onClick={() => {
                  const key = prompt('Enter set key (e.g., permis-xyz):', selectedArticleSet || 'custom-set');
                  const name = prompt('Enter set name:', articleSets.find(s => s.key === selectedArticleSet!)?.name || 'Custom Set');
                  if (key && name) {
                    saveArticleSet(key, name, articles);
                    setArticleSets(listArticleSets());
                    setSelectedArticleSet(key);
                  }
                }}
                title="Save current articles as set"
              >
                <FiSave />
              </button>
              <button
                className={styles.iconBtn}
                onClick={() => exportArticleSet(articleSets.find(s => s.key === selectedArticleSet!)?.name || 'Articles', articles)}
                title="Export current set"
              >
                <FiDownload />
              </button>
              <input
                ref={importInputRef}
                type="file"
                accept="application/json"
                style={{ display: 'none' }}
                onChange={async (e) => {
                  const inputEl = importInputRef.current;
                  const f = inputEl?.files?.[0];
                  if (!f) return;
                  const res = await importArticleSet(f);
                  setArticleSets(listArticleSets());
                  if (res) changeArticleSet(res.key);
                  if (inputEl) inputEl.value = '';
                }}
              />
              <button className={styles.iconBtn} onClick={() => importInputRef.current?.click()} title="Import set JSON">
                <FileUp/>
              </button>
            </div>
            <div className={styles.zoomGroup}>
              <button className={styles.iconBtn} onClick={() => handleZoom('out')}>-</button>
              <div className={styles.zoomDisplay}>{Math.round(zoom * 100)}%</div>
              <button className={styles.iconBtn} onClick={() => handleZoom('in')}>+</button>
            </div>
            <div className={styles.resizeGroup}>
              <button className={styles.iconBtn} onClick={() => handleResizeCanvas('width', -50)} title="Decrease Width">
                <FiChevronLeft /> W
              </button>
              <button className={styles.iconBtn} onClick={() => handleResizeCanvas('width', 50)} title="Increase Width">
                <FiChevronRight /> W
              </button>
              <button className={styles.iconBtn} onClick={() => handleResizeCanvas('height', -50)} title="Decrease Height">
                <FiChevronUp /> H
              </button>
              <button className={styles.iconBtn} onClick={() => handleResizeCanvas('height', 50)} title="Increase Height">
                <FiChevronDown /> H
              </button>
              <button className={styles.iconBtn} onClick={handleResetCanvasSize} title="Reset Size">
                <FiRefreshCw />
              </button>
            </div>
            <div className={styles.templateSection}>
              <select
                className={styles.templateSelect}
                value={activeTemplate !== null ? activeTemplate.toString() : ''}
                onChange={(e) => handleApplyTemplate(e.target.value)}
                disabled={loadingTemplates}
              >
                <option value="">Select Template ({templates.length} available)</option>
                {loadingTemplates ? (
                  <option value="">Loading templates...</option>
                ) : (
                  templates.map(t => (
                    <option key={t.id} value={t.id.toString()}>
                      {t.name} (v{t.version})
                    </option>
                  ))
                )}
              </select>
              {activeTemplate && (
                <div className={styles.templateActions}>
                  <button
                    className={styles.iconBtn}
                    onClick={() => handleDeleteTemplate(activeTemplate)}
                    title="Delete Template"
                  >
                    <FiTrash2 />
                  </button>
                  <button
                    className={styles.iconBtn}
                    onClick={() => {
                      const newName = prompt('Rename template:',
                        templates.find(t => t.id === activeTemplate)?.name);
                      if (newName) {
                        handleRenameTemplate(activeTemplate, newName);
                      }
                    }}
                    title="Rename Template"
                  >
                    <FiEdit2 />
                  </button>
                </div>
              )}
            </div>
            <div style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
              <label style={{ fontSize: 12, opacity: 0.8 }}>PDF:</label>
              <select
                value={pdfQuality}
                onChange={(e) => { setPdfQuality(e.target.value as any); try { localStorage.setItem('pdf_export_quality', e.target.value); } catch {} }}
                style={{ height: 30, fontSize: 12 }}
                title="Qualité d'export PDF"
              >
                <option value="high">Haute (PNG)</option>
                <option value="balanced">Équilibrée (JPEG)</option>
                <option value="small">Petite (JPEG)</option>
              </select>
              <button className={`${styles.actionBtn}`} onClick={handleGeneratePDF} disabled={isLoading}><FiDownload /> <span>PDF</span></button>
            </div>
            <button
              className={`${styles.actionBtn}`}
              onClick={handleSavePermisOnly}
              disabled={savingPermis || isLoading || !!savedPermisId}
              title={savedPermisId ? 'Permis already exists' : 'Save Permis'}
            >
              {savingPermis ? (
                <FiRefreshCw className={styles.spinner} />
              ) : (
                <FiSave />
              )}
              <span>{savingPermis ? 'Saving...' : 'Save Permis'}</span>
            </button>
            <button
              className={`${styles.actionBtn}`}
              onClick={handleSaveTemplateOnly}
              // disabled={savingTemplate || isLoading || !savedPermisId || loadingTemplates}
              title={!savedPermisId ? 'Save the permis first' : 'Save Template'}
            >
              {savingTemplate ? (
                <FiRefreshCw className={styles.spinner} />
              ) : (
                <FiSave />
              )}
              <span>{savingTemplate ? 'Saving...' : 'Save Template'}</span>
            </button>
            <button
              className={`${styles.actionBtn}`}
              onClick={handleCreateNewTemplate}
              // disabled={savingTemplate || isLoading || !savedPermisId}
              title={!savedPermisId ? 'Save the permis first' : 'Create New Template'}
            >
              <FiSave />
              <span>{savingTemplate ? 'Creating...' : 'New Template'}</span>
            </button>
            <button
              className={styles.iconBtn}
              onClick={handleRefreshTemplates}
              title="Refresh Templates"
              disabled={loadingTemplates || !savedPermisId}
            >
              <FiRefreshCw className={loadingTemplates ? styles.spinner : ''} />
            </button>
              <button
                className={`${styles.toolButton} ${tool === 'qrcode'}`}
                onClick={async () => {
                  try {
                    setTool('qrcode');
                    const targetId = savedPermisId || initialData.id_demande || initialData.id;
                    let qrPayload = '';
                    if (targetId) {
                      try {
                        let by = '';
                        try {
                          by = localStorage.getItem('auth_user_username') || localStorage.getItem('auth_user_name') || '';
                        } catch {}
                        const resp = await axios.post(`${apiURL}/api/permis/${encodeURIComponent(targetId)}/qrcode/generate`, { by, username: by });
                        const code = resp?.data?.QrCode || '';
                        // Use the unique code as QR payload
                        qrPayload = code;
                      } catch (e) {
                        console.warn('QR generation request failed, falling back', e);
                      }
                    }
                    // Place QR at a fixed position/size as requested
                    const qrX = 752.0206384470126;
                    const qrY = 24.802348503240424;
                    const qrW = 130.95322004714174;
                    const qrH = 145.8437867490381;
                    const qrUrl = generateQRCodeData(initialData, qrPayload);
                    const qrElement = {
                      ...createQRCodeElement(initialData, qrX, qrY),
                      width: qrW,
                      height: qrH,
                      qrData: qrUrl
                    } as PermisElement;
                    setElementsForCurrent(prev => [...prev, qrElement]);
                    setSelectedIds([qrElement.id]);
                    // Also place the QR data (code only) as text at bottom-center of the first page
                    setPages(prev => {
                      const next = [...prev] as PermisPages;
                      const page0 = next[0] ? [...next[0]] : [];
                      const bottomTextWidth = 500;
                      const bottomMargin = 20;
                      const bottomY = (DEFAULT_CANVAS.height - bottomMargin - 20);
                      const displayCode = qrPayload || String(initialData?.code_demande || initialData?.codeDemande || '');
                      const centerX = Math.max(0, Math.floor((DEFAULT_CANVAS.width - bottomTextWidth) / 2));
                      page0.push({
                        id: uuidv4(),
                        type: 'text',
                        x: centerX,
                        y: bottomY,
                        width: bottomTextWidth,
                        text: displayCode,
                        fontSize: 14,
                        fontFamily: 'Arial',
                        color: '#000',
                        draggable: true,
                        textAlign: 'center',
                        opacity: 1,
                        wrap: 'none',
                        lineHeight: 1.2,
                        meta: { pageIndex: 0, isFooter: true }
                      } as any);
                      next[0] = page0;
                      pushHistory(next);
                      return next;
                    });
                  } catch (e) {
                    console.error('Failed to add QR', e);
                  }
                }}
                title="Add QR Code"
              >
              <AiOutlineQrcode size={16} />
            </button>
          </div>
        </div>
      </div>
      <div className={styles.body}>
        {currentPage === PAGES.ARTICLES && (
          <aside className={styles.sidebar}>
            <ArticlesPanel
              articles={articles}
              selectedIds={selectedArticleIds}
              onToggle={onToggleArticle}
              onAddCustom={onAddCustomArticle}
              onRemove={onRemoveArticle}
              onUpdate={onUpdateArticle}
            />
          </aside>
        )}
        <main className={styles.canvasArea} ref={containerRef}>
          <div className={styles.canvasWrap}>
            <Stage
              width={currentCanvasSize.width}
              height={currentCanvasSize.height}
              ref={stageRef}
              scaleX={zoom}
              scaleY={zoom}
              onClick={handleStageClick}
              onTap={handleStageClick}
              onMouseDown={handleStageMouseDown}
              onMouseMove={handleStageMouseMove}
              onMouseUp={handleStageMouseUp}
              onTouchStart={handleStageMouseDown}
              onTouchMove={handleStageMouseMove}
              onTouchEnd={handleStageMouseUp}
              style={{
                backgroundImage: showGrid ? gridBackground(zoom) : 'none',
                backgroundSize: `${20 * zoom}px ${20 * zoom}px`,
              }}
            >
              <Layer>
                {elements.map(element => {
                  const isSelected = selectedIds.includes(element.id);
                  return (
                      <ElementRenderer
                      key={element.id}
                      element={element}
                      isSelected={isSelected}
                      zoom={zoom}
                      onClickElement={handleElementClick}
                      onDragEnd={handleDragEnd}
                      onTransformEnd={handleTransformEnd}
                      onDblClickText={() => openTextEditor(element)}
                      onTransform={handleTransform}
                    />
                  );
                })}
                <Transformer
                  ref={transformerRef}
                  anchorSize={8}
                  anchorStrokeWidth={1}
                  borderStrokeWidth={1}
                  flipEnabled={true}
                  enabledAnchors={onlyLineSelected ? ['middle-left', 'middle-right'] : undefined}
                  boundBoxFunc={(oldBox, newBox) => {
                    const nodes = transformerRef.current?.nodes?.() || [];
                    const hasLine = nodes.some((n: any) => n.getClassName && n.getClassName() === 'Line');
                    if (hasLine) return newBox;
                    const minW = 4;
                    const minH = 4;
                    const shrinkingW = newBox.width < oldBox.width;
                    const shrinkingH = newBox.height < oldBox.height;
                    // Allow width-only resize even if height is very small
                    // Block only when a dimension is being shrunk below the minimum
                    if (shrinkingW && newBox.width < minW) return oldBox;
                    if (shrinkingH && newBox.height < minH) return oldBox;
                    return newBox;
                  }}
                />
                {isSelecting && selectionRect && (
                  <Rect
                    x={selectionRect.x}
                    y={selectionRect.y}
                    width={selectionRect.width}
                    height={selectionRect.height}
                    fill="rgba(41,161,255,0.12)"
                    stroke="#29a1ff"
                    strokeWidth={1}
                    dash={[4, 4]}
                    listening={false}
                  />
                )}
              </Layer>
            </Stage>
          </div>
          {textOverlay && (
            <textarea
              autoFocus
              value={textOverlay.value}
              onChange={e =>
                setTextOverlay(prev =>
                  prev ? { ...prev, value: e.target.value } : prev
                )
              }
              onSelect={e => {
                const target = e.target as HTMLTextAreaElement;
                setTextOverlay(prev => prev ? { ...prev, selectionStart: target.selectionStart || 0, selectionEnd: target.selectionEnd || 0 } : prev);
              }}
              onBlur={() => commitTextEditor(true)}
              onKeyDown={onTextAreaKeyDown}
              className={styles.textOverlay}
              style={{
                left: textOverlay.left,
                top: textOverlay.top,
                width: textOverlay.width,
                // Ensure editor stays usable and scrollable within viewport
                height: Math.max(120, textOverlay.height || 0),
                maxHeight: '80vh',
                fontSize: textOverlay.fontSize,
                fontFamily: textOverlay.fontFamily,
                color: textOverlay.color,
                lineHeight: `${textOverlay.lineHeight}`,
                direction: textOverlay.direction === 'rtl' ? 'rtl' : 'ltr',
                textAlign: textOverlay.textAlign as any,
                overflowY: 'auto',
                overflowX: 'hidden',
              }}
            />
          )}
        </main>
        <aside className={styles.properties}>
          {selectedIds.length > 0 && firstSelected ? (
            <div className={styles.propsInner}>
              <h3>Properties <span className={styles.small}>({selectedIds.length})</span></h3>

              <div className={styles.propRow}>
                <label>Position</label>
                <div className={styles.inlineInputs}>
                  <input type="number" value={firstSelected.x || 0} onChange={(e) => handlePropertyChange('x', parseFloat(e.target.value || '0'))} />
                  <input type="number" value={firstSelected.y || 0} onChange={(e) => handlePropertyChange('y', parseFloat(e.target.value || '0'))} />
                </div>
              </div>

              <div className={styles.propRow}>
                <label>Size</label>
                <div className={styles.inlineInputs}>
                  <input type="number" value={firstSelected.width || 0} onChange={(e) => handlePropertyChange('width', parseFloat(e.target.value || '0'))} />
                  <input type="number" value={firstSelected.height || 0} onChange={(e) => handlePropertyChange('height', parseFloat(e.target.value || '0'))} />
                </div>
              </div>
{firstSelected.type === 'qrcode' && (
  <div className={styles.propRow}>
    <label>QR Data</label>
    <textarea 
      value={firstSelected.qrData || ''} 
      onChange={(e) => handlePropertyChange('qrData', e.target.value)} 
      rows={4}
      placeholder="Enter data for QR code"
    />
    <div className={styles.small}>
      This data will be encoded in the QR code
    </div>
  </div>
)}
              {firstSelected.type === 'text' && (
                <>
                  <div className={styles.propRow}>
                    <label>Text</label>
                    <textarea value={firstSelected.text || ''} onChange={(e) => handleTextChange(firstSelected.id, e.target.value)} rows={3} />
                  </div>

                  <div className={styles.propRow}>
                    <label>Language</label>
                    <div className={styles.inlineInputs}>
                      <select value={firstSelected.language || 'fr'} onChange={(e) => handlePropertyChange('language', e.target.value as any)}>
                        <option value="ar">Arabic</option>
                        <option value="fr">French</option>
                        <option value="en">English</option>
                      </select>
                      <select value={firstSelected.direction || 'ltr'} onChange={(e) => handlePropertyChange('direction', e.target.value as any)}>
                        <option value="rtl">RTL</option>
                        <option value="ltr">LTR</option>
                      </select>
                    </div>
                  </div>

                  <div className={styles.propRow}>
                    <label>Font</label>
                    <select value={firstSelected.fontFamily || (firstSelected.language === 'ar' ? ARABIC_FONTS[0] : FONT_FAMILIES[0])}
                      onChange={(e) => handlePropertyChange('fontFamily', e.target.value)}>
                      {(firstSelected.language === 'ar' ? ARABIC_FONTS : FONT_FAMILIES).map(font => <option key={font} value={font}>{font}</option>)}
                    </select>
                  </div>

                  <div className={styles.propRow}>
                    <label>Font Size</label>
                    <input type="number" value={firstSelected.fontSize || 20} onChange={(e) => handlePropertyChange('fontSize', parseInt(e.target.value || '20'))} />
                  </div>

                  <div className={styles.propRow}>
                    <label>Color</label>
                    <div className={styles.colorRow}>
                      <input type="color" value={normalizeHexColor(firstSelected.color, '#000000')} onChange={(e) => handlePropertyChange('color', e.target.value)} />
                      <div className={styles.colorCode}>{firstSelected.color || '#000000'}</div>
                    </div>
                  </div>

                  <div className={styles.propRow}>
                    <label>Alignment</label>
                    <select value={(firstSelected as any).textAlign || ((firstSelected as any).direction === 'rtl' ? 'right' : 'left')} onChange={(e) => handlePropertyChange('textAlign', e.target.value)}>
                      <option value="left">Left</option>
                      <option value="center">Center</option>
                      <option value="right">Right</option>
                    </select>
                  </div>

                  <div className={styles.propRow}>
                    <label>Line Height</label>
                    <input type="number" step="0.05" value={(firstSelected as any).lineHeight || 1.3} onChange={(e) => handlePropertyChange('lineHeight', parseFloat(e.target.value || '1.3'))} />
                  </div>
                </>
              )}

              {(firstSelected.type === 'rectangle' || firstSelected.type === 'line') && (
                <>
                  <div className={styles.propRow}>
                    <label>Fill</label>
                    <div className={styles.colorRow}>
                      <input type="color" value={normalizeHexColor((firstSelected as any).fill, '#ffffff')} onChange={(e) => handlePropertyChange('fill', e.target.value)} />
                      <div className={styles.colorCode}>{(firstSelected as any).fill || '#ffffff'}</div>
                    </div>
                  </div>

                  <div className={styles.propRow}>
                    <label>Border</label>
                    <div className={styles.inlineInputs}>
                      <input type="color" value={normalizeHexColor((firstSelected as any).stroke, '#000000')} onChange={(e) => handlePropertyChange('stroke', e.target.value)} />
                      <input type="number" value={(firstSelected as any).strokeWidth || 1} onChange={(e) => handlePropertyChange('strokeWidth', parseInt(e.target.value || '1'))} />
                    </div>
                  </div>

                  {firstSelected.type === 'rectangle' && (
                    <div className={styles.propRow}>
                      <label>Corner Radius</label>
                      <input type="number" value={(firstSelected as any).cornerRadius || 0} onChange={(e) => handlePropertyChange('cornerRadius', parseInt(e.target.value || '0'))} />
                    </div>
                  )}
                </>
              )}

              {firstSelected.type === 'table' && (
                <>
                  <div className={styles.propRow}>
                    <label>Header Text</label>
                    <input type="text" value={(firstSelected as any).headerText || ''} onChange={(e) => handlePropertyChange('headerText', e.target.value)} />
                  </div>
                  <div className={styles.propRow}>
                    <label>Structure</label>
                    <div className={styles.inlineInputs}>
                      <div className={styles.small}>Blocks</div>
                      <input type="number" min={1} max={5} value={(firstSelected as any).blockCols || 1} onChange={(e) => handlePropertyChange('blockCols', Math.max(1, Math.min(5, parseInt(e.target.value || '1'))))} />
                      <div className={styles.small}>Rows/Block</div>
                      <input type="number" min={1} max={60} value={(firstSelected as any).rowsPerCol || 10} onChange={(e) => handlePropertyChange('rowsPerCol', Math.max(1, Math.min(60, parseInt(e.target.value || '10'))))} />
                    </div>
                  </div>
                  <div className={styles.propRow}>
                    <label>Sizes</label>
                    <div className={styles.inlineInputs}>
                      <div className={styles.small}>Row</div>
                      <input type="number" min={12} max={80} value={(firstSelected as any).rowHeight || 34} onChange={(e) => handlePropertyChange('rowHeight', Math.max(12, parseInt(e.target.value || '34')))} />
                      <div className={styles.small}>Header H</div>
                      <input type="number" min={0} max={120} value={(firstSelected as any).headerHeight || 48} onChange={(e) => handlePropertyChange('headerHeight', Math.max(0, parseInt(e.target.value || '48')))} />
                    </div>
                  </div>
                  <div className={styles.propRow}>
                    <label>Colors</label>
                    <div className={styles.inlineInputs}>
                      <div className={styles.small}>Header</div>
                      <input type="color" value={normalizeHexColor((firstSelected as any).headerFill, '#f5f5f5')} onChange={(e) => handlePropertyChange('headerFill', e.target.value)} />
                      <div className={styles.small}>Alt Row</div>
                      <input type="color" value={normalizeHexColor((firstSelected as any).altRowFill, '#f8f8f8')} onChange={(e) => handlePropertyChange('altRowFill', e.target.value)} />
                      <div className={styles.small}>Grid</div>
                      <input type="color" value={normalizeHexColor((firstSelected as any).tableGridColor, '#d0d0d0')} onChange={(e) => handlePropertyChange('tableGridColor', e.target.value)} />
                      <div className={styles.small}>Grid W</div>
                      <input type="number" min={0} max={4} step={0.2} value={(firstSelected as any).tableGridWidth ?? 1} onChange={(e) => handlePropertyChange('tableGridWidth', parseFloat(e.target.value || '1'))} />
                    </div>
                  </div>
                  <div className={styles.propRow}>
                    <label>Borders</label>
                    <div className={styles.inlineInputs}>
                      <div className={styles.small}>Cell Borders</div>
                      <select value={(firstSelected as any).showCellBorders === false ? 'off' : 'on'} onChange={(e) => handlePropertyChange('showCellBorders', e.target.value === 'on')}>
                        <option value="on">On</option>
                        <option value="off">Off</option>
                      </select>
                      <div className={styles.small}>Outer</div>
                      <input type="color" value={normalizeHexColor((firstSelected as any).outerBorderColor || (firstSelected as any).stroke, '#000000')} onChange={(e) => handlePropertyChange('outerBorderColor', e.target.value)} />
                      <input type="number" min={0} max={4} step={0.2} value={(firstSelected as any).outerBorderWidth ?? (firstSelected as any).strokeWidth ?? 1.2} onChange={(e) => handlePropertyChange('outerBorderWidth', parseFloat(e.target.value || '1.2'))} />
                    </div>
                  </div>
                  <div className={styles.propRow}>
                    <label>Header Align</label>
                    <select value={(firstSelected as any).headerTextAlign || 'center'} onChange={(e) => handlePropertyChange('headerTextAlign', e.target.value)}>
                      <option value="left">Left</option>
                      <option value="center">Center</option>
                      <option value="right">Right</option>
                    </select>
                  </div>
                  <div className={styles.propRow}>
                    <label>Padding</label>
                    <input type="number" min={0} max={32} value={(firstSelected as any).cellPadding ?? 8} onChange={(e) => handlePropertyChange('cellPadding', parseInt(e.target.value || '8'))} />
                  </div>
                  <div className={styles.propRow}>
                    <label>Column Widths</label>
                    <input
                      type="text"
                      value={((firstSelected as any).colWidths || []).join(',')}
                      placeholder="60,90,90"
                      onChange={(e) => {
                        const parts = e.target.value.split(',').map(s => parseInt(s.trim() || '0')).filter(n => !isNaN(n) && n > 0);
                        handlePropertyChange('colWidths', parts.length ? parts : [60,90,90]);
                      }}
                    />
                  </div>
                  <div className={styles.propRow}>
                    <label>Rows</label>
                    <div className={styles.inlineInputs}>
                      <div className={styles.small}>{Array.isArray((firstSelected as any).tableData) ? (firstSelected as any).tableData.length : 0}</div>
                      <button className={styles.iconBtn} onClick={() => {
                        setElementsForCurrent(prev => prev.map(el => {
                          if (el.id !== firstSelected.id) return el;
                          const rows = Array.isArray((el as any).tableData) ? [...(el as any).tableData] : [];
                          rows.push({});
                          return { ...(el as any), tableData: rows } as any;
                        }));
                      }}>Add Row</button>
                      <button className={styles.iconBtn} onClick={() => {
                        setElementsForCurrent(prev => prev.map(el => {
                          if (el.id !== firstSelected.id) return el;
                          const rows = Array.isArray((el as any).tableData) ? [...(el as any).tableData] : [];
                          rows.pop();
                          return { ...(el as any), tableData: rows } as any;
                        }));
                      }}>Remove Last</button>
                      <button className={styles.iconBtn} onClick={() => {
                        setElementsForCurrent(prev => prev.map(el => (el.id === firstSelected.id ? { ...(el as any), tableData: [] } as any : el)));
                      }}>Clear</button>
                    </div>
                  </div>
                  <div className={styles.propRow}>
                    <label>Columns</label>
                    <div className={styles.inlineInputs}>
                      <button className={styles.iconBtn} onClick={() => {
                        setElementsForCurrent(prev => prev.map(el => {
                          if (el.id !== firstSelected.id) return el;
                          const cols = Array.isArray((el as any).tableColumns) ? [...(el as any).tableColumns] : [];
                          const idx = cols.length + 1;
                          cols.push({ key: `col${idx}`, title: `Col ${idx}`, width: 100, align: 'left' });
                          const cws = Array.isArray((el as any).colWidths) ? [...(el as any).colWidths] : [];
                          cws.push(100);
                          return { ...(el as any), tableColumns: cols, colWidths: cws } as any;
                        }));
                      }}>Add Column</button>
                      <button className={styles.iconBtn} onClick={() => {
                        setElementsForCurrent(prev => prev.map(el => {
                          if (el.id !== firstSelected.id) return el;
                          const cols = Array.isArray((el as any).tableColumns) ? [...(el as any).tableColumns] : [];
                          cols.pop();
                          const cws = Array.isArray((el as any).colWidths) ? [...(el as any).colWidths] : [];
                          cws.pop();
                          return { ...(el as any), tableColumns: cols, colWidths: cws } as any;
                        }));
                      }}>Remove Last</button>
                    </div>
                  </div>
                  <div className={styles.propRow}>
                    <label>Edit Column</label>
                    <div className={styles.inlineInputs}>
                      <input type="number" min={0} value={colEditIndex} onChange={(e) => setColEditIndex(parseInt(e.target.value || '0'))} />
                      <input type="text" placeholder="key" value={cellEditKey} onChange={(e) => setCellEditKey(e.target.value)} />
                      <input type="text" placeholder="title" value={cellEditValue} onChange={(e) => setCellEditValue(e.target.value)} />
                      <button className={styles.iconBtn} onClick={() => {
                        setElementsForCurrent(prev => prev.map(el => {
                          if (el.id !== firstSelected.id) return el;
                          const cols = Array.isArray((el as any).tableColumns) ? [...(el as any).tableColumns] : [];
                          if (colEditIndex >= 0 && colEditIndex < cols.length) {
                            const c = { ...cols[colEditIndex] } as any;
                            if (cellEditKey) c.key = cellEditKey;
                            if (cellEditValue) c.title = cellEditValue;
                            cols[colEditIndex] = c;
                          }
                          return { ...(el as any), tableColumns: cols } as any;
                        }));
                      }}>Apply</button>
                    </div>
                  </div>
                  <div className={styles.propRow}>
                    <label>Set Cell</label>
                    <div className={styles.inlineInputs}>
                      <input type="number" min={0} value={cellEditRow} onChange={(e) => setCellEditRow(parseInt(e.target.value || '0'))} />
                      <select value={cellEditKey} onChange={(e) => setCellEditKey(e.target.value)}>
                        {((firstSelected as any).tableColumns || []).map((c: any) => <option key={c.key} value={c.key}>{c.key}</option>)}
                      </select>
                      <input type="text" value={cellEditValue} onChange={(e) => setCellEditValue(e.target.value)} />
                      <button className={styles.iconBtn} onClick={() => {
                        setElementsForCurrent(prev => prev.map(el => {
                          if (el.id !== firstSelected.id) return el;
                          const rows = Array.isArray((el as any).tableData) ? [...(el as any).tableData] : [];
                          while (cellEditRow >= rows.length) rows.push({});
                          const r = { ...(rows[cellEditRow] || {}) } as any;
                          r[cellEditKey] = cellEditValue;
                          rows[cellEditRow] = r;
                          return { ...(el as any), tableData: rows } as any;
                        }));
                      }}>Apply</button>
                    </div>
                  </div>
                  <div className={styles.propRow}>
                    <button className={styles.actionBtn} onClick={() => {
                      const coords: any[] = Array.isArray(initialData?.coordinates) ? initialData.coordinates : [];
                      const dataRows = coords.map((c: any) => ({ point: undefined, x: String(c.x ?? ''), y: String(c.y ?? '') }));
                      handlePropertyChange('tableData', dataRows);
                    }}>Reload from coordinates</button>
                  </div>
                </>
              )}

              <div className={styles.propRow}>
                <label>Opacity</label>
                <div className={styles.inlineInputs}>
                  <input type="range" min="0" max="1" step="0.05" value={firstSelected.opacity || 1} onChange={(e) => handlePropertyChange('opacity', parseFloat(e.target.value || '1'))} />
                  <div className={styles.small}>{Math.round((firstSelected.opacity || 1) * 100)}%</div>
                </div>
              </div>

              <div className={styles.propRow}>
                <label>Rotation</label>
                <input type="number" min={0} max={360} value={firstSelected.rotation || 0} onChange={(e) => handlePropertyChange('rotation', parseInt(e.target.value || '0'))} />
              </div>
            </div>
          ) : (
            <div className={styles.emptyProps}>
              <h4>No elements selected</h4>
              <p>Click an element to edit properties</p>
            </div>
          )}
        </aside>
      </div>
    </div>
  );
};

export default PermisDesigner;





