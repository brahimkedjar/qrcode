// Shared helpers for deriving table geometry
import { DEFAULT_CANVAS, PAGE_MARGINS } from './constants';
import type { PermisElement } from './types';

type TableColumn = {
  key: string;
  title: string;
  width: number;
  align?: 'left' | 'center' | 'right';
};

export type TableLayoutConfig = {
  blockCols: number;
  rowsPerCol: number;
  rowHeight: number;
  headerText: string;
  headerHeight: number;
  headerFill: string;
  altRowFill: string;
  stroke: string;
  strokeWidth: number;
  headerTextAlign: string;
  showCellBorders: boolean;
  gridColor: string;
  gridWidth: number;
  outerBorderColor: string;
  outerBorderWidth: number;
  cellPadding: number;
  fontFamily: string;
  fontSize: number;
  textAlign: string;
  data: Array<Record<string, string | number>>;
  columns: TableColumn[];
  width: number;
  height: number;
  blockW: number;
  showHeader: boolean;
};

const DEFAULT_COL_WIDTHS = [60, 90, 90];
const DEFAULT_COLUMNS: TableColumn[] = [
  { key: 'x', title: '\u0639', width: DEFAULT_COL_WIDTHS[0], align: 'right' },
  { key: 'y', title: '\u0633', width: DEFAULT_COL_WIDTHS[1], align: 'right' },
  { key: 'point', title: '\u0627\u0644\u0646\u0642\u0637\u0629', width: DEFAULT_COL_WIDTHS[2], align: 'right' },
];

const MIN_ROW_HEIGHT = 14;
const MIN_COLUMN_WIDTH = 10;
const MAX_DEFAULT_BLOCK_COLS = 16;

const clamp = (value: number, min: number, max: number) => Math.min(Math.max(value, min), max);

const toNumber = (value: unknown): number | undefined => {
  if (value == null) return undefined;
  const n = Number(value);
  return Number.isFinite(n) ? n : undefined;
};

export const deriveTableLayout = (element: PermisElement): TableLayoutConfig => {
  const rawBlockCols = Math.max(1, Math.floor(toNumber((element as any).blockCols) || 1));
  const headerText = element.headerText || '';
  const headerFill = element.headerFill || '#f5f5f5';
  const altRowFill = element.altRowFill || '#f8f8f8';
  const stroke = element.stroke || '#000';
  const strokeWidth = toNumber((element as any).strokeWidth) ?? 1.2;
  const fontFamily = element.tableFontFamily || element.fontFamily || 'Arial';
  const rawFontSize = toNumber((element as any).tableFontSize ?? (element as any).fontSize) ?? 17;
  const fontSize = Math.max(20, rawFontSize);
  const textAlign = (element as any).tableTextAlign || 'center';
  const headerTextAlign = (element as any).headerTextAlign || 'center';
  const showHeader = (element as any).showHeader !== false;
  const data = Array.isArray(element.tableData) ? element.tableData : [];

  let headerHeight = Math.max(0, toNumber((element as any).headerHeight) ?? 28);
  let rowHeight = Math.max(MIN_ROW_HEIGHT, Math.floor(toNumber((element as any).rowHeight) || 34));
  const minHeightForFont = Math.max(MIN_ROW_HEIGHT, Math.ceil(fontSize * 1.6));
  if (rowHeight < minHeightForFont) {
    rowHeight = minHeightForFont;
  }

  const columnsSource = Array.isArray(element.tableColumns) && element.tableColumns.length > 0
    ? element.tableColumns
    : DEFAULT_COLUMNS;

  const columnsWithWidth: TableColumn[] = columnsSource.map((col, index) => ({
    ...col,
    width: Math.max(
      MIN_COLUMN_WIDTH,
      Math.floor(
        toNumber((col as any).width) ??
        DEFAULT_COL_WIDTHS[index] ??
        DEFAULT_COL_WIDTHS[DEFAULT_COL_WIDTHS.length - 1]
      )
    ),
  }));

  const baseColWidths = Array.isArray(element.colWidths) && element.colWidths.length > 0
    ? element.colWidths
    : columnsWithWidth.map(col => col.width);

  const hasHeaderRow = showHeader && headerHeight > 0;
  const headerRowCount = hasHeaderRow ? 1 : 0;
  const dataLength = data.length;

  const explicitRows = toNumber((element as any).rowsPerCol);
  const rowsConstraint = explicitRows && explicitRows > 0 ? Math.floor(explicitRows) : undefined;

  const explicitHeight = toNumber((element as any).height);
  const maxWidth = toNumber((element as any).maxWidth);
  const maxHeight = toNumber((element as any).maxHeight);
  const canvasWidth = toNumber((element as any).canvasWidth) ?? DEFAULT_CANVAS.width;
  const canvasHeight = toNumber((element as any).canvasHeight) ?? DEFAULT_CANVAS.height;
  const posY = toNumber(element.y) ?? 0;
  const leftMargin = toNumber((element as any).pageLeftMargin) ?? (PAGE_MARGINS.left ?? 0);
  const rightMargin = toNumber((element as any).pageRightMargin) ?? (PAGE_MARGINS.right ?? 0);
  const bottomMargin = toNumber((element as any).pageBottomMargin) ?? (PAGE_MARGINS.bottom ?? 0);

  const heightLimits: number[] = [];
  if (maxHeight && maxHeight > 0) heightLimits.push(maxHeight);
  const canvasHeightLimit = canvasHeight - Math.max(posY, 0) - bottomMargin;
  if (canvasHeightLimit > 0) heightLimits.push(canvasHeightLimit);
  if (explicitHeight && explicitHeight > 0) heightLimits.push(explicitHeight);
  const heightLimit = heightLimits.length > 0 ? Math.min(...heightLimits) : undefined;

  if (heightLimit !== undefined && heightLimit > 0) {
    const maxHeaderHeight = Math.max(
      0,
      heightLimit - MIN_ROW_HEIGHT * Math.max(1, 1 + headerRowCount)
    );
    headerHeight = Math.min(headerHeight, maxHeaderHeight);
  }

  const blockBaseWidth = columnsWithWidth.reduce((acc, col, index) => {
    const base = toNumber((col as any).width) ?? baseColWidths[index] ?? col.width;
    return acc + Math.max(MIN_COLUMN_WIDTH, base);
  }, 0);

  const rawMaxBlockCols = Math.floor(toNumber((element as any).maxBlockCols) || MAX_DEFAULT_BLOCK_COLS);
  const maxAutoBlockCols = clamp(
    Math.max(rawBlockCols, rawMaxBlockCols),
    rawBlockCols,
    Math.max(rawBlockCols, 24)
  );

  const fallbackRows = Math.max(1, Math.ceil((dataLength || 1) / Math.max(rawBlockCols, 1)));

  const calcRowsCap = (rh: number): number => {
    let cap = rowsConstraint ?? Number.POSITIVE_INFINITY;
    if (heightLimit && heightLimit > 0) {
      const usable = Math.max(0, heightLimit - headerHeight);
      const byHeight = Math.floor(usable / rh) - headerRowCount;
      cap = Math.min(cap, Math.max(1, byHeight));
    }
    if (!Number.isFinite(cap) || cap < 1) cap = fallbackRows;
    return Math.max(1, Math.floor(cap));
  };

  let effectiveRowHeight = rowHeight;
  let blockCols = Math.max(rawBlockCols, 1);
  let rowsPerCol = Math.max(1, Math.ceil((dataLength || 1) / blockCols));

  for (let attempt = 0; attempt < 8; attempt++) {
    const rowsCap = calcRowsCap(effectiveRowHeight);
    const requiredCols = Math.max(rawBlockCols, Math.ceil((dataLength || 1) / rowsCap));
    blockCols = clamp(requiredCols, rawBlockCols, maxAutoBlockCols);

    rowsPerCol = Math.max(1, Math.ceil((dataLength || 1) / blockCols));
    rowsPerCol = Math.min(rowsPerCol, rowsCap);

    const currentHeight = headerHeight + effectiveRowHeight * (rowsPerCol + headerRowCount);
    const fitsHeight = !heightLimit || currentHeight <= heightLimit + 0.5;
    const coversData = rowsPerCol * blockCols >= dataLength;

    if (fitsHeight && coversData) break;

    let adjusted = false;

    if (!coversData && blockCols < maxAutoBlockCols) {
      blockCols = Math.min(maxAutoBlockCols, blockCols + 1);
      adjusted = true;
    }

    if (!adjusted && heightLimit && currentHeight > heightLimit + 0.5 && effectiveRowHeight > MIN_ROW_HEIGHT) {
      const maxRowHeight = Math.floor(
        (heightLimit - headerHeight) / Math.max(1, rowsPerCol + headerRowCount)
      );
      const nextRowHeight = Math.max(
        MIN_ROW_HEIGHT,
        Math.min(effectiveRowHeight - 1, maxRowHeight)
      );
      if (nextRowHeight < effectiveRowHeight) {
        effectiveRowHeight = nextRowHeight;
        adjusted = true;
      }
    }

    if (!adjusted && blockCols < maxAutoBlockCols) {
      blockCols = Math.min(maxAutoBlockCols, blockCols + 1);
      adjusted = true;
    }

    if (!adjusted) {
      rowsPerCol = Math.max(rowsPerCol, Math.ceil((dataLength || 1) / blockCols));
      break;
    }
  }

  rowsPerCol = Math.max(rowsPerCol, Math.ceil((dataLength || 1) / blockCols));

  const availablePageWidth = Math.max(120, canvasWidth - leftMargin - rightMargin);
  let targetWidth = Math.max(120, availablePageWidth * 0.97);
  if (maxWidth && maxWidth > 0) targetWidth = Math.min(targetWidth, maxWidth);

  const perBlockWidth = targetWidth / Math.max(blockCols, 1);
  const scale = blockBaseWidth > 0 ? perBlockWidth / blockBaseWidth : 1;

  const scaledColumns = columnsWithWidth.map(col => ({
    ...col,
    width: Math.max(MIN_COLUMN_WIDTH, Math.round(col.width * scale)),
    align: (col as any).align || textAlign,
  }));

  const blockW = scaledColumns.reduce((acc, col) => acc + col.width, 0);

  const height = headerHeight + effectiveRowHeight * (rowsPerCol + headerRowCount);

  return {
    blockCols,
    rowsPerCol,
    rowHeight: effectiveRowHeight,
    headerText,
    headerHeight,
    headerFill,
    altRowFill,
    stroke,
    strokeWidth,
    headerTextAlign,
    showCellBorders: (element as any).showCellBorders !== false,
    gridColor: (element as any).tableGridColor || '#4F4040',
    gridWidth: toNumber((element as any).tableGridWidth) ?? 1,
    outerBorderColor: (element as any).outerBorderColor || stroke,
    outerBorderWidth: toNumber((element as any).outerBorderWidth) ?? strokeWidth,
    cellPadding: toNumber((element as any).cellPadding) ?? 8,
    fontFamily,
    fontSize,
    textAlign,
    data,
    columns: scaledColumns,
    width: targetWidth,
    height,
    blockW,
    showHeader: hasHeaderRow,
  };
};

export const computeTableHeight = (element: PermisElement): number => {
  const cfg = deriveTableLayout(element);
  return cfg.height;
};
