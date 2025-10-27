// =============================================================
// File: components/elements/types.ts
// =============================================================
export interface PermisSaveResponse { id: number; [key: string]: any; }

export type LanguageCode = 'ar' | 'fr' | 'en' | string;
export interface PermisElement {
  id: string;
  type: 'text' | 'rectangle' | 'image' | 'line' | 'qrcode' | 'table';
  x: number;
  y: number;
  width?: number;
  height?: number;
  text?: string;
  fontSize?: number;
  fontFamily?: string;
  color?: string;
  draggable?: boolean;
  rotation?: number;
  fill?: string;
  stroke?: string;
  strokeWidth?: number;
  opacity?: number;
  cornerRadius?: number;
  lineCap?: 'butt' | 'round' | 'square';
  lineJoin?: 'bevel' | 'round' | 'miter';
  textAlign?: 'left' | 'center' | 'right' | string;
  verticalAlign?: 'top' | 'middle' | 'bottom';
  padding?: number;
  letterSpacing?: number;
  lineHeight?: number;
  wrap?: 'word' | 'char' | 'none' | string;
  ellipsis?: boolean;
  qrData?: string;
  src?: string;
  // RTL / language support
  language?: LanguageCode;
  direction?: 'rtl' | 'ltr' | string;
  scaleX?: any;
  scaleY?: any;
  // Rich text styling ranges (indices in 'text')
  styledRanges?: Array<{
    start: number;
    end: number; // exclusive
    fontWeight?: 'bold' | 'normal';
    fontSize?: number;
    underline?: boolean;
    color?: string;
  }>;
  // Articles linkage
  isArticle?: boolean;
  articleId?: string;
  className?: string;
  dash?: number[];
  points?: number[];  // Fixed: use lowercase 'number[]'
  pageIndex?: any;
  meta?: any;
  align?: 'left' | 'center' | 'right';
  fontWeight?: string;

  // Table-specific (when type === 'table')
  rowsPerCol?: number;       // number of rows per block column
  blockCols?: number;        // number of repeated blocks horizontally
  colWidths?: number[];      // widths of columns in a block (scaled on render)
  rowHeight?: number;        // base row height
  headerText?: string;       // text above the table
  headerHeight?: number;     // header band height
  headerFill?: string;       // header background color
  altRowFill?: string;       // alternate row fill color
  showHeader?: boolean;      // toggle header band
  tableFontFamily?: string;  // font for table text
  tableFontSize?: number;    // font size for table text
  tableTextAlign?: 'left' | 'center' | 'right';
  // visual options
  showCellBorders?: boolean;
  tableGridColor?: string;
  tableGridWidth?: number;
  outerBorderColor?: string;
  outerBorderWidth?: number;
  headerTextAlign?: 'left' | 'center' | 'right';
  cellPadding?: number;
  // data matrix: each item is a row object for the logical dataset (mapped across blocks)
  tableData?: Array<Record<string, string | number>>;
  tableColumns?: Array<{ key: string; title: string; width?: number; align?: 'left' | 'center' | 'right' }>;
}

export interface PermisDesignerProps {
  procedureId?: number | null;
  initialData: any;
  onSaveTemplate?: (design: any) => Promise<void>;
  permisSaved?: boolean;
  permisId?: number | null;
  onSave: (design: any) => Promise<void>;
  onGeneratePdf: (design: any) => Promise<Blob>;
  onSavePermis: (permisData: any) => Promise<PermisSaveResponse>;
}

export type CommonKonvaProps = {
  id: string;
  x: number;
  y: number;
  width?: number;
  height?: number;
  rotation?: number;
  draggable?: boolean;
  onClick?: (e: any) => void;
  onTap?: (e: any) => void;
  onDragEnd?: (e: any) => void;
  onTransformEnd?: (e: any) => void;
  stroke?: string;
  strokeWidth?: number;
  shadowEnabled?: boolean;
  shadowColor?: string;
  shadowBlur?: number;
  shadowOpacity?: number;
  cornerRadius?: number;
  lineCap?: 'butt' | 'round' | 'square';
  lineJoin?: 'bevel' | 'round' | 'miter';
  opacity?: number;
};

export interface TextEditOverlay {
  id: string;
  value: string;
  left: number;
  top: number;
  width: number;
  fontSize: number;
  fontFamily: string;
  color: string;
  direction?: 'rtl' | 'ltr' | string;
  textAlign?: 'left' | 'center' | 'right';
  lineHeight?: number;
  height?: number;
  selectionStart?: number;
  selectionEnd?: number;
}

export interface ArticleItem {
  id: string;
  title: string;       // e.g., "المادة 1"
  content: string;     // Arabic text
  preselected?: boolean;
}

export interface QRCodeData {
  typePermis: string;
  codeDemande: string;
  detenteur: string;
  superficie: number;
  duree: string;
  localisation: string;
  dateCreation: string;
  coordinates?: any[];
}
