// File: constants.ts
export const FONT_FAMILIES = [
  'Arial',
  'Helvetica',
  'Times New Roman',
  'Courier New',
  'Verdana',
  'Georgia',
  'Palatino',
  'Garamond',
  'Comic Sans MS',
  'Impact'
];

export const ARABIC_FONTS = [
  'Traditional Arabic',
    'Simplified Arabic',
  'Arabic Typesetting',
  'Aldhabi',
  'Sakkal Majalla',
  'Microsoft Sans Serif',
  'Tahoma',
  'Segoe UI'
];

export const DEFAULT_CANVAS = {
  // A7 aspect ratio (~74mm x 105mm) scaled up for on‑screen editing
  width: 900,
  height: 1276
};

export const PAGE_MARGINS = {
  top: 40,
  right: 40,
  bottom: 40,
  left: 40
};

export const COLORS = {
  primary: '#1a5276',
  secondary: '#2e86c1',
  accent: '#3498db',
  background: '#ffffff',
  text: '#000000',
  border: '#cccccc',
  highlight: '#00a1ff',
  success: '#27ae60',
  warning: '#f39c12',
  error: '#e74c3c'
};

export const TOOLBAR_CONFIG = {
  tools: ['select', 'text', 'rectangle', 'line', 'qrcode'] as const,
  zoomLevels: [0.5, 0.75, 1, 1.25, 1.5, 2, 3],
  defaultZoom: 1
};

export const PDF_CONFIG = {
  format: 'a7' as const,
  unit: 'pt' as const,
  orientation: 'portrait' as const,
  quality: 2,
  margins: {
    top: 40,
    right: 40,
    bottom: 40,
    left: 40
  }
};

export const ARTICLE_DEFAULTS = {
  fontSize: 14,
  lineHeight: 1.4,
  fontFamily: 'Traditional Arabic',
  textAlign: 'right' as const,
  direction: 'rtl' as const,
  color: '#000000',
  spacing: 20,
  padding: 10
};
