// =============================================================
// File: components/elements/TextElement.tsx
// =============================================================
import React, { useEffect, useMemo, useRef, useState } from 'react';
import { Group, Rect, Text } from 'react-konva';
import type { PermisElement } from './types';

interface TextElementProps {
  element: PermisElement;
  isSelected: boolean;
  zoom: number;
  onClickElement: (e: any) => void;
  onDragEnd: (e: any) => void;
  onTransformEnd: (e: any) => void;
  onTransform?: (e: any) => void;
  onDblClickText: () => void;
}

export const TextElement: React.FC<TextElementProps> = ({
  element, isSelected, zoom, onClickElement, onDragEnd, onTransformEnd, onTransform, onDblClickText
}) => {
  const textNodeRef = useRef<any>(null);
  const [textBounds, setTextBounds] = useState({ width: element.width || 240, height: element.height || 40 });

  const safeFontFamily = (fam?: string) => {
    if (!fam) return fam as any;
    const trimmed = String(fam).trim();
    if (/^['"].*['"]$/.test(trimmed)) return trimmed; // already quoted
    return /\s/.test(trimmed) ? `"${trimmed}"` : trimmed;
  };

  // Canvas text measurement context (memoized)
  let __measureCtx: CanvasRenderingContext2D | null = null;
  const ensureMeasureCtx = () => {
    try {
      if (__measureCtx) return __measureCtx;
      const cnv = document.createElement('canvas');
      const ctx = cnv.getContext('2d');
      if (ctx) __measureCtx = ctx;
      return __measureCtx;
    } catch {}
    return null;
  };
  const measureWidth = (text: string, fontSize: number, fontFamily?: string, fontWeight?: 'bold' | 'normal') => {
    const s = String(text || '');
    if (!s) return 0;
    const ctx = ensureMeasureCtx();
    if (ctx) {
      try {
        const fam = safeFontFamily(fontFamily) || 'Arial';
        const weight = fontWeight === 'bold' ? 'bold' : 'normal';
        ctx.font = `${weight} ${Math.max(8, fontSize)}px ${fam}`;
        const m = ctx.measureText(s);
        return Math.ceil(m.width || 0);
      } catch {}
    }
    const factor = 0.48;
    return Math.ceil(s.length * fontSize * factor);
  };

  const measureMetrics = (fontSize: number, fontFamily?: string, fontWeight?: 'bold' | 'normal') => {
    const fam = safeFontFamily(fontFamily) || 'Arial';
    const weight = fontWeight === 'bold' ? 'bold' : 'normal';
    const ctx = ensureMeasureCtx();
    if (ctx) {
      try {
        ctx.font = `${weight} ${Math.max(8, fontSize)}px ${fam}`;
        const m = ctx.measureText('Hg');
        const ascent = (m as any).actualBoundingBoxAscent ?? fontSize * 0.78;
        const descent = (m as any).actualBoundingBoxDescent ?? fontSize * 0.22;
        return { ascent, descent };
      } catch {}
    }
    return { ascent: fontSize * 0.78, descent: fontSize * 0.22 };
  };

  // Build styled runs from base text and element.styledRanges
  const styledLayout = useMemo(() => {
    if (!element.styledRanges || element.styledRanges.length === 0) return null;
    const text = String(element.text || '');
    const baseFont = element.fontSize || 20;
    const baseFamily = element.fontFamily;
    const baseColor = element.color || '#000';
    const dir = (element.direction || 'ltr') as 'rtl' | 'ltr';
    const align = (element.textAlign || (dir === 'rtl' ? 'right' : 'left')) as 'left' | 'center' | 'right';
    const widthLimit = Math.max(10, element.width || 0);
    const lineHMultiplier = element.lineHeight || 1.2;

    type Run = { start: number; end: number; fontSize: number; fontWeight: 'bold'|'normal'; color?: string; underline?: boolean };
    let runs: Run[] = [{ start: 0, end: text.length, fontSize: baseFont, fontWeight: 'normal', color: baseColor, underline: false }];
    const ranges = [...(element.styledRanges || [])].sort((a,b) => a.start - b.start);
    for (const rg of ranges) {
      const ns: Run[] = [];
      for (const run of runs) {
        if (rg.end <= run.start || rg.start >= run.end) {
          ns.push(run); // no overlap
          continue;
        }
        if (run.start < rg.start) ns.push({ ...run, end: rg.start });
        const mid: Run = {
          ...run,
          start: Math.max(run.start, rg.start),
          end: Math.min(run.end, rg.end),
          fontSize: typeof rg.fontSize === 'number' ? rg.fontSize : run.fontSize,
          fontWeight: (rg.fontWeight as any) || run.fontWeight,
          color: rg.color || run.color,
          underline: typeof (rg as any).underline === 'boolean' ? !!(rg as any).underline : !!run.underline,
        };
        ns.push(mid);
        if (rg.end < run.end) ns.push({ ...run, start: rg.end });
      }
      runs = ns;
    }

    // Tokenize runs into space/newline-aware tokens
    type Token = { text: string; width: number; fontSize: number; fontWeight: 'bold'|'normal'; color?: string; underline?: boolean };
    const tokens: Token[] = [];
    for (const run of runs) {
      const slice = text.slice(run.start, run.end);
      // Split by newline while preserving spaces as tokens
      let buf = '';
      const flushBuf = () => {
        if (!buf) return;
        tokens.push({ text: buf, width: measureWidth(buf, run.fontSize, baseFamily, run.fontWeight), fontSize: run.fontSize, fontWeight: run.fontWeight, color: run.color, underline: run.underline });
        buf = '';
      };
      for (let i = 0; i < slice.length; i++) {
        const ch = slice[i];
        if (ch === '\n') {
          flushBuf();
          tokens.push({ text: '\n', width: 0, fontSize: run.fontSize, fontWeight: run.fontWeight, color: run.color, underline: run.underline });
        } else if (/\s/.test(ch)) {
          flushBuf();
          const spaceW = measureWidth(ch, run.fontSize, baseFamily, run.fontWeight);
          tokens.push({ text: ch, width: spaceW, fontSize: run.fontSize, fontWeight: run.fontWeight, color: run.color, underline: run.underline });
        } else {
          buf += ch;
        }
      }
      flushBuf();
    }

    // Line layout with wrapping
    type Segment = { x: number; y: number; text: string; fontSize: number; fontWeight: 'bold'|'normal'; color?: string; ascent: number; descent: number; underline?: boolean };
    type Line = { width: number; height: number; ascent: number; descent: number; segments: Segment[] };
    const lines: Line[] = [];
    let curSegs: Segment[] = [];
    let curWidth = 0;
    let curMaxAscent = measureMetrics(baseFont, baseFamily, 'normal').ascent;
    let curMaxDescent = measureMetrics(baseFont, baseFamily, 'normal').descent;
    const pushLine = () => {
      const lineWidth = curSegs.reduce((a,b) => a + measureWidth(b.text, b.fontSize, baseFamily, b.fontWeight), 0);
      const baseHeight = curMaxAscent + curMaxDescent;
      const lineHeight = Math.ceil(baseHeight * lineHMultiplier);
      lines.push({ width: lineWidth, height: lineHeight, ascent: curMaxAscent, descent: curMaxDescent, segments: curSegs });
      curSegs = [];
      curWidth = 0;
      const baseM = measureMetrics(baseFont, baseFamily, 'normal');
      curMaxAscent = baseM.ascent;
      curMaxDescent = baseM.descent;
    };
    const tryAddToken = (tk: Token) => {
      // newline forces break
      if (tk.text === '\n') { pushLine(); return; }
      const isSpace = /\s+/.test(tk.text);
      const tokenWidth = tk.width;
      const limit = element.width ? widthLimit : Infinity;
      if (curWidth > 0 && (curWidth + tokenWidth) > limit && limit < Infinity) {
        // break line before token
        pushLine();
      }
      // if token wider than limit and we are at line start, split token by chars
      if (tokenWidth > limit && limit < Infinity) {
        let piece = '';
        let pieceW = 0;
        for (const ch of tk.text) {
          const w = measureWidth(ch, tk.fontSize, baseFamily, tk.fontWeight);
          if (pieceW + w > limit && piece) {
            const mm = measureMetrics(tk.fontSize, baseFamily, tk.fontWeight);
            curSegs.push({ x: 0, y: 0, text: piece, fontSize: tk.fontSize, fontWeight: tk.fontWeight, color: tk.color, ascent: mm.ascent, descent: mm.descent, underline: tk.underline });
            curWidth = 0; // we'll compute actual x later
            curMaxAscent = Math.max(curMaxAscent, mm.ascent);
            curMaxDescent = Math.max(curMaxDescent, mm.descent);
            pushLine();
            piece = ch;
            pieceW = w;
          } else {
            piece += ch;
            pieceW += w;
          }
        }
        if (piece) {
          const mm = measureMetrics(tk.fontSize, baseFamily, tk.fontWeight);
          curSegs.push({ x: 0, y: 0, text: piece, fontSize: tk.fontSize, fontWeight: tk.fontWeight, color: tk.color, ascent: mm.ascent, descent: mm.descent, underline: tk.underline });
          curWidth += pieceW;
          curMaxAscent = Math.max(curMaxAscent, mm.ascent);
          curMaxDescent = Math.max(curMaxDescent, mm.descent);
        }
        return;
      }
      const mm = measureMetrics(tk.fontSize, baseFamily, tk.fontWeight);
      curSegs.push({ x: 0, y: 0, text: tk.text, fontSize: tk.fontSize, fontWeight: tk.fontWeight, color: tk.color, ascent: mm.ascent, descent: mm.descent, underline: tk.underline });
      curWidth += tokenWidth;
      curMaxAscent = Math.max(curMaxAscent, mm.ascent);
      curMaxDescent = Math.max(curMaxDescent, mm.descent);
    };
    tokens.forEach(tryAddToken);
    // push last line
    if (curSegs.length > 0) pushLine();

    // Position segments in each line according to direction/alignment
    const maxWidth = element.width || Math.max(1, ...lines.map(l => l.width));
    let y = 0;
    const placed: Segment[] = [];
    for (const line of lines) {
      const lineWidth = Math.min(line.width, maxWidth);
      let startX = 0;
      if (align === 'center') startX = Math.max(0, (maxWidth - lineWidth) / 2);
      else if (align === 'right') startX = Math.max(0, (maxWidth - lineWidth));
      let acc = 0;
      if (dir === 'rtl') {
        // place segments from right to left
        for (const seg of line.segments) {
          const segW = measureWidth(seg.text, seg.fontSize, baseFamily, seg.fontWeight);
          const x = startX + (lineWidth - (acc + segW));
          // baseline align: y offset so baselines match
          const ySeg = y + (line.ascent - seg.ascent);
          placed.push({ ...seg, x, y: ySeg });
          acc += segW;
        }
      } else {
        for (const seg of line.segments) {
          const segW = measureWidth(seg.text, seg.fontSize, baseFamily, seg.fontWeight);
          const x = startX + acc;
          const ySeg = y + (line.ascent - seg.ascent);
          placed.push({ ...seg, x, y: ySeg });
          acc += segW;
        }
      }
      y += line.height;
    }

    return {
      placed,
      totalWidth: maxWidth,
      totalHeight: Math.max(y, (element.height || 0)),
    };
  }, [element.text, element.styledRanges, element.width, element.fontSize, element.fontFamily, element.lineHeight, element.textAlign, element.direction]);

  // Update bounds for simple or styled rendering
  useEffect(() => {
    if (styledLayout) {
      setTextBounds({ width: styledLayout.totalWidth, height: styledLayout.totalHeight });
      return;
    }
    if (textNodeRef.current) {
      const box = textNodeRef.current.getClientRect();
      setTextBounds({ width: box.width, height: box.height });
    }
  }, [styledLayout, element.text, element.fontSize, element.fontFamily, element.lineHeight, element.wrap, element.width]);

  // Use the element's explicit width for the transform box so side anchors resize as expected.
  // Fall back to measured bounds if no width is set yet.
  const boxWidth = element.width ?? textBounds.width;
  const boxHeight = element.height ?? textBounds.height;

  const common = {
    id: element.id,
    x: element.x,
    y: element.y,
    width: boxWidth,
    height: boxHeight,
    rotation: element.rotation || 0,
    scaleX: element.scaleX || 1,
    scaleY: element.scaleY || 1,
    draggable: element.draggable,
    onClick: onClickElement,
    onTap: onClickElement,
    onDblClick: onDblClickText,
    onDblTap: onDblClickText,
    onDragEnd,
    onTransformEnd,
    onTransform,
    opacity: element.opacity || 1,
  } as any;

  return (
    <Group {...common}>
      {isSelected && (
        <Rect
          x={0}
          y={0}
          width={boxWidth}
          height={boxHeight}
          fill="rgba(52, 152, 219, 0.1)"
          stroke="#3498db"
          strokeWidth={1}
        />
      )}
      {(!element.styledRanges || element.styledRanges.length === 0) ? (
        <Text
          ref={textNodeRef}
          x={0}
          y={0}
          text={element.text}
          fontSize={element.fontSize}
          fontFamily={safeFontFamily(element.fontFamily) as any}
          fontStyle={element.fontWeight === 'bold' ? 'bold' : 'normal'}
          fill={element.color}
          align={element.textAlign}
          lineHeight={element.lineHeight}
          wrap={element.wrap}
          onDblClick={onDblClickText}
          direction={element.direction || 'ltr'}
          listening={true}
          perfectDrawEnabled={false}
          width={element.width}
        />
      ) : (
        // Styled rich text renderer with wrapping and RTL-aware alignment
        (styledLayout?.placed || []).map((seg, i) => (
          <Text
            key={`${element.id}-seg-${i}`}
            x={seg.x}
            y={seg.y}
            text={seg.text}
            fontSize={seg.fontSize}
            fontFamily={safeFontFamily(element.fontFamily) as any}
            fontStyle={seg.fontWeight === 'bold' ? 'bold' : 'normal'}
            fill={seg.color || element.color}
            textDecoration={seg.underline ? 'underline' : undefined}
            align={'left'}
            lineHeight={element.lineHeight || 1.2}
            wrap={'none'}
            direction={element.direction || 'ltr'}
            listening={false}
          />
        ))
      )}
    </Group>
  );
};
