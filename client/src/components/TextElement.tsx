// =============================================================
// File: components/elements/TextElement.tsx
// =============================================================
import React, { useEffect, useRef, useState } from 'react';
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

  // Update bounds whenever text or style changes
  useEffect(() => {
    if (textNodeRef.current) {
      const box = textNodeRef.current.getClientRect();
      setTextBounds({
        width: box.width,
        height: box.height,
      });
    }
  }, [
    element.text,
    element.fontSize,
    element.fontFamily,
    element.lineHeight,
    element.wrap,
    element.width, // Added to react to width changes
  ]);

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
    draggable: element.draggable,
    onClick: onClickElement,
    onTap: onClickElement,
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
          fontFamily={element.fontFamily}
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
        // Basic rich text renderer: split by lines and styled ranges, no wrapping
        (() => {
          const baseFontSize = element.fontSize || 20;
          const lh = element.lineHeight || 1.2;
          const lines = String(element.text || '').split(/\r?\n/);
          const ranges = element.styledRanges || [];
          const estWidth = (s: string, fs: number) => Math.ceil((s || '').length * fs * 0.48);
          let yOff = 0;
          const nodes: any[] = [];
          let idxBase = 0;
          for (const line of lines) {
            let xOff = 0;
            const lineStart = idxBase;
            const lineEnd = idxBase + line.length;
            // Build segments by slicing ranges intersecting this line
            let pos = lineStart;
            while (pos < lineEnd) {
              // find next range starting at or after pos
              const range = ranges.find(r => r.start < lineEnd && r.end > pos);
              if (!range || range.start >= lineEnd || range.end <= pos) {
                // no style here: push normal until lineEnd or next range start
                const nextStart = Math.min(lineEnd, range ? Math.max(range.start, pos) : lineEnd);
                const substr = String(element.text || '').slice(pos, nextStart);
                if (substr) {
                  const fs = baseFontSize;
                  nodes.push(
                    <Text key={`${element.id}-seg-${pos}`}
                      x={xOff}
                      y={yOff}
                      text={substr}
                      fontSize={fs}
                      fontFamily={element.fontFamily}
                      fill={element.color}
                      align={element.textAlign}
                      lineHeight={lh}
                      wrap={'none'}
                      listening={false}
                      width={undefined as any}
                    />
                  );
                  xOff += estWidth(substr, fs);
                }
                pos = nextStart;
              } else {
                // styled range intersects this position
                const segStart = Math.max(pos, range.start);
                const segEnd = Math.min(lineEnd, range.end);
                const substr = String(element.text || '').slice(segStart, segEnd);
                const fs = range.fontSize || baseFontSize;
                const fontStyle = range.fontWeight === 'bold' ? 'bold' : 'normal';
                const textDecoration = range.underline ? 'underline' : undefined;
                nodes.push(
                  <Text key={`${element.id}-seg-${segStart}`}
                    x={xOff}
                    y={yOff}
                    text={substr}
                    fontSize={fs}
                    fontFamily={element.fontFamily}
                    fontStyle={fontStyle as any}
                    textDecoration={textDecoration as any}
                    fill={range.color || element.color}
                    align={element.textAlign}
                    lineHeight={lh}
                    wrap={'none'}
                    listening={false}
                    width={undefined as any}
                  />
                );
                xOff += estWidth(substr, fs);
                pos = segEnd;
              }
            }
            yOff += Math.ceil(baseFontSize * lh);
            idxBase += line.length + 1; // include newline
          }
          return nodes;
        })()
      )}
    </Group>
  );
};
