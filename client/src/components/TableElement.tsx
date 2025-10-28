// =============================================================
// File: components/elements/TableElement.tsx
// =============================================================
import React, { useMemo, useRef } from 'react';
import { Group, Rect, Line, Text } from 'react-konva';
import type { PermisElement } from './types';
import { deriveTableLayout } from './tableLayout';

interface TableElementProps {
  element: PermisElement;
  isSelected: boolean;
  onClickElement: (e: any) => void;
  onDragEnd: (e: any) => void;
  onTransformEnd: (e: any) => void;
  onTransform?: (e: any) => void;
  onCellDblClick?: (info: { element: PermisElement; rowIndex: number; colKey: string; local: { x: number; y: number; width: number; height: number } }) => void;
}

export const TableElement: React.FC<TableElementProps> = ({
  element,
  isSelected,
  onClickElement,
  onDragEnd,
  onTransformEnd,
  onTransform,
  onCellDblClick,
}) => {
  const groupRef = useRef<any>(null);
  const cfg = useMemo(() => deriveTableLayout(element), [element]);

  const common = {
    id: element.id,
    x: element.x,
    y: element.y,
    width: cfg.width,
    height: cfg.height,
    rotation: element.rotation || 0,
    draggable: element.draggable,
    onClick: onClickElement,
    onTap: onClickElement,
    onDragEnd,
    onTransformEnd,
    onTransform,
    opacity: element.opacity || 1,
  } as any;

  const totalRows = cfg.rowsPerCol;
  const headerY = 0;
  const hasHeader = cfg.showHeader && cfg.headerHeight > 0;
  const tableStartY = headerY + (hasHeader ? cfg.headerHeight : 0);

  const texts: React.ReactNode[] = [];
  const shapes: React.ReactNode[] = [];

  // Header band spanning full width
  if (hasHeader && cfg.headerText && cfg.headerHeight > 0) {
    shapes.push(
      <Rect key="hdr-bg" x={0} y={headerY} width={cfg.width} height={cfg.headerHeight} stroke={cfg.outerBorderColor} strokeWidth={cfg.outerBorderWidth} fill={cfg.headerFill} />
    );
    texts.push(
      <Text
        key="hdr-text"
        x={0}
        y={headerY}
        width={cfg.width}
        height={cfg.headerHeight}
        text={cfg.headerText}
        fontSize={Math.max(12, cfg.fontSize + 6)}
        fontFamily={cfg.fontFamily}
        fill="#000"
        align={cfg.headerTextAlign as any}
        verticalAlign="middle"
        direction="rtl"
      />
    );
  }

  // Render blocks from right to left to respect RTL layout
  for (let b = 0; b < cfg.blockCols; b++) {
    const bx = cfg.width - cfg.blockW * (b + 1);
    // Header row inside block
    if (hasHeader) {
      shapes.push(
        <Rect key={`col-hdr-${b}`} x={bx} y={tableStartY} width={cfg.blockW} height={cfg.rowHeight} stroke={cfg.outerBorderColor} strokeWidth={cfg.outerBorderWidth} fill="#eeeeee" />
      );
    }
    // Vertical separators (height depends on rows in this block)
    let cx = bx;
    cfg.columns.forEach((col, cIdx) => {
      // column header text
      if (hasHeader) {
        texts.push(
          <Text
            key={`hdrtext-${b}-${cIdx}`}
            x={cx}
            y={tableStartY}
            width={(col.width || 0)}
            height={cfg.rowHeight}
            text={String(col.title)}
            fontSize={cfg.fontSize}
            fontFamily={cfg.fontFamily}
            fill="#000"
            align={col.align || 'center'}
            verticalAlign="middle"
          />
        );
      }
      // vertical line after column
      cx += (col.width || 0);
      // Will draw after we know how many rows this block has
    });
    // Rows
    const startIndex = b * totalRows;
    const remaining = Math.max(0, cfg.data.length - startIndex);
    const rowsInBlock = Math.min(totalRows, remaining);
    // Draw vertical separators with correct height per block
    cx = bx;
    cfg.columns.forEach((col, cIdx) => {
      cx += (col.width || 0);
      shapes.push(
        <Line key={`v-${b}-${cIdx}`} x={cx} y={tableStartY} points={[0, 0, 0, cfg.rowHeight * (rowsInBlock + (hasHeader ? 1 : 0))]} stroke={cfg.outerBorderColor} strokeWidth={cfg.outerBorderWidth} />
      );
    });
    for (let r = 0; r < rowsInBlock; r++) {
      const ry = tableStartY + cfg.rowHeight * (r + (hasHeader ? 1 : 0));
      const fill = r % 2 === 0 ? cfg.altRowFill : '#ffffff';
      shapes.push(
        <Rect key={`rowbg-${b}-${r}`} x={bx} y={ry} width={cfg.blockW} height={cfg.rowHeight} stroke="#cccccc" strokeWidth={0.5} fill={fill} />
      );
      const idx = startIndex + r;
      if (idx < cfg.data.length) {
        const row = (cfg.data[idx] || {}) as any;
        let cx2 = bx;
        cfg.columns.forEach((col, cIdx) => {
          // Draw a thin cell border around each cell to clearly separate
          if (cfg.showCellBorders) {
            shapes.push(
              <Rect key={`cellbox-${b}-${r}-${cIdx}`} x={cx2} y={ry} width={(col.width || 0)} height={cfg.rowHeight} stroke={cfg.gridColor} strokeWidth={cfg.gridWidth} fill="transparent" />
            );
          }
          let val = row[col.key] != null ? String(row[col.key]) : '';
          if (!val && col.key === 'point') val = String(idx + 1);
          texts.push(
            <Text
              key={`cell-${b}-${r}-${cIdx}`}
              x={cx2}
              y={ry}
              width={(col.width || 0)}
              height={cfg.rowHeight}
              text={val}
              fontSize={cfg.fontSize}
              fontFamily={cfg.fontFamily}
              fill="#000"
              align={col.align || 'center'}
              verticalAlign="middle"
              onDblClick={(e) => {
                e.cancelBubble = true;
                onCellDblClick?.({
                  element,
                  rowIndex: idx,
                  colKey: String(col.key || ''),
                  local: { x: cx2, y: ry, width: (col.width || 0), height: cfg.rowHeight }
                });
              }}
              onDblTap={(e) => {
                e.cancelBubble = true;
                onCellDblClick?.({
                  element,
                  rowIndex: idx,
                  colKey: String(col.key || ''),
                  local: { x: cx2, y: ry, width: (col.width || 0), height: cfg.rowHeight }
                });
              }}
            />
          );
          cx2 += (col.width || 0);
        });
      }
      // Horizontal grid line
      shapes.push(<Line key={`h-${b}-${r}`} x={bx} y={ry} points={[0, 0, cfg.blockW, 0]} stroke={cfg.gridColor} strokeWidth={cfg.gridWidth} />);
    }
    // Outer border for the block
    shapes.push(<Rect key={`block-${b}`} x={bx} y={tableStartY} width={cfg.blockW} height={cfg.rowHeight * (rowsInBlock + (hasHeader ? 1 : 0))} stroke={cfg.outerBorderColor} strokeWidth={cfg.outerBorderWidth} fill="transparent" />);
  }

  return (
    <Group {...common} ref={groupRef}>
      {shapes}
      {texts}
    </Group>
  );
};

export default TableElement;

