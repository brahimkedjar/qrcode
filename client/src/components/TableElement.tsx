// =============================================================
// File: components/elements/TableElement.tsx
// =============================================================
import React, { useMemo, useRef } from 'react';
import { Group, Rect, Line, Text } from 'react-konva';
import type { PermisElement } from './types';

interface TableElementProps {
  element: PermisElement;
  isSelected: boolean;
  onClickElement: (e: any) => void;
  onDragEnd: (e: any) => void;
  onTransformEnd: (e: any) => void;
  onTransform?: (e: any) => void;
}

export const TableElement: React.FC<TableElementProps> = ({
  element,
  isSelected,
  onClickElement,
  onDragEnd,
  onTransformEnd,
  onTransform,
}) => {
  const groupRef = useRef<any>(null);

  const cfg = useMemo(() => {
    const blockCols = Math.max(1, Math.min(5, Number(element.blockCols || 1)));
    const rowHeight = Math.max(14, Number(element.rowHeight || 34));
    const baseColWidths = (element.colWidths && element.colWidths.length > 0) ? element.colWidths : [60, 90, 90];
    const headerText = element.headerText || '';
    const headerHeight = Math.max(0, Number(element.headerHeight || 48));
    const headerFill = element.headerFill || '#f5f5f5';
    const altRowFill = element.altRowFill || '#f8f8f8';
    const stroke = element.stroke || '#000';
    const strokeWidth = Number(element.strokeWidth || 1.2);
    const fontFamily = element.tableFontFamily || element.fontFamily || 'Arial';
    const fontSize = Number(element.tableFontSize || 14);
    const textAlign = element.tableTextAlign || 'left';
    const data = Array.isArray(element.tableData) ? element.tableData : [];
    // Fit rows exactly to the dataset (no extra empty rows)
    const rowsPerCol = Math.max(1, Math.ceil((data.length || 1) / blockCols));
    const columns = (element.tableColumns && element.tableColumns.length > 0)
      ? element.tableColumns
      : [
        { key: 'point', title: 'Point', width: baseColWidths[0], align: 'left' },
        { key: 'x', title: 'X', width: baseColWidths[1], align: 'left' },
        { key: 'y', title: 'Y', width: baseColWidths[2], align: 'left' },
      ];
    const blockBaseW = (columns.map(c => c.width || 0).reduce((a, b) => a + b, 0)) || (baseColWidths.reduce((a, b) => a + b, 0));
    // For small coordinate lists, widen tables visually without adding empty rows
    const minBlocksForWidth = (Array.isArray(data) && data.length < 10) ? 2 : 1;
    const desiredWidth = Math.max(120, Number(element.width || blockBaseW * Math.max(blockCols, minBlocksForWidth)));
    const desiredHeight = Math.max(60, Number(element.height || (headerHeight + rowHeight * (rowsPerCol + 1))));
    // scale columns to fit desired width equally per block
    const perBlockWidth = desiredWidth / blockCols;
    const scale = blockBaseW > 0 ? perBlockWidth / blockBaseW : 1;
    const scaledCols = columns.map(c => ({ ...c, width: Math.max(10, Math.round((c.width || 0) * scale)) }));
    const blockW = scaledCols.reduce((a, b) => a + (b.width || 0), 0);

    return {
      blockCols,
      rowsPerCol,
      rowHeight,
      headerText,
      headerHeight,
      headerFill,
      altRowFill,
      stroke,
      strokeWidth,
      headerTextAlign: (element as any).headerTextAlign || 'center',
      showCellBorders: (element as any).showCellBorders !== false,
      gridColor: (element as any).tableGridColor || '#4F4040',
      gridWidth: typeof (element as any).tableGridWidth === 'number' ? (element as any).tableGridWidth : 1,
      outerBorderColor: (element as any).outerBorderColor || stroke,
      outerBorderWidth: typeof (element as any).outerBorderWidth === 'number' ? (element as any).outerBorderWidth : strokeWidth,
      cellPadding: typeof (element as any).cellPadding === 'number' ? (element as any).cellPadding : 8,
      fontFamily,
      fontSize,
      textAlign,
      data,
      columns: scaledCols,
      width: desiredWidth,
      height: desiredHeight,
      blockW,
    } as const;
  }, [element]);

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
  const tableStartY = headerY + cfg.headerHeight;

  const texts: React.ReactNode[] = [];
  const shapes: React.ReactNode[] = [];

  // Header band spanning full width
  if (cfg.headerText && cfg.headerHeight > 0) {
    shapes.push(
      <Rect key="hdr-bg" x={0} y={headerY} width={cfg.width} height={cfg.headerHeight} stroke={cfg.outerBorderColor} strokeWidth={cfg.outerBorderWidth} fill={cfg.headerFill} />
    );
    texts.push(
      <Text
        key="hdr-text"
        x={6}
        y={headerY + 4}
        width={cfg.width - 12}
        text={cfg.headerText}
        fontSize={Math.max(12, cfg.fontSize + 6)}
        fontFamily={cfg.fontFamily}
        fill="#000"
        align={cfg.headerTextAlign as any}
        direction="rtl"
      />
    );
  }

  for (let b = 0; b < cfg.blockCols; b++) {
    const bx = b * cfg.blockW;
    // Header row inside block
    shapes.push(
      <Rect key={`col-hdr-${b}`} x={bx} y={tableStartY} width={cfg.blockW} height={cfg.rowHeight} stroke={cfg.outerBorderColor} strokeWidth={cfg.outerBorderWidth} fill="#eeeeee" />
    );
    // Vertical separators (height depends on rows in this block)
    let cx = bx;
    cfg.columns.forEach((col, cIdx) => {
      // column header text
      texts.push(
        <Text key={`hdrtext-${b}-${cIdx}`} x={cx + 8} y={tableStartY + 4} width={(col.width || 0) - 16} text={String(col.title)} fontSize={cfg.fontSize} fontFamily={cfg.fontFamily} fill="#000" align={col.align || 'left'} />
      );
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
        <Line key={`v-${b}-${cIdx}`} x={cx} y={tableStartY} points={[0, 0, 0, cfg.rowHeight * (rowsInBlock + 1)]} stroke={cfg.outerBorderColor} strokeWidth={cfg.outerBorderWidth} />
      );
    });
    for (let r = 0; r < rowsInBlock; r++) {
      const ry = tableStartY + cfg.rowHeight * (r + 1);
      const fill = r % 2 === 0 ? cfg.altRowFill : '#ffffff';
      shapes.push(
        <Rect key={`rowbg-${b}-${r}`} x={bx} y={ry} width={cfg.blockW} height={cfg.rowHeight} stroke="#cccccc" strokeWidth={0.5} fill={fill} />
      );
      const idx = startIndex + r;
      if (idx < cfg.data.length) {
        const row = cfg.data[idx] || {} as any;
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
            <Text key={`cell-${b}-${r}-${cIdx}`} x={cx2 + cfg.cellPadding} y={ry + 4} width={(col.width || 0) - cfg.cellPadding * 2} text={val} fontSize={cfg.fontSize} fontFamily={cfg.fontFamily} fill="#000" align={col.align || 'left'} />
          );
          cx2 += (col.width || 0);
        });
      }
      // Horizontal grid line
      shapes.push(<Line key={`h-${b}-${r}`} x={bx} y={ry} points={[0, 0, cfg.blockW, 0]} stroke={cfg.gridColor} strokeWidth={cfg.gridWidth} />);
    }
    // Outer border for the block
    shapes.push(<Rect key={`block-${b}`} x={bx} y={tableStartY} width={cfg.blockW} height={cfg.rowHeight * (rowsInBlock + 1)} stroke={cfg.outerBorderColor} strokeWidth={cfg.outerBorderWidth} fill="transparent" />);
  }

  return (
    <Group {...common} ref={groupRef}>
      {shapes}
      {texts}
    </Group>
  );
};

export default TableElement;
