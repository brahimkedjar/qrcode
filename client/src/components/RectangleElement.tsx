// =============================================================
// File: components/elements/RectangleElement.tsx
// =============================================================
import React from 'react';
import { Rect } from 'react-konva';
import type { PermisElement } from './types';

interface RectangleElementProps {
  element: PermisElement;
  isSelected: boolean;
  onClickElement: (e: any) => void;
  onDragEnd: (e: any) => void;
  onTransformEnd: (e: any) => void;
  onTransform?: (e: any) => void;
}

export const RectangleElement: React.FC<RectangleElementProps> = ({ element, isSelected, onClickElement, onDragEnd, onTransformEnd, onTransform }) => {
  // Make decorative rectangles (borders, grids) non-interactive so underlying text is selectable
  const nonInteractive = !!(element.meta?.isBorder || element.meta?.nonInteractive || element.meta?.isGrid);
  return (
    <Rect
      id={element.id}
      x={element.x}
      y={element.y}
      width={element.width}
      height={element.height}
      rotation={element.rotation || 0}
      draggable={element.draggable && !nonInteractive}
      onClick={onClickElement}
      onTap={onClickElement}
      onDragEnd={onDragEnd}
      onTransformEnd={onTransformEnd}
      onTransform={onTransform}
      fill={element.fill}
      stroke={isSelected ? '#3498db' : element.stroke}
      strokeWidth={isSelected ? 1 : element.strokeWidth || 1}
      cornerRadius={element.cornerRadius}
      opacity={element.opacity || 1}
      shadowEnabled={isSelected}
      shadowColor={'#3498db'}
      shadowBlur={isSelected ? 5 : 0}
      shadowOpacity={isSelected ? 0.3 : 0}
      listening={!nonInteractive}
    />
  );
};
