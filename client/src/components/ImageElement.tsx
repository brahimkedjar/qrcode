// =============================================================
// File: components/elements/ImageElement.tsx
// (Placeholder box with label – swap for real image loading when needed)
// =============================================================
import React from 'react';
import { Group, Rect, Text, Image as KImage } from 'react-konva';
import type { PermisElement } from './types';
import useImage from 'use-image';

interface ImageElementProps {
  element: PermisElement;
  isSelected: boolean;
  onClickElement: (e: any) => void;
  onDragEnd: (e: any) => void;
  onTransformEnd: (e: any) => void;
  onTransform?: (e: any) => void;
}

export const ImageElement: React.FC<ImageElementProps> = ({ element, isSelected, onClickElement, onDragEnd, onTransformEnd, onTransform }) => {
  const [img] = useImage(element.src || '');
  return (
    <Group
      id={element.id}
      x={element.x}
      y={element.y}
      width={element.width}
      height={element.height}
      rotation={element.rotation || 0}
      draggable={element.draggable}
      onClick={onClickElement}
      onTap={onClickElement}
      onDragEnd={onDragEnd}
      onTransformEnd={onTransformEnd}
      onTransform={onTransform}
      opacity={element.opacity || 1}
    >
      {img ? (
        <KImage image={img} width={element.width} height={element.height} listening={false} />
      ) : (
        <>
          <Rect
            width={element.width}
            height={element.height}
            fill="#f0f0f0"
            stroke={isSelected ? '#3498db' : '#cccccc'}
            strokeWidth={1}
          />
          <Text
            text="[Image]"
            fontSize={12}
            fontFamily="Arial"
            fill="#666666"
            width={element.width}
            height={element.height}
            align="center"
            verticalAlign="middle"
          />
        </>
      )}
    </Group>
  );
};
