// File: components/elements/QRCodeElement.tsx
import React, { useEffect, useRef, useState } from 'react';
import { Group, Rect, Image } from 'react-konva';
import type { PermisElement } from './types';
import * as QRCode from 'qrcode';
import useImage from 'use-image';

interface QRCodeElementProps {
  element: PermisElement;
  isSelected: boolean;
  zoom: number;
  onClickElement: (e: any) => void;
  onDragEnd: (e: any) => void;
  onTransformEnd: (e: any) => void;
  onTransform?: (e: any) => void;
}

export const QRCodeElement: React.FC<QRCodeElementProps> = ({ 
  element, 
  isSelected, 
  zoom, 
  onClickElement, 
  onDragEnd, 
  onTransformEnd,
  onTransform
}) => {
  const [qrDataUrl, setQrDataUrl] = useState<string | null>(null);
  const [image] = useImage(qrDataUrl || '');

  useEffect(() => {
    const generateQRCode = async () => {
      try {
        if (element.qrData) {
          const dataUrl = await QRCode.toDataURL(element.qrData, {
            // Increase resolution and error correction to keep QR readable
            width: 512,
            margin: 4,
            errorCorrectionLevel: 'H',
            color: {
              dark: '#000000FF',
              light: '#FFFFFFFF'
            }
          });
          setQrDataUrl(dataUrl);
        }
      } catch (error) {
        console.error("Failed to generate QR code", error);
      }
    };

    generateQRCode();
  }, [element.qrData]);

  return (
    <Group
      id={element.id}
      x={element.x}
      y={element.y}
      width={element.width}
      height={element.height}
      draggable={element.draggable}
      onDragEnd={onDragEnd}
      onClick={onClickElement}
      onTap={onClickElement}
      onTransformEnd={onTransformEnd}
      onTransform={onTransform}
      rotation={element.rotation || 0}
      opacity={element.opacity || 1}
    >
      <Rect
        width={element.width}
        height={element.height}
        fill="#ffffff"
        stroke={isSelected ? '#3498db' : 'transparent'}
        strokeWidth={isSelected ? 2 : 0}
      />
      {image && (
        <Image
          image={image}
          width={element.width}
          height={element.height}
          perfectDrawEnabled={false}
          listening={false}
        />
      )}
    </Group>
  );
};
