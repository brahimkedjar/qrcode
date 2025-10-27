// File: layout.ts
export const gridBackground = (zoom: number) => {
  const size = 20 * zoom;
  return `
    linear-gradient(90deg, rgba(0,0,0,0.1) 1px, transparent 1px),
    linear-gradient(rgba(0,0,0,0.1) 1px, transparent 1px)
  `.replace(/[\n\s]/g, '');
};

export const clamp = (value: number, min: number, max: number) => {
  return Math.min(Math.max(value, min), max);
};

export const calculateTextDimensions = (
  text: string,
  fontSize: number,
  fontFamily: string,
  maxWidth: number,
  lineHeight: number = 1.4
): { width: number; height: number; lines: number } => {
  // Create a temporary canvas to measure text
  const canvas = document.createElement('canvas');
  const ctx = canvas.getContext('2d');
  
  if (!ctx) return { width: 0, height: 0, lines: 0 };
  
  ctx.font = `${fontSize}px ${fontFamily}`;
  
  const words = text.split(' ');
  let line = '';
  let lines = 0;
  let maxLineWidth = 0;
  
  for (let i = 0; i < words.length; i++) {
    const testLine = line + words[i] + ' ';
    const metrics = ctx.measureText(testLine);
    const testWidth = metrics.width;
    
    if (testWidth > maxWidth && i > 0) {
      lines++;
      maxLineWidth = Math.max(maxLineWidth, ctx.measureText(line).width);
      line = words[i] + ' ';
    } else {
      line = testLine;
    }
  }
  
  lines++;
  maxLineWidth = Math.max(maxLineWidth, ctx.measureText(line).width);
  
  return {
    width: maxLineWidth,
    height: lines * fontSize * lineHeight,
    lines
  };
};

export const fitTextToContainer = (
  text: string,
  containerWidth: number,
  containerHeight: number,
  fontFamily: string,
  maxFontSize: number = 48,
  minFontSize: number = 8,
  lineHeight: number = 1.4
): { fontSize: number; text: string; fitted: boolean } => {
  let fontSize = maxFontSize;
  let fittedText = text;
  let fitted = false;
  
  while (fontSize >= minFontSize) {
    const dimensions = calculateTextDimensions(
      fittedText,
      fontSize,
      fontFamily,
      containerWidth,
      lineHeight
    );
    
    if (dimensions.height <= containerHeight && dimensions.width <= containerWidth) {
      fitted = true;
      break;
    }
    
    fontSize -= 1;
  }
  
  // If still doesn't fit, truncate with ellipsis
  if (!fitted && fontSize >= minFontSize) {
    let truncated = text;
    while (truncated.length > 10 && fontSize >= minFontSize) {
      truncated = truncated.slice(0, -4) + '...';
      const dimensions = calculateTextDimensions(
        truncated,
        fontSize,
        fontFamily,
        containerWidth,
        lineHeight
      );
      
      if (dimensions.height <= containerHeight && dimensions.width <= containerWidth) {
        fittedText = truncated;
        fitted = true;
        break;
      }
    }
  }
  
  return { fontSize, text: fittedText, fitted };
};

export const generateColumnLayout = (
  elements: any[],
  containerWidth: number,
  containerHeight: number,
  margin: number = 40,
  gap: number = 20
): { x: number; y: number; width: number }[] => {
  const positions = [];
  const columnWidth = (containerWidth - margin * 2 - gap) / 2;
  let currentY1 = margin; // First column
  let currentY2 = margin; // Second column
  
  for (const element of elements) {
    const elementHeight = element.height || 100;
    
    // Choose column with more space
    if (currentY1 <= currentY2 && currentY1 + elementHeight <= containerHeight) {
      positions.push({ x: margin, y: currentY1, width: columnWidth });
      currentY1 += elementHeight + gap;
    } else if (currentY2 + elementHeight <= containerHeight) {
      positions.push({ x: margin + columnWidth + gap, y: currentY2, width: columnWidth });
      currentY2 += elementHeight + gap;
    } else {
      // If both columns are full, expand container
      positions.push({ x: margin, y: Math.max(currentY1, currentY2), width: columnWidth });
      currentY1 = Math.max(currentY1, currentY2) + elementHeight + gap;
    }
  }
  
  return positions;
};