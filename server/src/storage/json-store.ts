import fs from 'node:fs';
import path from 'node:path';

export class JsonStore<T = any> {
  private file: string;

  constructor(filename: string) {
    const dataDir = path.resolve(process.cwd(), 'tm-app', 'server', 'data');
    if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
    this.file = path.join(dataDir, filename);
    if (!fs.existsSync(this.file)) {
      fs.writeFileSync(this.file, JSON.stringify({}, null, 2), 'utf8');
    }
  }

  read(): Record<string, T> {
    try {
      const raw = fs.readFileSync(this.file, 'utf8');
      const obj = JSON.parse(raw);
      return (obj && typeof obj === 'object') ? obj : {};
    } catch {
      return {} as any;
    }
  }

  write(data: Record<string, T>) {
    fs.writeFileSync(this.file, JSON.stringify(data, null, 2), 'utf8');
  }
}

