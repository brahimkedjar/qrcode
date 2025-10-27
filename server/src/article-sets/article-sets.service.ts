import { Injectable } from '@nestjs/common';
import { JsonStore } from '../storage/json-store';

type ArticleItem = any;
type ArticleSet = { name: string; articles: ArticleItem[] };

@Injectable()
export class ArticleSetsService {
  private store = new JsonStore<any>('article-sets.json');

  list() {
    const db = this.store.read() as Record<string, ArticleSet>;
    return Object.entries(db).map(([key, v]) => ({ key, name: v.name }));
  }

  get(key: string) {
    const db = this.store.read() as Record<string, ArticleSet>;
    return db[key] || null;
  }

  put(key: string, data: ArticleSet) {
    const db = this.store.read() as Record<string, ArticleSet>;
    db[key] = { name: data.name, articles: Array.isArray(data.articles) ? data.articles : [] };
    this.store.write(db);
  }

  import(data: any) {
    const name: string = data?.name || data?.templateName || 'Imported Set';
    const key = name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
    this.put(key, { name, articles: Array.isArray(data?.articles) ? data.articles : [] });
    return { key, name };
  }
}
