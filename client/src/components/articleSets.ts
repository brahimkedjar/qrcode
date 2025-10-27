"use client";
import type { ArticleItem } from "./types";
import TEM from "./articles-tem.json";
import TEC from "./articles-tec.json";
import TECEXT from "./articles-tec-extension.json";
import APM from "./articles-apm.json";
import AAC from "./articles-aac.json";
import AAM from "./articles-aam-mines.json";
import AXH from "./articles-axh.json";
import TXC from "./articles-txc.json";
import TXCREN from "./articles-txc-renouvellement.json";
import TXCSUITE from "./articles-txc-suite-exploration.json";
import TXM from "./articles-txm.json";
import TXMREN from "./articles-txm-renouvellement.json";
import TXMSUITE from "./articles-txm-suite-exploration.json";

export type ArticleSetMeta = {
  key: string;
  name: string;
  source: "static" | "custom";
};

type ArticleSetData = {
  name: string;
  articles: ArticleItem[];
};

const STATIC_SETS: Record<string, ArticleSetData> = {
  "permis-tem": { name: (TEM as any).name || "TEM - ترخيص الاستكشاف", articles: (TEM as any).articles || [] },
  "permis-tec": { name: (TEC as any).name || "TEC", articles: (TEC as any).articles || [] },
  "permis-tec-extension": { name: (TECEXT as any).name || "TEC EXTENSION", articles: (TECEXT as any).articles || [] },
  "permis-apm": { name: (APM as any).name || "APM", articles: (APM as any).articles || [] },
  "permis-aac": { name: (AAC as any).name || "AAC", articles: (AAC as any).articles || [] },
  "permis-aam-mines": { name: (AAM as any).name || "AAM MINES", articles: (AAM as any).articles || [] },
  "permis-axh": { name: (AXH as any).name || "AXH", articles: (AXH as any).articles || [] },
  "permis-txc": { name: (TXC as any).name || "TXC", articles: (TXC as any).articles || [] },
  "permis-txc-renouvellement": { name: (TXCREN as any).name || "TXC RENOUVELLEMENT", articles: (TXCREN as any).articles || [] },
  "permis-txc-suite-exploration": { name: (TXCSUITE as any).name || "TXC SUITE A EXPLORATION", articles: (TXCSUITE as any).articles || [] },
  "permis-txm": { name: (TXM as any).name || "TXM", articles: (TXM as any).articles || [] },
  "permis-txm-renouvellement": { name: (TXMREN as any).name || "TXM RENOUVELLEMENT", articles: (TXMREN as any).articles || [] },
  "permis-txm-suite-exploration": { name: (TXMSUITE as any).name || "TXM SUITE A EXPLORATION", articles: (TXMSUITE as any).articles || [] }
};

const CUSTOM_KEY = "customArticleSets";

function readCustom(): Record<string, ArticleSetData> {
  if (typeof window === "undefined") return {};
  try {
    const raw = localStorage.getItem(CUSTOM_KEY);
    if (!raw) return {};
    const parsed = JSON.parse(raw);
    if (parsed && typeof parsed === "object") return parsed;
  } catch {}
  return {};
}

function writeCustom(data: Record<string, ArticleSetData>) {
  if (typeof window === "undefined") return;
  try {
    localStorage.setItem(CUSTOM_KEY, JSON.stringify(data));
  } catch {}
}

const apiBase = ((globalThis as any).process?.env?.NEXT_PUBLIC_API_URL) || '';

export function listArticleSets(): ArticleSetMeta[] {
  // synchronous fallback listing (static + local)
  const out: ArticleSetMeta[] = [];
  Object.entries(STATIC_SETS).forEach(([key, v]) => out.push({ key, name: v.name, source: 'static' }));
  const custom = readCustom();
  Object.entries(custom).forEach(([key, v]) => out.push({ key, name: v.name, source: 'custom' }));
  // async server listing can be fetched by UI if needed; here we return fallback + let UI refresh if needed
  return out;
}

export async function listServerArticleSets(): Promise<ArticleSetMeta[]> {
  const res = await fetch(`${apiBase}/api/article-sets`);
  if (!res.ok) throw new Error('Failed to list server article sets');
  const data = await res.json();
  return (data || []).map((x: any) => ({ key: x.key, name: x.name, source: 'custom' as const }));
}

export async function getArticlesForSet(key: string): Promise<ArticleItem[]> {
  if (STATIC_SETS[key]) return (STATIC_SETS[key].articles || []) as ArticleItem[];
  try {
    const res = await fetch(`${apiBase}/api/article-sets/${encodeURIComponent(key)}`);
    if (res.ok) {
      const data = await res.json();
      return (data?.articles || []) as ArticleItem[];
    }
  } catch {}
  const custom = readCustom();
  return (custom[key]?.articles || []) as ArticleItem[];
}

export async function saveArticleSet(key: string, name: string, articles: ArticleItem[]) {
  try {
    const res = await fetch(`${apiBase}/api/article-sets/${encodeURIComponent(key)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, articles })
    });
    if (!res.ok) throw new Error('Failed to save on server');
    return;
  } catch {
    const custom = readCustom();
    custom[key] = { name, articles };
    writeCustom(custom);
  }
}

export function deleteArticleSet(key: string) {
  const custom = readCustom();
  if (custom[key]) {
    delete custom[key];
    writeCustom(custom);
  }
}

export function exportArticleSet(name: string, articles: ArticleItem[]) {
  const blob = new Blob([JSON.stringify({ name, articles }, null, 2)], { type: "application/json" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = `${name.replace(/\s+/g, "_")}.json`;
  a.click();
  URL.revokeObjectURL(a.href);
}

export async function importArticleSet(file: File): Promise<{ key: string; name: string } | null> {
  const text = await file.text();
  try {
    const data = JSON.parse(text);
    // Try server first
    try {
      const res = await fetch(`${apiBase}/api/article-sets/import`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(data)
      });
      if (res.ok) return (await res.json()) as { key: string; name: string };
    } catch {}
    // Fallback to local storage
    const name: string = data.name || data.templateName || file.name.replace(/\.json$/i, "");
    const key = name.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
    const articles: ArticleItem[] = Array.isArray(data.articles) ? data.articles : [];
    const custom = readCustom();
    custom[key] = { name, articles };
    writeCustom(custom);
    return { key, name };
  } catch (e) {
    console.error('Failed to import article set', e);
    return null;
  }
}

export function inferDefaultArticleSetKey(initialData: any, sets: ArticleSetMeta[]): string {
  const code = String(initialData?.code_demande || "").toLowerCase();
  const type = String(initialData?.typePermis?.lib_type || initialData?.type || "").toLowerCase();
  // try direct match on known static key
  if (code.includes("5419") || code.includes("pxc") || type.includes("pxc")) {
    if (sets.some(s => s.key === "permis-5419-pxc")) return "permis-5419-pxc";
  }
  // map permit type TEM
  if (type.includes("tem") || type.includes("الاستكشاف") || type.includes("ترخيص")) {
    if (sets.some(s => s.key === "permis-tem")) return "permis-tem";
  }
  // TEC (exploitation)
  if (type.includes("tec") || type.includes("استغلال")) {
    if (type.includes("extension") || type.includes("تمديد")) {
      if (sets.some(s => s.key === "permis-tec-extension")) return "permis-tec-extension";
    }
    if (sets.some(s => s.key === "permis-tec")) return "permis-tec";
  }
  // APM
  if (type.includes("apm")) {
    if (sets.some(s => s.key === "permis-apm")) return "permis-apm";
  }
  // AAC
  if (type.includes("aac")) {
    if (sets.some(s => s.key === "permis-aac")) return "permis-aac";
  }
  // AAM
  if (type.includes("aam")) {
    if (sets.some(s => s.key === "permis-aam-mines")) return "permis-aam-mines";
  }
  // AXH
  if (type.includes("axh")) {
    if (sets.some(s => s.key === "permis-axh")) return "permis-axh";
  }
  // TXC variants
  if (type.includes("txc")) {
    if (type.includes("renouvel") || type.includes("تجديد")) {
      if (sets.some(s => s.key === "permis-txc-renouvellement")) return "permis-txc-renouvellement";
    }
    if (type.includes("suite") || type.includes("اثر") || type.includes("إثر") || type.includes("apres") || type.includes("بعد")) {
      if (sets.some(s => s.key === "permis-txc-suite-exploration")) return "permis-txc-suite-exploration";
    }
    if (sets.some(s => s.key === "permis-txc")) return "permis-txc";
  }
  // TXM variants
  if (type.includes("txm")) {
    if (type.includes("renouvel") || type.includes("تجديد")) {
      if (sets.some(s => s.key === "permis-txm-renouvellement")) return "permis-txm-renouvellement";
    }
    if (type.includes("suite") || type.includes("اثر") || type.includes("إثر") || type.includes("apres") || type.includes("بعد")) {
      if (sets.some(s => s.key === "permis-txm-suite-exploration")) return "permis-txm-suite-exploration";
    }
    if (sets.some(s => s.key === "permis-txm")) return "permis-txm";
  }
  // fallback to first available
  return sets[0]?.key || "permis-5419-pxc";
}
