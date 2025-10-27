import React, { useEffect, useRef, useState } from 'react';
import axios from 'axios';
import styles from './PdfGallery.module.css';

type FlatItem = { dir: string; name: string; url: string; mtime: number; size: number };
type GroupItem = { dir: string; files: { name: string; url: string; mtime: number; size: number }[] };

export default function PdfGallery() {
  const API_URL = (import.meta as any).env?.VITE_API_URL || '';
  const [q, setQ] = useState('');
  const [flatItems, setFlatItems] = useState<FlatItem[]>([]);
  const [groupedItems, setGroupedItems] = useState<GroupItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState('');
  const [page, setPage] = useState(1);
  const [hasMore, setHasMore] = useState(true);
  const [mode, setMode] = useState<'flat' | 'group'>('flat');
  const [sort, setSort] = useState<'recent' | 'name'>('recent');
  const obsRef = useRef<HTMLDivElement | null>(null);

  const search = async (reset = false) => {
    setLoading(true); setErr('');
    try {
      const nextPage = reset ? 1 : page;
      const { data } = await axios.get(`${API_URL}/api/permis/templates/pdf/search`, { params: { q, page: nextPage, size: 50, mode, sort } });
      const total = typeof data?.total === 'number' ? data.total : 0;
      if (mode === 'flat') {
        const arr: FlatItem[] = Array.isArray(data?.items) ? data.items : [];
        setFlatItems(prev => reset ? arr : [...prev, ...arr]);
        const newCount = (reset ? 0 : flatItems.length) + arr.length;
        setHasMore(newCount < total);
      } else {
        const arr: GroupItem[] = Array.isArray(data?.items) ? data.items : [];
        setGroupedItems(prev => reset ? arr : [...prev, ...arr]);
        const newCount = (reset ? 0 : groupedItems.length) + arr.length;
        setHasMore(newCount < total);
      }
      setPage(reset ? 2 : nextPage + 1);
    } catch (e: any) {
      setErr(e?.message || 'Failed to load');
    } finally { setLoading(false); }
  };

  useEffect(() => { search(true); /* reset */ }, [mode, sort]);

  // Debounce search on q
  useEffect(() => {
    const t = setTimeout(() => search(true), 400);
    return () => clearTimeout(t);
  }, [q]);

  // Infinite scroll
  useEffect(() => {
    if (!obsRef.current) return;
    const el = obsRef.current;
    const io = new IntersectionObserver((entries) => {
      const first = entries[0];
      if (first.isIntersecting && !loading && hasMore) search();
    });
    io.observe(el);
    return () => io.disconnect();
  }, [loading, hasMore, mode, sort, q, page]);

  const doDownload = async (dir: string, name: string) => {
    try {
      const url = `${API_URL}/api/permis/templates/pdf/download?dir=${encodeURIComponent(dir)}&file=${encodeURIComponent(name)}`;
      const resp = await axios.get(url, { responseType: 'blob' });
      const blob = new Blob([resp.data], { type: 'application/pdf' });
      const a = document.createElement('a');
      a.href = URL.createObjectURL(blob);
      a.download = name;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(a.href), 1000);
    } catch (e) {
      setErr('Échec du téléchargement');
    }
  };

  return (
    <div className={styles.wrap}>
      <div className={styles.bar}>
        <input className={styles.input} value={q} onChange={(e) => setQ(e.target.value)} placeholder="Rechercher (ex: PM47, code:51, type:PM)" />
        <select className={styles.btn} value={sort} onChange={(e) => setSort(e.target.value as any)}>
          <option value="recent">Récents</option>
          <option value="name">Nom</option>
        </select>
        <button className={styles.btn} onClick={() => setMode(mode === 'flat' ? 'group' : 'flat')}>
          {mode === 'flat' ? 'Grouper par permis' : 'Vue à plat'}
        </button>
      </div>
      {err && <div className={styles.err}>{err}</div>}
      {(mode === 'flat' ? flatItems.length === 0 : groupedItems.length === 0) ? (
        <div className={styles.empty}>Aucun PDF trouvé.</div>
      ) : (
        <div className={styles.grid}>
          {mode === 'flat' ? (
            flatItems.map((f) => (
              <div key={f.url} className={styles.card}>
                <div className={styles.dir}>{f.dir}</div>
                <ul className={styles.fileList}>
                  <li className={styles.fileItem}>
                    <div className={styles.fileName}>{f.name}</div>
                    <div className={styles.fileActions}>
                      <button className={styles.dlBtn} onClick={() => doDownload(f.dir, f.name)}>Télécharger</button>
                      <a className={styles.linkBtn} href={`${API_URL}${f.url}`} target="_blank" rel="noreferrer">Ouvrir</a>
                    </div>
                  </li>
                </ul>
              </div>
            ))
          ) : (
            groupedItems.map((it) => (
              <div key={it.dir} className={styles.card}>
                <div className={styles.dir}>{it.dir}</div>
                <ul className={styles.fileList}>
                  {it.files.map(f => (
                    <li key={f.url} className={styles.fileItem}>
                      <div className={styles.fileName}>{f.name}</div>
                      <div className={styles.fileActions}>
                        <button className={styles.dlBtn} onClick={() => doDownload(it.dir, f.name)}>Télécharger</button>
                        <a className={styles.linkBtn} href={`${API_URL}${f.url}`} target="_blank" rel="noreferrer">Ouvrir</a>
                      </div>
                    </li>
                  ))}
                </ul>
              </div>
            ))
          )}
        </div>
      )}
      <div ref={obsRef} style={{ height: 1 }} />
    </div>
  );
}
