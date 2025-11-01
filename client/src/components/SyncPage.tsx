import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { toast } from 'react-toastify';

const API_URL = (import.meta as any).env?.VITE_API_URL || '';

export default function SyncPage() {
  const [running, setRunning] = useState(false);
  const [lines, setLines] = useState<string[]>([]);
  const [exitCode, setExitCode] = useState<number | null>(null);
  const [script, setScript] = useState('');
  const [source, setSource] = useState('');
  const [dest, setDest] = useState('');
  const [tables, setTables] = useState<string[]>([]);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [loadingTables, setLoadingTables] = useState(false);
  const [tablesError, setTablesError] = useState('');
  const [resume, setResume] = useState(false);
  const sourceRef = useRef<EventSource | null>(null);
  const logRef = useRef<HTMLDivElement>(null);

  const reset = useCallback(() => {
    setLines([]);
    setExitCode(null);
  }, []);

  const start = useCallback(() => {
    reset();
    setRunning(true);
    try { toast.info('Synchronisation démarrée'); } catch {}
    const url = new URL(`${API_URL}/api/sync/run`);
    if (script.trim()) url.searchParams.set('script', script.trim());
    if (source.trim()) url.searchParams.set('source', source.trim());
    if (dest.trim()) url.searchParams.set('dest', dest.trim());
    const picked = Array.from(selected);
    if (picked.length > 0) url.searchParams.set('tables', picked.join(','));
    if (resume) url.searchParams.set('resume', '1');
    const es = new EventSource(url.toString());
    sourceRef.current = es;
    es.addEventListener('info', (e: any) => {
      try { const d = JSON.parse(e.data); setLines((ls) => [...ls, `[INFO] ${d.message} (${d.script || ''})`]); } catch { setLines((ls) => [...ls, `[INFO] ${e.data}`]); }
    });
    es.addEventListener('log', (e: any) => setLines((ls) => [...ls, e.data]));
    es.addEventListener('error', (e: any) => setLines((ls) => [...ls, `[ERR] ${e.data}`]));
    es.addEventListener('done', (e: any) => {
      let code = null;
      try { const d = JSON.parse(e.data); code = Number(d.code); } catch {}
      setExitCode(code as any);
      setRunning(false);
      es.close();
      sourceRef.current = null;
      try { if (code === 0) toast.success('Synchronisation terminée avec succès'); else toast.error(`Synchronisation terminée avec code ${code}`); } catch {}
    });
    es.onerror = () => {
      setLines((ls) => [...ls, '[ERR] Connexion SSE interrompue']);
      try { toast.error('Connexion SSE interrompue'); } catch {}
    };
  }, [API_URL, script, source, dest, selected, resume, reset]);

  const stop = useCallback(() => {
    if (sourceRef.current) {
      sourceRef.current.close();
      sourceRef.current = null;
      setRunning(false);
      setLines((ls) => [...ls, "[INFO] Arrêt demandé par l'utilisateur"]);
    }
  }, []);

  const loadTables = useCallback(async () => {
    setTablesError('');
    setLoadingTables(true);
    try {
      const db = source.trim();
      if (!db) throw new Error('Veuillez saisir le chemin source');
      const resp = await fetch(`${API_URL}/api/sync/tables?db=${encodeURIComponent(db)}`);
      const data = await resp.json();
      if (!resp.ok || !data?.ok) throw new Error(data?.error || `Erreur ${resp.status}`);
      const names: string[] = Array.isArray(data.tables) ? data.tables : [];
      setTables(names);
      setSelected(new Set(names));
      try { toast.success(`Tables chargées (${names.length})`); } catch {}
    } catch (e: any) {
      setTables([]);
      setSelected(new Set());
      setTablesError(e?.message || 'Échec de chargement des tables');
      try { toast.error(e?.message || 'Échec de chargement des tables'); } catch {}
    } finally {
      setLoadingTables(false);
    }
  }, [API_URL, source]);

  useEffect(() => {
    if (logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [lines]);

  const statusText = useMemo(() => {
    if (running) return 'Synchronisation en cours…';
    if (exitCode === null) return 'En attente';
    return exitCode === 0 ? 'Terminée avec succès' : `Terminée avec code ${exitCode}`;
  }, [running, exitCode]);

  return (
    <div style={{ padding: 16, display: 'flex', flexDirection: 'column', gap: 16 }}>
      <h2 style={{ margin: 0 }}>Synchronisation des bases CMADONNEES</h2>
      <div style={{ fontSize: 12, opacity: 0.8 }}>{statusText}</div>

      <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
        <div style={{ fontWeight: 600 }}>Chemins</div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
          <input
            style={{ flex: 1, minWidth: 320, padding: 8 }}
            placeholder="Chemin source (SourceDbPath)"
            value={source}
            onChange={(e) => setSource(e.target.value)}
          />
          <input
            style={{ flex: 1, minWidth: 320, padding: 8 }}
            placeholder="Chemin destination (DestinationDbPath)"
            value={dest}
            onChange={(e) => setDest(e.target.value)}
          />
          <input
            style={{ flex: 1, minWidth: 320, padding: 8 }}
            placeholder="Chemin personnalise du script (optionnel)"
            value={script}
            onChange={(e) => setScript(e.target.value)}
          />
        </div>
      </div>

      <div style={{ display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
        <div style={{ fontWeight: 600 }}>Options</div>
        <label style={{ display:'flex', alignItems:'center', gap:6 }}>
          <input type="checkbox" checked={resume} onChange={(e) => setResume(e.target.checked)} />
          <span>Reprendre depuis le dernier</span>
        </label>
      </div>

      <div style={{ display: 'flex', gap: 8, alignItems: 'center', flexWrap: 'wrap' }}>
        <button onClick={start} disabled={running || !source.trim() || !dest.trim()} style={{ padding: '8px 14px' }}>Démarrer</button>
        <button onClick={stop} disabled={!running} style={{ padding: '8px 14px' }}>Arrêter</button>
      </div>

      <div style={{ display:'flex', gap: 12, alignItems:'center', flexWrap:'wrap' }}>
        <div style={{ fontWeight: 600 }}>Mémoire</div>
        <button
          onClick={async () => {
            try {
              const url = new URL(`${API_URL}/api/sync/state/reset`);
              if (script.trim()) url.searchParams.set('script', script.trim());
              const resp = await fetch(url.toString(), { method: 'POST' });
              const data = await resp.json().catch(() => null);
              if (resp.ok && data?.ok) {
                setLines((ls) => [...ls, `[INFO] Mémoire réinitialisée (${data.statePath})`]);
              } else {
                setLines((ls) => [...ls, `[ERR] Échec de réinitialisation: ${data?.error || resp.status}`]);
              }
            } catch (e: any) {
              setLines((ls) => [...ls, `[ERR] ${e?.message || 'Erreur reseau'}`]);
            }
          }}
          disabled={running}
          style={{ padding: '6px 12px' }}
        >
          Réinitialiser la mémoire
        </button>
        <button
          onClick={async () => {
            try {
              const url = new URL(`${API_URL}/api/sync/state`);
              if (script.trim()) url.searchParams.set('script', script.trim());
              const resp = await fetch(url.toString());
              const data = await resp.json().catch(() => null);
              if (resp.ok && data?.ok) {
                const state = data.state || {};
                const display = typeof state === 'string' ? state : JSON.stringify(state, null, 2);
                setLines((ls) => [...ls, `[INFO] État mémoire (${data.statePath}):`, display]);
              } else {
                setLines((ls) => [...ls, `[ERR] Échec de lecture de la mémoire: ${data?.error || resp.status}`]);
              }
            } catch (e: any) {
              setLines((ls) => [...ls, `[ERR] ${e?.message || 'Erreur reseau'}`]);
            }
          }}
          disabled={running}
          style={{ padding: '6px 12px' }}
        >
          Voir la mémoire
        </button>
      </div>

      <div style={{ display: 'flex', gap: 12, alignItems: 'center', flexWrap: 'wrap' }}>
        <div style={{ fontWeight: 600 }}>Tables</div>
        <button onClick={loadTables} disabled={loadingTables || !source.trim()} style={{ padding: '6px 12px' }}>
          {loadingTables ? 'Chargement…' : 'Charger les tables (source)'}
        </button>
        <button onClick={() => setSelected(new Set(tables))} disabled={tables.length === 0} style={{ padding: '6px 12px' }}>Tout sélectionner</button>
        <button onClick={() => setSelected(new Set())} disabled={tables.length === 0} style={{ padding: '6px 12px' }}>Tout désélectionner</button>
        <div style={{ fontSize: 12, opacity: .7 }}>Selectionnees: {selected.size} / {tables.length}</div>
      </div>
      {tablesError && <div style={{ color: '#b00' }}>{tablesError}</div>}
      <div style={{
        display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))', gap: 6,
        maxHeight: 240, overflow: 'auto', border: '1px solid #ddd', borderRadius: 6, padding: 8
      }}>
        {tables.map((t) => (
          <label key={t} style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <input
              type="checkbox"
              checked={selected.has(t)}
              onChange={(e) => {
                setSelected((prev) => {
                  const next = new Set(prev);
                  if (e.target.checked) next.add(t); else next.delete(t);
                  return next;
                });
              }}
            />
            <span style={{ overflow: 'hidden', textOverflow: 'ellipsis' }}>{t}</span>
          </label>
        ))}
        {tables.length === 0 && !loadingTables && (
          <div style={{ opacity: .6 }}>Aucune table chargée</div>
        )}
      </div>
      <div style={{ opacity: .7, fontSize: 12 }}>
        Astuce: la selection des tables est transmise au script via la variable d'environnement <code>SYNC_TABLES</code> (CSV) et <code>SYNC_TABLES_JSON</code>. Exemple PowerShell: <code>$env:SYNC_TABLES</code>.
      </div>
      <div ref={logRef} style={{
        background: '#0c0c0c', color: '#d0d0d0', padding: 12,
        fontFamily: 'Consolas, Menlo, monospace', fontSize: 12,
        minHeight: 320, maxHeight: 520, overflow: 'auto', borderRadius: 6,
        border: '1px solid #222'
      }}>
        {lines.length === 0 ? <div style={{ opacity: .6 }}>Aucun log pour le moment...</div> : (
          lines.map((l, i) => <div key={i}>{l}</div>)
        )}
      </div>
    </div>
  );
}














