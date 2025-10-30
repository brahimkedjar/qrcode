import React, { useMemo, useState } from 'react';
import axios from 'axios';
import TaxesPreviewModal from './TaxesPreviewModal';
import { generatePDFForPreview, generateUniqueOrderNumber } from './pdfGenerator';
import styles from './TaxesPage.module.css';
const API_URL = (import.meta as any).env?.VITE_API_URL || '';

type TaxeSupRow = {
  id: number;
  idTitre: number;
  NumeroPerc?: string;
  PAR?: string;
  Date?: string;
  Surface?: string | number;
  Taxe?: string | number;
  Paye?: boolean;
  Comment?: string;
  DatePerDebut?: string;
  datePerFin?: string;
  TS_SurfaceMin?: string | number;
  TS_SurfaceMax?: string | number;
  TS_DroitFixe?: string | number;
  TS_PerInit?: string | number;
  TS_PremierRen?: string | number;
  TS_DeuRen?: string | number;
  dateremiseop?: string;
  datepaiement?: string;
  num_quittance?: string;
};

type DeaRow = {
  id: number;
  idTitre: number;
  idTypeProcedure?: number;
  idProcedure?: number;
  NumeroPerc?: string;
  date?: string;
  droit?: string | number;
  paye?: boolean;
  DUN?: string;
  PARLA?: string;
  Commentaire?: string;
  dateremiseop?: string;
  datepaiement?: string;
  num_quittance?: string;
};

export default function TaxesPage() {
  const [idTitre, setIdTitre] = useState('');
  const [loading, setLoading] = useState(false);
  const [taxes, setTaxes] = useState<TaxeSupRow[]>([]);
  const [deas, setDeas] = useState<DeaRow[]>([]);
  const [err, setErr] = useState<string | null>(null);
  const [permisInfo, setPermisInfo] = useState<any | null>(null);

  // Filters
  const [numeroFilter, setNumeroFilter] = useState('');
  const [fromDate, setFromDate] = useState('');
  const [toDate, setToDate] = useState('');
  const [selectedTs, setSelectedTs] = useState<Record<number, boolean>>({});
  const [selectedDea, setSelectedDea] = useState<Record<number, boolean>>({});

  const search = async () => {
    setErr(null);
    const rawInput = idTitre.trim();
    if (!rawInput) {
      setErr("Veuillez saisir un identifiant ou un code de titre.");
      setTaxes([]);
      setDeas([]);
      setPermisInfo(null);
      return;
    }
    setLoading(true);
    try {
      let resolvedId = rawInput;
      let resolvedPermis: any | null = null;
      const isNumeric = /^\d+$/.test(rawInput);
      if (!isNumeric) {
        const searchResp = await axios.get(`${API_URL}/api/permis/search`, { params: { q: rawInput } });
        const data = searchResp?.data;
        if (!data || data.exists === false) {
          throw new Error('Titre introuvable');
        }
        const candidateId = data?.id ?? data?.permisId ?? data?.permis?.id;
        if (!candidateId || !/^\d+$/.test(String(candidateId))) {
          throw new Error('Titre introuvable');
        }
        resolvedId = String(candidateId);
        resolvedPermis = data?.permis ?? data;
        setIdTitre(resolvedId);
      }
      const [ts, da, pm] = await Promise.all([
        axios.get(`${API_URL}/api/finance/taxes-sup`, { params: { idTitre: resolvedId } }),
        axios.get(`${API_URL}/api/finance/dea`, { params: { idTitre: resolvedId } }),
        axios.get(`${API_URL}/api/permis/${encodeURIComponent(resolvedId)}`).catch(() => ({ data: null }))
      ]);
      setTaxes((ts.data?.rows || []) as TaxeSupRow[]);
      setDeas((da.data?.rows || []) as DeaRow[]);
      const permisData = pm?.data && Object.keys(pm.data || {}).length ? pm.data : resolvedPermis;
      setPermisInfo(permisData || null);
      setSelectedTs({});
      setSelectedDea({});
    } catch (e: any) {
      console.error('[Taxes] search failed', e);
      setErr(e?.response?.data?.message || e?.message || 'Echec du chargement.');
      setTaxes([]);
      setDeas([]);
      setPermisInfo(null);
    } finally {
      setLoading(false);
    }
  };

  
  // Preview state
  const [previewOpen, setPreviewOpen] = useState(false);
  const [previewType, setPreviewType] = useState<'DEA' | 'TS' | 'PRODUIT_ATTRIBUTION'>('TS');
  const [previewData, setPreviewData] = useState<any | null>(null);const parseAccessDate = (s?: string): Date | null => {
    if (!s) return null;
    const t = s.trim();
    const m = t.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
    if (m) {
      const d = new Date(Number(m[3]), Number(m[2]) - 1, Number(m[1]));
      return isNaN(+d) ? null : d;
    }
    const d2 = new Date(t);
    return isNaN(+d2) ? null : d2;
  };

  const parseAmount = (v?: string | number): number => {
    if (typeof v === 'number') return v;
    const s = String(v || '').replace(/[^0-9,\.]/g, '').replace(/\.(?=.*\.)/g, '');
    // Replace comma with dot if present
    const n = parseFloat(s.replace(/,/g, '.'));
    return Number.isFinite(n) ? n : 0;
  };

  const blobFromDataUrl = async (dataUrl: string): Promise<Blob> => {
    const res = await fetch(dataUrl);
    return await res.blob();
  };
  const openPreviewTS = async (row: TaxeSupRow) => {
    const orderNumber = generateUniqueOrderNumber('TS', 0, row.id, row.idTitre, new Date().getFullYear());
    const period = [row.DatePerDebut, row.datePerFin].filter(Boolean).join(' → ');
    const data = {
      ...makeOrderBase(),
      amount: parseAmount(row.Taxe),
      orderNumber,
      date: new Date(),
      period,
    };
    setPreviewType('TS');
    setPreviewData(data);
    setPreviewOpen(true);
  };

  const openPreviewDEA = async (row: DeaRow) => {
    const orderNumber = generateUniqueOrderNumber('DEA', 0, row.id, row.idTitre, new Date().getFullYear());
    const data = {
      ...makeOrderBase(),
      amount: parseAmount(row.droit),
      orderNumber,
      date: parseAccessDate(row.date) || new Date(),
    };
    setPreviewType('DEA');
    setPreviewData(data);
    setPreviewOpen(true);
  };

  const makeOrderBase = () => {
    const detName = permisInfo?.detenteur?.nom || permisInfo?.detenteur?.Nom || '';
    const lieuDit = (permisInfo?.lieudit || permisInfo?.LieuDit || '').toString();
    const daira = (permisInfo?.daira || permisInfo?.Daira || '').toString();
    const baseLoc = lieuDit || (permisInfo?.localisation || '-');
    const fullLoc = baseLoc + (daira ? `, Daira de ${daira}` : '');
    return {
      companyName: detName || '-',
      permitType: permisInfo?.typePermis?.nom || permisInfo?.typePermis?.code || '-',
      permitCode: permisInfo?.codeDemande || permisInfo?.Code || String(idTitre || ''),
      location: fullLoc,
      lieuDit,
      daira,
      taxReceiver: 'Receveur des impôts',
      taxReceiverAddress: '-',
      signatureName: 'Seddik BENABBES',
      president: 'P/ Le Président du Comité de Direction',
    } as any;
  };

  const downloadTaxe = async (row: TaxeSupRow) => {
    const orderNumber = generateUniqueOrderNumber('TS', 0, row.id, row.idTitre, new Date().getFullYear());
    const period = [row.DatePerDebut, row.datePerFin].filter(Boolean).join(' → ');
    const data = {
      ...makeOrderBase(),
      amount: parseAmount(row.Taxe),
      orderNumber,
      date: new Date(),
      period,
    };
    const url = await generatePDFForPreview('TS', data);
    const blob = await blobFromDataUrl(url);
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `TaxeSuperficiaire_${row.idTitre}_${row.id}.pdf`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 1500);
  };

  const downloadDea = async (row: DeaRow) => {
    const orderNumber = generateUniqueOrderNumber('DEA', 0, row.id, row.idTitre, new Date().getFullYear());
    const data = {
      ...makeOrderBase(),
      amount: parseAmount(row.droit),
      orderNumber,
      date: parseAccessDate(row.date) || new Date(),
    };
    const url = await generatePDFForPreview('DEA', data);
    const blob = await blobFromDataUrl(url);
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `DEA_${row.idTitre}_${row.id}.pdf`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 1500);
  };

  // Filtering
  const filteredTaxes = useMemo(() => {
    const num = numeroFilter.trim().toLowerCase();
    const from = fromDate ? new Date(fromDate) : null;
    const to = toDate ? new Date(toDate) : null;
    return taxes.filter((t) => {
      if (num && !(String(t.NumeroPerc || '').toLowerCase().includes(num))) return false;
      const dStart = parseAccessDate(t.DatePerDebut);
      const dEnd = parseAccessDate(t.datePerFin);
      if (from && dStart && dStart < from) return false;
      if (to && dEnd && dEnd > to) return false;
      return true;
    });
  }, [taxes, numeroFilter, fromDate, toDate]);

  const filteredDeas = useMemo(() => {
    const num = numeroFilter.trim().toLowerCase();
    const from = fromDate ? new Date(fromDate) : null;
    const to = toDate ? new Date(toDate) : null;
    return deas.filter((d) => {
      if (num && !(String(d.NumeroPerc || '').toLowerCase().includes(num))) return false;
      const dd = parseAccessDate(d.date);
      if (from && dd && dd < from) return false;
      if (to && dd && dd > to) return false;
      return true;
    });
  }, [deas, numeroFilter, fromDate, toDate]);

  // CSV export
  const exportCsv = (rows: any[], filename: string) => {
    if (!rows.length) return;
    const headers = Object.keys(rows[0]);
    const csv = [headers.join(';')].concat(rows.map(r => headers.map(h => {
      const v = r[h];
      const s = v == null ? '' : String(v).replace(/[\n\r;]/g, ' ');
      return `"${s.replace(/"/g, '""')}"`;
    }).join(';'))).join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = filename;
    a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 1500);
  };

  // Batch download
  const batchDownloadTs = async () => {
    for (const row of filteredTaxes) {
      if (!selectedTs[row.id]) continue;
      // eslint-disable-next-line no-await-in-loop
      await downloadTaxe(row);
    }
  };
  const batchDownloadDea = async () => {
    for (const row of filteredDeas) {
      if (!selectedDea[row.id]) continue;
      // eslint-disable-next-line no-await-in-loop
      await downloadDea(row);
    }
  };

  return (
    <div className={styles.wrap}>
      <div className={styles.panel}>
        <div className={styles.searchBar}>
          <input
            className={styles.input}
            type="text"
            placeholder="Rechercher par idTitre ou code (ex: 21 ou TEC 8375)"
            value={idTitre}
            onChange={(e) => setIdTitre(e.target.value)}
            onKeyDown={(e) => { if (e.key === 'Enter') search(); }}
          />
          <button className={styles.btn} disabled={loading || !idTitre.trim()} onClick={search}>
            {loading ? 'Chargement...' : 'Rechercher'}
          </button>
        </div>
        <div className={styles.filtersRow}>
          <div>
            <div className={styles.label}>Numéro perception</div>
            <input className={styles.input} placeholder="contient..." value={numeroFilter} onChange={(e) => setNumeroFilter(e.target.value)} />
          </div>
          <div>
            <div className={styles.label}>Période début</div>
            <input className={styles.input} type="date" value={fromDate} onChange={(e) => setFromDate(e.target.value)} />
          </div>
          <div>
            <div className={styles.label}>Période fin</div>
            <input className={styles.input} type="date" value={toDate} onChange={(e) => setToDate(e.target.value)} />
          </div>
          <div className={styles.toolbar}>
            <button className={styles.btnSecondary} onClick={() => { setNumeroFilter(''); setFromDate(''); setToDate(''); }}>Réinitialiser</button>
          </div>
        </div>
        {err && <div style={{ color: '#b91c1c', marginBottom: 12 }}>{err}</div>}
        {permisInfo && (
          <div className={styles.metaCard}>
            <div className={styles.metaRow}>
              <span className={styles.badge}>Titre: {permisInfo?.id}</span>
              <span className={styles.badge}>Code: {permisInfo?.codeDemande || permisInfo?.Code}</span>
              <span className={styles.badge}>Société: {permisInfo?.detenteur?.Nom || permisInfo?.detenteur?.nom || '-'}</span>
              <span className={styles.badge}>Lieu: {permisInfo?.lieudit || permisInfo?.LieuDit || '-'}</span>
              <span className={styles.badge}>Daira: {permisInfo?.daira || '-'}</span>
              <span className={styles.badge}>Wilaya: {permisInfo?.wilaya || '-'}</span>
            </div>
          </div>
        )}        <div className={styles.grid}>
          <div className={styles.card}>
            <div className={styles.cardHeader}>
              <div className={styles.title}>Taxe Superficiaire</div>
              <div className={styles.count}>{filteredTaxes.length} affichés / {taxes.length} trouvés</div>
            </div>
            <div className={styles.toolbar}>
              <button className={styles.batchBtn} onClick={batchDownloadTs}>Télécharger sélection</button>
              <button className={styles.btnSecondary} onClick={() => exportCsv(filteredTaxes, `taxes_${idTitre}.csv`)}>Exporter CSV</button>
              <span className={styles.chip}>Sélection: {Object.values(selectedTs).filter(Boolean).length}</span>
            </div>
            {taxes.length === 0 ? (
              <div className={styles.empty}>Aucune taxe trouvée. Recherchez un idTitre pour voir les résultats.</div>
            ) : (
              <table className={styles.table}>
                <thead>
                  <tr>
                    <th className={`${styles.th} ${styles.selectCell}`}><input type="checkbox" onChange={(e) => {
                      const all: Record<number, boolean> = {};
                      filteredTaxes.forEach((t) => { all[t.id] = e.target.checked; });
                      setSelectedTs(all);
                    }} /></th>
                    <th className={styles.th}>Date</th>
                    <th className={styles.th}>Période</th>
                    <th className={`${styles.th} ${styles.thRight}`}>Surface</th>
                    <th className={`${styles.th} ${styles.thRight}`}>Taxe</th>
                    <th className={`${styles.th} ${styles.thCenter}`}>Payé</th>
                    <th className={`${styles.th} ${styles.thCenter}`}>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredTaxes.map((t) => (
                    <tr key={t.id}>
                      <td className={`${styles.td} ${styles.selectCell}`}><input type="checkbox" checked={!!selectedTs[t.id]} onChange={(e) => setSelectedTs({ ...selectedTs, [t.id]: e.target.checked })} /></td>
                      <td className={styles.td}>{t.Date || ''}</td>
                      <td className={styles.td}>{[t.DatePerDebut, t.datePerFin].filter(Boolean).join(' → ')}</td>
                      <td className={`${styles.td} ${styles.tdRight}`}>{t.Surface}</td>
                      <td className={`${styles.td} ${styles.tdRight}`}>{t.Taxe}</td>
                      <td className={`${styles.td} ${styles.tdCenter}`}>
                        <span className={`${styles.status} ${t.Paye ? styles.paid : styles.unpaid}`}>{t.Paye ? 'Oui' : 'Non'}</span>
                      </td>
                      <td className={`${styles.td} ${styles.tdCenter}`}>
                        <div className={styles.actions}>
                          <button className={styles.btnSecondary} onClick={() => openPreviewTS(t)}>Aperçu</button>
                          <button className={styles.downloadBtn} onClick={() => downloadTaxe(t)}>Télécharger</button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
            <div className={styles.help}>Colonnes supplémentaires (Numéro perception, Quittance, etc.) visibles dans le PDF.</div>
          </div>
          <div className={styles.card}>
            <div className={styles.cardHeader}>
              <div className={styles.title}>Droit d'établissement d'acte (DEA)</div>
              <div className={styles.count}>{filteredDeas.length} affichés / {deas.length} trouvés</div>
            </div>
            <div className={styles.toolbar}>
              <button className={styles.batchBtn} onClick={batchDownloadDea}>Télécharger sélection</button>
              <button className={styles.btnSecondary} onClick={() => exportCsv(filteredDeas, `dea_${idTitre}.csv`)}>Exporter CSV</button>
              <span className={styles.chip}>Sélection: {Object.values(selectedDea).filter(Boolean).length}</span>
            </div>
            {deas.length === 0 ? (
              <div className={styles.empty}>Aucun DEA trouvé. Recherchez un idTitre pour voir les résultats.</div>
            ) : (
              <table className={styles.table}>
                <thead>
                  <tr>
                    <th className={`${styles.th} ${styles.selectCell}`}><input type="checkbox" onChange={(e) => {
                      const all: Record<number, boolean> = {};
                      filteredDeas.forEach((d) => { all[d.id] = e.target.checked; });
                      setSelectedDea(all);
                    }} /></th>
                    <th className={styles.th}>Date</th>
                    <th className={styles.th}>Num. perception</th>
                    <th className={`${styles.th} ${styles.thRight}`}>Droit</th>
                    <th className={`${styles.th} ${styles.thCenter}`}>Payé</th>
                    <th className={`${styles.th} ${styles.thCenter}`}>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {filteredDeas.map((d) => (
                    <tr key={d.id}>
                      <td className={`${styles.td} ${styles.selectCell}`}><input type="checkbox" checked={!!selectedDea[d.id]} onChange={(e) => setSelectedDea({ ...selectedDea, [d.id]: e.target.checked })} /></td>
                      <td className={styles.td}>{d.date || ''}</td>
                      <td className={styles.td}>{d.NumeroPerc || ''}</td>
                      <td className={`${styles.td} ${styles.tdRight}`}>{d.droit}</td>
                      <td className={`${styles.td} ${styles.tdCenter}`}>
                        <span className={`${styles.status} ${d.paye ? styles.paid : styles.unpaid}`}>{d.paye ? 'Oui' : 'Non'}</span>
                      </td>
                      <td className={`${styles.td} ${styles.tdCenter}`}>
                        <div className={styles.actions}>
                          <button className={styles.btnSecondary} onClick={() => openPreviewDEA(d)}>Aperçu</button>
                          <button className={styles.downloadBtn} onClick={() => downloadDea(d)}>Télécharger</button>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>
      <TaxesPreviewModal
        open={previewOpen}
        type={previewType}
        initialData={previewData || {}}
        onClose={() => setPreviewOpen(false)}
      />
    </div>
  );
}
