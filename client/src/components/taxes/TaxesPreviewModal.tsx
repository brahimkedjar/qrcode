import React, { useEffect, useMemo, useState } from 'react';
import styles from './TaxesPage.module.css';
import { generatePDFForPreview } from './pdfGenerator';

export type PreviewType = 'DEA' | 'TS' | 'PRODUIT_ATTRIBUTION';

type Props = {
  open: boolean;
  type: PreviewType;
  initialData: any; // OrderData shape from pdfGenerator
  onClose: () => void;
  onDownload?: (blob: Blob) => void;
};

const toBlob = async (dataUrl: string): Promise<Blob> => {
  const res = await fetch(dataUrl);
  return await res.blob();
};

export default function TaxesPreviewModal({ open, type, initialData, onClose, onDownload }: Props) {
  const [form, setForm] = useState<any>(initialData || {});
  const [pdfUrl, setPdfUrl] = useState<string>('');
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    setForm(initialData || {});
  }, [initialData]);

  const refresh = async () => {
    setBusy(true);
    try {
      const url = await generatePDFForPreview(type as any, form);
      setPdfUrl(url);
    } finally {
      setBusy(false);
    }
  };

  useEffect(() => {
    if (open) { refresh(); }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, type, form.orderNumber]);

  const onChange = (field: string, value: any) => {
    setForm((f: any) => ({ ...f, [field]: value }));
  };

  const handleDownload = async () => {
    const url = await generatePDFForPreview(type as any, form);
    const blob = await toBlob(url);
    if (onDownload) onDownload(blob);
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `${type}_${form.permitCode || ''}_${new Date().toISOString().slice(0,10)}.pdf`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(a.href), 1200);
  };

  if (!open) return null;
  return (
    <div className={styles.overlay}>
      <div className={styles.modal}>
        <div className={styles.modalHeader}>
          <div>
            <div className={styles.title}>Aperçu PDF — {type}</div>
            <div className={styles.count}>Modifier les métadonnées puis télécharger</div>
          </div>
          <button className={styles.closeBtn} onClick={onClose}>✕</button>
        </div>
        <div className={styles.modalBody}>
          <div className={styles.formGrid}>
            <div className={styles.fieldCol}>
              <label className={styles.label}>Société</label>
              <input className={styles.input} value={form.companyName || ''} onChange={(e) => onChange('companyName', e.target.value)} />
              <div className={styles.row}>
                <div style={{ flex: 1 }}>
                  <label className={styles.label}>Type permis</label>
                  <input className={styles.input} value={form.permitType || ''} onChange={(e) => onChange('permitType', e.target.value)} />
                </div>
                <div style={{ width: 140 }}>
                  <label className={styles.label}>Code</label>
                  <input className={styles.input} value={form.permitCode || ''} onChange={(e) => onChange('permitCode', e.target.value)} />
                </div>
              </div>
              <label className={styles.label}>Lieu</label>
              <input className={styles.input} value={form.location || ''} onChange={(e) => onChange('location', e.target.value)} />
              {type === 'TS' && (
                <>
                  <label className={styles.label}>Période</label>
                  <input className={styles.input} value={form.period || ''} onChange={(e) => onChange('period', e.target.value)} />
                </>
              )}
              <div className={styles.row}>
                <div style={{ flex: 1 }}>
                  <label className={styles.label}>Montant (DA)</label>
                  <input className={styles.input} value={String(form.amount ?? '')} onChange={(e) => onChange('amount', Number(e.target.value.replace(/[^0-9.]/g, '')) || 0)} />
                </div>
                <div style={{ width: 180 }}>
                  <label className={styles.label}>Ordre n°</label>
                  <input className={styles.input} value={form.orderNumber || ''} onChange={(e) => onChange('orderNumber', e.target.value)} />
                </div>
              </div>
              <div className={styles.row}>
                <div style={{ flex: 1 }}>
                  <label className={styles.label}>Destinataire</label>
                  <input className={styles.input} value={form.taxReceiver || ''} onChange={(e) => onChange('taxReceiver', e.target.value)} />
                </div>
                <div style={{ flex: 1 }}>
                  <label className={styles.label}>Adresse</label>
                  <select
                    className={styles.input}
                    value={form.taxReceiverAddress || ''}
                    onChange={(e) => onChange('taxReceiverAddress', e.target.value)}
                  >
                    <option value="17 rue Arezki Hammani, 3ème étage –Alger">17 rue Arezki Hammani, 3ème étage –Alger</option>
                    <option value="18 rue beniourtilane , 4ème étage –Alger">18 rue beniourtilane , 4ème étage –Alger</option>
                  </select>
                </div>
              </div>
              <div className={styles.row}>
                <div style={{ width: 180 }}>
                  <label className={styles.label}>Date</label>
                  <input className={styles.input} type="date" value={(form.date ? new Date(form.date).toISOString().slice(0,10) : '')} onChange={(e) => onChange('date', new Date(e.target.value))} />
                </div>
                <div style={{ flex: 1 }}>
                  <label className={styles.label}>Lieu</label>
                  <input className={styles.input} value={form.place || ''} onChange={(e) => onChange('place', e.target.value)} placeholder="Alger" />
                </div>
              </div>
              <div className={styles.row}>
                <div style={{ flex: 1 }}>
                  <label className={styles.label}>Président (mention)</label>
                  <input className={styles.input} value={form.president || ''} onChange={(e) => onChange('president', e.target.value)} placeholder="P/ Le Président du Comité de Direction" />
                </div>
                <div style={{ flex: 1 }}>
                  <label className={styles.label}>Signature</label>
                  <input className={styles.input} value={form.signatureName || ''} onChange={(e) => onChange('signatureName', e.target.value)} />
                </div>
              </div>
              <div className={styles.toolbar}>
                <button className={styles.btnSecondary} onClick={refresh} disabled={busy}>{busy ? 'Génération…' : 'Actualiser aperçu'}</button>
                <button className={styles.primaryBtn} onClick={handleDownload}>Télécharger</button>
              </div>
            </div>
            <div className={styles.fieldCol}>
              {pdfUrl ? (
                <iframe className={styles.pdfFrame} src={pdfUrl} title="Aperçu PDF" />
              ) : (
                <div className={styles.dataLoadingContent}>Préparation de l'aperçu…</div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
