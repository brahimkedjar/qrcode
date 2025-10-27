import React, { useState } from 'react';
import styles from './VerifyQrView.module.css';

const API_URL = (import.meta as any).env?.VITE_API_URL || '';

export default function VerifyQrView() {
  const [code, setCode] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<any | null>(null);

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setResult(null);
    setLoading(true);
    try {
      const url = `${API_URL}/api/permis/verify?code=${encodeURIComponent(code.trim())}`;
      try { console.log('[VerifyQr] URL:', url, 'API_URL=', API_URL); } catch {}
      const res = await fetch(url);
      const data = await res.json();
      if (!data || data.exists === false) {
        setResult({ exists: false });
      } else {
        setResult({ exists: true, ...data });
      }
    } catch (err: any) {
      setError(err?.message || 'Network error');
    } finally {
      setLoading(false);
    }
  };

  const row = (label: string, value: any) => (
    <div className={styles.row}>
      <div className={styles.label}>{label}</div>
      <div className={styles.value}>{String(value ?? '')}</div>
    </div>
  );

  return (
    <div className={styles.wrap}>
      <div className={styles.panel}>
        <div className={styles.heading}>Vérification du Code QR</div>
        <div className={styles.sub}>Entrez le code QR (ex: XXXXX-XXXXX-XXXXX-XXXXX)</div>
        <form className={styles.form} onSubmit={onSubmit}>
          <input className={styles.input} value={code} onChange={e => setCode(e.target.value)} placeholder="Saisir le code QR" />
          <button className={styles.btn} disabled={!code.trim() || loading} type="submit">{loading ? 'Vérification…' : 'Vérifier'}</button>
        </form>
        {error && <div className={styles.err}>{error}</div>}

        <div className={styles.result}>
          {result && result.exists === false && (
            <div className={styles.empty}>Aucun permis trouvé pour ce code QR.</div>
          )}
          {result && result.exists && (
            <div className={styles.card}>
              {row('Code Demande', result?.permis?.codeDemande)}
              {row('Type (code)', result?.permis?.typePermis?.code)}
              {row('Type (libellé)', result?.permis?.typePermis?.nom)}
              {row('Détenteur', result?.permis?.detenteur?.nom)}
              {row('Localisation', result?.permis?.localisation)}
              {row('Superficie', result?.permis?.superficie)}
              {row('QR Code', result?.permis?.QrCode)}
              {row('Inséré par', result?.permis?.Qrinsererpar)}
              {row('Horodatage', result?.permis?.DateHeureSysteme)}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
