import React, { useEffect, useState } from 'react';
import axios from 'axios';
import './app.css';

import PermisDesigner from './components/PermisDesigner';
import LoginView from './components/auth/LoginView';
import AuthHeader from './components/auth/AuthHeader';
import PdfGallery from './components/PdfGallery';
import VerifyQrView from './components/verify/VerifyQrView';
import panelStyles from './components/loader/PermisLoader.module.css';

const API_URL = (import.meta as any).env?.VITE_API_URL || '';

export default function App() {
  const [authUser, setAuthUser] = useState<string>('');
  const [view, setView] = useState<'designer' | 'verify' | 'pdfs'>('designer');
  const [permisId, setPermisId] = useState<string>('');
  const [initialData, setInitialData] = useState<any | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    (window as any).process = (window as any).process || { env: {} };
    (window as any).process.env = (window as any).process.env || {};
    (window as any).process.env.NEXT_PUBLIC_API_URL = API_URL;
    try {
      const u = localStorage.getItem('auth_user_name') || '';
      const t = localStorage.getItem('auth_token') || '';
      if (u && t) setAuthUser(u);
    } catch {}
  }, []);

  const loadPermis = async () => {
    setError(null);
    setLoading(true);
    try {
      const { data } = await axios.get(`${API_URL}/api/permis/${encodeURIComponent(permisId)}`);
      setInitialData(data);
    } catch (e: any) {
      setError(e?.message || 'Failed to load permis');
    } finally {
      setLoading(false);
    }
  };

  if (!authUser) {
    return <LoginView onLoggedIn={(name) => setAuthUser(name)} />;
  }

  const doLogout = () => {
    try { localStorage.removeItem('auth_user_name'); } catch {}
    try { localStorage.removeItem('auth_token'); } catch {}
    setAuthUser('');
  };

  return (
    <div style={{ fontFamily: 'Inter, system-ui, Arial' }}>
      <AuthHeader userName={authUser} onLogout={doLogout} view={view} onChangeView={setView} />
      {view === 'verify' ? (
        <VerifyQrView />
      ) : view === 'pdfs' ? (
        <PdfGallery />
      ) : (
        <div className={panelStyles.wrap}>
          <div className={panelStyles.hero}>
            <div className={panelStyles.titleRow}>
              <div className={panelStyles.title}>Titre Minier Designer</div>
              <div className={panelStyles.meta}>API: {API_URL || 'N/A'}</div>
            </div>
            <form className={panelStyles.form} onSubmit={(e) => { e.preventDefault(); loadPermis(); }}>
              <div className={panelStyles.field}>
                <input
                  className={panelStyles.input}
                  type="text"
                  placeholder="Entrer l'identifiant/code du permis"
                  value={permisId}
                  onChange={(e) => setPermisId(e.target.value)}
                />
              </div>
              <button className={panelStyles.btn} type="submit" disabled={!permisId || loading}>
                {loading ? 'Chargement...' : 'Charger les donnees du permis'}
              </button>
            </form>
            {error && <div className={panelStyles.err}>{error}</div>}
            {!initialData && !error && (
              <div className={panelStyles.empty}>
                Saisissez l'identifiant du permis puis cliquez sur "Charger" pour commencer.
              </div>
            )}
            <div className={panelStyles.content}>
              {initialData ? (
                <PermisDesigner
                  initialData={initialData}
                  onSave={async ({ elements, permisId, name, templateId }: any) => {
                    try {
                      const createdBy = authUser || '';
                      if (templateId) {
                        const { data } = await axios.patch(`${API_URL}/api/permis/templates/${templateId}`, { name, elements });
                        return data;
                      }
                      const payload = { name, elements, permisId, createdBy };
                      const { data } = await axios.post(`${API_URL}/api/permis/templates`, payload);
                      return data;
                    } catch (e) {
                      console.error('Failed to save template', e);
                      throw e;
                    }
                  }}
                  onGeneratePdf={async (design: any) => {
                    // return an empty PDF blob as a placeholder to satisfy the expected Promise<Blob> signature
                    return new Blob([], { type: 'application/pdf' });
                  }}
                  onSavePermis={async (_permisData: any) => { return {} as any; }}
                  procedureId={Number(permisId)}
                />
              ) : null}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
