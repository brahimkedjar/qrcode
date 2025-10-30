import React, { useMemo, useState } from 'react';
import styles from './login.module.css';

const API_URL = (import.meta as any).env?.VITE_API_URL || '';

const ALLOWED_USERNAMES = new Set([
  'ANAM1122',
  'ANAM1126',
  'ANAM1432',
  'ANAM1364',
  'ANAM1363',
  'ANAM1433',
  'ANAM1358',
  'ANAM1405',
  'ANAM1113',
  'ANAM1206'
]);

const CADASTRE_USERS = new Set([
  'ANAM1113',
  'ANAM1405',
  'ANAM1206'
]);

type Props = {
  onLoggedIn: (name: string, token: string, username: string, groups: string[]) => void;
};

export default function LoginView({ onLoggedIn }: Props) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const normalizedUsername = useMemo(() => username.trim().toUpperCase(), [username]);
  const isUsernameAllowed = normalizedUsername.length > 0 && ALLOWED_USERNAMES.has(normalizedUsername);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    if (!isUsernameAllowed) {
      setError('Utilisateur non autorise');
      return;
    }
    setLoading(true);
    try {
      const res = await fetch(`${API_URL}/api/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: normalizedUsername, password })
      });
      // Gracefully handle non-JSON responses (e.g., NGINX 502 HTML)
      let data: any = null;
      const text = await res.text();
      try { data = JSON.parse(text); } catch { data = null; }
      if (!res.ok) {
        const msg = (data && (data.error || data.message)) || `Server error ${res.status}`;
        setError(msg);
        setLoading(false);
        return;
      }
      if (!data) {
        setError('Invalid server response');
        setLoading(false);
        return;
      }
      if (!data?.ok) {
        setError(data?.error || 'Authentication failed');
        setLoading(false);
        return;
      }
      const groups: string[] = [];
      if (CADASTRE_USERS.has(normalizedUsername)) {
        groups.push('cadastre');
      }
      try { localStorage.setItem('auth_token', data.token || ''); } catch {}
      try { localStorage.setItem('auth_user_name', data.user?.name || normalizedUsername); } catch {}
      try { localStorage.setItem('auth_user_username', normalizedUsername); } catch {}
      try { localStorage.setItem('auth_user_groups', JSON.stringify(groups)); } catch {}
      onLoggedIn(data.user?.name || normalizedUsername, data.token || '', normalizedUsername, groups);
    } catch (err: any) {
      setError(err?.message || 'Network error');
      setLoading(false);
    }
  };

  return (
    <div className={styles.wrapper}>
      <div className={styles.card}>
        <div className={styles.left}>
          <div className={styles.title}>منصة سندات منجمية</div>
          <div className={styles.subtitle}>واجهة حديثة لتصميم السندات وإدارتها</div>
          <div className={styles.illus}>
            <div style={{fontWeight:800, fontSize:18}}>تصميم تفاعلي</div>
            <div style={{opacity:.9, marginTop:8}}>اسحب وأسقط العناصر، نظّم المقالات، وأنشئ رموز QR بسهولة</div>
          </div>
          <div className={styles.note}>سيتم تفعيل تسجيل الدخول عبر LDAP لاحقًا</div>
        </div>
        <div className={styles.right}>
          <div className={styles.formTitle}>Bienvenue</div>
          <div className={styles.formSub}>Connectez-vous pour continuer</div>
          <form onSubmit={handleSubmit}>
            <div className={styles.row}>
              <label className={styles.label}>Nom d'utilisateur</label>
              <input className={styles.input} value={username} onChange={e => setUsername(e.target.value)} placeholder="ANAM1234" required />
            </div>
            <div className={styles.row}>
              <label className={styles.label}>Mot de passe</label>
              <input className={styles.input} type="password" value={password} onChange={e => setPassword(e.target.value)} placeholder="••••••••" />
            </div>
            {!isUsernameAllowed && normalizedUsername && (
              <div className={styles.err}>Utilisateur non autorise</div>
            )}
            {error && <div className={styles.err}>{error}</div>}
            <button className={styles.submit} disabled={loading || !isUsernameAllowed} type="submit">{loading ? 'Connexion…' : 'Se connecter'}</button>
          </form>
          <div className={styles.hint}>LDAP sera activé ultérieurement</div>
        </div>
      </div>
    </div>
  );
}

