import React, { useState } from 'react';
import styles from './login.module.css';

const API_URL = (import.meta as any).env?.VITE_API_URL || '';

type Props = {
  onLoggedIn: (name: string, token: string) => void;
};

export default function LoginView({ onLoggedIn }: Props) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    try {
      const res = await fetch(`${API_URL}/api/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password })
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
      try { localStorage.setItem('auth_token', data.token || ''); } catch {}
      try { localStorage.setItem('auth_user_name', data.user?.name || username); } catch {}
      try { localStorage.setItem('auth_user_username', data.user?.username || username); } catch {}
      onLoggedIn(data.user?.name || username, data.token || '');
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
              <input className={styles.input} value={username} onChange={e => setUsername(e.target.value)} placeholder="john.doe" required />
            </div>
            <div className={styles.row}>
              <label className={styles.label}>Mot de passe</label>
              <input className={styles.input} type="password" value={password} onChange={e => setPassword(e.target.value)} placeholder="••••••••" />
            </div>
            {error && <div className={styles.err}>{error}</div>}
            <button className={styles.submit} disabled={loading} type="submit">{loading ? 'Connexion…' : 'Se connecter'}</button>
          </form>
          <div className={styles.hint}>LDAP sera activé ultérieurement</div>
        </div>
      </div>
    </div>
  );
}

