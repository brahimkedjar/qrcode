import React from 'react';
import styles from './header.module.css';

type Props = {
  userName: string;
  onLogout: () => void;
  view?: 'designer' | 'verify' | 'pdfs';
  onChangeView?: (v: 'designer' | 'verify' | 'pdfs') => void;
};

export default function AuthHeader({ userName, onLogout, view = 'designer', onChangeView }: Props) {
  return (
    <div className={styles.bar}>
      <div className={styles.brand}>
        <div>ANAM</div>
        <div className={styles.badge}>Système des Titres Miniers</div>
      </div>
      <div className={styles.right}>
        {onChangeView && (
          <div className={styles.nav}>
            <button className={`${styles.tab} ${view === 'designer' ? styles.tabActive : ''}`} onClick={() => onChangeView('designer')}>Designer</button>
            <button className={`${styles.tab} ${view === 'verify' ? styles.tabActive : ''}`} onClick={() => onChangeView('verify')}>Vérifier QR</button>
            <button className={`${styles.tab} ${view === 'pdfs' ? styles.tabActive : ''}`} onClick={() => onChangeView('pdfs')}>PDFs</button>
          </div>
        )}
        <div className={styles.user}>{userName}</div>
        <button className={styles.logout} onClick={onLogout}>Déconnexion</button>
      </div>
    </div>
  );
}
