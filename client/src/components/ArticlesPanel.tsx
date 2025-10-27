// =============================================================
// File: components/panels/ArticlesPanel.tsx
// Side panel with Arabic articles: checkboxes + CRUD for custom
// =============================================================
import React, { useState } from 'react';
import type { ArticleItem } from './types';
import { v4 as uuidv4 } from 'uuid';

type Props = {
  articles: ArticleItem[];
  selectedIds: string[];
  onToggle: (id: string, checked: boolean) => void;
  onAddCustom: (article: ArticleItem) => void;
  onRemove: (id: string) => void;
  onUpdate: (article: ArticleItem) => void;
};

export const ArticlesPanel: React.FC<Props> = ({
  articles,
  selectedIds,
  onToggle,
  onAddCustom,
  onRemove,
  onUpdate
}) => {
  const [title, setTitle] = useState('المادة المضافة');
  const [content, setContent] = useState('');

  
  return (
    <aside className="articles-panel">
      <div className="articles-header">
        <h3>المواد القانونية</h3>
        <p className="muted">اختر المواد لإظهارها في رخصة الاستغلال. يمكنك إضافة مواد مخصصة.</p>
      </div>

      <div className="articles-list" dir="rtl">
        {articles.map((a) => {
          const checked = selectedIds.includes(a.id);
          return (
            <div key={a.id} className="article-row">
              <label className="checkbox">
                <input
                  type="checkbox"
                  checked={checked}
                  onChange={(e) => onToggle(a.id, e.target.checked)}
                />
                <span className="title">{a.title}</span>
              </label>
              <textarea
                className="article-content"
                value={a.content}
                onChange={(e) => onUpdate({ ...a, content: e.target.value })}
                rows={3}
                placeholder="نص المادة..."
              />
              <button className="btn small danger" onClick={() => onRemove(a.id)}>حذف</button>
            </div>
          );
        })}
        {articles.length === 0 && <div className="muted">لا توجد مواد محددة مسبقًا لهذا النوع من الرخص.</div>}
      </div>

      <div className="custom-add" dir="rtl">
        <h4>إضافة مادة جديدة</h4>
        <input value={title} onChange={(e) => setTitle(e.target.value)} placeholder="عنوان المادة (مثال: المادة 1)" />
        <textarea value={content} onChange={(e) => setContent(e.target.value)} rows={3} placeholder="نص المادة باللغة العربية" />
        <button
          className="btn primary"
          onClick={() => {
            if (!content.trim()) return;
            onAddCustom({ id: uuidv4(), title: title.trim() || 'مادة', content: content.trim() });
            setContent('');
          }}
        >
          إضافة
        </button>
      </div>

      <style>{`
        .articles-panel {
          width: 300px;
          background: #fff;
          border-right: 1px solid #ddd;
          padding: 12px;
          overflow-y: auto;
        }
        .articles-header h3 { margin: 4px 0 6px; }
        .muted { color: #7f8c8d; font-size: 13px; }
        .articles-list { display: flex; flex-direction: column; gap: 10px; margin-top: 6px; }
        .article-row { border: 1px solid #eee; border-radius: 6px; padding: 8px; background: #fafafa; }
        .checkbox { display: flex; align-items: center; gap: 8px; }
        .checkbox input { transform: scale(1.1); }
        .title { font-weight: 600; }
        .article-content { width: 94%; margin-top: 6px; padding: 8px; border-radius: 6px; border: 1px solid #ddd; font-family: 'Noto Naskh Arabic', 'Amiri', serif; }
        .btn { border: none; padding: 8px 12px; border-radius: 6px; cursor: pointer; }
        .btn.small { padding: 6px 8px; font-size: 12px; }
        .btn.primary { background: #2c7be5; color: #fff; }
        .btn.danger { background: #e74c3c; color: #fff; }
        .custom-add { border-top: 1px dashed #eee; margin-top: 10px; padding-top: 10px; display: flex; flex-direction: column; gap: 6px; }
        .custom-add input, .custom-add textarea { border: 1px solid #ddd; border-radius: 6px; padding: 8px; }
      `}</style>
    </aside>
  );
};
