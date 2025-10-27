/*
  Script: tm-app/scripts/gen-article-sets.js
  Extracts Arabic text from DOCX models in tm-app/models and
  generates article JSON sets in tm-app/client/src/components.
*/
const fs = require('fs');
const path = require('path');
const cp = require('child_process');

const MODELS_DIR = path.join(__dirname, '..', 'models');
const OUT_DIR = path.join(__dirname, '..', 'client', 'src', 'components');

const MAP = [
  { doc: 'new modele titre TEM.docx', out: 'articles-tem.json', name: 'TEM - ترخيص الاستكشاف عن المناجم' },
  { doc: 'new modele titre TEC.docx', out: 'articles-tec.json', name: 'TEC - عقد استغلال المنجم' },
  { doc: '8375 TEC EXTENSION.docx', out: 'articles-tec-extension.json', name: 'TEC EXTENSION - تمديد عقد الاستغلال' },
  { doc: 'new modele titre APM.docx', out: 'articles-apm.json', name: 'APM - رخصة البحث المنجمي' },
  { doc: 'new modele titre AAC.docx', out: 'articles-aac.json', name: 'AAC - ترخيص الاستغلال الحرفي' },
  { doc: 'new modele titre AAM MINES.docx', out: 'articles-aam-mines.json', name: 'AAM MINES - رخصة النشاطات المنجمية' },
  { doc: 'new modele titre AXH.docx', out: 'articles-axh.json', name: 'AXH - ترخيص الاستغلال خارج المحاجر' },
  { doc: 'TXC RENOUVELLEMENT.docx', out: 'articles-txc-renouvellement.json', name: 'TXC RENOUVELLEMENT - تجديد رخصة استغلال المقلع' },
  { doc: 'TXC SUITE A EXPLORATION.docx', out: 'articles-txc-suite-exploration.json', name: 'TXC SUITE A EXPLORATION - رخصة استغلال إثر الاستكشاف' },
  { doc: 'TXM RENOUVELLEMENT.docx', out: 'articles-txm-renouvellement.json', name: 'TXM RENOUVELLEMENT - تجديد رخصة الاستغلال المنجمي' },
  { doc: 'TXM SUITE A EXPLORATION.docx', out: 'articles-txm-suite-exploration.json', name: 'TXM SUITE A EXPLORATION - رخصة استغلال منجمي إثر الاستكشاف' },
];

function decodeEntities(s) {
  return s
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'");
}

function extractParagraphsFromDocx(docPath) {
  const basename = path.basename(docPath, '.docx');
  const tmpRoot = path.join(__dirname, '..', 'tmp');
  if (!fs.existsSync(tmpRoot)) fs.mkdirSync(tmpRoot, { recursive: true });
  const tmpDir = path.join(tmpRoot, `docx_${basename}_${Date.now()}`);
  fs.mkdirSync(tmpDir, { recursive: true });
  const zipPath = path.join(tmpDir, basename + '.zip');
  // Copy .docx to .zip and expand with PowerShell
  cp.execFileSync('powershell.exe', ['-NoProfile', '-Command', `Copy-Item -LiteralPath '${docPath}' -Destination '${zipPath}'; Expand-Archive -LiteralPath '${zipPath}' -DestinationPath '${tmpDir}' -Force`], { stdio: 'ignore' });
  const xmlPath = path.join(tmpDir, 'word', 'document.xml');
  const xml = fs.readFileSync(xmlPath, 'utf8');
  const withTabs = xml.replace(/<w:tab\s*\/\s*>/g, ' ');
  const paras = [];
  const pRegex = /<w:p[\s\S]*?<\/w:p>/g;
  const tRegex = /<w:t[^>]*>([\s\S]*?)<\/w:t>/g;
  let m;
  while ((m = pRegex.exec(withTabs)) !== null) {
    const block = m[0];
    const texts = [];
    let t;
    while ((t = tRegex.exec(block)) !== null) {
      texts.push(decodeEntities(t[1]));
    }
    const pText = texts.join('');
    const txt = pText.replace(/[\u200e\u200f]/g, '').replace(/\s+/g, ' ').trim();
    if (txt) paras.push(txt);
  }
  return paras;
}

function normalizeArabicDigits(s) {
  // Keep as-is; no conversion
  return s;
}

function isArticleHeading(line) {
  // Matches: المادة 1: ... or المـادة 1 ...
  const re = /^(?:ال)?م.?ادة\s*([0-9٠-٩]+)\s*[:：]?\s*(.*)$/;
  const m = line.match(re);
  if (!m) return null;
  return { num: normalizeArabicDigits(m[1] || ''), rest: (m[2] || '').trim() };
}

function buildArticles(paragraphs, name) {
  const items = [];
  const lines = paragraphs.slice();

  // Intro header (fixed phrase)
  items.push({ id: 'intro_header', title: 'إن رئـيس اللجنة المديرة', content: '', preselected: true });

  // Considerations up to decision
  const decisionIdx = lines.findIndex(l => /يقرر/.test(l));
  const consStart = lines.findIndex(l => /بمقتضى|وبناء/.test(l));
  if (consStart >= 0 && decisionIdx > consStart) {
    const cons = lines.slice(consStart, decisionIdx);
    const content = cons.map(l => (l.startsWith('-') ? l : `- ${l}`)).join('\n');
    items.push({ id: 'considerations', title: '', content, preselected: true });
  }

  if (decisionIdx >= 0) {
    items.push({ id: 'decision', title: 'يــقــرر مــا يــلــي :', content: '', preselected: true });
  }

  // Articles
  // From decisionIdx onwards, parse materials
  let i = decisionIdx >= 0 ? decisionIdx + 1 : 0;
  while (i < lines.length) {
    const h = isArticleHeading(lines[i]);
    if (!h) { i++; continue; }
    const title = `المـادة ${h.num}:`;
    let content = h.rest || '';
    i++;
    // Accumulate until next heading or signature
    while (i < lines.length && !isArticleHeading(lines[i]) && !/رئ.?يس\s+اللجنة\s+المديرة|ح.?رر\s+بالجزائر/.test(lines[i])) {
      const l = lines[i];
      if (l) content += (content ? ' ' : '') + l;
      i++;
    }
    items.push({ id: `art${h.num}`, title, content: content.trim(), preselected: true });
  }

  // Signature
  const sigLine = lines.find(l => /رئ.?يس\s+اللجنة\s+المديرة/.test(l));
  const dateLine = lines.find(l => /ح.?رر\s+بالجزائر/.test(l));
  if (sigLine || dateLine) {
    const sigContent = [sigLine || '', dateLine || ''].filter(Boolean).join('\n');
    items.push({ id: 'signature', title: 'رئـيس اللجنة المديرة', content: sigContent, preselected: true });
  }

  return { name, articles: items };
}

function run() {
  MAP.forEach(({ doc, out, name }) => {
    const docPath = path.join(MODELS_DIR, doc);
    if (!fs.existsSync(docPath)) {
      console.warn('Missing model:', docPath);
      return;
    }
    try {
      const paras = extractParagraphsFromDocx(docPath);
      const data = buildArticles(paras, name);
      const outPath = path.join(OUT_DIR, out);
      if (fs.existsSync(outPath)) {
        console.log('Skip existing', outPath);
        return;
      }
      fs.writeFileSync(outPath, JSON.stringify(data, null, 2), 'utf8');
      console.log('Wrote', outPath);
    } catch (e) {
      console.error('Failed to process', docPath, e.message);
    }
  });
}

if (require.main === module) run();
