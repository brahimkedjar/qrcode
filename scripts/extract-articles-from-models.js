/*
  Script: tm-app/scripts/extract-articles-from-models.js
  Purpose: Iterate all DOCX models in tm-app/models, extract Arabic
  "Ø§Ù„Ù…Ø§Ø¯Ø©" articles and write/update JSON files in
  tm-app/client/src/components (like articles-tem.json).
*/
const fs = require('fs');
const path = require('path');
const cp = require('child_process');

const MODELS_DIR = path.join(__dirname, '..', 'models');
const OUT_DIR = path.join(__dirname, '..', 'client', 'src', 'components');

// Known mapping so outputs match what the app imports
const FIXED_MAP = new Map([
  ['new modele titre tem.docx', { out: 'articles-tem.json', name: 'TEM' }],
  ['new modele titre tec.docx', { out: 'articles-tec.json', name: 'TEC' }],
  ['8375 tec extension.docx', { out: 'articles-tec-extension.json', name: 'TEC EXTENSION' }],
  ['new modele titre apm.docx', { out: 'articles-apm.json', name: 'APM' }],
  ['new modele titre aac.docx', { out: 'articles-aac.json', name: 'AAC' }],
  ['new modele titre aam mines.docx', { out: 'articles-aam-mines.json', name: 'AAM MINES' }],
  ['new modele titre axh.docx', { out: 'articles-axh.json', name: 'AXH' }],
  ['txc renouvellement.docx', { out: 'articles-txc-renouvellement.json', name: 'TXC RENOUVELLEMENT' }],
  ['txc suite a exploration.docx', { out: 'articles-txc-suite-exploration.json', name: 'TXC SUITE A EXPLORATION' }],
  ['txm renouvellement.docx', { out: 'articles-txm-renouvellement.json', name: 'TXM RENOUVELLEMENT' }],
  ['txm suite a exploration.docx', { out: 'articles-txm-suite-exploration.json', name: 'TXM SUITE A EXPLORATION' }],
]);

function decodeEntities(s) {
  return String(s)
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
  // Copy .docx to .zip and expand with PowerShell (available in this environment)
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
  if (!s) return s;
  const arabicIndic = '\u0660\u0661\u0662\u0663\u0664\u0665\u0666\u0667\u0668\u0669';
  const persian = '\u06F0\u06F1\u06F2\u06F3\u06F4\u06F5\u06F6\u06F7\u06F8\u06F9';
  const map = {};
  for (let i = 0; i < 10; i++) {
    map[arabicIndic[i]] = String(i);
    map[persian[i]] = String(i);
  }
  return String(s).split('').map(ch => map[ch] || ch).join('');
}

function isArticleHeading(line) {
  const re = /^\s*الم[\u0640]*ادة\s+([0-9\u0660-\u0669\u06F0-\u06F9]+)\s*[:\-]?[\s\u200e\u200f]*(.*)$/;
  const m = (line || '').match(re);
  if (!m) return null;
  const numRaw = m[1] || '';
  const rest = (m[2] || '').trim();
  return { num: normalizeArabicDigits(numRaw), rest };
}

function buildArticles(paragraphs, name) {
  const items = [];
  const lines = paragraphs.slice();
  const seen = new Map(); // track duplicate article numbers

  // Extract preamble (considerations) and decision header if present
  const preambleRe = /إن\s*ر[ئ]?ـ?يس\s+اللجنة\s+المديرة/;
  const decisionRe = /ي[\u0640\s]*ق[\u0640\s]*ر[\u0640\s]*ر[\u0640\s]*\s+م[\u0640\s]*ا[\u0640\s]*\s+ي[\u0640\s]*ل[\u0640\s]*ي/;
  const preambleIdx = lines.findIndex(l => preambleRe.test(l));
  let decisionIdx = -1;
  const sanitize = (s) => String(s || '').replace(/<[^>]+>/g, '').replace(/[\uFEFF]/g, '').replace(/\s+>\s*$/g, '').trim();
  if (preambleIdx >= 0) {
    decisionIdx = lines.findIndex((l, idx) => idx > preambleIdx && decisionRe.test(l));
    // Intro header
    items.push({ id: 'intro_header', title: 'إن رئـيس اللجنة المديرة', content: '', preselected: true });
    // Considerations content between intro and decision
    if (decisionIdx > preambleIdx) {
      const cons = lines.slice(preambleIdx + 1, decisionIdx).map(sanitize).filter(Boolean);
      const content = cons.map(l => (l.startsWith('-') ? l : `- ${l}`)).join('\n');
      items.push({ id: 'considerations', title: '', content, preselected: true });
      items.push({ id: 'decision', title: 'يــقــرر مــا يــلــي :', content: '', preselected: true });
    }
  } else {
    // Fallback: try to find decision alone
    decisionIdx = lines.findIndex(l => decisionRe.test(l));
    if (decisionIdx >= 0) items.push({ id: 'decision', title: 'يــقــرر مــا يــلــي :', content: '', preselected: true });
  }

  // Start parsing articles after the decision header if found
  let i = decisionIdx >= 0 ? (decisionIdx + 1) : lines.findIndex(l => !!isArticleHeading(l));
  if (i < 0) i = 0;
  while (i < lines.length) {
    const head = isArticleHeading(lines[i]);
    if (!head) { i++; continue; }
    const title = `المادة ${head.num}:`;
    let content = head.rest || '';
    i++;
    while (i < lines.length && !isArticleHeading(lines[i])) {
      const l = sanitize(lines[i]);
      if (/حرر\s+بالجزائر|رئيس\s+اللجنة|الموقَع|الوكالة\s+الوطنية/i.test(l)) break;
      if (l) content += (content ? '\n' : '') + l;
      i++;
    }
    // Unique id per article number (art5, art5a, art5b, ...)
    const baseId = `art${head.num}`;
    const count = (seen.get(baseId) || 0);
    seen.set(baseId, count + 1);
    const id = count === 0 ? baseId : `${baseId}${String.fromCharCode(96 + count + 1)}`; // a,b,c...
    items.push({ id, title, content: content.trim(), preselected: true });
  }
  return { name, articles: items };
}

function deriveOutAndName(fname) {
  const key = fname.toLowerCase();
  if (FIXED_MAP.has(key)) return FIXED_MAP.get(key);
  // Derive a conservative output name for unknown models
  const baseName = path.basename(fname, path.extname(fname));
  const name = baseName;
  const base = baseName
    .replace(/\s*new\s*modele\s*titre\s*/i, '')
    .replace(/\s+suite\s+a\s+exploration/i, 'suite-exploration')
    .replace(/\s+renouvellement/i, 'renouvellement')
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-');
  return { out: `articles-${base}.json`, name };
}

function run() {
  if (!fs.existsSync(MODELS_DIR)) {
    console.error('Models directory not found:', MODELS_DIR);
    process.exit(1);
  }
  const files = fs.readdirSync(MODELS_DIR).filter(f => f.toLowerCase().endsWith('.docx'));
  files.forEach((fname) => {
    const docPath = path.join(MODELS_DIR, fname);
    const { out, name } = deriveOutAndName(fname);
    try {
      const paras = extractParagraphsFromDocx(docPath);
      const data = buildArticles(paras, name);
      // Use the first Arabic paragraph as the display name, if present
      const firstTitle = (paras || []).find(p => /[\u0600-\u06FF]/.test(String(p || '').trim()));
      if (firstTitle) data.name = String(firstTitle).trim();
      const outPath = path.join(OUT_DIR, out);
      fs.writeFileSync(outPath, JSON.stringify(data, null, 2), 'utf8');
      console.log('Wrote', outPath, `(${data.articles.length} articles)`);
    } catch (e) {
      console.error('Failed to process', docPath, e.message);
    }
  });
}

if (require.main === module) run();



