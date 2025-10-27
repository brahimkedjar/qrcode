const fs = require('fs');
const path = require('path');
const cp = require('child_process');

const MODELS_DIR = path.join(__dirname, '..', 'models');

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
  const tmpDir = path.join(tmpRoot, `dbg_${basename}_${Date.now()}`);
  fs.mkdirSync(tmpDir, { recursive: true });
  const zipPath = path.join(tmpDir, basename + '.zip');
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

const fname = process.argv[2] || 'new modele titre TEM.docx';
const docPath = path.join(MODELS_DIR, fname);
if (!fs.existsSync(docPath)) {
  console.error('Missing file:', docPath);
  process.exit(1);
}
const paras = extractParagraphsFromDocx(docPath);
paras.forEach((p, i) => {
  if (/\u0627|\u0645|\u062F|\u0629/.test(p)) {
    console.log(String(i).padStart(4, '0'), p);
  }
});

