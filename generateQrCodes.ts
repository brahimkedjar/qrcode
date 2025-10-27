import fs from "fs";
import * as crypto from "crypto"; // module pour le hachage SHA-256

// Minimal CSV utilities (semicolon delimiter, supports quotes)
function splitCsvLine(line: string, delimiter = ";"): string[] {
  const out: string[] = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i]!;
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') { // escaped quote
        cur += '"'; i++;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (!inQuotes && ch === delimiter) { out.push(cur); cur = ""; continue; }
    if (!inQuotes && (ch === "\r" || ch === "\n")) { continue; }
    cur += ch;
  }
  out.push(cur);
  return out;
}

function parseCsv(content: string, delimiter = ";"): string[][] {
  const lines = content.split(/\r?\n/);
  const rows: string[][] = [];
  for (const raw of lines) {
    if (raw == null) continue;
    const line = raw.replace(/^\ufeff/, "");
    if (line.trim() === "") continue; // skip empty
    rows.push(splitCsvLine(line, delimiter));
  }
  return rows;
}

function stringifyCsv(rows: string[][], delimiter = ";"): string {
  const esc = (v: string) => {
    if (v == null) return "";
    const needs = /["\n\r]/.test(v) || v.includes(delimiter);
    let s = String(v);
    if (needs) { s = '"' + s.replace(/"/g, '""') + '"'; }
    return s;
  };
  return rows.map(r => r.map(esc).join(delimiter)).join("\n");
}

// Définition du type pour les tables utilisés
interface Titre {
  [key: string]: string;
  id: string;
  Code: string;
  idType: string;
  idDetenteur: string;
  DateDemande: string;
  code_wilaya: string;
  DateHeureSysteme: string;
  QrCode: string;
}

interface Detenteur {
  id: string;
  Nom: string;
}

interface TypeTitre {
  id: string;
  Code: string;
}

//  lire un CSV et le convertir en tableau d’objets
async function readCSV<T>(path: string): Promise<T[]> {
  const content = await fs.promises.readFile(path, "utf8");
  const rows = parseCsv(content, ";");
  if (rows.length === 0) return [];
  const header = rows[0]!.map(h => String(h || "").replace(/^\ufeff/, "").trim());
  const out: T[] = [] as any;
  for (let i = 1; i < rows.length; i++) {
    const r = rows[i]!;
    const obj: any = {};
    for (let c = 0; c < header.length; c++) {
      obj[header[c]!] = r[c] ?? "";
    }
    out.push(obj);
  }
  return out;
}

//écrire un CSV en conservant l’ordre des colonnes
function writeCSV<T extends Record<string, string>>(
  path: string,
  data: T[],
  columns: string[]
) {
  const rows: string[][] = [];
  rows.push(columns.slice());
  for (const entry of data) {
    const r: string[] = [];
    for (const col of columns) {
      r.push(entry[col] ?? "");
    }
    rows.push(r);
  }
  const csvOutput = stringifyCsv(rows, ";");
  fs.writeFileSync(path, csvOutput, "utf8");
}

//  générer un code QR unique basé sur SHA-256
function genererCodeUnique(
  code_permis: string,
  type_titre: string,
  date_demande: string,
  code_wilaya: string,
  nom_societe: string
) {
  const date_systeme = new Date();
  const date_heure_systeme = date_systeme.toISOString();
  const horodatage_hash = date_heure_systeme.replace(/[-:TZ.]/g, "");
  const combined = `${code_permis}${type_titre}${date_demande}${code_wilaya}${nom_societe}${horodatage_hash}`;
  const hash = crypto
    .createHash("sha256")
    .update(combined)
    .digest("hex")
    .toUpperCase();
  const baseCode = hash.substring(0, 20);
  const code_unique = baseCode.match(/.{1,5}/g)?.join("-") ?? baseCode;
  return {
    code_unique,
    date_heure_systeme,
  };
}

async function main() {
  const titres = await readCSV<Titre>("data/df_titreQRCode.csv");
  const detenteurs = await readCSV<Detenteur>("data/Detenteurs.csv");
  const typeTitres = await readCSV<TypeTitre>("data/TypesTitres.csv");
  const titreColumns = titres.length > 0 ? Object.keys(titres[0]!) : [];

  console.log(`Titres charges : ${titres.length}`);
  console.log(`Detenteurs charges : ${detenteurs.length}`);
  console.log(`TypesTitres charges : ${typeTitres.length}`);
  console.log("--------------------------------");

  let codesGen = 0;
  let codesIgn = 0;

  for (const t of titres) {
    const idDet = String(t.idDetenteur).replace(".0", "").trim();
    const idType = String(t.idType).replace(".0", "").trim();

    // Chercher le détenteur et le type correspondant
    const detenteur = detenteurs.find(
      (d) => String(d.id).replace(".0", "").trim() === idDet
    );
    const typeTitre = typeTitres.find(
      (tt) => String(tt.id).replace(".0", "").trim() === idType
    );

    console.log(
      "Recherche detenteur et type pour titre :",
      t.id,
      "idDetenteur:",
      t.idDetenteur,
      "idType:",
      t.idType
    );

// Si détenteur ou type introuvable
    if (!detenteur || !typeTitre) {
      console.warn(
        `Attention: titre ${t.id || "inconnu"} ignore (idDet=${idDet}, idType=${idType})`
      );
      console.log("   Titre brut :", {
        Code: t.Code,
        idType: t.idType,
        idDetenteur: t.idDetenteur,
        code_wilaya: t.code_wilaya,
      });
      t.DateHeureSysteme = "";
      t.QrCode = "";
      codesIgn++;
      continue;
    }

// Générer le QR code
    const { code_unique, date_heure_systeme } = genererCodeUnique(
      t.Code.padStart(5, "0"),
      typeTitre.Code,
      t.DateDemande.replace(/[^0-9]/g, ""),
      t.code_wilaya.padStart(2, "0"),
      detenteur.Nom
    );

    console.log({
      id: t.id,
      code_permis: t.Code,
      type_titre: typeTitre.Code,
      nom_societe: detenteur.Nom,
      date_heure_systeme,
      code_unique,
    });
     // Mettre à jour le titre
    t.DateHeureSysteme = date_heure_systeme;
    t.QrCode = code_unique;
    codesGen++;
  }

  // Écriture du CSV mis à jour
  if (titres.length > 0 && titreColumns.length > 0) {
    const outputPath = "data/df_titreQRCode.csv";
    try {
      writeCSV(outputPath, titres, titreColumns);
    } catch (error) {
      const err = error as NodeJS.ErrnoException;
      if (err.code === "EBUSY" || err.code === "EPERM") {
        const fallbackPath = "data/df_titreQRCode.generated.csv";
        console.warn(
          `Attention: impossible d'ecrire dans ${outputPath} (fichier occupe). Sauvegarde dans ${fallbackPath}.`
        );
        try {
          writeCSV(fallbackPath, titres, titreColumns);
        } catch (fallbackErr) {
          const fallbackError = fallbackErr as NodeJS.ErrnoException;
          console.warn(
            `Attention: echec de la sauvegarde dans ${fallbackPath} (${fallbackError.code ?? "erreur inconnue"}). Le contenu reste seulement en memoire.`
          );
        }
      } else {
        throw error;
      }
    }
  }

  console.log("--------------------------------");
  console.log(`Codes generes : ${codesGen}`);
  console.log(`Enregistrements ignores : ${codesIgn}`);
  console.log("--------------------------------");
}

main().catch(console.error);
