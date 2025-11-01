#!/usr/bin/env python3
import sys
import os
import json
import re
from datetime import datetime
from typing import List, Dict, Tuple, Any, Optional

try:
    import pyodbc  # type: ignore
except Exception as e:  # pragma: no cover
    sys.stderr.write("[ERR] pyodbc is required: {}\n".format(e))
    sys.exit(2)


def log_info(msg: str) -> None:
    print(msg, flush=True)


def q(name: str) -> str:
    return f"[{name}]"


def connect_access(path: str) -> pyodbc.Connection:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Database not found: {path}")
    conn_str = (
        f"Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={path};Uid=Admin;Pwd=;"
    )
    return pyodbc.connect(conn_str, autocommit=False)


def get_columns(conn: pyodbc.Connection, table: str) -> List[Tuple[str, int]]:
    """
    Returns list of (name, type_code) for columns in table.
    Prefers result-set description (robust for odd metadata encodings) and
    falls back to ODBC catalog if needed.
    """
    # 1) Try SELECT â€¦ WHERE 1=0 to obtain description-based metadata
    cur = conn.cursor()
    try:
        try:
            cur.execute(f"SELECT * FROM {q(table)} WHERE 1=0")
            desc = cur.description or []
            cols: List[Tuple[str, int]] = []
            for d in desc:
                name = str(d[0])
                # d[1] is type_code (may be None for some drivers). Default to 0.
                try:
                    dtype = int(d[1]) if d[1] is not None else 0
                except Exception:
                    dtype = 0
                cols.append((name, dtype))
            if cols:
                return cols
        except Exception:
            pass
    finally:
        try:
            cur.close()
        except Exception:
            pass

    # 2) Fallback to ODBC catalog metadata
    cols: List[Tuple[str, int]] = []
    cur = conn.cursor()
    try:
        try:
            # Some drivers require explicit metadata decoding
            try:
                conn.setdecoding(pyodbc.SQL_WMETADATA, encoding='utf-16le')  # best-effort
            except Exception:
                pass
            for row in cur.columns(table=table):
                try:
                    name = str(row.column_name)
                except Exception:
                    # last resort: represent as bytes repr
                    name = str(row.column_name).encode('utf-8', 'ignore').decode('utf-8', 'ignore')
                try:
                    dtype = int(row.data_type)
                except Exception:
                    dtype = 0
                cols.append((name, dtype))
        except Exception:
            cols = []
    finally:
        try:
            cur.close()
        except Exception:
            pass
    return cols


def fetch_all(conn: pyodbc.Connection, table: str, cols: List[str]) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    def run_select(select_sql: str, alias_map: Optional[Dict[str, str]] = None) -> List[Dict[str, Any]]:
        cur.execute(select_sql)
        rows = cur.fetchall()
        result: List[Dict[str, Any]] = []
        raw_names = [d[0] for d in cur.description]
        # Map alias back to original names if provided
        names = [alias_map.get(n, n) if alias_map else n for n in raw_names]
        for r in rows:
            row_dict: Dict[str, Any] = {}
            for i in range(len(names)):
                row_dict[names[i]] = r[i]
            result.append(row_dict)
        return result
    try:
        sel = ", ".join(q(c) for c in cols)
        try:
            return run_select(f"SELECT {sel} FROM {q(table)}")
        except pyodbc.DataError as e:
            msg = str(e)
            # Fallback for invalid datetime values: coerce bad dates to NULL
            if '22007' in msg or 'Invalid datetime' in msg:
                def is_date_like(name: str) -> bool:
                    return bool(re.search(r"date|time|jour", name, flags=re.IGNORECASE))
                exprs: List[str] = []
                alias_map: Dict[str, str] = {}
                for c in cols:
                    if is_date_like(c):
                        # Force ODBC to return text for date-like fields to avoid 22007
                        # Use concatenation with empty string: ([col] & '') AS [col__coerced]
                        alias = f"{c}__coerced"
                        exprs.append(f"({q(c)} & '') AS {q(alias)}")
                        alias_map[alias] = c
                    else:
                        exprs.append(q(c))
                safe_sel = ", ".join(exprs)
                try:
                    return run_select(f"SELECT {safe_sel} FROM {q(table)}", alias_map)
                except pyodbc.DataError:
                    # Last resort: coerce every column to text and fetch as strings
                    exprs2: List[str] = []
                    alias_map2: Dict[str, str] = {}
                    for c in cols:
                        alias = f"{c}__txt"
                        exprs2.append(f"({q(c)} & '') AS {q(alias)}")
                        alias_map2[alias] = c
                    all_txt_sel = ", ".join(exprs2)
                    return run_select(f"SELECT {all_txt_sel} FROM {q(table)}", alias_map2)
            raise
    finally:
        try:
            cur.close()
        except Exception:
            pass


def norm_text(v: Any) -> str:
    if v is None:
        return ""
    s = str(v)
    s = s.replace("\u00A0", " ").strip().upper()
    return s


_DATE_LIKE_RE = re.compile(r"date|time|jour", re.IGNORECASE)
_NUM_LIKE_RE = re.compile(r"^(TS_|Surface|Taxe|Montant|Total|PerInit|PremierRen|DeuRen)", re.IGNORECASE)


def is_date_like(name: str) -> bool:
    try:
        return bool(_DATE_LIKE_RE.search(name or ""))
    except Exception:
        return False


def is_numeric_like(name: str) -> bool:
    try:
        return bool(_NUM_LIKE_RE.search(name or ""))
    except Exception:
        return False


def parse_date_string(s: str) -> Optional[str]:
    if not s:
        return None
    s = s.strip().replace("\u00A0", " ")
    # Common formats: dd/MM/yyyy, dd-MM-yyyy, yyyy-MM-dd, yyyy/MM/dd
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    # Fallback: extract digits and try dd/mm/yyyy
    m = re.findall(r"\d+", s)
    if len(m) >= 3:
        # Heuristic: if first group has 4 digits -> yyyy m d else d m yyyy
        if len(m[0]) == 4:
            y, mth, d = m[0], m[1].zfill(2), m[2].zfill(2)
        else:
            d, mth, y = m[0].zfill(2), m[1].zfill(2), m[2]
        try:
            dt = datetime.strptime(f"{y}-{mth}-{d}", "%Y-%m-%d")
            return dt.strftime("%Y-%m-%d")
        except Exception:
            return None
    return None


def parse_numeric_string(s: str) -> Optional[float]:
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return float(s)
    try:
        txt = str(s)
        txt = txt.replace("\u202F", " ").replace("\u00A0", " ")
        # Remove every non digit, comma, dot, minus
        txt = re.sub(r"[^0-9,.-]", "", txt)
        # If both comma and dot exist, assume comma is thousands -> remove commas
        if "," in txt and "." in txt:
            txt = txt.replace(",", "")
        else:
            # Otherwise treat comma as decimal
            txt = txt.replace(",", ".")
        txt = txt.strip()
        if not txt:
            return None
        return float(txt)
    except Exception:
        return None


def sanitize_value(val: Any, col: str) -> Any:
    # Leave None as-is
    if val is None:
        return None
    # Dates
    if is_date_like(col):
        if isinstance(val, (datetime,)):
            return val.strftime("%Y-%m-%d")
        sv = str(val)
        iso = parse_date_string(sv)
        return iso if iso is not None else None
    # Numeric-like
    if is_numeric_like(col):
        num = parse_numeric_string(val)
        return num if num is not None else None
    # Boolean heuristics
    if isinstance(val, str):
        v = val.strip().lower()
        if v in ("yes", "true", "1", "vrai", "oui"):
            return True
        if v in ("no", "false", "0", "faux", "non"):
            return False
    return val


def load_state(path: str) -> Dict[str, Any]:
    try:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                raw = f.read().strip()
                if not raw:
                    return {}
                return json.loads(raw)
    except Exception:
        return {}
    return {}


def save_state(path: str, data: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def detect_natural_keys(table: str, dest_cols: List[str]) -> List[List[str]]:
    upper = {c.lower() for c in dest_cols}
    if table.lower() == "typestitres":
        combos = []
        if "code" in upper:
            combos.append(["Code"])
        if "nom" in upper:
            combos.append(["Nom"])
        return combos
    if table.lower() == "typesprocedures":
        if "idtypetitre" in upper and "procedure" in upper:
            return [["idTypeTitre", "Procedure"]]
    # Generic heuristic: try common label-like columns
    candidates = ["Code", "Nom", "Libelle", "Label", "Name"]
    combos: List[List[str]] = []
    for c in candidates:
        if c.lower() in upper:
            combos.append([c])
    return combos


def build_dest_nk_index(
    conn: pyodbc.Connection,
    table: str,
    key_col: str,
    nk_combos: List[List[str]],
) -> Tuple[Dict[str, Any], Dict[Tuple[int, Tuple[str, ...]], Any]]:
    dest_ids = {}
    nk_map: Dict[Tuple[int, Tuple[str, ...]], Any] = {}
    if not nk_combos:
        return dest_ids, nk_map
    # Build a superset of needed columns
    needed = {key_col}
    for combo in nk_combos:
        for c in combo:
            needed.add(c)
    cols = list(needed)
    cur = conn.cursor()
    try:
        sel = ", ".join(q(c) for c in cols)
        cur.execute(f"SELECT {sel} FROM {q(table)}")
        names = [d[0] for d in cur.description]
        idx_key = names.index(key_col)
        all_rows = cur.fetchall()
        for r in all_rows:
            rid = r[idx_key]
            dest_ids[rid] = True
            for combo in nk_combos:
                parts = []
                ok = True
                for c in combo:
                    try:
                        v = r[names.index(c)]
                    except Exception:
                        ok = False
                        break
                    parts.append(norm_text(v))
                if not ok:
                    continue
                nk_map[(len(combo), tuple(parts))] = rid
    finally:
        cur.close()
    return dest_ids, nk_map


def run_sync(source: str, dest: str, tables: List[str], resume: bool = False, state_path: Optional[str] = None) -> int:
    script_path = os.path.abspath(__file__)
    log_info(f"[INFO] DÃ©marrage de la synchronisation ({script_path})")
    log_info(f"Source : {source}")
    log_info(f"Destination : {dest}")

    src = connect_access(source)
    dst = connect_access(dest)
    if not state_path:
        state_path = os.path.join(os.path.dirname(script_path), 'sync-state.json')
    state = load_state(state_path)
    try:
        for table in tables:
            key_col = "id"
            log_info(f"=== Syncing table [{table}] ===")

            # Columns intersection
            src_cols_meta = get_columns(src, table)
            dst_cols_meta = get_columns(dst, table)
            if not dst_cols_meta or not src_cols_meta:
                continue
            dst_cols = [c for c, _ in dst_cols_meta]
            src_cols = [c for c, _ in src_cols_meta]
            common = [c for c in dst_cols if c in src_cols]
            if not common:
                log_info("  No matching columns - skipping.")
                continue
            log_info("Selected columns: " + ", ".join(common))
            if key_col not in common:
                log_info(f"  Key column [{key_col}] missing in common set - will rely on natural keys and no-key inserts.")

            # Detect natural keys and prebuild index in destination
            nk_combos = detect_natural_keys(table, dst_cols)
            if nk_combos:
                nk_str = " OR ".join("(" + ", ".join(c) + ")" for c in nk_combos)
                log_info(f"  Natural keys: {nk_str}")
            dest_ids, nk_index = build_dest_nk_index(dst, table, key_col, nk_combos)

            # Fetch all source rows with robust fallback
            fetch_cols = list(common)
            try:
                src_rows = fetch_all(src, table, fetch_cols)
            except pyodbc.Error:
                dropped = [c for c in fetch_cols if is_date_like(c)]
                fetch_cols = [c for c in fetch_cols if c not in dropped]
                if dropped:
                    log_info("  Note: refetching without date-like columns: " + ", ".join(dropped))
                src_rows = fetch_all(src, table, fetch_cols)
            upd_cols = [c for c in fetch_cols if c.lower() != key_col.lower()]
            upd_set = ", ".join(f"{q(c)}=?" for c in upd_cols)
            ins_cols = list(fetch_cols)
            ins_ph = ", ".join(["?"] * len(ins_cols))
            ins_cols_no_key = [c for c in fetch_cols if c.lower() != key_col.lower()]
            ins_ph_no_key = ", ".join(["?"] * len(ins_cols_no_key))

            cur = dst.cursor()
            inserted = 0
            updated = 0
            # Resume state
            table_state = None
            try:
                table_state = (state.get('tables', {}) if isinstance(state, dict) else {}).get(table)
            except Exception:
                table_state = None
            last_numeric_key = None
            try:
                if isinstance(table_state, dict):
                    lnk = table_state.get('lastNumericKey')
                    if isinstance(lnk, (int, float)):
                        last_numeric_key = int(lnk)
                    elif isinstance(lnk, str) and lnk.strip().isdigit():
                        last_numeric_key = int(lnk.strip())
            except Exception:
                last_numeric_key = None
            if resume and last_numeric_key is not None:
                log_info(f"  Resume: skipping rows with {key_col} <= {last_numeric_key}")
            max_numeric_seen = last_numeric_key
            try:
                # Transaction control is handled by the connection (autocommit=False).
                # Access ODBC does not accept explicit BEGIN TRANSACTION here.
                for row in src_rows:
                    raw_id = row.get(key_col)
                    # Skip already processed rows if resume enabled
                    if resume and last_numeric_key is not None:
                        try:
                            sval = str(raw_id).strip() if raw_id is not None else ''
                            if sval:
                                rid_num = int(sval)
                                if rid_num <= last_numeric_key:
                                    if (max_numeric_seen is None) or (rid_num > max_numeric_seen):
                                        max_numeric_seen = rid_num
                                    continue
                        except Exception:
                            # Non-numeric IDs cannot be used for resume threshold
                            pass
                    # Normalize texts for NK
                    # Try to find existing by id
                    existing_id: Optional[Any] = None
                    if raw_id is not None and raw_id != "":
                        try:
                            # Try quick existence by id
                            cur.execute(f"SELECT COUNT(1) FROM {q(table)} WHERE {q(key_col)}=?", (raw_id,))
                            if cur.fetchone()[0] > 0:
                                existing_id = raw_id
                        except Exception:
                            existing_id = None
                    # Track max numeric id encountered
                    try:
                        sval2 = str(raw_id).strip() if raw_id is not None else ''
                        if sval2:
                            rid_num2 = int(sval2)
                            if (max_numeric_seen is None) or (rid_num2 > max_numeric_seen):
                                max_numeric_seen = rid_num2
                    except Exception:
                        pass
                    # Try natural key lookup if id not found
                    if existing_id is None and nk_combos:
                        for combo in nk_combos:
                            parts = tuple(norm_text(row.get(c)) for c in combo)
                            rid = nk_index.get((len(combo), parts))
                            if rid is not None:
                                existing_id = rid
                                break

                    if existing_id is not None:
                        # UPDATE
                        try:
                            params = [sanitize_value(row.get(c), c) for c in upd_cols] + [existing_id]
                            cur.execute(
                                f"UPDATE {q(table)} SET {upd_set} WHERE {q(key_col)}=?",
                                params,
                            )
                            updated += 1
                            continue
                        except Exception as e:
                            # If update fails, continue to fallback insert
                            pass

                    # INSERT path
                    try:
                        params = [sanitize_value(row.get(c), c) for c in ins_cols]
                        cur.execute(
                            f"INSERT INTO {q(table)} ({', '.join(q(c) for c in ins_cols)}) VALUES ({ins_ph})",
                            params,
                        )
                        inserted += 1
                        # Update NK index with new row id if autoincrement
                        try:
                            if key_col not in row or row.get(key_col) in (None, ""):
                                # retrieve last identity
                                rid = cur.execute("SELECT @@IDENTITY").fetchone()[0]
                                # extend nk index
                                for combo in nk_combos:
                                    parts = tuple(norm_text(row.get(c)) for c in combo)
                                    nk_index[(len(combo), parts)] = rid
                        except Exception:
                            pass
                        continue
                    except pyodbc.Error as e:
                        # Duplicate or constraint: try insert without key (autonumber)
                        try:
                            params = [sanitize_value(row.get(c), c) for c in ins_cols_no_key]
                            cur.execute(
                                f"INSERT INTO {q(table)} ({', '.join(q(c) for c in ins_cols_no_key)}) VALUES ({ins_ph_no_key})",
                                params,
                            )
                            inserted += 1
                            # record new id in NK index
                            try:
                                rid = cur.execute("SELECT @@IDENTITY").fetchone()[0]
                                for combo in nk_combos:
                                    parts = tuple(norm_text(row.get(c)) for c in combo)
                                    nk_index[(len(combo), parts)] = rid
                            except Exception:
                                pass
                            continue
                        except pyodbc.Error:
                            # Try to recover by heuristic NK update
                            if nk_combos:
                                # Try update by NK map if we can find now
                                existing_id2 = None
                                for combo in nk_combos:
                                    parts = tuple(norm_text(row.get(c)) for c in combo)
                                    rid = nk_index.get((len(combo), parts))
                                    if rid is not None:
                                        existing_id2 = rid
                                        break
                                if existing_id2 is not None:
                                    try:
                                        params = [sanitize_value(row.get(c), c) for c in upd_cols] + [existing_id2]
                                        cur.execute(
                                            f"UPDATE {q(table)} SET {upd_set} WHERE {q(key_col)}=?",
                                            params,
                                        )
                                        updated += 1
                                        continue
                                    except Exception:
                                        pass
                            # Give up on this row; keep syncing others
                            code = row.get("Code"); nom = row.get("Nom")
                            sys.stderr.write(
                                f"[WARN] Skip row in {table}: Code='{code}' Nom='{nom}' due to duplicate/constraint.\n"
                            )
                            continue

                dst.commit()
                log_info(f"  Updated {updated} row(s), inserted {inserted} row(s).")
                # Optional second-pass: fix date-like columns for TaxesSup by fetching as text and parsing safely
                try:
                    if table.lower() == 'taxessup':
                        # Determine date-like columns present in destination
                        date_cols = [c for c in dst_cols if is_date_like(c)]
                        if date_cols:
                            log_info("  Post-pass: normalising dates for TaxesSup")
                            total_scanned = 0
                            total_parsed = 0
                            total_updated = 0
                            # Process each date column independently to avoid ODBC errors on multi-column coercion
                            for dc in date_cols:
                                # First fetch only ids with valid dates for this column
                                src_cur_ids = src.cursor()
                                try:
                                    src_cur_ids.execute(f"SELECT {q('id')} FROM {q(table)} WHERE IsDate({q(dc)})")
                                    id_rows = src_cur_ids.fetchall()
                                finally:
                                    try: src_cur_ids.close()
                                    except Exception: pass

                                dst_cur = dst.cursor()
                                try:
                                    scanned_rows = 0
                                    parsed_values = 0
                                    updated_rows = 0
                                    for row_id in id_rows:
                                        try:
                                            rid = row_id[0]
                                        except Exception:
                                            continue
                                        scanned_rows += 1
                                        # Fetch the raw value for this id/column
                                        src_cur_val = src.cursor()
                                        try:
                                            src_cur_val.execute(f"SELECT ({q(dc)} & '') AS val_txt FROM {q(table)} WHERE {q('id')}=?", (rid,))
                                            fr = src_cur_val.fetchone()
                                        finally:
                                            try: src_cur_val.close()
                                            except Exception: pass
                                        if not fr:
                                            continue
                                        val = fr[0]
                                        dt = None
                                        iso = parse_date_string(str(val or ''))
                                        if iso:
                                            try:
                                                dt = datetime.strptime(iso, '%Y-%m-%d')
                                            except Exception:
                                                dt = None
                                        if dt is None:
                                            continue
                                        parsed_values += 1
                                        try:
                                            dst_cur.execute(f"UPDATE {q(table)} SET {q(dc)}=? WHERE {q('id')}=?", (dt, rid))
                                            updated_rows += 1
                                        except Exception:
                                            pass
                                    if updated_rows:
                                        dst.commit()
                                    total_scanned += scanned_rows
                                    total_parsed += parsed_values
                                    total_updated += updated_rows
                                finally:
                                    try: dst_cur.close()
                                    except Exception: pass
                            log_info(f"  Post-pass: scanned {total_scanned} row(s); parsed {total_parsed} value(s); updated {total_updated} row(s).")
                except Exception as e:
                    # Best-effort: report and continue
                    try:
                        log_info(f"  Post-pass: skipped due to error: {e}")
                    except Exception:
                        pass
                # Save resume state per table
                try:
                    if isinstance(state, dict):
                        troot = state.get('tables')
                        if not isinstance(troot, dict):
                            troot = {}
                            state['tables'] = troot
                        # Use timezone-aware UTC timestamp to avoid deprecation warnings
                        try:
                            from datetime import timezone as _tz
                            _now_utc = datetime.now(_tz.utc)
                            _updated_at = _now_utc.replace(microsecond=0).isoformat().replace('+00:00', 'Z')
                        except Exception:
                            _updated_at = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                        troot[table] = {
                            'lastNumericKey': int(max_numeric_seen) if isinstance(max_numeric_seen, int) else (max_numeric_seen if max_numeric_seen is not None else None),
                            'updatedAt': _updated_at,
                        }
                        if state_path:
                            save_state(state_path, state)
                except Exception:
                    pass
            except Exception:
                dst.rollback()
                raise
            finally:
                cur.close()
        log_info("Sync complete.")
        return 0
    finally:
        try:
            src.close()
        except Exception:
            pass
        try:
            dst.close()
        except Exception:
            pass


def parse_args(argv: List[str]) -> Tuple[str, str, List[str], bool, Optional[str]]:
    source = ""
    dest = ""
    tables: List[str] = []
    resume = False
    state_path: Optional[str] = None
    it = iter(range(len(argv)))
    i = 0
    while i < len(argv):
        a = argv[i]
        if a in ("--source", "-s") and i + 1 < len(argv):
            source = argv[i + 1]; i += 2; continue
        if a in ("--dest", "--destination", "-d") and i + 1 < len(argv):
            dest = argv[i + 1]; i += 2; continue
        if a in ("--tables", "-t") and i + 1 < len(argv):
            raw = (argv[i + 1] or "").strip()
            if raw:
                tables = [x.strip() for x in raw.split(",") if x.strip()]
            i += 2; continue
        if a == "--resume":
            resume = True; i += 1; continue
        if a in ("--state",) and i + 1 < len(argv):
            state_path = argv[i + 1]; i += 2; continue
        i += 1
    if not source or not dest:
        sys.stderr.write("Usage: sync_cma.py --source <path> --dest <path> [--tables CSV]\n")
        sys.exit(1)
    if not tables:
        # Default tables to sync if none provided
        tables = [
            "Titres", "TypesTitres", "Detenteur", "coordonees", "TaxesSup", "DroitsEtabl"
        ]
    return source, dest, tables, resume, state_path


def main() -> None:
    src, dst, tables, resume, state_path = parse_args(sys.argv[1:])
    sys.exit(run_sync(src, dst, tables, resume=resume, state_path=state_path))


if __name__ == "__main__":
    main()





