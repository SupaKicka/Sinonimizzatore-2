#!/usr/bin/env python3
"""
Generatore di forme flesse da dump Wiktionary italiano.

Scarica il dump XML completo del Wiktionary italiano (~58 MB compresso),
lo parsa in locale ed estrae tutte le tavole di coniugazione/declinazione
per i lemmi incompleti nel DB.

Zero richieste HTTP durante il parsing — tutto in locale, tutta CPU.

Uso:
    python generate_inflections.py                  # scarica dump + processa tutto
    python generate_inflections.py --dump-only      # solo scarica il dump
    python generate_inflections.py --skip-download  # usa dump già scaricato
    python generate_inflections.py --test parlare   # mostra forme di un verbo dal dump
    python generate_inflections.py --pos VER        # solo verbi
    python generate_inflections.py --limit 100      # solo i primi 100
    python generate_inflections.py --dry-run        # mostra senza scrivere nel DB

Requisiti: nessuno (solo stdlib). BeautifulSoup NON è necessario.
"""

import sqlite3
import os
import sys
import time
import argparse
import logging
import re
import bz2
import xml.etree.ElementTree as ET
from collections import defaultdict
from urllib.request import urlretrieve, Request, urlopen

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "morphit.db")
DUMP_URL = "https://dumps.wikimedia.org/itwiktionary/latest/itwiktionary-latest-pages-articles.xml.bz2"
DUMP_PATH = os.path.join(SCRIPT_DIR, "itwiktionary-dump.xml.bz2")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(SCRIPT_DIR, "inflections.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ─── Download dump ───────────────────────────────────────────────────────────

def download_dump():
    """Scarica il dump di Wiktionary italiano."""
    if os.path.exists(DUMP_PATH):
        size_mb = os.path.getsize(DUMP_PATH) / 1024 / 1024
        log.info(f"Dump già presente: {DUMP_PATH} ({size_mb:.0f} MB)")
        return

    log.info(f"Scaricamento dump da {DUMP_URL}...")
    log.info("(~58 MB, potrebbe richiedere qualche minuto)")

    def progress(block_num, block_size, total_size):
        downloaded = block_num * block_size
        pct = downloaded * 100 / total_size if total_size > 0 else 0
        mb = downloaded / 1024 / 1024
        total_mb = total_size / 1024 / 1024
        sys.stdout.write(f"\r  {mb:.1f}/{total_mb:.1f} MB ({pct:.0f}%)")
        sys.stdout.flush()

    urlretrieve(DUMP_URL, DUMP_PATH, reporthook=progress)
    print()
    log.info("Download completato.")


# ─── Parser Wikitext per coniugazioni ────────────────────────────────────────

# Template di coniugazione usati su it.wiktionary
# Esempio: {{it-conj|parl|are|avere}}
# I template generano le tavole di coniugazione

_MOOD_MAP = {
    "indicativo": "ind", "congiuntivo": "sub", "condizionale": "cond",
    "imperativo": "impr", "infinito": "inf", "gerundio": "ger",
    "participio": "part",
}

_TENSE_MAP = {
    "presente": "pres", "imperfetto": "impf", "passato remoto": "past",
    "futuro semplice": "fut", "futuro": "fut", "passato": "past",
}

_PERSONS = [
    ("1", "s"), ("2", "s"), ("3", "s"),
    ("1", "p"), ("2", "p"), ("3", "p"),
]


def parse_it_conj_template(wikitext, title):
    """Parsa il template {{it-conj}} dal wikitext e genera le forme.

    Formato: {{it-conj|RADICE|CONIUGAZIONE|AUSILIARE|eventuali irregolarità}}
    Esempio: {{it-conj|parl|are|avere}}
    """
    # Cerca il template it-conj
    match = re.search(r'\{\{[Ii]t-conj\|([^}]+)\}\}', wikitext)
    if not match:
        return []

    params = match.group(1).split("|")
    if len(params) < 2:
        return []

    stem = params[0]
    conjugation = params[1]  # "are", "ere", "ire", "ire (isc)"

    if not stem or not conjugation:
        return []

    forms = []
    is_isc = "isc" in conjugation
    conj = conjugation.replace(" (isc)", "").replace("(isc)", "").strip()

    # Infinito
    forms.append((stem + conj, None, None, "inf", "pres", None))

    # Gerundio
    if conj == "are":
        forms.append((stem + "ando", None, None, "ger", "pres", None))
    else:
        forms.append((stem + "endo", None, None, "ger", "pres", None))

    # Participio presente
    if conj == "are":
        forms.append((stem + "ante", None, "s", "part", "pres", None))
    else:
        forms.append((stem + "ente", None, "s", "part", "pres", None))

    # Participio passato
    if conj == "are":
        pp = stem + "ato"
    elif conj == "ere":
        pp = stem + "uto"
    else:
        pp = stem + "ito"

    # Cerca participio irregolare nei parametri
    for p in params[3:]:
        p = p.strip()
        if p.startswith("pp=") or p.startswith("pp2="):
            pp = p.split("=", 1)[1]
            break

    forms.append((pp, "m", "s", "part", "past", None))
    if pp.endswith("o"):
        b = pp[:-1]
        forms.append((b + "a", "f", "s", "part", "past", None))
        forms.append((b + "i", "m", "p", "part", "past", None))
        forms.append((b + "e", "f", "p", "part", "past", None))

    # ── Indicativo ──

    # Presente
    if conj == "are":
        suffixes = ["o", "i", "a", "iamo", "ate", "ano"]
    elif conj == "ere":
        suffixes = ["o", "i", "e", "iamo", "ete", "ono"]
    else:  # ire
        if is_isc:
            suffixes = ["isco", "isci", "isce", "iamo", "ite", "iscono"]
        else:
            suffixes = ["o", "i", "e", "iamo", "ite", "ono"]

    for suf, (person, number) in zip(suffixes, _PERSONS):
        forms.append((stem + suf, None, number, "ind", "pres", person))

    # Imperfetto
    if conj == "are":
        suffixes = ["avo", "avi", "ava", "avamo", "avate", "avano"]
    elif conj == "ere":
        suffixes = ["evo", "evi", "eva", "evamo", "evate", "evano"]
    else:
        suffixes = ["ivo", "ivi", "iva", "ivamo", "ivate", "ivano"]

    for suf, (person, number) in zip(suffixes, _PERSONS):
        forms.append((stem + suf, None, number, "ind", "impf", person))

    # Passato remoto
    if conj == "are":
        suffixes = ["ai", "asti", "ò", "ammo", "aste", "arono"]
    elif conj == "ere":
        suffixes = ["ei", "esti", "é", "emmo", "este", "erono"]
    else:
        suffixes = ["ii", "isti", "ì", "immo", "iste", "irono"]

    for suf, (person, number) in zip(suffixes, _PERSONS):
        forms.append((stem + suf, None, number, "ind", "past", person))

    # Futuro
    if conj == "are":
        suffixes = ["erò", "erai", "erà", "eremo", "erete", "eranno"]
    elif conj == "ere":
        suffixes = ["erò", "erai", "erà", "eremo", "erete", "eranno"]
    else:
        suffixes = ["irò", "irai", "irà", "iremo", "irete", "iranno"]

    for suf, (person, number) in zip(suffixes, _PERSONS):
        forms.append((stem + suf, None, number, "ind", "fut", person))

    # ── Condizionale presente ──
    if conj == "are":
        suffixes = ["erei", "eresti", "erebbe", "eremmo", "ereste", "erebbero"]
    elif conj == "ere":
        suffixes = ["erei", "eresti", "erebbe", "eremmo", "ereste", "erebbero"]
    else:
        suffixes = ["irei", "iresti", "irebbe", "iremmo", "ireste", "irebbero"]

    for suf, (person, number) in zip(suffixes, _PERSONS):
        forms.append((stem + suf, None, number, "cond", "pres", person))

    # ── Congiuntivo ──

    # Presente
    if conj == "are":
        suffixes = ["i", "i", "i", "iamo", "iate", "ino"]
    elif conj == "ere":
        suffixes = ["a", "a", "a", "iamo", "iate", "ano"]
    else:
        if is_isc:
            suffixes = ["isca", "isca", "isca", "iamo", "iate", "iscano"]
        else:
            suffixes = ["a", "a", "a", "iamo", "iate", "ano"]

    for suf, (person, number) in zip(suffixes, _PERSONS):
        forms.append((stem + suf, None, number, "sub", "pres", person))

    # Imperfetto
    if conj == "are":
        suffixes = ["assi", "assi", "asse", "assimo", "aste", "assero"]
    elif conj == "ere":
        suffixes = ["essi", "essi", "esse", "essimo", "este", "essero"]
    else:
        suffixes = ["issi", "issi", "isse", "issimo", "iste", "issero"]

    for suf, (person, number) in zip(suffixes, _PERSONS):
        forms.append((stem + suf, None, number, "sub", "impf", person))

    # ── Imperativo ──
    if conj == "are":
        impr = [("a", "2", "s"), ("i", "3", "s"), ("iamo", "1", "p"), ("ate", "2", "p"), ("ino", "3", "p")]
    elif conj == "ere":
        impr = [("i", "2", "s"), ("a", "3", "s"), ("iamo", "1", "p"), ("ete", "2", "p"), ("ano", "3", "p")]
    else:
        if is_isc:
            impr = [("isci", "2", "s"), ("isca", "3", "s"), ("iamo", "1", "p"), ("ite", "2", "p"), ("iscano", "3", "p")]
        else:
            impr = [("i", "2", "s"), ("a", "3", "s"), ("iamo", "1", "p"), ("ite", "2", "p"), ("ano", "3", "p")]

    for suf, person, number in impr:
        forms.append((stem + suf, None, number, "impr", "pres", person))

    return forms


def parse_noun_adj_wikitext(wikitext, title, pos):
    """Estrai forme flesse di nomi/aggettivi dal wikitext.

    Cerca pattern come {{Tabs|casa|case}} o {{Pn|w=casa|ms=casa|mp=casi|fs=casa|fp=case}}
    o semplicemente le sezioni con link a forme flesse.
    """
    forms = []
    title_lower = title.lower()

    # Pattern 1: {{Pn}} (paradigma nominale)
    pn_match = re.search(r'\{\{Pn[^}]*\}\}', wikitext)
    if pn_match:
        pn = pn_match.group(0)
        for key, gender, number in [("ms=", "m", "s"), ("fs=", "f", "s"),
                                     ("mp=", "m", "p"), ("fp=", "f", "p")]:
            m = re.search(key + r'([^|}\s]+)', pn)
            if m:
                forms.append((m.group(1).lower(), gender, number))

    # Pattern 2: {{Tabs|singolare|plurale}}
    tabs_match = re.search(r'\{\{Tabs\|([^}]+)\}\}', wikitext)
    if tabs_match:
        parts = tabs_match.group(1).split("|")
        if len(parts) >= 2:
            sing = parts[0].strip().lower()
            plur = parts[1].strip().lower()
            if sing:
                g = "f" if sing.endswith("a") else "m" if sing.endswith("o") else None
                forms.append((sing, g, "s"))
            if plur and plur != sing:
                g = "f" if plur.endswith("e") and sing.endswith("a") else "m" if plur.endswith("i") else None
                forms.append((plur, g, "p"))

    # Pattern 3: per aggettivi, {{It-decl-agg}} o {{It-decl-adj}}
    adj_match = re.search(r'\{\{(?:It-decl-agg|It-decl-adj)[^}]*\|([^}]+)\}\}', wikitext, re.IGNORECASE)
    if adj_match:
        parts = adj_match.group(1).split("|")
        # Tipicamente: radice, o ms/fs/mp/fp
        for p in parts:
            p = p.strip().lower()
            if "=" in p:
                k, v = p.split("=", 1)
                if k.strip() in ("ms", "fs", "mp", "fp"):
                    g = "m" if "m" in k else "f"
                    n = "s" if "s" in k else "p"
                    forms.append((v.strip(), g, n))

    # Se non trovate forme specifiche, genera da regole base per la parola stessa
    if not forms:
        w = title_lower
        if pos == "ADJ":
            if w.endswith("o"):
                b = w[:-1]
                forms = [(b+"o","m","s"), (b+"a","f","s"), (b+"i","m","p"), (b+"e","f","p")]
            elif w.endswith("e"):
                forms = [(w,"m","s"), (w,"f","s"), (w[:-1]+"i","m","p"), (w[:-1]+"i","f","p")]
        elif pos == "NOUN":
            if w.endswith("o"):
                forms = [(w,"m","s"), (w[:-1]+"i","m","p")]
            elif w.endswith("a"):
                forms = [(w,"f","s"), (w[:-1]+"e","f","p")]
            elif w.endswith("e"):
                forms = [(w,None,"s"), (w[:-1]+"i",None,"p")]

    if not forms:
        forms = [(title_lower, None, None)]

    return forms


# ─── Parser XML del dump ─────────────────────────────────────────────────────

def iter_pages(dump_path):
    """Itera le pagine del dump XML Wiktionary, yielding (title, wikitext)."""
    log.info(f"Apertura dump: {dump_path}")

    if dump_path.endswith(".bz2"):
        fileobj = bz2.open(dump_path, "rt", encoding="utf-8")
    else:
        fileobj = open(dump_path, "r", encoding="utf-8")

    # Parsing incrementale per non caricare tutto in RAM
    ns = ""
    in_page = False
    title = None
    text_parts = []
    in_text = False
    page_count = 0

    for event, elem in ET.iterparse(fileobj, events=("start", "end")):
        # Rileva namespace
        if event == "start" and not ns and "}" in elem.tag:
            ns = elem.tag.split("}")[0] + "}"

        tag = elem.tag.replace(ns, "")

        if event == "start" and tag == "page":
            in_page = True
            title = None
            text_parts = []
            in_text = False

        elif event == "end" and tag == "title" and in_page:
            title = elem.text or ""

        elif event == "end" and tag == "text" and in_page:
            wikitext = elem.text or ""
            if title and wikitext and (":" not in title or title.startswith("Appendice:Coniugazioni")):
                page_count += 1
                yield title, wikitext
            elem.clear()

        elif event == "end" and tag == "page":
            in_page = False
            elem.clear()

    fileobj.close()
    log.info(f"Pagine lette: {page_count:,}")


def extract_all_forms(dump_path, target_words=None):
    """Estrai forme flesse per tutte le parole (o solo quelle in target_words).

    Ritorna dict: word_lower -> {
        "ver": [(form, gender, number, mood, tense, person), ...],
        "noun": [(form, gender, number), ...],
        "adj": [(form, gender, number), ...],
    }
    """
    results = {}
    target_set = {w.lower() for w in target_words} if target_words else None

    for title, wikitext in iter_pages(dump_path):
        title_lower = title.lower().strip()

        # Filtra: solo pagine che ci interessano
        if target_set and title_lower not in target_set:
            # Ma controlla anche le pagine Appendice:Coniugazioni
            if not title.startswith("Appendice:Coniugazioni/Italiano/"):
                continue
            # Estrai il nome del verbo dall'appendice
            verb_name = title.split("/")[-1].lower()
            if verb_name not in target_set:
                continue
            title_lower = verb_name

        # Controlla se è una pagina di coniugazione (Appendice)
        if re.search(r'[Ii]t-conj', wikitext) or title.startswith("Appendice:Coniugazioni"):
            forms = parse_it_conj_template(wikitext, title_lower)
            if forms:
                if title_lower not in results:
                    results[title_lower] = {}
                results[title_lower]["ver"] = forms

        # Controlla se è un sostantivo
        if "{{-sost-" in wikitext or "{{-nome-" in wikitext or "Sostantivo" in wikitext:
            forms = parse_noun_adj_wikitext(wikitext, title_lower, "NOUN")
            if forms:
                if title_lower not in results:
                    results[title_lower] = {}
                results[title_lower]["noun"] = forms

        # Controlla se è un aggettivo
        if "{{-agg-" in wikitext or "Aggettivo" in wikitext:
            forms = parse_noun_adj_wikitext(wikitext, title_lower, "ADJ")
            if forms:
                if title_lower not in results:
                    results[title_lower] = {}
                results[title_lower]["adj"] = forms

    return results


# ─── Database ────────────────────────────────────────────────────────────────

def get_incomplete_lemmas(conn, pos_filter=None):
    """Ritorna lemmi con 1 o meno forme."""
    cur = conn.cursor()
    if pos_filter:
        cur.execute("""
            SELECT l.id, l.lemma, l.pos FROM lemmas l
            WHERE l.pos = ?
            AND (SELECT COUNT(*) FROM forms f WHERE f.lemma_id = l.id) <= 1
            ORDER BY l.lemma
        """, (pos_filter,))
    else:
        cur.execute("""
            SELECT l.id, l.lemma, l.pos FROM lemmas l
            WHERE l.pos IN ('NOUN', 'VER', 'ADJ', 'ADV')
            AND (SELECT COUNT(*) FROM forms f WHERE f.lemma_id = l.id) <= 1
            ORDER BY l.lemma
        """)
    return cur.fetchall()


def insert_forms(conn, lemma_id, pos, forms_data):
    """Inserisce forme nel DB. forms_data è lista di tuple (dipende dal POS)."""
    cur = conn.cursor()
    cur.execute("SELECT LOWER(form) FROM forms WHERE lemma_id = ?", (lemma_id,))
    existing = {r[0] for r in cur.fetchall()}

    count = 0
    for form_tuple in forms_data:
        if len(form_tuple) == 6:
            form, gender, number, mood, tense, person = form_tuple
        elif len(form_tuple) == 3:
            form, gender, number = form_tuple
            mood, tense, person = None, None, None
        else:
            continue

        f = form.lower()
        if f in existing:
            continue
        existing.add(f)

        pos_full = pos
        if mood:
            pos_full = f"VER:{mood}+{tense or '?'}"
        elif gender or number:
            pos_full = f"{pos}-{gender or '?'}:{number or '?'}"

        cur.execute("""
            INSERT OR IGNORE INTO forms (lemma_id, form, pos_full, gender, number, person, mood, tense)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (lemma_id, f, pos_full, gender, number, person, mood, tense))
        count += 1

    return count


# ─── Main ────────────────────────────────────────────────────────────────────

def test_word(word, dump_path):
    """Mostra le forme di una parola dal dump."""
    results = extract_all_forms(dump_path, target_words=[word])

    if word.lower() not in results:
        print(f"'{word}' non trovato nel dump.")
        return

    data = results[word.lower()]
    for pos_key, forms_data in data.items():
        print(f"\n{pos_key.upper()}: {len(forms_data)} forme")
        if pos_key == "ver":
            by_mood = defaultdict(list)
            for ft in forms_data:
                form, gender, number, mood, tense, person = ft
                key = f"{mood} {tense}"
                by_mood[key].append(f"{form} ({person or ''}{number or ''})")
            for key in sorted(by_mood.keys()):
                print(f"  {key}: {', '.join(by_mood[key])}")
        else:
            for ft in forms_data:
                form, gender, number = ft
                print(f"  {form} (g={gender}, n={number})")


def main():
    parser = argparse.ArgumentParser(description="Generatore forme flesse da dump Wiktionary")
    parser.add_argument("--dump-only", action="store_true", help="Solo scarica il dump")
    parser.add_argument("--skip-download", action="store_true", help="Usa dump già scaricato")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--test", type=str, help="Testa una parola specifica")
    parser.add_argument("--pos", type=str, default=None, help="Filtra per POS (NOUN/VER/ADJ/ADV)")
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    # Step 1: Download dump
    if not args.skip_download:
        download_dump()

    if args.dump_only:
        return

    if not os.path.exists(DUMP_PATH):
        log.error(f"Dump non trovato: {DUMP_PATH}")
        log.error("Esegui prima senza --skip-download per scaricarlo.")
        return

    # Step 2: Test mode
    if args.test:
        test_word(args.test, DUMP_PATH)
        return

    # Step 3: Carica lemmi incompleti dal DB
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    incomplete = get_incomplete_lemmas(conn, pos_filter=args.pos)
    if args.limit:
        incomplete = incomplete[:args.limit]

    log.info(f"Lemmi incompleti: {len(incomplete)}")
    by_pos = defaultdict(int)
    for _, _, pos in incomplete:
        by_pos[pos] += 1
    for pos, cnt in sorted(by_pos.items(), key=lambda x: -x[1]):
        log.info(f"  {pos}: {cnt}")

    if not incomplete:
        log.info("Nessun lemma da processare.")
        return

    # Prepara set di parole target
    target_words = [lemma for _, lemma, _ in incomplete]
    lemma_map = {}  # word_lower -> [(lemma_id, pos), ...]
    for lid, lemma, pos in incomplete:
        lemma_map.setdefault(lemma.lower(), []).append((lid, pos))

    # Step 4: Parsa il dump
    log.info("Parsing dump Wiktionary...")
    t0 = time.time()
    all_forms = extract_all_forms(DUMP_PATH, target_words=target_words)
    parse_time = time.time() - t0
    log.info(f"Parsing completato in {parse_time:.1f}s. Parole trovate: {len(all_forms)}")

    # Step 5: Inserisci nel DB
    stats = {"matched": 0, "forms_added": 0, "not_found": 0}

    for word_lower, entries in lemma_map.items():
        if word_lower not in all_forms:
            stats["not_found"] += 1
            continue

        word_data = all_forms[word_lower]

        for lid, pos in entries:
            pos_key = {"VER": "ver", "NOUN": "noun", "ADJ": "adj"}.get(pos)
            if not pos_key or pos_key not in word_data:
                continue

            forms_data = word_data[pos_key]
            stats["matched"] += 1

            if not args.dry_run:
                n = insert_forms(conn, lid, pos, forms_data)
                stats["forms_added"] += n
            else:
                stats["forms_added"] += len(forms_data)

    if not args.dry_run:
        conn.commit()

    log.info(f"Risultati:")
    log.info(f"  Lemmi matchati nel dump: {stats['matched']:,}")
    log.info(f"  Lemmi non trovati: {stats['not_found']:,}")
    log.info(f"  Forme {'(stimate)' if args.dry_run else 'aggiunte'}: {stats['forms_added']:,}")

    if not args.dry_run:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM forms")
        log.info(f"  Forme totali nel DB: {cur.fetchone()[0]:,}")

    conn.close()


if __name__ == "__main__":
    main()
