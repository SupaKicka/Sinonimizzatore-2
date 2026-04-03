#!/usr/bin/env python3
"""
Scraper sinonimi dal Wikizionario italiano (it.wiktionary.org).
Scarica i sinonimi per i lemmi che non ne hanno ancora nel DB.

Requisiti:
    pip install aiohttp beautifulsoup4

Uso:
    python scrape_wiktionary.py                  # scrapa lemmi senza sinonimi
    python scrape_wiktionary.py --limit 100      # solo i primi 100
    python scrape_wiktionary.py --test casa      # testa una singola parola
    python scrape_wiktionary.py --resume         # riprende da dove si era fermato
    python scrape_wiktionary.py --stats          # mostra statistiche
    python scrape_wiktionary.py --workers 5      # 5 worker paralleli (default: 3)
"""

import sqlite3
import asyncio
import aiohttp
import os
import sys
import json
import time
import argparse
import logging
from datetime import timedelta
from bs4 import BeautifulSoup
from collections import defaultdict

# ─── Configurazione ──────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "morphit.db")
STATE_PATH = os.path.join(SCRIPT_DIR, "wiktionary_state.json")
LOG_PATH = os.path.join(SCRIPT_DIR, "wiktionary_scrape.log")

BASE_URL = "https://it.wiktionary.org/wiki/{word}"
TIMEOUT = 15
MAX_RETRIES = 3
CHECKPOINT_EVERY = 100

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)

# ─── Parser HTML ─────────────────────────────────────────────────────────────


def parse_wiktionary_synonyms(html, word):
    """Estrai sinonimi dalla pagina Wiktionary italiana.

    Struttura moderna: <section aria-labelledby="Sinonimi"> contiene <ul> con <a> links.
    Struttura legacy: <h3> con span "Sinonimi" seguito da <ul>.
    """
    soup = BeautifulSoup(html, "html.parser")
    synonyms = set()

    # Qualificatori d'uso Wiktionary — appaiono tra parentesi come link
    _QUALIFIERS = {
        "raro", "letterario", "familiare", "popolare", "volgare", "regionale",
        "arcaico", "obsoleto", "formale", "informale", "colloquiale", "gergale",
        "spregiativo", "dispregiativo", "ironico", "scherzoso", "burocratico",
        "tecnico", "dialettale", "poetico", "figurato", "estensivo",
        "senso figurato", "per estensione", "antico",
    }

    def _extract_from_element(container):
        """Estrai sinonimi dai link dentro un container, ignorando qualificatori tra parentesi."""
        for li in container.find_all("li"):
            # Identifica quali <a> sono dentro parentesi nel testo del <li>
            li_html = str(li)
            for a in li.find_all("a"):
                href = a.get("href", "")
                if not (("wiktionary.org/wiki/" in href or href.startswith("/wiki/")) and ":" not in href):
                    continue
                text = a.get_text(strip=True).lower().strip(".,;:!?()[]\"' ")
                if not text or len(text) <= 1 or " " in text or text == "sinonimi":
                    continue
                # Controlla se questo link è dentro parentesi tonde nell'HTML
                a_str = str(a)
                a_pos = li_html.find(a_str)
                if a_pos > 0:
                    before = li_html[:a_pos]
                    open_parens = before.count("(") - before.count(")")
                    if open_parens > 0:
                        continue  # Link dentro parentesi — è un qualificatore
                # Filtra anche qualificatori noti
                if text in _QUALIFIERS:
                    continue
                synonyms.add(text)

    # Metodo 1: <section aria-labelledby="Sinonimi"> (struttura moderna)
    for section in soup.find_all("section", attrs={"aria-labelledby": "Sinonimi"}):
        for ul in section.find_all("ul"):
            _extract_from_element(ul)

    # Metodo 2: <h3 id="Sinonimi"> o <h3> con testo "Sinonimi" (struttura legacy)
    if not synonyms:
        for heading in soup.find_all(["h3", "h4", "h5"]):
            heading_id = heading.get("id", "")
            heading_text = heading.get_text(strip=True).lower()
            if heading_id == "Sinonimi" or "sinonimi" in heading_text:
                sibling = heading.find_next_sibling()
                while sibling:
                    if sibling.name in ("h2", "h3", "h4", "h5", "section"):
                        break
                    if sibling.name in ("ul", "ol"):
                        _extract_from_element(sibling)
                        break
                    sibling = sibling.find_next_sibling()

    synonyms.discard(word.lower())
    return list(synonyms)


# ─── Fetch asincrono ─────────────────────────────────────────────────────────


def fetch_one_sync(word, delay=0.5):
    """Scarica i sinonimi di una singola parola (sincrono, più affidabile)."""
    import urllib.request
    import urllib.parse
    import urllib.error
    url = BASE_URL.format(word=urllib.parse.quote(word, safe=''))
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "it-IT,it;q=0.9",
        },
    )

    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
                html = resp.read().decode("utf-8")
            synonyms = parse_wiktionary_synonyms(html, word)
            if delay > 0:
                time.sleep(delay)
            return word, synonyms

        except urllib.error.HTTPError as e:
            if e.code == 404:
                return word, None
            if e.code == 429:
                wait = 2 ** (attempt + 2)
                log.warning(f"429 su '{word}', attendo {wait}s...")
                time.sleep(wait)
                continue
            log.warning(f"HTTP {e.code} per '{word}'")
            return word, []

        except Exception as e:
            wait = 2 ** (attempt + 1)
            log.warning(f"Errore per '{word}': {e}, tentativo {attempt+1}/{MAX_RETRIES}")
            time.sleep(wait)

    return word, []


# ─── Database ────────────────────────────────────────────────────────────────


def build_lemma_lookup(conn):
    cur = conn.cursor()
    cur.execute("SELECT id, lemma, pos FROM lemmas")
    lookup = defaultdict(list)
    for lid, lemma, pos in cur.fetchall():
        lookup[lemma.lower()].append((lid, pos))
    return lookup


def guess_pos(word):
    """Indovina il POS di una parola italiana dalla terminazione.
    Euristica semplice: -are/-ere/-ire → VER, -mente → ADV, -oso/-abile/-ibile → ADJ, resto → NOUN."""
    w = word.lower()
    if w.endswith(("are", "ere", "ire", "arre", "orre", "urre")):
        return "VER"
    if w.endswith("mente"):
        return "ADV"
    if w.endswith(("oso", "osa", "osi", "ose", "abile", "ibile", "ale", "ali",
                   "ivo", "iva", "ivi", "ive", "esco", "esca", "eschi", "esche")):
        return "ADJ"
    return "NOUN"


def guess_gender_number(word, pos):
    """Indovina genere e numero dalla terminazione. Ritorna (gender, number)."""
    w = word.lower()
    if pos == "VER" or pos == "ADV":
        return None, None
    if pos == "ADJ":
        if w.endswith("o"):
            return "m", "s"
        if w.endswith("a"):
            return "f", "s"
        if w.endswith("i"):
            return "m", "p"
        if w.endswith("e"):
            return None, "s"  # genere ambiguo
        return None, None
    # NOUN
    if w.endswith("o"):
        return "m", "s"
    if w.endswith("a"):
        return "f", "s"
    if w.endswith("i"):
        return "m", "p"
    if w.endswith("e"):
        return None, "s"
    return None, None


def ensure_lemma_exists(conn, word, lemma_lookup, source_pos=None):
    """Se il lemma non esiste nel DB, lo crea con forma base.
    Ritorna la lista di (lemma_id, pos) entries (esistenti o appena create).
    Aggiorna lemma_lookup in-place."""
    word_lower = word.lower()
    existing = lemma_lookup.get(word_lower)
    if existing:
        return existing

    pos = source_pos or guess_pos(word)
    gender, number = guess_gender_number(word, pos)

    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO lemmas (lemma, pos) VALUES (?, ?)", (word_lower, pos))
        lid = cur.lastrowid

        # Crea forma base
        if pos == "VER":
            cur.execute(
                "INSERT INTO forms (lemma_id, form, pos_full, mood, tense) VALUES (?, ?, ?, 'inf', 'pres')",
                (lid, word_lower, f"VER:inf+pres")
            )
        elif pos == "ADV":
            cur.execute(
                "INSERT INTO forms (lemma_id, form, pos_full) VALUES (?, ?, ?)",
                (lid, word_lower, "ADV")
            )
        else:
            pos_full = f"{pos}-{gender or '?'}:{number or '?'}"
            cur.execute(
                "INSERT INTO forms (lemma_id, form, pos_full, gender, number) VALUES (?, ?, ?, ?, ?)",
                (lid, word_lower, pos_full, gender, number)
            )

        # Aggiorna lookup in-place
        lemma_lookup[word_lower].append((lid, pos))
        return [(lid, pos)]

    except Exception:
        return []


def normalize_reflexive(word):
    """Normalizza verbi riflessivi: 'addolorarsi' → 'addolorare', 'perdersi' → 'perdere'.
    Ritorna la forma base se è un riflessivo, altrimenti la parola originale."""
    w = word.lower()
    # -arsi → -are, -ersi → -ere, -irsi → -ire
    for refl, base in [("arsi", "are"), ("ersi", "ere"), ("irsi", "ire"),
                        ("arci", "are"), ("arvi", "are"), ("armi", "are"), ("arti", "are"),
                        ("erci", "ere"), ("ervi", "ere"), ("ermi", "ere"), ("erti", "ere"),
                        ("irci", "ire"), ("irvi", "ire"), ("irmi", "ire"), ("irti", "ire")]:
        if w.endswith(refl) and len(w) > len(refl) + 1:
            return w[:-len(refl)] + base
    return w


def get_lemmas_without_synonyms(conn):
    """Ritorna lemmi NOUN/VER/ADJ/ADV senza sinonimi."""
    cur = conn.cursor()
    cur.execute("""
        SELECT l.lemma FROM lemmas l
        WHERE l.pos IN ('NOUN','VER','ADJ','ADV')
        AND l.id NOT IN (SELECT lemma_id_1 FROM synonyms)
        AND l.id NOT IN (SELECT lemma_id_2 FROM synonyms)
        ORDER BY l.lemma
    """)
    return [r[0] for r in cur.fetchall()]


def get_all_lemmas(conn):
    """Ritorna TUTTI i lemmi NOUN/VER/ADJ/ADV (anche quelli che hanno già sinonimi)."""
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT l.lemma FROM lemmas l
        WHERE l.pos IN ('NOUN','VER','ADJ','ADV')
        ORDER BY l.lemma
    """)
    return [r[0] for r in cur.fetchall()]


def insert_synonym(conn, id1, id2, source="wiktionary"):
    lid1, lid2 = min(id1, id2), max(id1, id2)
    if lid1 == lid2:
        return False
    try:
        conn.execute(
            "INSERT OR IGNORE INTO synonyms (lemma_id_1, lemma_id_2, type, source) VALUES (?, ?, 'synonym', ?)",
            (lid1, lid2, source),
        )
        return True
    except sqlite3.IntegrityError:
        return False


def process_results(results, conn, lemma_lookup, stats):
    found_words = []
    not_found_words = []

    for word, synonyms in results:
        stats["total"] += 1

        if synonyms is None or not synonyms:
            stats["not_found"] += 1
            not_found_words.append(word)
            continue

        stats["found"] += 1
        found_words.append(word)

        source_entries = lemma_lookup.get(word.lower(), [])

        for syn_word in synonyms:
            syn_lower = syn_word.lower()
            target_entries = lemma_lookup.get(syn_lower, [])
            if not target_entries:
                # Prova normalizzazione riflessivo: addolorarsi → addolorare
                normalized = normalize_reflexive(syn_lower)
                if normalized != syn_lower:
                    target_entries = lemma_lookup.get(normalized, [])
            if not target_entries:
                # Lemma mancante: crealo con forma base (usa la forma normalizzata)
                word_to_create = normalize_reflexive(syn_lower)
                target_entries = ensure_lemma_exists(conn, word_to_create, lemma_lookup)
                if not target_entries:
                    continue
                stats["lemmas_created"] = stats.get("lemmas_created", 0) + 1

            matched = False
            for src_id, src_pos in source_entries:
                for tgt_id, tgt_pos in target_entries:
                    if src_pos == tgt_pos:
                        if insert_synonym(conn, src_id, tgt_id):
                            stats["synonyms_added"] += 1
                        matched = True

            if not matched and source_entries and target_entries:
                if insert_synonym(conn, source_entries[0][0], target_entries[0][0]):
                    stats["synonyms_added"] += 1

    return found_words, not_found_words


# ─── State management ────────────────────────────────────────────────────────


def load_state():
    if os.path.exists(STATE_PATH):
        with open(STATE_PATH, "r") as f:
            return json.load(f)
    return {"processed": [], "not_found": []}


def save_state(state):
    with open(STATE_PATH, "w") as f:
        json.dump(state, f)


# ─── Main ────────────────────────────────────────────────────────────────────


def run_scraper(words, conn, lemma_lookup, workers=3, delay=0.5, state=None,
                dry_run=False):
    from concurrent.futures import ThreadPoolExecutor

    if state is None:
        state = load_state()

    already = set(state.get("processed", []))
    words = [w for w in words if w not in already]
    total = len(words)
    log.info(f"Parole da processare: {total} (già fatte: {len(already)})")

    if total == 0:
        log.info("Nessuna parola da processare.")
        return

    stats = {"total": 0, "found": 0, "not_found": 0, "synonyms_added": 0}
    start_time = time.time()

    report_file = None
    if dry_run:
        report_path = os.path.join(SCRIPT_DIR, "wiktionary_dry_run.txt")
        report_file = open(report_path, "w", encoding="utf-8")
        log.info(f"DRY RUN — risultati salvati in {report_path}")

    batch_size = 20
    for i in range(0, total, batch_size):
        batch = words[i : i + batch_size]

        with ThreadPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(
                lambda w: fetch_one_sync(w, delay), batch
            ))

        if dry_run:
            for word, synonyms in results:
                stats["total"] += 1
                if synonyms:
                    stats["found"] += 1
                    in_db = [s for s in synonyms if s.lower() in lemma_lookup]
                    stats["synonyms_added"] += len(in_db)
                    report_file.write(f"{word} -> {', '.join(in_db)}")
                    if len(synonyms) > len(in_db):
                        not_in_db = [s for s in synonyms if s.lower() not in lemma_lookup]
                        report_file.write(f"  (non in DB: {', '.join(not_in_db)})")
                    report_file.write("\n")
                else:
                    stats["not_found"] += 1
        else:
            found, not_found = process_results(results, conn, lemma_lookup, stats)
            conn.commit()

            state["processed"].extend(batch)
            state["not_found"].extend(not_found)

            if (i + batch_size) % CHECKPOINT_EVERY == 0:
                save_state(state)

        elapsed = time.time() - start_time
        done = i + len(batch)
        rate = done / elapsed if elapsed > 0 else 0
        eta = (total - done) / rate if rate > 0 else 0
        log.info(
            f"[{done}/{total}] Trovati: {stats['found']}, "
            f"Non trovati: {stats['not_found']}, "
            f"Sinonimi utilizzabili: {stats['synonyms_added']}, "
            f"ETA: {timedelta(seconds=int(eta))}"
        )

    if dry_run:
        report_file.close()
        log.info(f"DRY RUN completato. Report: {report_path}")
    else:
        save_state(state)

    log.info(
        f"Completato. Totale: {stats['total']}, Trovati: {stats['found']}, "
        f"Sinonimi utilizzabili: {stats['synonyms_added']}"
    )


def test_word(word):
    """Testa il parsing per una singola parola."""
    import urllib.request

    url = BASE_URL.format(word=word)
    print(f"Scaricando {url}...")
    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8")
        synonyms = parse_wiktionary_synonyms(html, word)
        print(f"Sinonimi trovati per '{word}': {synonyms}")
    except Exception as e:
        print(f"Errore: {e}")


def show_stats():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM synonyms WHERE source = 'wiktionary'")
    wikt = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM synonyms")
    total = c.fetchone()[0]
    c.execute("""
        SELECT COUNT(*) FROM lemmas l
        WHERE l.pos IN ('NOUN','VER','ADJ','ADV')
        AND l.id NOT IN (SELECT lemma_id_1 FROM synonyms)
        AND l.id NOT IN (SELECT lemma_id_2 FROM synonyms)
    """)
    no_syn = c.fetchone()[0]
    print(f"Sinonimi totali: {total:,}")
    print(f"  da Wiktionary: {wikt:,}")
    print(f"Lemmi ancora senza sinonimi: {no_syn:,}")
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Wiktionary synonym scraper")
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--delay", type=float, default=0.5)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--dry-run", action="store_true",
                        help="Scarica e mostra risultati senza modificare il DB")
    parser.add_argument("--test", type=str)
    parser.add_argument("--stats", action="store_true")
    args = parser.parse_args()

    if args.test:
        test_word(args.test)
        return

    if args.stats:
        show_stats()
        return

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    lemma_lookup = build_lemma_lookup(conn)

    words = get_lemmas_without_synonyms(conn)
    log.info(f"Lemmi senza sinonimi: {len(words)}")

    if args.limit:
        words = words[: args.limit]

    state = load_state() if args.resume else {"processed": [], "not_found": []}

    run_scraper(
        words, conn, lemma_lookup, workers=args.workers, delay=args.delay,
        state=state, dry_run=args.dry_run
    )
    conn.close()


if __name__ == "__main__":
    main()
