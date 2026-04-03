#!/usr/bin/env python3
"""
Scraper sinonimi dal dizionario Treccani sinonimi (treccani.it/vocabolario/).
Complementare al Wiktionary scraper — utile per parole non coperte.

Requisiti:
    pip install aiohttp beautifulsoup4

Uso:
    python scrape_treccani.py                    # scrapa lemmi senza sinonimi
    python scrape_treccani.py --limit 100        # solo i primi 100
    python scrape_treccani.py --test casa        # testa una singola parola
    python scrape_treccani.py --resume           # riprende da dove si era fermato
    python scrape_treccani.py --stats            # mostra statistiche
    python scrape_treccani.py --workers 2        # 2 worker paralleli (default: 2, Treccani è più restrittivo)

ATTENZIONE: Treccani ha rate limiting più aggressivo. Usare pochi worker e delay alto.
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
STATE_PATH = os.path.join(SCRIPT_DIR, "treccani_state.json")
LOG_PATH = os.path.join(SCRIPT_DIR, "treccani_scrape.log")

BASE_URL = "https://www.treccani.it/vocabolario/{word}_(Sinonimi-e-Contrari)/"
TIMEOUT = 20
MAX_RETRIES = 3
CHECKPOINT_EVERY = 50

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


def parse_treccani_synonyms(html, word):
    """Estrai sinonimi dalla pagina Treccani Sinonimi e Contrari.

    Struttura tipica: il contenuto è dentro un div con le sezioni
    di sinonimi, spesso marcate con ≈ (sinonimi) e ↔ (contrari).
    I sinonimi sono parole in corsivo o link.
    """
    soup = BeautifulSoup(html, "html.parser")
    synonyms = set()

    # Il contenuto può essere in vari div a seconda della versione della pagina
    body = (
        soup.find("div", class_="text")
        or soup.find("div", class_="singletext")
        or soup.find("div", class_="module-article__text")
        or soup.find("body")
    )
    if not body:
        return []

    text = body.get_text()

    # I sinonimi Treccani sono spesso in formato:
    # "≈ sinonimo1, sinonimo2, sinonimo3. ↔ contrario1, contrario2"
    # Prendiamo tutto tra ≈ e ↔ (o fine paragrafo)
    import re

    # Pattern: ≈ seguito da lista di parole fino a ↔, punto, o fine
    syn_sections = re.findall(r"≈\s*([^↔\n]+)", text)

    for section in syn_sections:
        # Pulisci e splitta
        for part in re.split(r"[,;.]", section):
            word_clean = part.strip().lower()
            # Rimuovi qualificatori tra parentesi
            word_clean = re.sub(r"\([^)]*\)", "", word_clean).strip()
            # Rimuovi asterischi e altri simboli
            word_clean = re.sub(r"[*†‡→⇑⇓▲▼]", "", word_clean).strip()
            # Solo parole singole senza spazi
            if word_clean and len(word_clean) > 1 and " " not in word_clean:
                # Verifica che sia una parola ragionevole
                if re.match(r"^[a-zA-ZàèéìòùÀÈÉÌÒÙäëïöüâêîôûçñ]+$", word_clean):
                    synonyms.add(word_clean)

    # Fallback: cerca anche link a vocabolario
    for a in body.find_all("a", href=True):
        href = a.get("href", "")
        if "/vocabolario/" in href and "Sinonimi" not in href:
            text = a.get_text(strip=True).lower().strip(".,;:!?()[]\"' ")
            if text and len(text) > 1 and " " not in text:
                if re.match(r"^[a-zA-ZàèéìòùÀÈÉÌÒÙäëïöüâêîôûçñ]+$", text):
                    synonyms.add(text)

    # Rimuovi abbreviazioni e parole funzionali comuni nel testo Treccani
    noise = {
        "fig", "lett", "fam", "pop", "volg", "disus", "burocr", "estens",
        "spec", "iperb", "anche", "locuz", "prep", "avv", "agg", "sost",
        "propr", "essere", "stare", "avere", "fare", "dire", "dare",
        "propria", "editrice", "vocabolario", "sinonimi", "contrari",
        "recipr", "intr", "tr", "pron", "iron", "scherz", "region",
    }
    synonyms -= noise
    synonyms.discard(word.lower())
    return list(synonyms)


# ─── Fetch asincrono ─────────────────────────────────────────────────────────


def fetch_one_sync(word, delay=1.0):
    """Scarica i sinonimi di una singola parola (sincrono)."""
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
            synonyms = parse_treccani_synonyms(html, word)
            if delay > 0:
                time.sleep(delay)
            return word, synonyms

        except urllib.error.HTTPError as e:
            if e.code == 404:
                return word, None
            if e.code == 429:
                wait = 5 * (attempt + 1)
                log.warning(f"429 su '{word}', attendo {wait}s...")
                time.sleep(wait)
                continue
            log.warning(f"HTTP {e.code} per '{word}'")
            return word, []

        except Exception as e:
            wait = 3 * (attempt + 1)
            log.warning(f"Errore per '{word}': {e}, tentativo {attempt+1}/{MAX_RETRIES}")
            time.sleep(wait)

    return word, []


# ─── Database (condiviso con scrape_wiktionary.py) ──────────────────────────

from scrape_wiktionary import (
    build_lemma_lookup,
    get_lemmas_without_synonyms,
    ensure_lemma_exists,
    normalize_reflexive,
)


def insert_synonym(conn, id1, id2, source="treccani"):
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
                normalized = normalize_reflexive(syn_lower)
                if normalized != syn_lower:
                    target_entries = lemma_lookup.get(normalized, [])
            if not target_entries:
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


def run_scraper(words, conn, lemma_lookup, workers=2, delay=1.0, state=None,
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
        report_path = os.path.join(SCRIPT_DIR, "treccani_dry_run.txt")
        report_file = open(report_path, "w", encoding="utf-8")
        log.info(f"DRY RUN — risultati salvati in {report_path}")

    batch_size = 10
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
    import urllib.request

    url = BASE_URL.format(word=word)
    print(f"Scaricando {url}...")
    req = urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8")
        synonyms = parse_treccani_synonyms(html, word)
        print(f"Sinonimi trovati per '{word}': {synonyms}")
    except Exception as e:
        print(f"Errore: {e}")


def show_stats():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM synonyms WHERE source = 'treccani'")
    trec = c.fetchone()[0]
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
    print(f"  da Treccani: {trec:,}")
    print(f"Lemmi ancora senza sinonimi: {no_syn:,}")
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Treccani synonym scraper")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--delay", type=float, default=1.0)
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
