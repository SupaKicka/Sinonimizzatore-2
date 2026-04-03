#!/usr/bin/env python3
"""
Parser di sinonimi dal dump Wiktionary italiano.

Parsa il dump XML già scaricato ed estrae i sinonimi per tutti i lemmi nel DB.
Molto più veloce dello scraping HTTP — tutto locale, ~30 secondi.

Uso:
    python dump_synonyms.py                    # processa tutto
    python dump_synonyms.py --dry-run          # mostra senza scrivere
    python dump_synonyms.py --test casa        # mostra sinonimi di una parola
    python dump_synonyms.py --only-missing     # solo lemmi senza sinonimi

Requisiti: nessuno (solo stdlib). Il dump deve essere già scaricato
           (esegui prima: python generate_inflections.py --dump-only)
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

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "morphit.db")
DUMP_PATH = os.path.join(SCRIPT_DIR, "itwiktionary-dump.xml.bz2")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(os.path.join(SCRIPT_DIR, "dump_synonyms.log"), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(__name__)


# ─── Parser sinonimi dal wikitext ────────────────────────────────────────────

def parse_synonyms_from_wikitext(wikitext, title):
    """Estrae sinonimi dal wikitext di una pagina Wiktionary.

    I sinonimi sono nella sezione che inizia con {{-sin-}} o ===Sinonimi===
    e contiene link [[parola]] o *[[parola]].
    """
    synonyms = set()
    in_syn_section = False

    for line in wikitext.split("\n"):
        line_stripped = line.strip()

        # Inizio sezione sinonimi
        if "{{-sin-" in line_stripped or "=== Sinonimi ===" in line_stripped or "===Sinonimi===" in line_stripped:
            in_syn_section = True
            continue

        # Fine sezione (nuova sezione)
        if in_syn_section and (line_stripped.startswith("{{-") or line_stripped.startswith("===")):
            if "-sin-" not in line_stripped and "Sinonimi" not in line_stripped:
                in_syn_section = False
                continue

        if not in_syn_section:
            continue

        # Estrai link [[parola]] dalla riga
        for match in re.finditer(r'\[\[([^\]|#]+?)(?:\|[^\]]*?)?\]\]', line_stripped):
            word = match.group(1).strip().lower()
            # Filtra: solo parole singole, no categorie, no link interni
            if (word and len(word) > 1 and " " not in word
                    and ":" not in word and word != title.lower()
                    and word.isalpha()):
                synonyms.add(word)

    return list(synonyms)


# ─── Iteratore dump ──────────────────────────────────────────────────────────

def iter_pages(dump_path):
    """Itera le pagine del dump, yielding (title, wikitext)."""
    log.info(f"Apertura dump: {dump_path}")

    if dump_path.endswith(".bz2"):
        fileobj = bz2.open(dump_path, "rt", encoding="utf-8")
    else:
        fileobj = open(dump_path, "r", encoding="utf-8")

    ns = ""
    in_page = False
    title = None
    page_count = 0

    for event, elem in ET.iterparse(fileobj, events=("start", "end")):
        if event == "start" and not ns and "}" in elem.tag:
            ns = elem.tag.split("}")[0] + "}"

        tag = elem.tag.replace(ns, "")

        if event == "start" and tag == "page":
            in_page = True
            title = None

        elif event == "end" and tag == "title" and in_page:
            title = elem.text or ""

        elif event == "end" and tag == "text" and in_page:
            wikitext = elem.text or ""
            # Solo pagine principali (no Appendice:, Categoria:, ecc.)
            if title and wikitext and ":" not in title:
                page_count += 1
                yield title, wikitext
            elem.clear()

        elif event == "end" and tag == "page":
            in_page = False
            elem.clear()

    fileobj.close()
    log.info(f"Pagine lette: {page_count:,}")


# ─── Database ────────────────────────────────────────────────────────────────

def build_lemma_lookup(conn):
    cur = conn.cursor()
    cur.execute("SELECT id, lemma, pos FROM lemmas")
    lookup = defaultdict(list)
    for lid, lemma, pos in cur.fetchall():
        lookup[lemma.lower()].append((lid, pos))
    return lookup


def get_all_lemmas_set(conn):
    """Tutti i lemmi NOUN/VER/ADJ/ADV come set lowercase."""
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT LOWER(lemma) FROM lemmas WHERE pos IN ('NOUN','VER','ADJ','ADV')")
    return {r[0] for r in cur.fetchall()}


def get_lemmas_without_synonyms_set(conn):
    """Lemmi senza sinonimi come set lowercase."""
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT LOWER(l.lemma) FROM lemmas l
        WHERE l.pos IN ('NOUN','VER','ADJ','ADV')
        AND l.id NOT IN (SELECT lemma_id_1 FROM synonyms)
        AND l.id NOT IN (SELECT lemma_id_2 FROM synonyms)
    """)
    return {r[0] for r in cur.fetchall()}


def normalize_reflexive(word):
    """addolorarsi → addolorare"""
    w = word.lower()
    for refl, base in [("arsi", "are"), ("ersi", "ere"), ("irsi", "ire")]:
        if w.endswith(refl) and len(w) > len(refl) + 1:
            return w[:-len(refl)] + base
    return w


def ensure_lemma_exists(conn, word, lemma_lookup):
    """Crea lemma se non esiste. Ritorna [(lemma_id, pos)] o []."""
    word_lower = word.lower()
    existing = lemma_lookup.get(word_lower)
    if existing:
        return existing

    # Indovina POS
    w = word_lower
    if w.endswith(("are", "ere", "ire")):
        pos = "VER"
    elif w.endswith("mente"):
        pos = "ADV"
    elif w.endswith(("oso", "osa", "abile", "ibile", "ale", "ivo", "iva", "esco", "esca")):
        pos = "ADJ"
    else:
        pos = "NOUN"

    # Indovina genere/numero
    gender, number = None, None
    if pos in ("NOUN", "ADJ"):
        if w.endswith("o"): gender, number = "m", "s"
        elif w.endswith("a"): gender, number = "f", "s"
        elif w.endswith("e"): gender, number = None, "s"

    cur = conn.cursor()
    try:
        cur.execute("INSERT INTO lemmas (lemma, pos) VALUES (?, ?)", (word_lower, pos))
        lid = cur.lastrowid

        if pos == "VER":
            cur.execute(
                "INSERT INTO forms (lemma_id, form, pos_full, mood, tense) VALUES (?, ?, 'VER:inf+pres', 'inf', 'pres')",
                (lid, word_lower))
        elif pos == "ADV":
            cur.execute(
                "INSERT INTO forms (lemma_id, form, pos_full) VALUES (?, ?, 'ADV')",
                (lid, word_lower))
        else:
            cur.execute(
                "INSERT INTO forms (lemma_id, form, pos_full, gender, number) VALUES (?, ?, ?, ?, ?)",
                (lid, word_lower, f"{pos}-{gender or '?'}:{number or '?'}", gender, number))

        lemma_lookup[word_lower].append((lid, pos))
        return [(lid, pos)]
    except Exception:
        return []


def insert_synonym(conn, id1, id2, source="wiktionary-dump"):
    lid1, lid2 = min(id1, id2), max(id1, id2)
    if lid1 == lid2:
        return False
    try:
        conn.execute(
            "INSERT OR IGNORE INTO synonyms (lemma_id_1, lemma_id_2, type, source) VALUES (?, ?, 'synonym', ?)",
            (lid1, lid2, source))
        return True
    except Exception:
        return False


# ─── Main ────────────────────────────────────────────────────────────────────

def test_word(word, dump_path):
    """Mostra i sinonimi di una parola dal dump."""
    for title, wikitext in iter_pages(dump_path):
        if title.lower() == word.lower():
            syns = parse_synonyms_from_wikitext(wikitext, title)
            print(f"Sinonimi di '{word}': {len(syns)} trovati")
            for s in sorted(syns):
                print(f"  {s}")
            return
    print(f"'{word}' non trovato nel dump.")


def main():
    parser = argparse.ArgumentParser(description="Parser sinonimi da dump Wiktionary")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--test", type=str)
    parser.add_argument("--only-missing", action="store_true",
                        help="Solo lemmi che non hanno ancora sinonimi")
    args = parser.parse_args()

    if not os.path.exists(DUMP_PATH):
        log.error(f"Dump non trovato: {DUMP_PATH}")
        log.error("Scaricalo con: python generate_inflections.py --dump-only")
        return

    if args.test:
        test_word(args.test, DUMP_PATH)
        return

    # Connessione DB
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    lemma_lookup = build_lemma_lookup(conn)

    all_lemmas = get_all_lemmas_set(conn)
    if args.only_missing:
        target_set = get_lemmas_without_synonyms_set(conn)
        log.info(f"Target: {len(target_set)} lemmi senza sinonimi")
    else:
        target_set = all_lemmas
        log.info(f"Target: {len(target_set)} lemmi totali")

    # Parsa dump
    stats = {"pages": 0, "with_syns": 0, "new_syns": 0, "lemmas_created": 0}
    t0 = time.time()

    for title, wikitext in iter_pages(DUMP_PATH):
        title_lower = title.lower().strip()

        if title_lower not in target_set:
            continue

        # Estrai sinonimi
        synonyms = parse_synonyms_from_wikitext(wikitext, title)
        if not synonyms:
            continue

        stats["pages"] += 1
        source_entries = lemma_lookup.get(title_lower, [])
        if not source_entries:
            continue

        found_any = False
        for syn_word in synonyms:
            # Normalizza riflessivi
            syn_lower = syn_word.lower()
            target_entries = lemma_lookup.get(syn_lower, [])
            if not target_entries:
                normalized = normalize_reflexive(syn_lower)
                if normalized != syn_lower:
                    target_entries = lemma_lookup.get(normalized, [])
            if not target_entries:
                # Crea lemma mancante
                word_to_create = normalize_reflexive(syn_lower)
                if not args.dry_run:
                    target_entries = ensure_lemma_exists(conn, word_to_create, lemma_lookup)
                    if target_entries:
                        stats["lemmas_created"] += 1
                if not target_entries:
                    continue

            # Inserisci sinonimi (match per POS)
            matched = False
            for src_id, src_pos in source_entries:
                for tgt_id, tgt_pos in target_entries:
                    if src_pos == tgt_pos:
                        if not args.dry_run:
                            if insert_synonym(conn, src_id, tgt_id):
                                stats["new_syns"] += 1
                                found_any = True
                        else:
                            stats["new_syns"] += 1
                            found_any = True
                        matched = True

            if not matched and source_entries and target_entries:
                if not args.dry_run:
                    if insert_synonym(conn, source_entries[0][0], target_entries[0][0]):
                        stats["new_syns"] += 1
                        found_any = True
                else:
                    stats["new_syns"] += 1
                    found_any = True

        if found_any:
            stats["with_syns"] += 1

        # Commit periodico
        if not args.dry_run and stats["pages"] % 1000 == 0:
            conn.commit()

    if not args.dry_run:
        conn.commit()

    elapsed = time.time() - t0
    log.info(f"Completato in {elapsed:.1f}s")
    log.info(f"  Pagine con sinonimi: {stats['with_syns']:,}")
    log.info(f"  Nuovi sinonimi {'(stimati)' if args.dry_run else 'aggiunti'}: {stats['new_syns']:,}")
    log.info(f"  Lemmi creati: {stats['lemmas_created']:,}")

    if not args.dry_run:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM synonyms")
        total = cur.fetchone()[0]
        cur.execute("SELECT source, COUNT(*) FROM synonyms GROUP BY source")
        by_source = cur.fetchall()
        log.info(f"  Sinonimi totali: {total:,}")
        for src, cnt in by_source:
            log.info(f"    {src}: {cnt:,}")

    conn.close()


if __name__ == "__main__":
    main()
