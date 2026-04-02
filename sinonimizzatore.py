#!/usr/bin/env python3
"""
Sinonimizzatore 2.0 — Applicazione web per riscrivere frasi italiane con sinonimi.

Utilizza il database morphit.db per:
  - Riconoscere le forme flesse delle parole
  - Trovare sinonimi appropriati
  - Generare la forma flessa corretta del sinonimo
  - Mantenere la coerenza grammaticale (genere, numero, tempo, modo, persona, grado)

Uso:
    python sinonimizzatore.py                # avvia su http://localhost:8080
    python sinonimizzatore.py --port 9000    # porta custom

Requisiti: nessuna dipendenza esterna (solo standard library + sqlite3).
"""

import sqlite3
import http.server
import json
import os
import sys
import re
import random
import argparse
from collections import defaultdict
from urllib.parse import urlparse, parse_qs

# ─── Configurazione ──────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "morphit.db")

# POS da NON sostituire (parole funzionali)
SKIP_POS = frozenset({
    "ART", "ARTPRE", "PRE", "CON", "PRO-PERS", "PRO-DEMO", "PRO-POSS",
    "PRO-WH", "DET-DEMO", "DET-POSS", "DET-WH", "DET-NUM-CARD", "DET-INDEF",
    "PRO-NUM", "PRO-INDEF", "PON", "SENT", "SYM", "SMI", "NPR",
    "AUX", "MOD", "ASP", "CAU", "CE", "CI", "NE", "SI",
    "WH", "WH-CHE", "TALE", "ABL"
})

# POS sostituibili
REPLACE_POS = frozenset({"NOUN", "VER", "ADJ", "ADV"})


# ─── Motore Sinonimizzatore ──────────────────────────────────────────────────

class SinonimizzatoreEngine:

    def __init__(self, db_path):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._build_indexes()

    def _build_indexes(self):
        """Costruisce indici in memoria per lookup veloce."""
        print("  Caricamento forme flesse...", end=" ", flush=True)
        cur = self.conn.cursor()

        # form_index: parola_minuscola -> [(lemma_id, lemma, pos, gender, number, person, mood, tense, degree, is_clitic)]
        self.form_index = defaultdict(list)
        cur.execute("""
            SELECT f.form, f.lemma_id, l.lemma, l.pos,
                   f.gender, f.number, f.person, f.mood, f.tense, f.degree, f.is_clitic
            FROM forms f
            JOIN lemmas l ON f.lemma_id = l.id
            WHERE l.pos IN ('NOUN','VER','ADJ','ADV')
              AND (f.is_clitic IS NULL OR f.is_clitic = 0)
        """)
        for row in cur.fetchall():
            form_lower = row[0].lower()
            entry = {
                "lemma_id": row[1], "lemma": row[2], "pos": row[3],
                "gender": row[4], "number": row[5], "person": row[6],
                "mood": row[7], "tense": row[8], "degree": row[9],
            }
            self.form_index[form_lower].append(entry)
        print(f"{len(self.form_index):,} forme indicizzate.")

        # synonym_index: lemma_id -> [(synonym_lemma_id, synonym_lemma, synonym_pos)]
        print("  Caricamento sinonimi...", end=" ", flush=True)
        self.synonym_index = defaultdict(list)
        cur.execute("""
            SELECT s.lemma_id_1, s.lemma_id_2, l1.lemma, l1.pos, l2.lemma, l2.pos
            FROM synonyms s
            JOIN lemmas l1 ON s.lemma_id_1 = l1.id
            JOIN lemmas l2 ON s.lemma_id_2 = l2.id
        """)
        for lid1, lid2, lem1, pos1, lem2, pos2 in cur.fetchall():
            self.synonym_index[lid1].append({"lemma_id": lid2, "lemma": lem2, "pos": pos2})
            self.synonym_index[lid2].append({"lemma_id": lid1, "lemma": lem1, "pos": pos1})
        print(f"{len(self.synonym_index):,} lemmi con sinonimi.")

        # lemma_forms: lemma_id -> [form_entry, ...]
        print("  Caricamento tavole di flessione...", end=" ", flush=True)
        self.lemma_forms = defaultdict(list)
        cur.execute("""
            SELECT f.lemma_id, f.form, f.gender, f.number, f.person,
                   f.mood, f.tense, f.degree, f.is_clitic, l.pos
            FROM forms f
            JOIN lemmas l ON f.lemma_id = l.id
            WHERE l.pos IN ('NOUN','VER','ADJ','ADV')
              AND (f.is_clitic IS NULL OR f.is_clitic = 0)
        """)
        for row in cur.fetchall():
            self.lemma_forms[row[0]].append({
                "form": row[1], "gender": row[2], "number": row[3],
                "person": row[4], "mood": row[5], "tense": row[6],
                "degree": row[7], "pos": row[9],
            })
        print(f"{len(self.lemma_forms):,} lemmi caricati.")

        # also_adj: set di lemma (lowercase) che esistono anche come ADJ.
        # Serve per filtrare sinonimi figurativi: "cane" non deve diventare "spietato",
        # perché "spietato" è primariamente un aggettivo usato come nome solo in senso figurato.
        print("  Costruzione indice ADJ/NOUN...", end=" ", flush=True)
        self.also_adj = set()
        cur.execute("SELECT LOWER(lemma) FROM lemmas WHERE pos = 'ADJ'")
        adj_lemmas = set(r[0] for r in cur.fetchall())
        cur.execute("SELECT id, LOWER(lemma) FROM lemmas WHERE pos = 'NOUN'")
        for lid, lemma_lower in cur.fetchall():
            if lemma_lower in adj_lemmas:
                self.also_adj.add(lid)
        print(f"{len(self.also_adj):,} nomi anche aggettivi.")
        print("  Pronto!\n")

    # ── Regole articoli italiani ────────────────────────────────────────────

    # Articoli riconosciuti: (tipo, genere, numero)
    # genere None = ambiguo (l' può essere m o f)
    ARTICLE_MAP = {
        "il": ("def", "m", "s"), "lo": ("def", "m", "s"),
        "la": ("def", "f", "s"), "l'": ("def", None, "s"),
        "i": ("def", "m", "p"), "gli": ("def", "m", "p"),
        "le": ("def", "f", "p"),
        "un": ("indef", "m", "s"), "uno": ("indef", "m", "s"),
        "una": ("indef", "f", "s"), "un'": ("indef", "f", "s"),
    }

    # Preposizioni articolate: mappa a (preposizione, tipo_articolo, genere, numero)
    ARTPRE_MAP = {}
    for _prep, _base in [("di", "de"), ("a", "a"), ("da", "da"), ("in", "ne"), ("su", "su")]:
        ARTPRE_MAP[_base + "l"] = (_prep, "def", "m", "s")       # del, al, dal, nel, sul
        ARTPRE_MAP[_base + "llo"] = (_prep, "def", "m", "s")     # dello, allo, ...
        ARTPRE_MAP[_base + "lla"] = (_prep, "def", "f", "s")     # della, alla, ...
        ARTPRE_MAP[_base + "ll'"] = (_prep, "def", None, "s")    # dell', all', ...
        ARTPRE_MAP[_base + "i"] = (_prep, "def", "m", "p")       # dei, ai, ...
        ARTPRE_MAP[_base + "gli"] = (_prep, "def", "m", "p")     # degli, agli, ...
        ARTPRE_MAP[_base + "lle"] = (_prep, "def", "f", "p")     # delle, alle, ...
    # "in" ha forme irregolari: nel/nello/nella/nell'/nei/negli/nelle
    # "con" + articolo (raro): col, collo, colla, coll', coi, cogli, colle
    for _form, _info in [("col", ("con", "def", "m", "s")), ("collo", ("con", "def", "m", "s")),
                         ("colla", ("con", "def", "f", "s")), ("coll'", ("con", "def", None, "s")),
                         ("coi", ("con", "def", "m", "p")), ("cogli", ("con", "def", "m", "p")),
                         ("colle", ("con", "def", "f", "p"))]:
        ARTPRE_MAP[_form] = _info

    @staticmethod
    def _needs_lo(word):
        """Verifica se una parola richiede l'articolo 'lo/gli/uno' (s+cons, z, gn, ps, pn, x, y)."""
        w = word.lower()
        if len(w) < 2:
            return False
        return (
            (w[0] == 's' and w[1] not in 'aeiouàèéìòù') or
            w[0] == 'z' or
            w[:2] in ('gn', 'ps', 'pn') or
            w[0] in ('x', 'y')
        )

    @staticmethod
    def _starts_with_vowel(word):
        return word and word[0].lower() in 'aeiouàèéìòùh'

    def _compute_article(self, word, gender, number, art_type):
        """Calcola la forma corretta dell'articolo per la parola seguente."""
        vowel = self._starts_with_vowel(word)
        lo = self._needs_lo(word)

        if art_type == "def":
            if number == "s":
                if gender == "m":
                    if vowel:
                        return "l'"
                    return "lo" if lo else "il"
                else:  # f
                    return "l'" if vowel else "la"
            else:  # p
                if gender == "m":
                    return "gli" if (vowel or lo) else "i"
                else:
                    return "le"
        else:  # indef
            if number == "s":
                if gender == "m":
                    return "uno" if lo else "un"
                else:
                    return "un'" if vowel else "una"
        return None

    def _compute_artpre(self, word, gender, number, preposition):
        """Calcola la preposizione articolata corretta per la parola seguente."""
        vowel = self._starts_with_vowel(word)
        lo = self._needs_lo(word)

        # Mappa preposizione -> base per la costruzione
        prep_base = {"di": "de", "a": "a", "da": "da", "in": "ne", "su": "su", "con": "co"}
        base = prep_base.get(preposition)
        if not base:
            return None

        if number == "s":
            if gender == "m":
                if vowel:
                    return base + "ll'"
                return base + "llo" if lo else base + "l"
            else:  # f
                if vowel:
                    return base + "ll'"
                return base + "lla"
        else:  # p
            if gender == "m":
                return base + "gli" if (vowel or lo) else base + "i"
            else:
                return base + "lle"

    def tokenize(self, text):
        """Tokenizza preservando spazi e punteggiatura. Separa apostrofi articolati."""
        # Ordine: parola+apostrofo (l', un', dell'), poi parola, poi singolo char, poi spazi
        tokens = re.findall(
            r"[a-zA-ZàèéìòùÀÈÉÌÒÙäëïöüâêîôûçñ]+'|[a-zA-ZàèéìòùÀÈÉÌÒÙäëïöüâêîôûçñ]+|[^\s]|\s+",
            text
        )
        return tokens

    # Articoli e determinanti che precedono un nome
    _DETERMINERS = frozenset({
        "il", "lo", "la", "i", "gli", "le", "l'",
        "un", "uno", "una", "un'",
        "del", "dello", "della", "dell'", "dei", "degli", "delle",
        "al", "allo", "alla", "all'", "ai", "agli", "alle",
        "dal", "dallo", "dalla", "dall'", "dai", "dagli", "dalle",
        "nel", "nello", "nella", "nell'", "nei", "negli", "nelle",
        "sul", "sullo", "sulla", "sull'", "sui", "sugli", "sulle",
        "col", "coi",
        "questo", "questa", "questi", "queste",
        "quel", "quello", "quella", "quell'", "quei", "quegli", "quelle",
        "mio", "mia", "miei", "mie", "tuo", "tua", "tuoi", "tue",
        "suo", "sua", "suoi", "sue", "nostro", "nostra", "nostri", "nostre",
        "vostro", "vostra", "vostri", "vostre", "loro",
        "ogni", "qualche", "alcuno", "alcuna", "alcuni", "alcune",
        "altro", "altra", "altri", "altre",
    })

    def _find_word_info(self, word, prev_word=None):
        """Trova info morfologiche per una parola. Ritorna lista di match."""
        matches = self.form_index.get(word.lower(), [])
        if not matches:
            return []

        has_noun = any(m["pos"] == "NOUN" for m in matches)
        has_conjugated_verb = any(m["pos"] == "VER" and m.get("person") and m.get("mood") == "ind" for m in matches)

        # Disambiguazione NOUN vs VER coniugato con contesto:
        # Se preceduta da articolo/determinante -> sicuramente NOUN
        #   Es: "un testo", "il canto", "del suono"
        # Altrimenti, VER coniugato è più probabile
        #   Es: "io suono", "scrivo un testo"
        if has_noun and has_conjugated_verb:
            if prev_word and prev_word.lower() in self._DETERMINERS:
                pos_priority = {"NOUN": 0, "VER": 1, "ADJ": 2, "ADV": 3}
            else:
                pos_priority = {"VER": 0, "NOUN": 1, "ADJ": 2, "ADV": 3}
        else:
            pos_priority = {"NOUN": 0, "VER": 1, "ADJ": 2, "ADV": 3}

        # Per i verbi: disambiguazione mood. In testo normale l'indicativo è
        # molto più frequente dell'imperativo o del congiuntivo.
        mood_priority = {"ind": 0, "sub": 1, "cond": 2, "inf": 3, "part": 4, "ger": 5, "impr": 6}

        def sort_key(m):
            p = pos_priority.get(m["pos"], 99)
            mood = mood_priority.get(m.get("mood") or "", 99)
            # Penalizza match senza genere/numero (meno informativi)
            has_info = 0 if (m.get("gender") or m.get("number") or m.get("person")) else 1
            return (p, mood, has_info)

        matches.sort(key=sort_key)
        return matches

    def _find_synonym_form(self, synonym_lemma_id, target_props, original_pos):
        """Trova la forma flessa del sinonimo che corrisponde alle proprietà target."""
        forms = self.lemma_forms.get(synonym_lemma_id, [])
        if not forms:
            return None

        if original_pos == "ADV":
            # Avverbi: prendono la forma base
            for f in forms:
                if f["pos"] == "ADV":
                    return f["form"]
            return None

        if original_pos == "NOUN":
            # Nomi: matcha genere + numero
            for f in forms:
                if (f["pos"] == "NOUN"
                        and f["gender"] == target_props.get("gender")
                        and f["number"] == target_props.get("number")):
                    return f["form"]
            # Fallback: solo numero (alcuni nomi cambiano genere)
            for f in forms:
                if f["pos"] == "NOUN" and f["number"] == target_props.get("number"):
                    return f["form"]
            return None

        if original_pos == "ADJ":
            # Aggettivi: matcha genere + numero + grado
            for f in forms:
                if (f["pos"] == "ADJ"
                        and f["gender"] == target_props.get("gender")
                        and f["number"] == target_props.get("number")
                        and f["degree"] == target_props.get("degree")):
                    return f["form"]
            # Fallback: senza grado
            for f in forms:
                if (f["pos"] == "ADJ"
                        and f["gender"] == target_props.get("gender")
                        and f["number"] == target_props.get("number")):
                    return f["form"]
            return None

        if original_pos == "VER":
            # Verbi: matcha mood + tense + person + number
            tp = target_props
            for f in forms:
                if (f["pos"] == "VER"
                        and f["mood"] == tp.get("mood")
                        and f["tense"] == tp.get("tense")
                        and f["person"] == tp.get("person")
                        and f["number"] == tp.get("number")):
                    return f["form"]
            # Fallback: mood + tense + person (senza numero per infinito, gerundio)
            for f in forms:
                if (f["pos"] == "VER"
                        and f["mood"] == tp.get("mood")
                        and f["tense"] == tp.get("tense")):
                    if tp.get("mood") in ("inf", "ger"):
                        return f["form"]
            return None

        return None

    def _get_synonyms_for_lemma(self, lemma_id, pos):
        """Ritorna sinonimi dello stesso POS."""
        syns = self.synonym_index.get(lemma_id, [])
        return [s for s in syns if s["pos"] == pos]

    def _apply_capitalization(self, original, replacement):
        """Applica lo stile di capitalizzazione dell'originale al replacement."""
        if not original or not replacement:
            return replacement
        if original[0].isupper():
            if original.isupper() and len(original) > 1:
                return replacement.upper()
            return replacement[0].upper() + replacement[1:]
        return replacement

    def _gender_of_noun_lemma(self, lemma_id):
        """Determina il genere prevalente di un lemma NOUN dalle sue forme."""
        forms = self.lemma_forms.get(lemma_id, [])
        genders = [f["gender"] for f in forms if f["pos"] == "NOUN" and f["gender"]]
        if not genders:
            return None
        # Ritorna il genere più frequente
        from collections import Counter
        return Counter(genders).most_common(1)[0][0]

    # POS che si flettono per genere/numero come gli aggettivi
    _GENDER_FLEX_POS = frozenset({"ADJ", "DET-POSS", "DET-DEMO", "DET-INDEF", "PRO-POSS", "PRO-DEMO"})

    def _adjust_adj_gender(self, word, new_gender, new_number):
        """Dato un aggettivo/possessivo/dimostrativo, trova la forma nel genere/numero richiesto."""
        word_lower = word.lower()

        # Cerca in tutte le forme del DB (non solo quelle indicizzate per NOUN/VER/ADJ/ADV)
        cur = self.conn.cursor()
        cur.execute("""
            SELECT f.lemma_id, l.pos, f.gender, f.number, f.degree
            FROM forms f JOIN lemmas l ON f.lemma_id = l.id
            WHERE LOWER(f.form) = ?
        """, (word_lower,))
        form_matches = cur.fetchall()

        for lemma_id, pos, g, n, degree in form_matches:
            if pos not in self._GENDER_FLEX_POS:
                continue

            # Cerca la forma con il nuovo genere/numero per questo lemma
            cur.execute("""
                SELECT f.form FROM forms f
                WHERE f.lemma_id = ? AND f.gender = ? AND f.number = ?
                ORDER BY f.form
                LIMIT 1
            """, (lemma_id, new_gender, new_number))
            row = cur.fetchone()
            if row:
                return row[0]

        return None

    def _is_adj_token(self, token_text):
        """Controlla se un token è un aggettivo, possessivo o dimostrativo."""
        word_lower = token_text.lower()
        # Prima cerca nell'indice veloce (ADJ)
        matches = self.form_index.get(word_lower, [])
        if any(m["pos"] == "ADJ" for m in matches):
            return True
        # Poi cerca possessivi/dimostrativi nel DB
        cur = self.conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM forms f JOIN lemmas l ON f.lemma_id = l.id
            WHERE LOWER(f.form) = ? AND l.pos IN ('DET-POSS','DET-DEMO','DET-INDEF','PRO-POSS','PRO-DEMO')
        """, (word_lower,))
        return cur.fetchone()[0] > 0

    def _apply_article_change(self, tok, new_form, results, i, j):
        """Applica il cambio di articolo gestendo apostrofi e spazi."""
        tok_lower = tok["original"].lower()
        new_form = self._apply_capitalization(tok["original"], new_form)

        if new_form.endswith("'"):
            if not tok["original"].endswith("'"):
                for k in range(i + 1, j):
                    results[k]["replacement"] = ""
            tok["replacement"] = new_form
        else:
            if tok["original"].endswith("'"):
                tok["replacement"] = new_form
                results.insert(i + 1, {
                    "original": "", "replacement": " ",
                    "replaced": False, "synonyms": []
                })
            else:
                tok["replacement"] = new_form

    def _postprocess_articles(self, results):
        """Aggiusta articoli, preposizioni articolate e aggettivi quando il nome cambia."""

        # Fase 1: Gestisci cambio genere — adatta aggettivi adiacenti al nome
        for i, tok in enumerate(results):
            if not tok.get("gender_changed"):
                continue

            new_gender = tok.get("gender")
            new_number = tok.get("number")
            if not new_gender or not new_number:
                continue

            # Cerca aggettivi PRIMA del nome (es. "la mia bella casa" → "il mio bel papiro")
            j = i - 1
            while j >= 0:
                r = results[j]
                text = r["replacement"].strip() if r.get("replacement") else ""
                if not text:
                    j -= 1
                    continue
                if self._is_adj_token(text):
                    new_adj = self._adjust_adj_gender(text, new_gender, new_number)
                    if new_adj:
                        r["replacement"] = self._apply_capitalization(text, new_adj)
                    j -= 1
                else:
                    break

            # Cerca aggettivi DOPO il nome (es. "casa grande" → "papiro grande" → ok, ma "casa bella" → "papiro bello")
            j = i + 1
            while j < len(results):
                r = results[j]
                text = r["replacement"].strip() if r.get("replacement") else ""
                if not text:
                    j += 1
                    continue
                if self._is_adj_token(text) and not r.get("replaced"):
                    new_adj = self._adjust_adj_gender(text, new_gender, new_number)
                    if new_adj:
                        r["replacement"] = self._apply_capitalization(text, new_adj)
                    j += 1
                else:
                    break

        # Fase 2: Aggiusta articoli e preposizioni articolate
        i = 0
        while i < len(results):
            tok = results[i]
            tok_lower = tok["replacement"].lower() if tok.get("replacement") else tok["original"].lower()

            # Controlla se è un articolo
            art_info = self.ARTICLE_MAP.get(tok_lower)
            is_artpre = False
            prep = None
            if not art_info:
                artpre_info = self.ARTPRE_MAP.get(tok_lower)
                if artpre_info:
                    prep, art_type, art_gender, art_number = artpre_info
                    art_info = (art_type, art_gender, art_number)
                    is_artpre = True

            if art_info:
                art_type, art_gender, art_number = art_info

                # Trova la prossima parola di contenuto (salta spazi)
                j = i + 1
                while j < len(results):
                    if results[j].get("replacement", results[j]["original"]).strip():
                        break
                    j += 1

                if j < len(results):
                    target = results[j]
                    next_word = target.get("replacement", target["original"])

                    # Determina il genere/numero da usare: dalla parola successiva se sostituita
                    # (o dal primo nome con gender_changed trovato più avanti)
                    rpl_gender = None
                    rpl_number = None

                    # Cerca il nome più vicino per determinare genere
                    for scan in range(j, min(j + 4, len(results))):
                        sr = results[scan]
                        if sr.get("gender") and sr.get("pos") == "NOUN":
                            rpl_gender = sr["gender"]
                            rpl_number = sr.get("number", art_number)
                            break
                        elif sr.get("replaced") and sr.get("gender"):
                            rpl_gender = sr["gender"]
                            rpl_number = sr.get("number", art_number)
                            break

                    if not rpl_gender:
                        rpl_gender = art_gender
                        rpl_number = art_number

                    if rpl_gender and rpl_number:
                        if is_artpre:
                            new_form = self._compute_artpre(next_word, rpl_gender, rpl_number, prep)
                        else:
                            new_form = self._compute_article(next_word, rpl_gender, rpl_number, art_type)

                        if new_form and new_form.lower() != tok_lower:
                            self._apply_article_change(tok, new_form, results, i, j)
            i += 1

    # Forme di "avere" e "essere" usate come ausiliari nei tempi composti
    _AVERE_FORMS = {
        "ho", "hai", "ha", "abbiamo", "avete", "hanno",           # ind pres
        "avevo", "avevi", "aveva", "avevamo", "avevate", "avevano",# ind impf
        "ebbi", "avesti", "ebbe", "avemmo", "aveste", "ebbero",   # ind past
        "avrò", "avrai", "avrà", "avremo", "avrete", "avranno",   # ind fut
        "abbia", "abbia", "abbia", "abbiamo", "abbiate", "abbiano",# sub pres
        "avessi", "avessi", "avesse", "avessimo", "aveste", "avessero", # sub impf
        "avrei", "avresti", "avrebbe", "avremmo", "avreste", "avrebbero", # cond
    }
    _ESSERE_FORMS = {
        "sono", "sei", "è", "siamo", "siete", "sono",              # ind pres
        "ero", "eri", "era", "eravamo", "eravate", "erano",        # ind impf
        "fui", "fosti", "fu", "fummo", "foste", "furono",          # ind past
        "sarò", "sarai", "sarà", "saremo", "sarete", "saranno",    # ind fut
        "sia", "sia", "sia", "siamo", "siate", "siano",            # sub pres
        "fossi", "fossi", "fosse", "fossimo", "foste", "fossero",  # sub impf
        "sarei", "saresti", "sarebbe", "saremmo", "sareste", "sarebbero", # cond
    }

    def _is_auxiliary_before_participle(self, tokens, idx):
        """Controlla se il token a posizione idx è un ausiliare seguito da participio passato."""
        token_lower = tokens[idx].lower()
        if token_lower not in self._AVERE_FORMS and token_lower not in self._ESSERE_FORMS:
            return False

        # Cerca il prossimo token-parola (salta spazi e punteggiatura)
        j = idx + 1
        while j < len(tokens):
            if re.match(r"[a-zA-ZàèéìòùÀÈÉÌÒÙäëïöüâêîôûçñ']+$", tokens[j]):
                break
            if not tokens[j].strip():
                j += 1
                continue
            return False  # punteggiatura prima di una parola -> non è ausiliare
            j += 1

        if j >= len(tokens):
            return False

        # Controlla se la parola successiva è un participio passato
        next_word = tokens[j].lower()
        next_matches = self.form_index.get(next_word, [])
        for m in next_matches:
            if m["pos"] == "VER" and m.get("mood") == "part" and m.get("tense") == "past":
                return True
            # Anche participi passati usati come aggettivi (es. "mangiato", "andato")
            if m["pos"] == "ADJ" and m.get("degree") is None:
                # Verifica se è anche un participio di qualche verbo
                pass

        # Gestisce anche avverbi interposti: "ho SEMPRE mangiato", "non ha MAI visto"
        if j < len(tokens):
            next_matches_adv = self.form_index.get(next_word, [])
            is_adv = any(m["pos"] == "ADV" for m in next_matches_adv)
            if is_adv:
                # Guarda ancora più avanti
                k = j + 1
                while k < len(tokens):
                    if re.match(r"[a-zA-ZàèéìòùÀÈÉÌÒÙäëïöüâêîôûçñ']+$", tokens[k]):
                        break
                    if not tokens[k].strip():
                        k += 1
                        continue
                    return False
                    k += 1
                if k < len(tokens):
                    far_word = tokens[k].lower()
                    far_matches = self.form_index.get(far_word, [])
                    for m in far_matches:
                        if m["pos"] == "VER" and m.get("mood") == "part" and m.get("tense") == "past":
                            return True

        return False

    def _is_previous_auxiliary(self, tokens, idx):
        """Controlla se la parola precedente (saltando spazi/avverbi) è un ausiliare."""
        j = idx - 1
        while j >= 0:
            if re.match(r"[a-zA-ZàèéìòùÀÈÉÌÒÙäëïöüâêîôûçñ']+$", tokens[j]):
                break
            if not tokens[j].strip():
                j -= 1
                continue
            return False
        if j < 0:
            return False

        word = tokens[j].lower()
        # Direttamente un ausiliare
        if word in self._AVERE_FORMS or word in self._ESSERE_FORMS:
            return True

        # Avverbio interposto: "ho SEMPRE bucato" → controlla ancora prima
        word_matches = self.form_index.get(word, [])
        if any(m["pos"] == "ADV" for m in word_matches):
            k = j - 1
            while k >= 0:
                if re.match(r"[a-zA-ZàèéìòùÀÈÉÌÒÙäëïöüâêîôûçñ']+$", tokens[k]):
                    break
                if not tokens[k].strip():
                    k -= 1
                    continue
                return False
            if k >= 0:
                prev_word = tokens[k].lower()
                if prev_word in self._AVERE_FORMS or prev_word in self._ESSERE_FORMS:
                    return True

        return False

    def sinonimizza(self, text, intensity=70, seed=None):
        """
        Sinonimizza il testo.

        Args:
            text: testo da elaborare
            intensity: 0-100, percentuale di parole da sostituire
            seed: seme random per riproducibilità (opzionale)

        Returns:
            Lista di token con info su originale/sostituzione
        """
        if seed is not None:
            rng = random.Random(seed)
        else:
            rng = random.Random()

        tokens = self.tokenize(text)
        results = []

        for ti, token in enumerate(tokens):
            # Skip spazi e punteggiatura
            if not re.match(r"[a-zA-ZàèéìòùÀÈÉÌÒÙäëïöüâêîôûçñ']+$", token):
                results.append({
                    "original": token,
                    "replacement": token,
                    "replaced": False,
                    "synonyms": [],
                })
                continue

            # Trova il token-parola precedente (saltando spazi/punteggiatura)
            prev_word = None
            for pi in range(ti - 1, -1, -1):
                if re.match(r"[a-zA-ZàèéìòùÀÈÉÌÒÙäëïöüâêîôûçñ']+$", tokens[pi]):
                    prev_word = tokens[pi]
                    break
                if tokens[pi].strip():
                    break  # punteggiatura, non continuare

            # Trova info morfologiche
            matches = self._find_word_info(token, prev_word=prev_word)
            if not matches:
                results.append({
                    "original": token,
                    "replacement": token,
                    "replaced": False,
                    "synonyms": [],
                })
                continue

            # Disambiguazione contestuale: se la parola precedente è un ausiliare,
            # questa parola è quasi certamente un participio passato (verbo), non un nome.
            # Es: "Ho bucato" → "bucato" = VER (part.pass di bucare), non NOUN (il bucato).
            prev_is_aux = self._is_previous_auxiliary(tokens, ti)
            if prev_is_aux:
                # Forza priorità VER participio passato
                verb_participles = [m for m in matches
                                    if m["pos"] == "VER" and m.get("mood") == "part"]
                if verb_participles:
                    matches = verb_participles + [m for m in matches if m not in verb_participles]

            # Prendi il match migliore (primo per priorità POS)
            best = matches[0]

            # Skip se POS non sostituibile
            if best["pos"] not in REPLACE_POS:
                results.append({
                    "original": token,
                    "replacement": token,
                    "replaced": False,
                    "synonyms": [],
                })
                continue

            # Skip ausiliari nei tempi composti: "ho mangiato", "è andato",
            # "aveva detto", "avrei voluto", "ha sempre creduto"
            if self._is_auxiliary_before_participle(tokens, ti):
                results.append({
                    "original": token,
                    "replacement": token,
                    "replaced": False,
                    "synonyms": [],
                })
                continue

            # Controlla intensità
            if rng.randint(1, 100) > intensity:
                results.append({
                    "original": token,
                    "replacement": token,
                    "replaced": False,
                    "synonyms": [],
                })
                continue

            # Trova sinonimi dello stesso POS
            synonyms = self._get_synonyms_for_lemma(best["lemma_id"], best["pos"])
            if not synonyms:
                results.append({
                    "original": token,
                    "replacement": token,
                    "replaced": False,
                    "synonyms": [],
                })
                continue

            # Per i nomi: separa sinonimi per genere, preferisci stesso genere
            same_gender_syns = []
            diff_gender_syns = []
            if best["pos"] == "NOUN" and best["gender"]:
                original_gender = best["gender"]
                for s in synonyms:
                    if s["lemma_id"] in self.also_adj:
                        continue
                    syn_gender = self._gender_of_noun_lemma(s["lemma_id"])
                    if syn_gender == original_gender:
                        same_gender_syns.append((s, original_gender))
                    elif syn_gender:
                        diff_gender_syns.append((s, syn_gender))

            # Proprietà target per la flessione
            target_props = {
                "gender": best["gender"],
                "number": best["number"],
                "person": best["person"],
                "mood": best["mood"],
                "tense": best["tense"],
                "degree": best["degree"],
            }

            # Prova sinonimi: prima stesso genere, poi diverso (fallback)
            all_alternatives = []
            chosen_gender = best.get("gender")

            if best["pos"] == "NOUN" and best["gender"]:
                # Fase 1: stesso genere
                rng.shuffle(same_gender_syns)
                for syn, sg in same_gender_syns:
                    form = self._find_synonym_form(syn["lemma_id"], target_props, "NOUN")
                    if form and form.lower() != token.lower():
                        all_alternatives.append((self._apply_capitalization(token, form), best["gender"]))

                # Fase 2: genere diverso (fallback) — articolo/aggettivi saranno adattati
                if not all_alternatives:
                    rng.shuffle(diff_gender_syns)
                    for syn, sg in diff_gender_syns:
                        diff_props = dict(target_props, gender=sg)
                        form = self._find_synonym_form(syn["lemma_id"], diff_props, "NOUN")
                        if form and form.lower() != token.lower():
                            all_alternatives.append((self._apply_capitalization(token, form), sg))
            else:
                # Non-NOUN: logica originale
                rng.shuffle(synonyms)
                for syn in synonyms:
                    form = self._find_synonym_form(syn["lemma_id"], target_props, best["pos"])
                    if form and form.lower() != token.lower():
                        all_alternatives.append((self._apply_capitalization(token, form), best.get("gender")))

            if all_alternatives:
                chosen, chosen_gender = all_alternatives[0]
                gender_changed = (best["pos"] == "NOUN" and chosen_gender != best.get("gender"))
                results.append({
                    "original": token,
                    "replacement": chosen,
                    "replaced": True,
                    "lemma": best["lemma"],
                    "pos": best["pos"],
                    "gender": chosen_gender,
                    "orig_gender": best.get("gender"),
                    "number": best["number"],
                    "gender_changed": gender_changed,
                    "synonyms": [a[0] for a in all_alternatives[:15]],
                })
            else:
                results.append({
                    "original": token,
                    "replacement": token,
                    "replaced": False,
                    "synonyms": [],
                })

        # Post-processing: aggiusta articoli e preposizioni articolate
        self._postprocess_articles(results)

        return results

    def get_stats(self):
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM lemmas")
        lemmas = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM forms")
        forms = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM synonyms")
        synonyms = cur.fetchone()[0]
        return {"lemmas": lemmas, "forms": forms, "synonyms": synonyms}


# ─── Interfaccia Web ─────────────────────────────────────────────────────────

HTML_PAGE = r"""<!DOCTYPE html>
<html lang="it">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Il Sinonimizzatore</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,400;0,600;0,700;1,400;1,600;1,700&family=IM+Fell+English+SC&display=swap" rel="stylesheet">
<style>
  :root {
    --parchment: #f5e6c8;
    --parchment-dark: #d4b896;
    --ink: #2c1810;
    --ink-light: #5a3e2b;
    --ink-faded: #8b7355;
    --gold: #8b6914;
    --gold-light: #c4a035;
    --gold-bright: #d4af37;
    --red-ink: #7a1a1a;
  }
  * { margin: 0; padding: 0; box-sizing: border-box; }

  body {
    font-family: 'Cormorant Garamond', Georgia, serif;
    background: #1a1209 url('/sfondo.png') center center / cover fixed;
    color: var(--ink);
    height: 100vh;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }

  /* ── Header ── */
  header {
    flex-shrink: 0;
    display: flex;
    align-items: baseline;
    justify-content: center;
    gap: 16px;
    padding: 8px 28px;
    border-bottom: 1px solid rgba(139,105,20,0.25);
    background: rgba(30,20,8,0.35);
    backdrop-filter: blur(6px);
  }
  header h1 {
    font-family: 'IM Fell English SC', serif;
    font-size: 20px;
    color: var(--gold-bright);
    letter-spacing: 3px;
    text-shadow: 0 1px 4px rgba(0,0,0,0.3);
  }
  header .sep { color: rgba(139,105,20,0.5); font-size: 14px; }
  header .subtitle {
    font-style: italic;
    font-size: 13px;
    color: #4a3a25;
    letter-spacing: 1px;
  }
  .db-stats {
    margin-left: auto;
    display: flex;
    gap: 14px;
    font-size: 11px;
    color: #4a3a25;
  }
  .db-stats strong { color: #3a2a18; }

  /* ── Main layout ── */
  .main {
    flex: 1;
    display: flex;
    gap: 28px;
    padding: 18px 28px 14px;
    overflow: hidden;
    max-width: 1500px;
    width: 100%;
    margin: 0 auto;
  }

  /* ── Left column: input + controls ── */
  .col-input {
    flex: 0 0 560px;
    display: flex;
    flex-direction: column;
    gap: 10px;
    min-width: 0;
  }

  .input-panel {
    flex: 1;
    display: flex;
    flex-direction: column;
    background: linear-gradient(160deg, var(--parchment) 0%, #eddcb5 100%);
    border: 2px solid var(--parchment-dark);
    border-radius: 5px;
    box-shadow: 0 6px 28px rgba(0,0,0,0.35), inset 0 1px 0 rgba(255,255,255,0.3);
    overflow: hidden;
  }
  .panel-bar {
    padding: 8px 18px;
    background: linear-gradient(180deg, rgba(139,105,20,0.14), rgba(139,105,20,0.04));
    border-bottom: 1px solid var(--parchment-dark);
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  .panel-bar h2 {
    font-family: 'IM Fell English SC', serif;
    font-size: 14px;
    color: var(--ink-light);
    letter-spacing: 2px;
  }
  .word-count {
    font-size: 12px;
    color: var(--ink-faded);
    font-style: italic;
  }
  textarea {
    flex: 1;
    width: 100%;
    min-height: 60px;
    padding: 16px 20px;
    background: transparent;
    border: none;
    color: var(--ink);
    font-family: 'Cormorant Garamond', serif;
    font-size: 18px;
    line-height: 1.75;
    resize: none;
    outline: none;
  }
  textarea::placeholder { color: var(--ink-faded); font-style: italic; }

  /* ── Control strip ── */
  .control-strip {
    flex-shrink: 0;
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 12px;
    flex-wrap: wrap;
    width: 100%;
  }

  .btn {
    padding: 10px 30px;
    border: 2px solid var(--gold);
    border-radius: 4px;
    font-family: 'IM Fell English SC', serif;
    font-size: 15px;
    cursor: pointer;
    transition: all 0.2s;
    letter-spacing: 1px;
  }
  .btn-primary {
    background: linear-gradient(180deg, #8b6914, #6b4f0e);
    color: var(--parchment);
    text-shadow: 0 1px 2px rgba(0,0,0,0.4);
    box-shadow: 0 3px 10px rgba(139,105,20,0.3);
  }
  .btn-primary:hover {
    background: linear-gradient(180deg, #a07a1a, #8b6914);
    box-shadow: 0 4px 18px rgba(212,175,55,0.35);
    transform: translateY(-1px);
  }
  .btn-secondary {
    background: linear-gradient(180deg, var(--parchment), #e0d0a8);
    color: var(--ink-light);
    border-color: var(--parchment-dark);
    padding: 10px 22px;
    font-size: 14px;
  }
  .btn-secondary:hover {
    background: linear-gradient(180deg, #f0e0c0, var(--parchment));
  }

  .slider-group {
    display: flex;
    align-items: center;
    gap: 10px;
    width: 100%;
  }
  .slider-group label {
    font-size: 15px;
    color: var(--ink-light);
    font-style: italic;
    white-space: nowrap;
  }
  .slider-group input[type=range] {
    flex: 1;
    accent-color: var(--gold);
  }
  .slider-group .val {
    font-size: 15px;
    font-weight: 700;
    color: var(--gold);
    min-width: 36px;
    text-align: right;
  }

  .loading {
    display: none;
    align-items: center;
    gap: 6px;
    color: var(--gold-light);
    font-style: italic;
    font-size: 14px;
  }
  .loading.active { display: flex; }
  .quill { display: inline-block; animation: qw .9s ease-in-out infinite; }
  @keyframes qw { 0%,100%{transform:rotate(-5deg)} 50%{transform:rotate(12deg) translateX(2px)} }

  /* ── Right column: pergamena ── */
  .col-output {
    flex: 1;
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 4px;
    min-width: 0;
    overflow: visible;
  }

  .pergamena-frame {
    position: relative;
    flex: 1;
    min-height: 0;
    display: flex;
    justify-content: center;
    align-items: flex-start;
  }
  .pergamena-inner {
    position: relative;
    /* Occupa tutta l'altezza disponibile, larghezza segue ratio 900:750 = 6:5 */
    height: 100%;
    aspect-ratio: 6 / 5;
    max-width: 100%;
    overflow: hidden;
  }
  .pergamena-inner img {
    width: 100%;
    height: 100%;
    display: block;
    filter: drop-shadow(0 8px 24px rgba(0,0,0,0.4));
  }
  .output-text {
    position: absolute;
    top: 26%;
    left: 28%;
    right: 28%;
    bottom: 20%;
    font-family: 'Cormorant Garamond', serif;
    font-style: italic;
    font-size: 15px;
    line-height: 1.7;
    color: var(--ink);
    text-align: center;
    overflow-y: auto;
    padding: 4px 6px;
    scrollbar-width: thin;
    scrollbar-color: rgba(139,105,20,0.25) transparent;
  }
  .output-text:empty::before {
    content: "Qui apparir\00e0  il Vostro testo, rielaborato con somma cura\2026";
    color: var(--ink-faded);
  }
  .output-text:not(:empty) {
    overflow-y: auto;
  }
  .output-text::-webkit-scrollbar { width: 3px; }
  .output-text::-webkit-scrollbar-thumb { background: rgba(139,105,20,0.25); border-radius: 2px; }

  /* ── Token ── */
  .token-replaced {
    color: inherit;
    font-weight: inherit;
  }

  /* ── Stats bar under pergamena ── */
  .stats-row {
    display: none;
    align-items: center;
    justify-content: center;
    gap: 14px;
    flex-shrink: 0;
    flex-wrap: wrap;
  }
  .stats-row span {
    font-size: 13px;
    color: var(--ink-light);
    font-style: italic;
  }
  .stats-row strong { color: var(--ink); }
  .stat-hl { color: var(--gold) !important; font-weight: 700; }

  .btn-copy {
    font-family: 'IM Fell English SC', serif;
    font-size: 12px;
    padding: 4px 14px;
    border-radius: 3px;
    background: linear-gradient(180deg, var(--parchment), #e0d0a8);
    color: var(--ink-light);
    border: 1px solid var(--parchment-dark);
    cursor: pointer;
    letter-spacing: 1px;
    transition: background 0.2s;
  }
  .btn-copy:hover { background: linear-gradient(180deg, #f5edd5, var(--parchment)); }

  /* ── Responsive ── */
  @media (max-width: 900px) {
    body { overflow: auto; height: auto; min-height: 100vh; }
    .main { flex-direction: column; padding: 12px 16px; gap: 16px; overflow: visible; }
    .col-input { flex: none; width: 100%; }
    .col-output { flex: none; width: 100%; }
    .pergamena-frame { height: auto; }
    .pergamena-inner { width: 90vw !important; height: auto !important; aspect-ratio: 6 / 5; margin: 0 auto; }
    .pergamena-inner img { width: 100% !important; height: auto !important; }
    .input-panel { min-height: 200px; }
    header { flex-wrap: wrap; }
  }
</style>
</head>
<body>

<header>
  <h1>Il Sinonimizzatore</h1>
  <span class="sep">&mdash;</span>
  <span class="subtitle">Macchina per la Rielaborazione delle Parole</span>
  <div class="db-stats">
    <span><strong id="dbLemmas">--</strong> lemmi</span>
    <span><strong id="dbForms">--</strong> forme</span>
    <span><strong id="dbSynonyms">--</strong> sinonimi</span>
  </div>
</header>

<div class="main">

  <!-- INPUT -->
  <div class="col-input">
    <div class="input-panel">
      <div class="panel-bar">
        <h2>Testo Originale</h2>
        <span class="word-count" id="inputCount">0 parole</span>
      </div>
      <textarea id="inputText" placeholder="Vergare qui il testo che si desidera rielaborare..." spellcheck="false"></textarea>
    </div>

    <div class="slider-group">
      <label>Intensit&agrave;</label>
      <input type="range" id="intensity" min="10" max="100" value="70" step="5">
      <span class="val" id="intensityVal">70%</span>
    </div>

    <div class="control-strip">
      <button class="btn btn-primary" id="btnSinonimizza" onclick="sinonimizza()">Sinonimizza</button>
      <button class="btn btn-secondary" onclick="reSinonimizza()">Rigenera</button>
      <div class="loading" id="loading">
        <span class="quill">&#9998;</span>
        <span>Lo scriba lavora&hellip;</span>
      </div>
    </div>
  </div>

  <!-- OUTPUT -->
  <div class="col-output">
    <div class="pergamena-frame">
      <div class="pergamena-inner">
        <img src="/pergamena.png" alt="">
        <div class="output-text" id="outputText"></div>
      </div>
    </div>
    <div class="stats-row" id="statsBar">
      <span>Parole <strong id="statTotal">0</strong></span>
      <span>Sostituite <strong class="stat-hl" id="statReplaced">0</strong></span>
      <span>(<strong class="stat-hl" id="statPct">0%</strong>)</span>
      <button class="btn-copy" onclick="copyOutput()">Copia</button>
    </div>
  </div>

</div>

<script>
let currentTokens=[], currentSeed=null;
const sl=document.getElementById('intensity'), sv=document.getElementById('intensityVal');
sl.addEventListener('input',()=>{sv.textContent=sl.value+'%'});

document.getElementById('inputText').addEventListener('input',function(){
  const w=this.value.trim().split(/\s+/).filter(w=>w.length>0);
  document.getElementById('inputCount').textContent=w.length+' parole';
});
document.getElementById('inputText').addEventListener('keydown',function(e){
  if(e.ctrlKey&&e.key==='Enter') sinonimizza();
});

async function sinonimizza(){
  const t=document.getElementById('inputText').value.trim();
  if(!t) return;
  currentSeed=Math.floor(Math.random()*1e6);
  await doRequest(t,currentSeed);
}
async function reSinonimizza(){
  const t=document.getElementById('inputText').value.trim();
  if(!t) return;
  currentSeed=Math.floor(Math.random()*1e6);
  await doRequest(t,currentSeed);
}

async function doRequest(text,seed){
  const btn=document.getElementById('btnSinonimizza'), ld=document.getElementById('loading');
  btn.disabled=true; ld.classList.add('active');
  try{
    const r=await fetch('/api/sinonimizza',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({text,intensity:parseInt(sl.value),seed})});
    currentTokens=(await r.json()).tokens;
    renderOutput();
  }catch(e){document.getElementById('outputText').textContent='Errore: '+e.message}
  btn.disabled=false; ld.classList.remove('active');
}

function renderOutput(){
  const c=document.getElementById('outputText');
  c.innerHTML='';
  let tw=0, rw=0;
  currentTokens.forEach(t=>{
    if(/^[a-zA-Z\u00C0-\u024F']+$/.test(t.original)) tw++;
    if(t.replaced){
      rw++;
      const s=document.createElement('span');
      s.className='token-replaced';
      s.textContent=t.replacement;
      s.title=t.original+' \u2192 '+t.replacement;
      c.appendChild(s);
    } else c.appendChild(document.createTextNode(t.replacement));
  });
  document.getElementById('statTotal').textContent=tw;
  document.getElementById('statReplaced').textContent=rw;
  document.getElementById('statPct').textContent=tw>0?Math.round(100*rw/tw)+'%':'0%';
  document.getElementById('statsBar').style.display='flex';
}

function copyOutput(){
  navigator.clipboard.writeText(currentTokens.map(t=>t.replacement).join('')).then(()=>{
    const b=document.querySelector('.btn-copy'), o=b.textContent;
    b.textContent='Copiato!'; setTimeout(()=>b.textContent=o,1500);
  });
}

fetch('/api/stats').then(r=>r.json()).then(d=>{
  document.getElementById('dbLemmas').textContent=d.lemmas?.toLocaleString('it-IT')||'--';
  document.getElementById('dbForms').textContent=d.forms?.toLocaleString('it-IT')||'--';
  document.getElementById('dbSynonyms').textContent=d.synonyms?.toLocaleString('it-IT')||'--';
});
</script>
</body>
</html>"""


# ─── HTTP Server ──────────────────────────────────────────────────────────────

engine = None


class SinonimizzatoreHandler(http.server.BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        path = urlparse(self.path).path
        if path in ("/", "/index.html"):
            self._send_html(HTML_PAGE)
        elif path == "/api/stats":
            self._send_json(engine.get_stats())
        elif path == "/pergamena.jpg":
            self._send_file(os.path.join(SCRIPT_DIR, "pergamena.jpg"), "image/jpeg")
        elif path == "/pergamena.png":
            self._send_file(os.path.join(SCRIPT_DIR, "pergamena.png"), "image/png")
        elif path == "/sfondo.png":
            self._send_file(os.path.join(SCRIPT_DIR, "sfondo.png"), "image/png")
        else:
            self.send_error(404)

    def do_POST(self):
        path = urlparse(self.path).path
        if path == "/api/sinonimizza":
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length))
            text = body.get("text", "")
            intensity = body.get("intensity", 70)
            seed = body.get("seed")
            tokens = engine.sinonimizza(text, intensity=intensity, seed=seed)
            self._send_json({"tokens": tokens})
        else:
            self.send_error(404)

    def _send_html(self, content):
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(content.encode("utf-8"))

    def _send_json(self, data):
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))

    def _send_file(self, filepath, content_type):
        try:
            with open(filepath, "rb") as f:
                data = f.read()
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "public, max-age=86400")
            self.end_headers()
            self.wfile.write(data)
        except FileNotFoundError:
            self.send_error(404)


def main():
    global engine

    parser = argparse.ArgumentParser(description="Sinonimizzatore 2.0")
    parser.add_argument("--port", type=int, default=8080, help="Porta (default: 8080)")
    args = parser.parse_args()

    print("Sinonimizzatore 2.0")
    print("=" * 40)
    print("  Caricamento database...")
    engine = SinonimizzatoreEngine(DB_PATH)

    stats = engine.get_stats()
    print(f"  DB: {stats['lemmas']:,} lemmi | {stats['forms']:,} forme | {stats['synonyms']:,} sinonimi")

    server = http.server.HTTPServer(("127.0.0.1", args.port), SinonimizzatoreHandler)
    print(f"\n  Apri: http://localhost:{args.port}")
    print("  Premi Ctrl+C per chiudere.\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nChiuso.")
        server.server_close()


if __name__ == "__main__":
    main()
