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
from collections import defaultdict, Counter
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

# Parole che non devono MAI essere sostituite (negazioni, particelle, ecc.)
# Hanno POS sostituibile (ADV) ma cambiarle altera il significato della frase
NEVER_REPLACE = frozenset({
    "non", "né", "neanche", "neppure", "nemmeno",  # negazioni
    "sì", "no",                                      # affermazione/negazione
    "più", "meno",                                   # comparativi strutturali
    "come", "così", "quanto",                        # comparativi/correlativi
    "dove", "quando", "perché", "perchè",            # interrogativi/relativi
    "anche", "pure", "proprio", "già", "ancora",     # particelle modali
})


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

        # multiword_index: prima_parola -> [(espressione_intera, [replacement, ...], num_token)]
        # Usato per matching longest-match delle espressioni multi-parola
        print("  Caricamento espressioni multi-parola...", end=" ", flush=True)
        self.multiword_index = defaultdict(list)
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='multiword'")
        if cur.fetchone():
            cur.execute("SELECT expression, replacement FROM multiword")
            expr_map = defaultdict(list)
            for expr, repl in cur.fetchall():
                expr_map[expr].append(repl)
            for expr, repls in expr_map.items():
                words = expr.split()
                if words:
                    first = words[0]
                    self.multiword_index[first].append((expr, repls, len(words)))
            # Ordina per lunghezza decrescente (longest match first)
            for key in self.multiword_index:
                self.multiword_index[key].sort(key=lambda x: -x[2])
            print(f"{len(expr_map):,} espressioni caricate.")
        else:
            print("tabella non trovata, skip.")
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

    # Dimostrativi: (genere, numero) — seguono le stesse regole fonetiche degli articoli
    # quel/quello/quell'/quella → singolare; quei/quegli/quelle → plurale
    DEMONSTRATIVE_MAP = {
        "quel": ("m", "s"), "quello": ("m", "s"), "quell'": (None, "s"),
        "quella": ("f", "s"),
        "quei": ("m", "p"), "quegli": ("m", "p"), "quelle": ("f", "p"),
        # Anche "bel/bello/bell'/bella/bei/begli/belle" seguono lo stesso pattern
        "bel": ("m", "s"), "bello": ("m", "s"), "bell'": (None, "s"),
        "bella": ("f", "s"),
        "bei": ("m", "p"), "begli": ("m", "p"), "belle": ("f", "p"),
        # "buon/buono/buon'/buona"
        "buon": ("m", "s"), "buono": ("m", "s"), "buon'": (None, "s"),
        "buona": ("f", "s"),
        "buoni": ("m", "p"), "buone": ("f", "p"),
        # "gran/grande/grand'"
        "gran": (None, "s"), "grande": (None, "s"), "grand'": (None, "s"),
        "grandi": (None, "p"),
        # "san/santo/sant'/santa"
        "san": ("m", "s"), "santo": ("m", "s"), "sant'": (None, "s"),
        "santa": ("f", "s"), "santi": ("m", "p"), "sante": ("f", "p"),
    }

    # Famiglie di dimostrativi: radice -> (forma_il, forma_lo, forma_elisione, forma_la, forma_i, forma_gli, forma_le)
    _DEMO_FAMILIES = {
        "quel":  ("quel", "quello", "quell'", "quella", "quei", "quegli", "quelle"),
        "bel":   ("bel", "bello", "bell'", "bella", "bei", "begli", "belle"),
        "buon":  ("buon", "buono", "buon'", "buona", "buoni", "buoni", "buone"),
        "san":   ("san", "santo", "sant'", "santa", "santi", "santi", "sante"),
    }

    def _get_demo_family(self, word_lower):
        """Trova la famiglia del dimostrativo dalla forma."""
        for root, forms in self._DEMO_FAMILIES.items():
            if word_lower.rstrip("'") in [f.rstrip("'") for f in forms] or word_lower in forms:
                return root, forms
        return None, None

    def _compute_demonstrative(self, demo_word, next_word, gender, number):
        """Calcola la forma corretta del dimostrativo per la parola seguente.

        Args:
            demo_word: il dimostrativo originale (es. "quel", "bello")
            next_word: la parola successiva (il nome) per le regole fonetiche
            gender: genere del nome
            number: numero del nome
        """
        vowel = self._starts_with_vowel(next_word)
        lo = self._needs_lo(next_word)

        root, forms = self._get_demo_family(demo_word.lower())
        if not forms:
            return None

        # forms = (il, lo, elisione, la, i, gli, le)
        if number == "s":
            if gender == "m":
                if vowel:
                    return forms[2]   # quell', bell', ...
                return forms[1] if lo else forms[0]  # quello/quel, bello/bel
            else:  # f
                return forms[2] if vowel else forms[3]  # quell'/quella, bell'/bella
        else:  # p
            if gender == "m":
                return forms[5] if (vowel or lo) else forms[4]  # quegli/quei, begli/bei
            else:
                return forms[6]  # quelle, belle

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

    # Regex per riconoscere token-parola (con lettere accentate italiane e trattino per composti)
    _WORD_RE = re.compile(r"[a-zA-ZàèéìòùÀÈÉÌÒÙäëïöüâêîôûçñ'\-]+$")
    _LETTER_CLASS = r"a-zA-ZàèéìòùÀÈÉÌÒÙäëïöüâêîôûçñ"

    def tokenize(self, text):
        """Tokenizza preservando spazi e punteggiatura. Separa apostrofi articolati.
        Cattura parole composte con trattino (nord-est, porta-finestra) come token unico."""
        # Ordine di priorità:
        # 1. parola-parola (composti con trattino)
        # 2. parola+apostrofo (l', un', dell')
        # 3. parola semplice
        # 4. singolo carattere non-spazio
        # 5. sequenze di spazi
        L = self._LETTER_CLASS
        tokens = re.findall(
            rf"[{L}]+(?:-[{L}]+)+'|[{L}]+(?:-[{L}]+)+|[{L}]+'|[{L}]+|[^\s]|\s+",
            text
        )
        return tokens

    # Pronomi soggetto: la parola successiva è quasi certamente un verbo
    # "io mangio", "tu voglia", "lei canta", "noi andiamo"
    _SUBJECT_PRONOUNS = frozenset({
        "io", "tu", "lui", "lei", "egli", "ella", "esso", "essa",
        "noi", "voi", "loro", "essi", "esse",
    })

    # Pronomi clitici che precedono un verbo (mi alzo, si veste, ci arrabbiamo)
    _CLITIC_PRONOUNS = frozenset({
        "mi", "ti", "si", "ci", "vi", "me", "te", "se", "ce", "ve",
        "lo", "la", "li", "le", "ne", "gli",
    })

    # Suffissi clitici postposti ai verbi (alzarsi, dimmi, portalo)
    # Ordine: dal più lungo al più corto per match greedy
    _CLITIC_SUFFIXES = [
        "glielo", "gliela", "glieli", "gliele", "gliene",
        "melo", "mela", "meli", "mele", "mene",
        "telo", "tela", "teli", "tele", "tene",
        "selo", "sela", "seli", "sele", "sene",
        "celo", "cela", "celi", "cele", "cene",
        "velo", "vela", "veli", "vele", "vene",
        "gli", "mi", "ti", "si", "ci", "vi",
        "lo", "la", "li", "le", "ne",
    ]

    # Preposizioni semplici e locuzioni prepositive — la parola dopo è quasi sempre NOUN
    _PREPOSITIONS = frozenset({
        "di", "a", "da", "in", "con", "su", "per", "tra", "fra",
        "verso", "dopo", "prima", "durante", "mediante", "nonostante",
        "oltre", "senza", "sotto", "sopra", "contro", "dentro", "fuori",
        "lungo", "presso", "secondo", "attraverso", "entro", "fino",
    })

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

    def _strip_clitic(self, word):
        """Prova a strippare un clitico postposto da un verbo.
        Ritorna (base_verb, clitic_suffix) o (word, None) se non è un verbo+clitico."""
        w = word.lower()
        for suffix in self._CLITIC_SUFFIXES:
            if w.endswith(suffix) and len(w) > len(suffix) + 2:
                base = w[:-len(suffix)]
                # Verifica che la base sia un verbo nel form_index
                # Per infiniti: alzar+si → alzare (aggiungi -e)
                # Per imperativi: alzati → alza (base diretta)
                candidates = [base]
                if not base.endswith(("are", "ere", "ire")):
                    # Potrebbe essere infinito troncato: parlar+si → parlare
                    candidates.append(base + "e")
                    # O imperativo: alzati → alza+ti, base=alza
                for cand in candidates:
                    if self.form_index.get(cand):
                        return cand, suffix
        return word, None

    def _find_word_info(self, word, prev_word=None, next_word=None):
        """Trova info morfologiche per una parola. Ritorna lista di match ordinata per priorità.

        Usa contesto (prev_word, next_word) per disambiguare NOUN/VER/ADJ:
        - prev_word = determinante → NOUN  ("il canto" → canto = nome)
        - prev_word = clitico → VER         ("mi alzo" → alzo = verbo)
        - next_word = determinante → VER   ("porto il cane" → porto = verbo)
        - next_word = NOUN → boost ADJ     ("caldo sole" → caldo = aggettivo)
        """
        matches = self.form_index.get(word.lower(), [])
        if not matches and '-' in word:
            # Fallback per parole composte: cerca la testa (primo componente)
            head = word.split('-')[0]
            matches = self.form_index.get(head.lower(), [])
        if not matches:
            # Fallback: prova a strippare clitico postposto (alzarsi → alzare)
            base, clitic = self._strip_clitic(word)
            if clitic:
                matches = self.form_index.get(base, [])
        if not matches:
            return []

        has_noun = any(m["pos"] == "NOUN" for m in matches)
        has_conjugated_verb = any(m["pos"] == "VER" and m.get("person") and m.get("mood") == "ind" for m in matches)
        has_finite_verb = any(m["pos"] == "VER" and m.get("person") for m in matches)
        has_adj = any(m["pos"] == "ADJ" for m in matches)
        has_adv = any(m["pos"] == "ADV" for m in matches)

        # Analisi contesto successivo
        next_is_determiner = next_word and next_word.lower() in self._DETERMINERS
        next_is_noun = False
        if next_word and not next_is_determiner:
            next_matches = self.form_index.get(next_word.lower(), [])
            next_is_noun = any(m["pos"] == "NOUN" for m in next_matches)

        # Contesto: clitico preposto → sicuramente VER
        # Ma lo/la/li/le sono ambigui (articoli O clitici) → trattali come determinanti
        prev_lw = prev_word.lower() if prev_word else ""
        prev_is_determiner = prev_lw in self._DETERMINERS
        prev_is_clitic = (prev_lw in self._CLITIC_PRONOUNS and not prev_is_determiner)

        # Se prev_word è una preposizione → questa parola è quasi certamente NOUN
        if prev_lw in self._PREPOSITIONS and has_noun:
            pos_priority = {"NOUN": 0, "ADJ": 1, "VER": 2, "ADV": 3}
            mood_priority = {"ind": 0, "sub": 1, "cond": 2, "inf": 3, "part": 4, "ger": 5, "impr": 6}
            def sort_key(m):
                p = pos_priority.get(m["pos"], 99)
                mood = mood_priority.get(m.get("mood") or "", 99)
                has_info = 0 if (m.get("gender") or m.get("number") or m.get("person")) else 1
                return (p, mood, has_info)
            matches.sort(key=sort_key)
            return matches

        # Disambiguazione NOUN vs VER vs ADJ con contesto bidirezionale
        # Regola 1 (universale): dopo un determinante → NOUN
        if prev_is_determiner and has_noun:
            pos_priority = {"NOUN": 0, "ADJ": 1, "VER": 2, "ADV": 3}
        # Regola 2: clitico → VER
        elif prev_is_clitic and has_conjugated_verb:
            pos_priority = {"VER": 0, "NOUN": 1, "ADJ": 2, "ADV": 3}
        # Regola 2b: pronome soggetto → VER (include congiuntivo/condizionale)
        # "tu voglia" → VER, "io parto" → VER, "lei canta" → VER
        elif prev_lw in self._SUBJECT_PRONOUNS and has_finite_verb:
            pos_priority = {"VER": 0, "NOUN": 1, "ADJ": 2, "ADV": 3}
        # Regola 2c: "non" → VER — "non" precede sempre un verbo in italiano
        # "non voglia" → VER, "non parto" → VER, "non canto" → VER
        elif prev_lw == "non" and has_finite_verb:
            pos_priority = {"VER": 0, "NOUN": 1, "ADJ": 2, "ADV": 3}
        # Regola 3: ambiguità NOUN/VER con contesto
        elif has_noun and has_conjugated_verb:
            if next_is_determiner:
                pos_priority = {"VER": 0, "NOUN": 1, "ADJ": 2, "ADV": 3}
            else:
                pos_priority = {"VER": 0, "NOUN": 1, "ADJ": 2, "ADV": 3}
        elif has_adj and has_noun and next_is_noun:
            # "caldo sole", "grande uomo" → ADJ che modifica il nome successivo
            pos_priority = {"ADJ": 0, "NOUN": 1, "VER": 2, "ADV": 3}
        elif has_adj and has_noun and prev_word:
            prev_lower = prev_word.lower()
            prev_matches = self.form_index.get(prev_lower, [])
            prev_is_noun = any(m["pos"] == "NOUN" for m in prev_matches)
            # "verso", "dopo", "prima", ecc. sono anche NOUN nel DB ma funzionano come preposizioni
            # Se prev_word è in _DETERMINERS o è una preposizione → boost NOUN
            prev_is_prep = prev_lower in self._PREPOSITIONS
            if prev_is_prep:
                # "verso fine", "dopo pranzo", "prima sera" → NOUN
                pos_priority = {"NOUN": 0, "ADJ": 1, "VER": 2, "ADV": 3}
            elif prev_is_noun and not any(m["pos"] in ("VER", "ADV") for m in prev_matches):
                # "amico fedele", "casa grande" → ADJ postposto
                # Ma solo se prev_word è puramente un NOUN (non anche VER/ADV/PREP)
                pos_priority = {"ADJ": 0, "NOUN": 1, "VER": 2, "ADV": 3}
            else:
                pos_priority = {"NOUN": 0, "VER": 1, "ADJ": 2, "ADV": 3}
        # Regola ADV/ADJ: dopo un verbo, parole come "spesso/forte/piano/molto"
        # sono quasi sempre avverbi, non aggettivi.
        # "vado spesso" → ADV, "sono spesso quelle" → ADV
        # Ma "muro spesso" → ADJ (prev è NOUN postposto dopo determinante)
        elif has_adj and has_adv and prev_word:
            prev_matches_all = self.form_index.get(prev_lw, [])
            prev_is_noun = any(m["pos"] == "NOUN" for m in prev_matches_all)
            prev_has_verb = any(m["pos"] == "VER" for m in prev_matches_all)
            if prev_is_noun and not prev_has_verb:
                # prev è solo NOUN: "strato spesso" → ADJ postposto
                pos_priority = {"ADJ": 0, "ADV": 1, "NOUN": 2, "VER": 3}
            elif not prev_is_noun and prev_has_verb:
                # prev è solo VER: "vado spesso", "parlo forte" → ADV
                pos_priority = {"ADV": 0, "ADJ": 1, "NOUN": 2, "VER": 3}
            elif prev_is_noun and prev_has_verb:
                # prev è sia NOUN che VER (es. "muro" = murare/muro, "sono" = essere/suono)
                # Se la parola successiva è un verbo → questa è ADJ postposta a un NOUN
                # "il muro spesso proteggeva" → next=proteggeva(VER) → spesso=ADJ
                # "sono spesso quelle" → next=quelle(DET) → spesso=ADV
                next_has_verb = False
                if next_word:
                    nxt = self.form_index.get(next_word.lower(), [])
                    next_has_verb = any(m["pos"] == "VER" for m in nxt)
                if next_has_verb and not next_is_determiner:
                    pos_priority = {"ADJ": 0, "ADV": 1, "NOUN": 2, "VER": 3}
                else:
                    pos_priority = {"ADV": 0, "ADJ": 1, "NOUN": 2, "VER": 3}
            else:
                pos_priority = {"ADV": 0, "ADJ": 1, "NOUN": 2, "VER": 3}
        else:
            pos_priority = {"NOUN": 0, "VER": 1, "ADJ": 2, "ADV": 3}

        # Per i verbi: disambiguazione mood. In testo normale l'indicativo è
        # molto più frequente dell'imperativo o del congiuntivo.
        mood_priority = {"ind": 0, "sub": 1, "cond": 2, "inf": 3, "part": 4, "ger": 5, "impr": 6}

        # Concordanza soggetto-verbo: se il nome/pronome precedente indica
        # un numero, preferisci la forma verbale concordante
        # "i giorni sono" → plurale → 3p; "io sono" → singolare → 1s
        _PRONOUN_NUMBER = {
            "io": ("s", "1"), "tu": ("s", "2"), "lui": ("s", "3"),
            "lei": ("s", "3"), "esso": ("s", "3"), "essa": ("s", "3"),
            "noi": ("p", "1"), "voi": ("p", "2"), "loro": ("p", "3"),
            "essi": ("p", "3"), "esse": ("p", "3"),
        }
        subject_number = None
        subject_person = None
        if prev_word:
            pn = _PRONOUN_NUMBER.get(prev_lw)
            if pn:
                subject_number, subject_person = pn
            else:
                prev_matches_all = self.form_index.get(prev_lw, [])
                for pm in prev_matches_all:
                    if pm["pos"] == "NOUN" and pm.get("number"):
                        subject_number = pm["number"]
                        subject_person = "3"  # nomi sono sempre 3a persona
                        break

        def sort_key(m):
            p = pos_priority.get(m["pos"], 99)
            mood = mood_priority.get(m.get("mood") or "", 99)
            # Penalizza match senza genere/numero (meno informativi)
            has_info = 0 if (m.get("gender") or m.get("number") or m.get("person")) else 1
            # Concordanza soggetto-verbo (numero e persona)
            number_mismatch = 0
            if m["pos"] == "VER" and (subject_number or subject_person):
                if subject_number and m.get("number") and m["number"] != subject_number:
                    number_mismatch = 1
                if subject_person and m.get("person") and m["person"] != subject_person:
                    number_mismatch = 1
            return (p, mood, number_mismatch, has_info)

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
            tp = target_props
            # Participi: devono matchare anche genere + numero
            if tp.get("mood") == "part":
                for f in forms:
                    if (f["pos"] == "VER"
                            and f["mood"] == "part"
                            and f["tense"] == tp.get("tense")
                            and f.get("gender") == tp.get("gender")
                            and f.get("number") == tp.get("number")):
                        return f["form"]
                # Fallback: solo mood + tense + number (senza genere)
                for f in forms:
                    if (f["pos"] == "VER"
                            and f["mood"] == "part"
                            and f["tense"] == tp.get("tense")
                            and f.get("number") == tp.get("number")):
                        return f["form"]
                # Ultimo fallback: qualsiasi participio dello stesso tense
                for f in forms:
                    if (f["pos"] == "VER"
                            and f["mood"] == "part"
                            and f["tense"] == tp.get("tense")):
                        return f["form"]
                return None

            # Verbi non-participi: matcha mood + tense + person + number
            for f in forms:
                if (f["pos"] == "VER"
                        and f["mood"] == tp.get("mood")
                        and f["tense"] == tp.get("tense")
                        and f["person"] == tp.get("person")
                        and f["number"] == tp.get("number")):
                    return f["form"]
            # Fallback: mood + tense (per infinito, gerundio)
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

        # Fase 3: Aggiusta dimostrativi (quel/quello/quell', bel/bello/bell', ecc.)
        i = 0
        while i < len(results):
            tok = results[i]
            tok_lower = tok["replacement"].lower() if tok.get("replacement") else tok["original"].lower()

            demo_info = self.DEMONSTRATIVE_MAP.get(tok_lower)
            if demo_info:
                demo_gender, demo_number = demo_info

                # Trova la prossima parola di contenuto (salta spazi)
                j = i + 1
                while j < len(results):
                    if results[j].get("replacement", results[j]["original"]).strip():
                        break
                    j += 1

                if j < len(results):
                    next_word = results[j].get("replacement", results[j]["original"])

                    # Determina genere/numero dal nome più vicino
                    rpl_gender = None
                    rpl_number = None
                    for scan in range(j, min(j + 4, len(results))):
                        sr = results[scan]
                        if sr.get("gender") and sr.get("pos") == "NOUN":
                            rpl_gender = sr["gender"]
                            rpl_number = sr.get("number", demo_number)
                            break
                        elif sr.get("replaced") and sr.get("gender"):
                            rpl_gender = sr["gender"]
                            rpl_number = sr.get("number", demo_number)
                            break

                    if not rpl_gender:
                        rpl_gender = demo_gender
                        rpl_number = demo_number

                    if rpl_gender and rpl_number:
                        new_form = self._compute_demonstrative(
                            tok_lower, next_word, rpl_gender, rpl_number
                        )
                        if new_form and new_form.lower() != tok_lower:
                            self._apply_article_change(tok, new_form, results, i, j)
            i += 1

    def _postprocess_participles(self, results):
        """Aggiusta participi passati quando l'ausiliare è 'essere'.

        Con 'essere', il participio concorda col soggetto in genere e numero:
          "Le ragazze sono andate" → se "ragazze" cambia genere → adattare "andate"
          "I gatti sono partiti" → se "gatti" → "bestie" (f) → "partite"
        """
        for i, tok in enumerate(results):
            # Cerco participi passati (parole che sono state sostituite come VER)
            if not tok.get("replaced") or tok.get("pos") != "VER":
                continue
            if tok.get("mood") != "part" or tok.get("tense") != "past":
                # Questo token potrebbe non avere mood/tense se non salvati.
                # Verifichiamo dalla forma originale nel form_index.
                orig_lower = tok["original"].lower()
                orig_matches = self.form_index.get(orig_lower, [])
                is_participle = any(
                    m["pos"] == "VER" and m.get("mood") == "part" and m.get("tense") == "past"
                    for m in orig_matches
                )
                if not is_participle:
                    continue

            # Cerco l'ausiliare all'indietro (saltando spazi e avverbi)
            aux_word = None
            j = i - 1
            while j >= 0:
                text_j = results[j].get("replacement", results[j]["original"]).strip()
                if not text_j:
                    j -= 1
                    continue
                text_j_lower = text_j.lower()
                # È un avverbio? Salta e continua a cercare
                if self.form_index.get(text_j_lower):
                    if any(m["pos"] == "ADV" for m in self.form_index[text_j_lower]):
                        j -= 1
                        continue
                aux_word = text_j_lower
                break

            if not aux_word or aux_word not in self._ESSERE_FORMS:
                continue

            # L'ausiliare è "essere" → cerco il soggetto NOUN all'indietro
            subj_tok = None
            k = j - 1
            while k >= 0:
                text_k = results[k].get("replacement", results[k]["original"]).strip()
                if not text_k:
                    k -= 1
                    continue
                if results[k].get("pos") == "NOUN":
                    subj_tok = results[k]
                    break
                # Se troviamo punteggiatura forte, stop
                if text_k in (".", "!", "?", ";"):
                    break
                k -= 1

            if not subj_tok or not subj_tok.get("gender_changed"):
                continue

            # Il soggetto ha cambiato genere → devo riadattare il participio
            new_gender = subj_tok["gender"]
            new_number = subj_tok.get("number", "s")

            # Trovo il lemma del participio sostituto e cerco la forma corretta
            repl_lower = tok["replacement"].lower()
            repl_matches = self.form_index.get(repl_lower, [])
            for m in repl_matches:
                if m["pos"] == "VER" and m.get("mood") == "part" and m.get("tense") == "past":
                    # Trovato il lemma: cerco la forma con il nuovo genere/numero
                    forms = self.lemma_forms.get(m["lemma_id"], [])
                    for f in forms:
                        if (f["pos"] == "VER" and f.get("mood") == "part"
                                and f.get("tense") == "past"
                                and f.get("gender") == new_gender
                                and f.get("number") == new_number):
                            tok["replacement"] = self._apply_capitalization(
                                tok["original"], f["form"]
                            )
                            break
                    break

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
            if self._WORD_RE.match(tokens[j]):
                break
            if not tokens[j].strip():
                j += 1
                continue
            return False  # punteggiatura prima di una parola -> non è ausiliare

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
                verb_matches = self.form_index.get(next_word, [])
                if any(vm["pos"] == "VER" and vm.get("mood") == "part" and vm.get("tense") == "past" for vm in verb_matches):
                    return True

        # Gestisce anche avverbi interposti: "ho SEMPRE mangiato", "non ha MAI visto"
        if j < len(tokens):
            next_matches_adv = self.form_index.get(next_word, [])
            is_adv = any(m["pos"] == "ADV" for m in next_matches_adv)
            if is_adv:
                # Guarda ancora più avanti
                k = j + 1
                while k < len(tokens):
                    if self._WORD_RE.match(tokens[k]):
                        break
                    if not tokens[k].strip():
                        k += 1
                        continue
                    return False
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
            if self._WORD_RE.match(tokens[j]):
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
                if self._WORD_RE.match(tokens[k]):
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

        # Fase 0: Matching espressioni multi-parola (longest-match, greedy)
        # Segna i token consumati e registra le sostituzioni
        multiword_replacements = {}  # ti -> (end_ti, replacement_text)
        multiword_consumed = set()   # indici di token consumati
        if self.multiword_index:
            # Estrai solo i token-parola con i loro indici
            word_tokens = [(i, t) for i, t in enumerate(tokens) if self._WORD_RE.match(t)]

            for wi, (ti_start, tok_start) in enumerate(word_tokens):
                if ti_start in multiword_consumed:
                    continue
                tok_lower = tok_start.lower()
                candidates = self.multiword_index.get(tok_lower, [])
                if not candidates:
                    continue

                # Costruisci la sequenza di parole da questa posizione in poi
                remaining_words = [t.lower() for _, t in word_tokens[wi:wi+6]]

                for expr, repls, num_words in candidates:
                    expr_words = expr.split()
                    if len(remaining_words) < num_words:
                        continue
                    if remaining_words[:num_words] == expr_words:
                        # Match! Scegli un replacement casuale
                        if rng.randint(1, 100) > intensity:
                            break  # Skip per intensità
                        repl = rng.choice(repls)
                        # Segna tutti i token di questa espressione come consumati
                        matched_indices = [word_tokens[wi + k][0] for k in range(num_words)]
                        for idx in matched_indices:
                            multiword_consumed.add(idx)
                        # Anche gli spazi tra i token sono consumati
                        for idx in range(matched_indices[0], matched_indices[-1] + 1):
                            multiword_consumed.add(idx)
                        multiword_replacements[matched_indices[0]] = (
                            matched_indices[-1],
                            self._apply_capitalization(tok_start, repl),
                            expr,
                            [self._apply_capitalization(tok_start, r) for r in repls],
                        )
                        break

        for ti, token in enumerate(tokens):
            # Se questo token fa parte di una espressione multi-parola sostituita
            if ti in multiword_consumed:
                if ti in multiword_replacements:
                    end_ti, repl_text, orig_expr, all_repls = multiword_replacements[ti]
                    # Ricostruisci il testo originale dell'espressione
                    orig_text = ''.join(tokens[ti:end_ti+1])
                    results.append({
                        "original": orig_text,
                        "replacement": repl_text,
                        "replaced": True,
                        "pos": "MULTI",
                        "synonyms": all_repls[:15],
                    })
                # Token intermedi dell'espressione: già inclusi nell'originale sopra
                continue

            # Skip spazi e punteggiatura
            if not self._WORD_RE.match(token):
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
                if self._WORD_RE.match(tokens[pi]):
                    prev_word = tokens[pi]
                    break
                if tokens[pi].strip():
                    break  # punteggiatura, non continuare

            # Trova il token-parola successivo (saltando spazi/punteggiatura)
            next_word = None
            for ni in range(ti + 1, len(tokens)):
                if self._WORD_RE.match(tokens[ni]):
                    next_word = tokens[ni]
                    break
                if tokens[ni].strip():
                    break  # punteggiatura, non continuare

            # Controlla se il token ha un clitico postposto (alzarsi, dimmi, portalo)
            clitic_suffix = None
            base_for_clitic = None
            stripped_base, stripped_clitic = self._strip_clitic(token)
            if stripped_clitic and self.form_index.get(stripped_base.lower()):
                clitic_suffix = stripped_clitic
                base_for_clitic = stripped_base

            # Trova info morfologiche
            matches = self._find_word_info(token, prev_word=prev_word, next_word=next_word)
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

            # Skip parole che non devono mai essere sostituite (negazioni, particelle)
            if token.lower() in NEVER_REPLACE:
                results.append({
                    "original": token,
                    "replacement": token,
                    "replaced": False,
                    "synonyms": [],
                })
                continue

            # Skip articoli e preposizioni articolate quando fungono da determinanti
            # (cioè quando la parola successiva è un nome o aggettivo).
            # Es: "dei fiori" → "dei" = ARTPRE (di+i), NON NOUN (plurale di dio).
            # Ma: "I dei sono potenti" → "dei" è NOUN, next_word è verbo → non skippare.
            token_lower = token.lower()
            if token_lower in self.ARTICLE_MAP or token_lower in self.ARTPRE_MAP:
                next_is_content = False
                if next_word:
                    nw_lower = next_word.lower()
                    nw_matches = self.form_index.get(nw_lower, [])
                    next_is_content = (
                        nw_lower in self._DETERMINERS
                        or any(m["pos"] in ("NOUN", "ADJ") for m in nw_matches)
                    )
                if next_is_content:
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

            # Skip clitici pronominali prima di un verbo (mi, ti, si, ci, vi, ne)
            # "ci arrabbiamo" → "ci" non va sostituito con "vi"
            if token.lower() in self._CLITIC_PRONOUNS and next_word:
                next_matches = self.form_index.get(next_word.lower(), [])
                if any(m["pos"] == "VER" for m in next_matches):
                    results.append({
                        "original": token,
                        "replacement": token,
                        "replaced": False,
                        "synonyms": [],
                    })
                    continue

            # Skip preposizioni quando seguite da un NOUN/determinante
            # "verso fine", "dopo pranzo", "durante la lezione" → non sostituire la preposizione
            if token.lower() in self._PREPOSITIONS and next_word:
                next_lower = next_word.lower()
                next_could_be_noun = (
                    next_lower in self._DETERMINERS
                    or any(m["pos"] == "NOUN" for m in self.form_index.get(next_lower, []))
                )
                if next_could_be_noun:
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
            # I sinonimi "figurativi" (lemmi che sono anche ADJ) sono usati come fallback
            same_gender_syns = []
            diff_gender_syns = []
            same_gender_figurative = []
            diff_gender_figurative = []
            if best["pos"] == "NOUN" and best["gender"]:
                original_gender = best["gender"]
                for s in synonyms:
                    is_figurative = s["lemma_id"] in self.also_adj
                    syn_gender = self._gender_of_noun_lemma(s["lemma_id"])
                    if syn_gender == original_gender:
                        if is_figurative:
                            same_gender_figurative.append((s, original_gender))
                        else:
                            same_gender_syns.append((s, original_gender))
                    elif syn_gender:
                        if is_figurative:
                            diff_gender_figurative.append((s, syn_gender))
                        else:
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

            # Prova sinonimi in ordine di priorità:
            # 1. stesso genere normali
            # 2. stesso genere figurativi
            # 3. genere diverso normali
            # 4. genere diverso figurativi
            all_alternatives = []
            chosen_gender = best.get("gender")

            if best["pos"] == "NOUN" and best["gender"]:
                # Fase 1: stesso genere (normali)
                rng.shuffle(same_gender_syns)
                for syn, sg in same_gender_syns:
                    form = self._find_synonym_form(syn["lemma_id"], target_props, "NOUN")
                    if form and form.lower() != token.lower():
                        all_alternatives.append((self._apply_capitalization(token, form), best["gender"]))

                # Fase 2: stesso genere (figurativi)
                if not all_alternatives:
                    rng.shuffle(same_gender_figurative)
                    for syn, sg in same_gender_figurative:
                        form = self._find_synonym_form(syn["lemma_id"], target_props, "NOUN")
                        if form and form.lower() != token.lower():
                            all_alternatives.append((self._apply_capitalization(token, form), best["gender"]))

                # Fase 3: genere diverso (normali) — articolo/aggettivi saranno adattati
                if not all_alternatives:
                    rng.shuffle(diff_gender_syns)
                    for syn, sg in diff_gender_syns:
                        diff_props = dict(target_props, gender=sg)
                        form = self._find_synonym_form(syn["lemma_id"], diff_props, "NOUN")
                        if form and form.lower() != token.lower():
                            all_alternatives.append((self._apply_capitalization(token, form), sg))

                # Fase 4: genere diverso (figurativi)
                if not all_alternatives:
                    rng.shuffle(diff_gender_figurative)
                    for syn, sg in diff_gender_figurative:
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

                # Riattacca clitico postposto se presente (alzarsi → sollevarsi)
                if clitic_suffix and best["pos"] == "VER":
                    chosen_clean = chosen.lower()
                    # Per infiniti: sollevare + si → sollevarsi (rimuovi -e finale)
                    if chosen_clean.endswith(("are", "ere", "ire")):
                        chosen = self._apply_capitalization(token, chosen_clean[:-1] + clitic_suffix)
                    else:
                        chosen = self._apply_capitalization(token, chosen_clean + clitic_suffix)
                    # Aggiorna anche le alternative
                    new_alts = []
                    for alt, g in all_alternatives:
                        a = alt.lower()
                        if a.endswith(("are", "ere", "ire")):
                            new_alts.append((self._apply_capitalization(token, a[:-1] + clitic_suffix), g))
                        else:
                            new_alts.append((self._apply_capitalization(token, a + clitic_suffix), g))
                    all_alternatives = new_alts

                gender_changed = (best["pos"] == "NOUN" and chosen_gender != best.get("gender"))
                result_entry = {
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
                }
                # Per i verbi: salva mood e tense per il post-processing dei participi
                if best["pos"] == "VER":
                    result_entry["mood"] = best.get("mood")
                    result_entry["tense"] = best.get("tense")
                results.append(result_entry)
            else:
                results.append({
                    "original": token,
                    "replacement": token,
                    "replaced": False,
                    "synonyms": [],
                })

        # Post-processing: aggiusta articoli, preposizioni articolate e dimostrativi
        self._postprocess_articles(results)
        # Post-processing: aggiusta participi passati con ausiliare "essere"
        self._postprocess_participles(results)

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

# Carica HTML da file esterno (index.html nella stessa directory)
HTML_PATH = os.path.join(SCRIPT_DIR, "index.html")
with open(HTML_PATH, "r", encoding="utf-8") as _f:
    HTML_PAGE = _f.read()



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
            self.send_header("Cache-Control", "no-cache")
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

    host = os.environ.get("HOST", "127.0.0.1")  # Cloud Run richiede 0.0.0.0
    server = http.server.ThreadingHTTPServer((host, args.port), SinonimizzatoreHandler)
    print(f"\n  Apri: http://localhost:{args.port}")
    print("  Premi Ctrl+C per chiudere.\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nChiuso.")
        server.server_close()


if __name__ == "__main__":
    main()
