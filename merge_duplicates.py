#!/usr/bin/env python3
"""
Merge lemmi duplicati nel database morphit.db.

Problema: gli scraper hanno creato lemmi separati per ogni forma flessa
(es. "vicino", "vicina", "vicini", "vicine" come 4 lemmi NOUN distinti),
quando dovrebbe essere un solo lemma "vicino" con 4 flessioni.

Strategia:
  1. Trova cluster di lemmi dello stesso POS che condividono forme flesse
  2. Per ogni cluster, sceglie un lemma canonico (quello con più sinonimi,
     poi più flessioni, poi nome m/s per NOUN)
  3. Migra i sinonimi dei duplicati al canonico (rimappando i target
     attraverso i loro canonici per evitare puntamenti a lemmi eliminati)
  4. Aggiunge flessioni mancanti al canonico
  5. Elimina lemmi e forme duplicati

Uso:
    python merge_duplicates.py              # dry-run: mostra cosa farebbe
    python merge_duplicates.py --apply      # applica le modifiche
    python merge_duplicates.py --verbose    # mostra dettagli per ogni cluster
"""

import sqlite3
import os
import sys
import argparse
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "morphit.db")


def load_data(conn):
    """Carica tutti i dati in memoria per velocità."""
    cur = conn.cursor()

    print("Caricamento dati...", flush=True)

    cur.execute("SELECT id, lemma, pos FROM lemmas")
    lemmas = {r[0]: {"lemma": r[1], "pos": r[2]} for r in cur.fetchall()}
    print(f"  {len(lemmas):,} lemmi")

    cur.execute("""
        SELECT id, lemma_id, form, gender, number, person, mood, tense, degree, pos_full
        FROM forms WHERE lemma_id IN (SELECT id FROM lemmas)
    """)
    forms_by_lemma = defaultdict(list)
    for r in cur.fetchall():
        if r[1] not in lemmas:
            continue
        forms_by_lemma[r[1]].append({
            "id": r[0], "form": r[2], "gender": r[3], "number": r[4],
            "person": r[5], "mood": r[6], "tense": r[7], "degree": r[8],
            "pos_full": r[9],
        })
    print(f"  {sum(len(v) for v in forms_by_lemma.values()):,} forme")

    cur.execute("SELECT id, lemma_id_1, lemma_id_2, type, weight, source FROM synonyms")
    synonyms = []
    syn_by_lemma = defaultdict(list)
    for r in cur.fetchall():
        entry = {"id": r[0], "l1": r[1], "l2": r[2], "type": r[3], "weight": r[4], "source": r[5]}
        synonyms.append(entry)
        syn_by_lemma[r[1]].append(entry)
        syn_by_lemma[r[2]].append(entry)
    print(f"  {len(synonyms):,} relazioni sinonimiche")

    return lemmas, forms_by_lemma, syn_by_lemma


def find_clusters(lemmas, forms_by_lemma, syn_by_lemma, pos_filter):
    """Trova cluster di lemmi duplicati per un dato POS.

    Per NOUN/ADJ: due lemmi sono collegati se condividono una forma flessa.
    Per VER: NON usare forme condivise (verbi diversi condividono forme per
    coincidenza: "agita" = agire 3s ind = agitare 2s impr). Invece, collega
    solo se il NOME di un lemma è una forma flessa dell'altro e ha poche
    flessioni (es. "scompare" VER con 1 forma → forma di "scomparire" VER).
    """
    # Indice: (form_lower, pos) -> set(lemma_id)
    form_to_lemmas = defaultdict(set)
    for lid, info in lemmas.items():
        if info["pos"] != pos_filter:
            continue
        for f in forms_by_lemma.get(lid, []):
            form_to_lemmas[f["form"].lower()].add(lid)

    graph = defaultdict(set)

    # Regola: due lemmi sono duplicati solo se il NOME di uno è una FORMA FLESSA
    # dell'altro E condividono una radice comune. Questo evita falsi positivi dove
    # un lemma-ponte (es. "arti") collega lemmi diversi ("arto" e "arte").
    for lid, info in lemmas.items():
        if info["pos"] != pos_filter:
            continue
        # Per i VER: solo lemmi con poche flessioni (altrimenti è un verbo reale)
        if pos_filter == "VER" and len(forms_by_lemma.get(lid, [])) > 3:
            continue
        lemma_lower = info["lemma"].lower()
        # Il nome di questo lemma è una forma di quale altro lemma?
        candidate_lids = form_to_lemmas.get(lemma_lower, set())
        for other_lid in candidate_lids:
            if other_lid == lid:
                continue
            if lemmas[other_lid]["pos"] != pos_filter:
                continue
            other_lower = lemmas[other_lid]["lemma"].lower()
            if other_lower == lemma_lower:
                continue  # stesso nome esatto
            # Verifica che i nomi siano varianti morfologiche dello stesso lemma.
            # I duplicati reali differiscono solo per desinenza di genere/numero:
            #   vicino/vicina/vicini/vicine (radice: vicin-)
            #   alto/alta/alti/alte (radice: alt-)
            # Falsi positivi hanno radici diverse:
            #   arto/arte (art+o vs art+e ma lemmi diversi)
            #   asso/asse (ass+o vs ass+e ma lemmi diversi)
            # Regola: i nomi devono condividere tutto tranne le ultime 1-2 lettere
            # (desinenze tipiche: -o/-a/-i/-e, -ore/-rice, -ino/-ina, ecc.)
            # e la parte condivisa deve essere almeno 3 caratteri.
            min_len = min(len(lemma_lower), len(other_lower))
            common = 0
            for c1, c2 in zip(lemma_lower, other_lower):
                if c1 == c2:
                    common += 1
                else:
                    break
            # La radice comune deve essere almeno min_len - 2 (max 2 caratteri diversi)
            # e almeno 3 caratteri di radice
            tail_diff = max(len(lemma_lower), len(other_lower)) - common
            if common < 3 or tail_diff > 3:
                continue
            # Extra: se i nomi differiscono di una sola lettera nel mezzo
            # (arto/arte, asso/asse) e hanno lunghezza uguale, rifiuta
            if len(lemma_lower) == len(other_lower) and common == len(lemma_lower) - 1:
                # Differenza di 1 solo carattere finale: potrebbe essere ok (alto/alta)
                # o no (arto/arte). Accetta solo se entrambi i caratteri finali
                # sono desinenze italiane standard (genere + numero: o/a/i/e)
                l_end = lemma_lower[common:]
                o_end = other_lower[common:]
                if l_end not in "aoie" or o_end not in "aoie":
                    continue
            # lid.lemma è una forma di other con radice simile → collegali
            graph[lid].add(other_lid)
            graph[other_lid].add(lid)

    # Costruisci cluster diretti, NON transitivi (no BFS).
    # Motivo: la BFS creerebbe catene spurie via lemmi-ponte.
    # Es: "calci" è forma sia di "calce" che di "calcio" → BFS li unirebbe
    # in un unico cluster, ma sono parole diverse.
    #
    # Strategia: ogni lemma duplicato si collega al suo "originale" migliore
    # (quello con più sinonimi/forme). Poi raggruppa per originale.
    dup_to_best_orig = {}  # dup_id -> orig_id
    for lid in graph:
        info = lemmas[lid]
        nforms = len(forms_by_lemma.get(lid, []))
        nsyn = len(syn_by_lemma.get(lid, []))

        # Tra i vicini nel grafo, trova quello che è il miglior "originale"
        # (cioè quello con più sinonimi e forme — il più completo)
        best_orig = None
        best_score = (-1, -1)
        for neighbor in graph[lid]:
            n_nsyn = len(syn_by_lemma.get(neighbor, []))
            n_nforms = len(forms_by_lemma.get(neighbor, []))
            score = (n_nsyn, n_nforms)
            if score > best_score:
                best_score = score
                best_orig = neighbor

        # lid è il duplicato solo se l'originale è "più ricco"
        if best_orig and best_score > (nsyn, nforms):
            dup_to_best_orig[lid] = best_orig

    # Raggruppa per originale
    orig_to_dups = defaultdict(set)
    for dup_id, orig_id in dup_to_best_orig.items():
        orig_to_dups[orig_id].add(dup_id)

    clusters = []
    for orig_id, dups in orig_to_dups.items():
        cluster = dups | {orig_id}
        clusters.append(cluster)

    return clusters


def pick_canonical(cluster, lemmas, forms_by_lemma, syn_by_lemma):
    """Sceglie il lemma canonico di un cluster.

    Priorità:
    1. Più sinonimi (preserva la rete più ricca)
    2. Più flessioni
    3. Per NOUN: nome in forma m/s o f/s (non plurale)
    4. ID più basso (probabilmente da morphit originale)
    """
    def score(lid):
        info = lemmas[lid]
        forms = forms_by_lemma.get(lid, [])
        nsyn = len(syn_by_lemma.get(lid, []))
        nforms = len(forms)

        # Bonus per forma canonica del nome
        name = info["lemma"]
        is_canonical_name = 0
        for f in forms:
            if f["form"].lower() == name.lower():
                if f["number"] == "s":
                    is_canonical_name = 2  # singolare
                elif f["gender"] and f["number"]:
                    is_canonical_name = 1

        return (nsyn, nforms, is_canonical_name, -lid)

    return max(cluster, key=score)


def plan_merge(clusters, lemmas, forms_by_lemma, syn_by_lemma):
    """Pianifica il merge di tutti i cluster. Ritorna le azioni da eseguire."""
    # Mappa: lemma_id duplicato -> lemma_id canonico
    dup_to_canonical = {}

    for cluster in clusters:
        canonical = pick_canonical(cluster, lemmas, forms_by_lemma, syn_by_lemma)
        for lid in cluster:
            if lid != canonical:
                dup_to_canonical[lid] = canonical

    # Pianifica le azioni
    actions = {
        "synonym_deletes": [],      # ID di synonyms da eliminare
        "synonym_inserts": [],      # (l1, l2, type, weight, source) da inserire
        "form_inserts": [],         # (lemma_id, form, pos_full, g, n, p, mood, tense, degree)
        "form_deletes": [],         # ID di forms da eliminare
        "lemma_deletes": [],        # ID di lemmas da eliminare
    }

    # Raccolta sinonimi esistenti per il canonico (per evitare duplicati)
    canonical_existing_syns = defaultdict(set)  # canonical_id -> set(other_canonical_id)
    for cluster in clusters:
        canonical = pick_canonical(cluster, lemmas, forms_by_lemma, syn_by_lemma)
        for syn in syn_by_lemma.get(canonical, []):
            other = syn["l2"] if syn["l1"] == canonical else syn["l1"]
            # Rimappa other al suo canonico
            other_canonical = dup_to_canonical.get(other, other)
            canonical_existing_syns[canonical].add(other_canonical)

    seen_synonym_pairs = set()  # (min_id, max_id) già pianificati

    # Pre-popola con i sinonimi esistenti del canonico
    for canonical_id, others in canonical_existing_syns.items():
        for other in others:
            pair = (min(canonical_id, other), max(canonical_id, other))
            seen_synonym_pairs.add(pair)

    for dup_id, canonical_id in dup_to_canonical.items():
        # 1. Migra sinonimi del duplicato al canonico
        for syn in syn_by_lemma.get(dup_id, []):
            other = syn["l2"] if syn["l1"] == dup_id else syn["l1"]

            # Rimappa other al suo canonico (potrebbe anche lui essere un duplicato)
            other_canonical = dup_to_canonical.get(other, other)

            # Non creare auto-sinonimi
            if other_canonical == canonical_id:
                actions["synonym_deletes"].append(syn["id"])
                continue

            # Controlla se questa relazione esiste già
            pair = (min(canonical_id, other_canonical), max(canonical_id, other_canonical))
            if pair in seen_synonym_pairs:
                actions["synonym_deletes"].append(syn["id"])
                continue

            seen_synonym_pairs.add(pair)
            actions["synonym_deletes"].append(syn["id"])
            actions["synonym_inserts"].append((
                pair[0], pair[1], syn["type"], syn["weight"], syn["source"]
            ))

        # 2. Verifica flessioni del canonico: aggiungi quelle mancanti
        canonical_forms = forms_by_lemma.get(canonical_id, [])
        canonical_form_keys = set()
        for f in canonical_forms:
            key = (f["form"].lower(), f["gender"], f["number"], f["person"],
                   f["mood"], f["tense"], f["degree"])
            canonical_form_keys.add(key)

        for f in forms_by_lemma.get(dup_id, []):
            key = (f["form"].lower(), f["gender"], f["number"], f["person"],
                   f["mood"], f["tense"], f["degree"])
            if key not in canonical_form_keys:
                # Determina il pos_full corretto
                pos_full = f["pos_full"]
                actions["form_inserts"].append((
                    canonical_id, f["form"], pos_full,
                    f["gender"], f["number"], f["person"],
                    f["mood"], f["tense"], f["degree"]
                ))
                canonical_form_keys.add(key)

        # 3. Elimina forme e lemma duplicato
        for f in forms_by_lemma.get(dup_id, []):
            actions["form_deletes"].append(f["id"])
        actions["lemma_deletes"].append(dup_id)

    # Deduplica
    actions["synonym_deletes"] = list(set(actions["synonym_deletes"]))

    return actions, dup_to_canonical


def print_summary(actions, dup_to_canonical, lemmas, verbose=False):
    """Stampa un riepilogo delle azioni pianificate."""
    print(f"\n{'='*60}")
    print(f"RIEPILOGO MERGE")
    print(f"{'='*60}")
    print(f"  Lemmi da eliminare:    {len(actions['lemma_deletes']):,}")
    print(f"  Forme da eliminare:    {len(actions['form_deletes']):,}")
    print(f"  Sinonimi da eliminare: {len(actions['synonym_deletes']):,}")
    print(f"  Forme da aggiungere:   {len(actions['form_inserts']):,}")
    print(f"  Sinonimi da migrare:   {len(actions['synonym_inserts']):,}")

    if verbose:
        print(f"\n--- Dettaglio lemmi eliminati (primi 30) ---")
        for lid in sorted(actions["lemma_deletes"])[:30]:
            canonical = dup_to_canonical[lid]
            print(f"  \"{lemmas[lid]['lemma']}\" (ID {lid}) -> merge in "
                  f"\"{lemmas[canonical]['lemma']}\" (ID {canonical})")


def apply_actions(conn, actions):
    """Applica le azioni al database."""
    cur = conn.cursor()

    print("\nApplicazione modifiche...", flush=True)

    # 1. Elimina sinonimi vecchi
    if actions["synonym_deletes"]:
        # Batch delete per velocità
        batch_size = 500
        ids = actions["synonym_deletes"]
        for i in range(0, len(ids), batch_size):
            batch = ids[i:i+batch_size]
            placeholders = ",".join("?" * len(batch))
            cur.execute(f"DELETE FROM synonyms WHERE id IN ({placeholders})", batch)
        print(f"  Eliminati {len(ids):,} sinonimi vecchi")

    # 2. Inserisci sinonimi migrati
    if actions["synonym_inserts"]:
        inserted = 0
        for l1, l2, stype, weight, source in actions["synonym_inserts"]:
            try:
                cur.execute("""
                    INSERT OR IGNORE INTO synonyms (lemma_id_1, lemma_id_2, type, weight, source)
                    VALUES (?, ?, ?, ?, ?)
                """, (l1, l2, stype, weight, source))
                inserted += cur.rowcount
            except sqlite3.IntegrityError:
                pass
        print(f"  Inseriti {inserted:,} sinonimi migrati")

    # 3. Inserisci forme mancanti
    if actions["form_inserts"]:
        for lemma_id, form, pos_full, g, n, p, mood, tense, degree in actions["form_inserts"]:
            cur.execute("""
                INSERT INTO forms (lemma_id, form, pos_full, gender, number, person, mood, tense, degree)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (lemma_id, form, pos_full, g, n, p, mood, tense, degree))
        print(f"  Aggiunte {len(actions['form_inserts']):,} forme mancanti")

    # 4. Elimina forme dei duplicati
    if actions["form_deletes"]:
        ids = actions["form_deletes"]
        for i in range(0, len(ids), batch_size):
            batch = ids[i:i+batch_size]
            placeholders = ",".join("?" * len(batch))
            cur.execute(f"DELETE FROM forms WHERE id IN ({placeholders})", batch)
        print(f"  Eliminate {len(ids):,} forme duplicate")

    # 5. Elimina lemmi duplicati
    if actions["lemma_deletes"]:
        ids = actions["lemma_deletes"]
        for i in range(0, len(ids), batch_size):
            batch = ids[i:i+batch_size]
            placeholders = ",".join("?" * len(batch))
            cur.execute(f"DELETE FROM lemmas WHERE id IN ({placeholders})", batch)
        print(f"  Eliminati {len(ids):,} lemmi duplicati")

    # 6. Pulizia: elimina sinonimi orfani (che puntano a lemmi eliminati)
    cur.execute("""
        DELETE FROM synonyms
        WHERE lemma_id_1 NOT IN (SELECT id FROM lemmas)
           OR lemma_id_2 NOT IN (SELECT id FROM lemmas)
    """)
    orphans = cur.rowcount
    if orphans:
        print(f"  Eliminati {orphans:,} sinonimi orfani")

    conn.commit()
    print("  Commit completato!")


def verify(conn):
    """Verifica l'integrità post-merge."""
    cur = conn.cursor()
    print("\nVerifica integrità...", flush=True)

    cur.execute("SELECT COUNT(*) FROM lemmas")
    nl = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM forms")
    nf = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM synonyms")
    ns = cur.fetchone()[0]
    print(f"  DB: {nl:,} lemmi | {nf:,} forme | {ns:,} sinonimi")

    # Sinonimi orfani
    cur.execute("""
        SELECT COUNT(*) FROM synonyms
        WHERE lemma_id_1 NOT IN (SELECT id FROM lemmas)
           OR lemma_id_2 NOT IN (SELECT id FROM lemmas)
    """)
    orphans = cur.fetchone()[0]
    print(f"  Sinonimi orfani: {orphans}")

    # Forme orfane
    cur.execute("""
        SELECT COUNT(*) FROM forms
        WHERE lemma_id NOT IN (SELECT id FROM lemmas)
    """)
    orphan_forms = cur.fetchone()[0]
    print(f"  Forme orfane: {orphan_forms}")

    # Cluster residui
    for pos in ["NOUN", "ADJ"]:
        form_to_lemmas = defaultdict(set)
        cur.execute("""
            SELECT f.form, f.lemma_id FROM forms f
            JOIN lemmas l ON f.lemma_id = l.id
            WHERE l.pos = ?
        """, (pos,))
        for form, lid in cur.fetchall():
            form_to_lemmas[form.lower()].add(lid)
        shared = sum(1 for lids in form_to_lemmas.values() if len(lids) > 1)
        print(f"  {pos}: {shared} forme ancora condivise tra lemmi diversi")


def main():
    parser = argparse.ArgumentParser(description="Merge lemmi duplicati in morphit.db")
    parser.add_argument("--apply", action="store_true", help="Applica le modifiche (default: dry-run)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Mostra dettagli")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")

    lemmas, forms_by_lemma, syn_by_lemma = load_data(conn)

    all_actions = {
        "synonym_deletes": [],
        "synonym_inserts": [],
        "form_inserts": [],
        "form_deletes": [],
        "lemma_deletes": [],
    }
    all_dup_to_canonical = {}

    for pos in ["NOUN", "ADJ", "VER"]:
        print(f"\n--- Analisi {pos} ---", flush=True)
        clusters = find_clusters(lemmas, forms_by_lemma, syn_by_lemma, pos)
        print(f"  {len(clusters)} cluster trovati")

        if not clusters:
            continue

        actions, dup_to_canonical = plan_merge(clusters, lemmas, forms_by_lemma, syn_by_lemma)

        for key in all_actions:
            all_actions[key].extend(actions[key])
        all_dup_to_canonical.update(dup_to_canonical)

        print_summary(actions, dup_to_canonical, lemmas, verbose=args.verbose)

    # Riepilogo totale
    print(f"\n{'='*60}")
    print(f"TOTALE")
    print(f"{'='*60}")
    print(f"  Lemmi da eliminare:    {len(all_actions['lemma_deletes']):,}")
    print(f"  Forme da eliminare:    {len(all_actions['form_deletes']):,}")
    print(f"  Sinonimi da eliminare: {len(all_actions['synonym_deletes']):,}")
    print(f"  Forme da aggiungere:   {len(all_actions['form_inserts']):,}")
    print(f"  Sinonimi da migrare:   {len(all_actions['synonym_inserts']):,}")

    if args.apply:
        # Backup
        import shutil
        backup_path = DB_PATH + ".pre-merge.bak"
        if not os.path.exists(backup_path):
            shutil.copy2(DB_PATH, backup_path)
            print(f"\nBackup creato: {backup_path}")

        apply_actions(conn, all_actions)
        verify(conn)
    else:
        print(f"\n*** DRY RUN — nessuna modifica applicata ***")
        print(f"*** Usa --apply per applicare le modifiche ***")

    conn.close()


if __name__ == "__main__":
    main()
