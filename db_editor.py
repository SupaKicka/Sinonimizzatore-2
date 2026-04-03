#!/usr/bin/env python3
"""
Editor visuale per il database morphit.db del Sinonimizzatore.
Permette di cercare parole, visualizzare lemmi/flessioni/sinonimi,
e modificare o eliminare entries.

Requisiti: Python 3 con tkinter (incluso di default).
"""

import sqlite3
import os
import tkinter as tk
from tkinter import ttk, messagebox

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "morphit.db")


class DBEditor:

    def __init__(self, root):
        self.root = root
        self.root.title("Sinonimizzatore — DB Editor")
        self.root.geometry("1200x750")
        self.root.minsize(900, 550)

        self.conn = sqlite3.connect(DB_PATH)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.selected_lemma_id = None

        self._build_ui()
        self._load_stats()

    # ── UI ───────────────────────────────────────────────────────────────────

    def _build_ui(self):
        # Stile
        style = ttk.Style()
        style.configure("Treeview", rowheight=24, font=("Segoe UI", 10))
        style.configure("Treeview.Heading", font=("Segoe UI", 10, "bold"))
        style.configure("TLabel", font=("Segoe UI", 10))
        style.configure("TButton", font=("Segoe UI", 10))
        style.configure("Header.TLabel", font=("Segoe UI", 12, "bold"))

        # ── Top bar: ricerca ─────────────────────────────────────────────
        top = ttk.Frame(self.root, padding=8)
        top.pack(fill=tk.X)

        ttk.Label(top, text="Cerca parola:").pack(side=tk.LEFT)
        self.search_var = tk.StringVar()
        self.search_entry = ttk.Entry(top, textvariable=self.search_var, width=30,
                                      font=("Segoe UI", 12))
        self.search_entry.pack(side=tk.LEFT, padx=(6, 4))
        self.search_entry.bind("<Return>", lambda e: self._search())

        ttk.Button(top, text="Cerca", command=self._search).pack(side=tk.LEFT, padx=2)
        ttk.Button(top, text="Cerca lemma esatto", command=self._search_lemma).pack(side=tk.LEFT, padx=2)

        self.stats_label = ttk.Label(top, text="", foreground="#666")
        self.stats_label.pack(side=tk.RIGHT)

        # ── Paned: sinistra (lemmi) | destra (dettagli) ─────────────────
        paned = ttk.PanedWindow(self.root, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))

        # ── Sinistra: lista lemmi ────────────────────────────────────────
        left = ttk.Frame(paned)
        paned.add(left, weight=1)

        ttk.Label(left, text="Lemmi trovati", style="Header.TLabel").pack(anchor=tk.W, pady=(4, 2))

        lemma_frame = ttk.Frame(left)
        lemma_frame.pack(fill=tk.BOTH, expand=True)

        self.lemma_tree = ttk.Treeview(lemma_frame, columns=("id", "lemma", "pos"),
                                       show="headings", selectmode="browse")
        self.lemma_tree.heading("id", text="ID")
        self.lemma_tree.heading("lemma", text="Lemma")
        self.lemma_tree.heading("pos", text="POS")
        self.lemma_tree.column("id", width=50, minwidth=40)
        self.lemma_tree.column("lemma", width=160, minwidth=100)
        self.lemma_tree.column("pos", width=60, minwidth=50)

        lemma_scroll = ttk.Scrollbar(lemma_frame, orient=tk.VERTICAL, command=self.lemma_tree.yview)
        self.lemma_tree.configure(yscrollcommand=lemma_scroll.set)
        self.lemma_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        lemma_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        self.lemma_tree.bind("<<TreeviewSelect>>", self._on_lemma_select)

        # ── Destra: flessioni + sinonimi ─────────────────────────────────
        right = ttk.Frame(paned)
        paned.add(right, weight=3)

        # Info lemma selezionato
        self.lemma_info = ttk.Label(right, text="Seleziona un lemma", style="Header.TLabel")
        self.lemma_info.pack(anchor=tk.W, pady=(4, 2))

        # Notebook con due tab
        notebook = ttk.Notebook(right)
        notebook.pack(fill=tk.BOTH, expand=True)

        # ── Tab Flessioni ────────────────────────────────────────────────
        forms_tab = ttk.Frame(notebook, padding=4)
        notebook.add(forms_tab, text="Flessioni")

        forms_toolbar = ttk.Frame(forms_tab)
        forms_toolbar.pack(fill=tk.X, pady=(0, 4))
        ttk.Button(forms_toolbar, text="Elimina selezionata",
                   command=self._delete_form).pack(side=tk.LEFT, padx=2)
        ttk.Button(forms_toolbar, text="Aggiungi flessione...",
                   command=self._add_form).pack(side=tk.LEFT, padx=2)

        forms_frame = ttk.Frame(forms_tab)
        forms_frame.pack(fill=tk.BOTH, expand=True)

        form_cols = ("id", "form", "pos_full", "gender", "number", "person", "mood", "tense", "degree")
        self.forms_tree = ttk.Treeview(forms_frame, columns=form_cols, show="headings",
                                       selectmode="browse")
        for col in form_cols:
            w = 50 if col in ("id", "gender", "number", "person", "degree") else 100
            self.forms_tree.heading(col, text=col.replace("_", " ").title())
            self.forms_tree.column(col, width=w, minwidth=40)

        forms_scroll = ttk.Scrollbar(forms_frame, orient=tk.VERTICAL, command=self.forms_tree.yview)
        self.forms_tree.configure(yscrollcommand=forms_scroll.set)
        self.forms_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        forms_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # ── Tab Sinonimi ─────────────────────────────────────────────────
        syn_tab = ttk.Frame(notebook, padding=4)
        notebook.add(syn_tab, text="Sinonimi")

        syn_toolbar = ttk.Frame(syn_tab)
        syn_toolbar.pack(fill=tk.X, pady=(0, 4))
        ttk.Button(syn_toolbar, text="Elimina selezionato",
                   command=self._delete_synonym).pack(side=tk.LEFT, padx=2)
        ttk.Button(syn_toolbar, text="Aggiungi sinonimo...",
                   command=self._add_synonym).pack(side=tk.LEFT, padx=2)

        syn_frame = ttk.Frame(syn_tab)
        syn_frame.pack(fill=tk.BOTH, expand=True)

        syn_cols = ("syn_id", "lemma_id", "lemma", "pos", "type", "weight", "source")
        self.syn_tree = ttk.Treeview(syn_frame, columns=syn_cols, show="headings",
                                     selectmode="browse")
        self.syn_tree.heading("syn_id", text="Rel ID")
        self.syn_tree.heading("lemma_id", text="Lemma ID")
        self.syn_tree.heading("lemma", text="Sinonimo")
        self.syn_tree.heading("pos", text="POS")
        self.syn_tree.heading("type", text="Tipo")
        self.syn_tree.heading("weight", text="Peso")
        self.syn_tree.heading("source", text="Fonte")
        self.syn_tree.column("syn_id", width=55, minwidth=40)
        self.syn_tree.column("lemma_id", width=65, minwidth=40)
        self.syn_tree.column("lemma", width=180, minwidth=100)
        self.syn_tree.column("pos", width=55, minwidth=40)
        self.syn_tree.column("type", width=75, minwidth=50)
        self.syn_tree.column("weight", width=50, minwidth=40)
        self.syn_tree.column("source", width=90, minwidth=50)

        syn_scroll = ttk.Scrollbar(syn_frame, orient=tk.VERTICAL, command=self.syn_tree.yview)
        self.syn_tree.configure(yscrollcommand=syn_scroll.set)
        self.syn_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        syn_scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # Double-click su sinonimo → vai a quel lemma
        self.syn_tree.bind("<Double-1>", self._goto_synonym)

        # Focus iniziale
        self.search_entry.focus_set()

    # ── Statistiche ──────────────────────────────────────────────────────

    def _load_stats(self):
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) FROM lemmas")
        nl = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM forms")
        nf = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM synonyms")
        ns = cur.fetchone()[0]
        self.stats_label.config(text=f"{nl:,} lemmi  |  {nf:,} forme  |  {ns:,} sinonimi")

    # ── Ricerca ──────────────────────────────────────────────────────────

    def _search(self):
        """Cerca per forma flessa (qualsiasi forma che contiene la stringa)."""
        q = self.search_var.get().strip()
        if not q:
            return
        cur = self.conn.cursor()
        # Cerca lemmi che hanno una forma che matcha
        cur.execute("""
            SELECT DISTINCT l.id, l.lemma, l.pos
            FROM lemmas l
            JOIN forms f ON f.lemma_id = l.id
            WHERE LOWER(f.form) = LOWER(?)
            ORDER BY l.pos, l.lemma
        """, (q,))
        rows = cur.fetchall()
        # Se nessun match esatto, cerca per prefisso
        if not rows:
            cur.execute("""
                SELECT DISTINCT l.id, l.lemma, l.pos
                FROM lemmas l
                JOIN forms f ON f.lemma_id = l.id
                WHERE LOWER(f.form) LIKE LOWER(? || '%')
                ORDER BY l.pos, l.lemma
                LIMIT 100
            """, (q,))
            rows = cur.fetchall()
        self._populate_lemma_tree(rows)

    def _search_lemma(self):
        """Cerca per lemma esatto."""
        q = self.search_var.get().strip()
        if not q:
            return
        cur = self.conn.cursor()
        cur.execute("""
            SELECT id, lemma, pos FROM lemmas
            WHERE LOWER(lemma) = LOWER(?)
            ORDER BY pos, lemma
        """, (q,))
        rows = cur.fetchall()
        if not rows:
            cur.execute("""
                SELECT id, lemma, pos FROM lemmas
                WHERE LOWER(lemma) LIKE LOWER(? || '%')
                ORDER BY pos, lemma
                LIMIT 100
            """, (q,))
            rows = cur.fetchall()
        self._populate_lemma_tree(rows)

    def _populate_lemma_tree(self, rows):
        self.lemma_tree.delete(*self.lemma_tree.get_children())
        for r in rows:
            self.lemma_tree.insert("", tk.END, values=r)
        if rows:
            first = self.lemma_tree.get_children()[0]
            self.lemma_tree.selection_set(first)
            self.lemma_tree.focus(first)

    # ── Selezione lemma ─────────────────────────────────────────────────

    def _on_lemma_select(self, event=None):
        sel = self.lemma_tree.selection()
        if not sel:
            return
        vals = self.lemma_tree.item(sel[0], "values")
        lemma_id = int(vals[0])
        lemma = vals[1]
        pos = vals[2]
        self.selected_lemma_id = lemma_id
        self.lemma_info.config(text=f"{lemma}  ({pos})  — ID {lemma_id}")
        self._load_forms(lemma_id)
        self._load_synonyms(lemma_id)

    def _load_forms(self, lemma_id):
        self.forms_tree.delete(*self.forms_tree.get_children())
        cur = self.conn.cursor()
        cur.execute("""
            SELECT id, form, pos_full, gender, number, person, mood, tense, degree
            FROM forms WHERE lemma_id = ?
            ORDER BY mood, tense, person, gender, number, form
        """, (lemma_id,))
        for r in cur.fetchall():
            display = tuple("—" if v is None else v for v in r)
            self.forms_tree.insert("", tk.END, values=display)

    def _load_synonyms(self, lemma_id):
        self.syn_tree.delete(*self.syn_tree.get_children())
        cur = self.conn.cursor()
        # Sinonimi bidirezionali (il lemma può essere in lemma_id_1 o lemma_id_2)
        cur.execute("""
            SELECT s.id,
                   CASE WHEN s.lemma_id_1 = ? THEN s.lemma_id_2 ELSE s.lemma_id_1 END AS other_id,
                   l.lemma, l.pos, s.type, s.weight, s.source
            FROM synonyms s
            JOIN lemmas l ON l.id = CASE WHEN s.lemma_id_1 = ? THEN s.lemma_id_2 ELSE s.lemma_id_1 END
            WHERE s.lemma_id_1 = ? OR s.lemma_id_2 = ?
            ORDER BY l.lemma
        """, (lemma_id, lemma_id, lemma_id, lemma_id))
        for r in cur.fetchall():
            display = tuple("—" if v is None else v for v in r)
            self.syn_tree.insert("", tk.END, values=display)

    # ── Navigazione sinonimi ─────────────────────────────────────────────

    def _goto_synonym(self, event=None):
        """Double-click su un sinonimo → seleziona quel lemma."""
        sel = self.syn_tree.selection()
        if not sel:
            return
        vals = self.syn_tree.item(sel[0], "values")
        target_id = int(vals[1])
        target_lemma = vals[2]

        # Cerca il lemma target
        cur = self.conn.cursor()
        cur.execute("SELECT id, lemma, pos FROM lemmas WHERE id = ?", (target_id,))
        row = cur.fetchone()
        if row:
            self.search_var.set(target_lemma)
            self._search_lemma()
            # Seleziona il lemma con l'ID esatto
            for item in self.lemma_tree.get_children():
                if int(self.lemma_tree.item(item, "values")[0]) == target_id:
                    self.lemma_tree.selection_set(item)
                    self.lemma_tree.focus(item)
                    self.lemma_tree.see(item)
                    break

    # ── Eliminazione ─────────────────────────────────────────────────────

    def _delete_form(self):
        sel = self.forms_tree.selection()
        if not sel:
            messagebox.showinfo("Info", "Seleziona una flessione da eliminare.")
            return
        vals = self.forms_tree.item(sel[0], "values")
        form_id = int(vals[0])
        form_text = vals[1]
        if not messagebox.askyesno("Conferma", f"Eliminare la flessione \"{form_text}\" (ID {form_id})?"):
            return
        self.conn.execute("DELETE FROM forms WHERE id = ?", (form_id,))
        self.conn.commit()
        self._load_forms(self.selected_lemma_id)
        self._load_stats()

    def _delete_synonym(self):
        sel = self.syn_tree.selection()
        if not sel:
            messagebox.showinfo("Info", "Seleziona un sinonimo da eliminare.")
            return
        vals = self.syn_tree.item(sel[0], "values")
        syn_id = int(vals[0])
        syn_lemma = vals[2]
        if not messagebox.askyesno("Conferma", f"Eliminare il sinonimo \"{syn_lemma}\" (rel ID {syn_id})?"):
            return
        self.conn.execute("DELETE FROM synonyms WHERE id = ?", (syn_id,))
        self.conn.commit()
        self._load_synonyms(self.selected_lemma_id)
        self._load_stats()

    # ── Aggiunta ─────────────────────────────────────────────────────────

    def _add_form(self):
        if not self.selected_lemma_id:
            messagebox.showinfo("Info", "Seleziona prima un lemma.")
            return
        AddFormDialog(self.root, self.conn, self.selected_lemma_id,
                      on_done=lambda: (self._load_forms(self.selected_lemma_id), self._load_stats()))

    def _add_synonym(self):
        if not self.selected_lemma_id:
            messagebox.showinfo("Info", "Seleziona prima un lemma.")
            return
        AddSynonymDialog(self.root, self.conn, self.selected_lemma_id,
                         on_done=lambda: (self._load_synonyms(self.selected_lemma_id), self._load_stats()))


# ── Dialog: Aggiungi flessione ───────────────────────────────────────────────

class AddFormDialog:
    def __init__(self, parent, conn, lemma_id, on_done=None):
        self.conn = conn
        self.lemma_id = lemma_id
        self.on_done = on_done

        self.win = tk.Toplevel(parent)
        self.win.title("Aggiungi flessione")
        self.win.geometry("420x340")
        self.win.grab_set()

        f = ttk.Frame(self.win, padding=12)
        f.pack(fill=tk.BOTH, expand=True)

        fields = [
            ("Forma flessa:", "form"),
            ("POS completo:", "pos_full", "es. VER:ind+pres+3+s"),
            ("Genere (m/f):", "gender"),
            ("Numero (s/p):", "number"),
            ("Persona (1/2/3):", "person"),
            ("Modo:", "mood", "ind/sub/cond/inf/part/ger/impr"),
            ("Tempo:", "tense", "pres/past/impf/fut"),
            ("Grado:", "degree", "pos/comp/sup/dim"),
        ]
        self.entries = {}
        for i, item in enumerate(fields):
            label = item[0]
            key = item[1]
            hint = item[2] if len(item) > 2 else ""
            ttk.Label(f, text=label).grid(row=i, column=0, sticky=tk.W, pady=2)
            e = ttk.Entry(f, width=25)
            e.grid(row=i, column=1, sticky=tk.W, padx=(6, 0), pady=2)
            if hint:
                ttk.Label(f, text=hint, foreground="#888").grid(row=i, column=2, sticky=tk.W, padx=(4, 0))
            self.entries[key] = e

        ttk.Button(f, text="Aggiungi", command=self._save).grid(row=len(fields), column=1, pady=12, sticky=tk.W)

    def _save(self):
        form = self.entries["form"].get().strip()
        pos_full = self.entries["pos_full"].get().strip()
        if not form or not pos_full:
            messagebox.showwarning("Attenzione", "Forma e POS completo sono obbligatori.", parent=self.win)
            return

        def val(key):
            v = self.entries[key].get().strip()
            return v if v else None

        self.conn.execute("""
            INSERT INTO forms (lemma_id, form, pos_full, gender, number, person, mood, tense, degree)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (self.lemma_id, form, pos_full, val("gender"), val("number"),
              val("person"), val("mood"), val("tense"), val("degree")))
        self.conn.commit()
        if self.on_done:
            self.on_done()
        self.win.destroy()


# ── Dialog: Aggiungi sinonimo ────────────────────────────────────────────────

class AddSynonymDialog:
    def __init__(self, parent, conn, lemma_id, on_done=None):
        self.conn = conn
        self.lemma_id = lemma_id
        self.on_done = on_done

        # Recupera info lemma corrente
        cur = conn.cursor()
        cur.execute("SELECT lemma, pos FROM lemmas WHERE id = ?", (lemma_id,))
        self.current_lemma, self.current_pos = cur.fetchone()

        self.win = tk.Toplevel(parent)
        self.win.title(f"Aggiungi sinonimo per \"{self.current_lemma}\" ({self.current_pos})")
        self.win.geometry("500x400")
        self.win.grab_set()

        f = ttk.Frame(self.win, padding=12)
        f.pack(fill=tk.BOTH, expand=True)

        ttk.Label(f, text="Cerca lemma sinonimo:").pack(anchor=tk.W)
        search_frame = ttk.Frame(f)
        search_frame.pack(fill=tk.X, pady=(2, 6))
        self.syn_search = ttk.Entry(search_frame, width=30, font=("Segoe UI", 11))
        self.syn_search.pack(side=tk.LEFT)
        self.syn_search.bind("<Return>", lambda e: self._search_syn())
        ttk.Button(search_frame, text="Cerca", command=self._search_syn).pack(side=tk.LEFT, padx=4)

        # Risultati
        list_frame = ttk.Frame(f)
        list_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 6))
        self.result_tree = ttk.Treeview(list_frame, columns=("id", "lemma", "pos"),
                                        show="headings", selectmode="browse")
        self.result_tree.heading("id", text="ID")
        self.result_tree.heading("lemma", text="Lemma")
        self.result_tree.heading("pos", text="POS")
        self.result_tree.column("id", width=50)
        self.result_tree.column("lemma", width=200)
        self.result_tree.column("pos", width=60)
        scroll = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.result_tree.yview)
        self.result_tree.configure(yscrollcommand=scroll.set)
        self.result_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scroll.pack(side=tk.RIGHT, fill=tk.Y)

        # Tipo e peso
        opts = ttk.Frame(f)
        opts.pack(fill=tk.X, pady=(0, 6))
        ttk.Label(opts, text="Tipo:").pack(side=tk.LEFT)
        self.type_var = tk.StringVar(value="synonym")
        ttk.Combobox(opts, textvariable=self.type_var, width=10,
                     values=["synonym", "antonym", "hypernym", "hyponym", "related"],
                     state="readonly").pack(side=tk.LEFT, padx=(4, 12))
        ttk.Label(opts, text="Peso:").pack(side=tk.LEFT)
        self.weight_var = tk.StringVar(value="1.0")
        ttk.Entry(opts, textvariable=self.weight_var, width=6).pack(side=tk.LEFT, padx=4)

        ttk.Button(f, text="Aggiungi sinonimo", command=self._save).pack(anchor=tk.W)
        self.syn_search.focus_set()

    def _search_syn(self):
        q = self.syn_search.get().strip()
        if not q:
            return
        cur = self.conn.cursor()
        # Filtra per stesso POS del lemma corrente
        cur.execute("""
            SELECT id, lemma, pos FROM lemmas
            WHERE LOWER(lemma) LIKE LOWER(? || '%') AND pos = ?
            ORDER BY lemma
            LIMIT 50
        """, (q, self.current_pos))
        rows = cur.fetchall()
        self.result_tree.delete(*self.result_tree.get_children())
        for r in rows:
            self.result_tree.insert("", tk.END, values=r)

    def _save(self):
        sel = self.result_tree.selection()
        if not sel:
            messagebox.showinfo("Info", "Seleziona un lemma dalla lista.", parent=self.win)
            return
        vals = self.result_tree.item(sel[0], "values")
        other_id = int(vals[0])
        other_lemma = vals[1]

        if other_id == self.lemma_id:
            messagebox.showwarning("Attenzione", "Non puoi aggiungere un lemma come sinonimo di se stesso.",
                                   parent=self.win)
            return

        # Ordina gli ID per rispettare CHECK(lemma_id_1 < lemma_id_2)
        id1, id2 = min(self.lemma_id, other_id), max(self.lemma_id, other_id)
        rel_type = self.type_var.get()
        try:
            weight = float(self.weight_var.get())
        except ValueError:
            weight = 1.0

        try:
            self.conn.execute("""
                INSERT INTO synonyms (lemma_id_1, lemma_id_2, type, weight, source)
                VALUES (?, ?, ?, ?, 'manual')
            """, (id1, id2, rel_type, weight))
            self.conn.commit()
        except sqlite3.IntegrityError:
            messagebox.showinfo("Info", f"Sinonimo \"{other_lemma}\" esiste già.", parent=self.win)
            return

        if self.on_done:
            self.on_done()
        self.win.destroy()


# ── Main ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    root = tk.Tk()
    app = DBEditor(root)
    root.mainloop()
