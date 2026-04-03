#!/usr/bin/env python3
"""
Dashboard unificata per gli scraper di sinonimi (Wiktionary + Treccani).

Interfaccia web per controllare entrambi gli scraper con:
  - Start/Stop per ogni source
  - Worker e delay regolabili in tempo reale
  - Monitoraggio CPU
  - Dry-run mode
  - Test singola parola
  - Log in tempo reale
  - Statistiche DB

Requisiti:
    pip install beautifulsoup4   (per i parser dei scraper)

Uso:
    python scraper_dashboard.py              # http://localhost:8060
    python scraper_dashboard.py --port 9000  # porta custom
"""

import sqlite3
import http.server
import json
import os
import sys
import threading
import time
import argparse
import re
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, parse_qs

# ─── Configurazione ──────────────────────────────────────────────────────────

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "morphit.db")

# Import parser e fetcher dai scraper esistenti
sys.path.insert(0, SCRIPT_DIR)
from scrape_wiktionary import (
    parse_wiktionary_synonyms,
    fetch_one_sync as wiktionary_fetch,
    build_lemma_lookup,
    get_lemmas_without_synonyms,
    ensure_lemma_exists,
    normalize_reflexive,
    get_all_lemmas,
    insert_synonym as wiktionary_insert,
    process_results as wiktionary_process,
    load_state as wiktionary_load_state,
    save_state as wiktionary_save_state,
    STATE_PATH as WIKTIONARY_STATE_PATH,
)
from scrape_treccani import (
    parse_treccani_synonyms,
    fetch_one_sync as treccani_fetch,
    insert_synonym as treccani_insert,
    process_results as treccani_process,
    load_state as treccani_load_state,
    save_state as treccani_save_state,
    STATE_PATH as TRECCANI_STATE_PATH,
)

# CPU monitoring
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False


# ─── Scraper Manager ─────────────────────────────────────────────────────────

class ScraperInstance:
    """Gestisce un singolo scraper (wiktionary o treccani) in background."""

    def __init__(self, source, fetch_fn, process_fn, load_state_fn, save_state_fn,
                 state_path, insert_fn):
        self.source = source
        self.fetch_fn = fetch_fn
        self.process_fn = process_fn
        self.load_state_fn = load_state_fn
        self.save_state_fn = save_state_fn
        self.state_path = state_path
        self.insert_fn = insert_fn

        self.running = False
        self.dry_run = False
        self.target_workers = 3 if source == "wiktionary" else 2
        self.current_workers = self.target_workers
        self.target_delay = 0.5 if source == "wiktionary" else 1.0
        self.current_delay = self.target_delay

        self.stats = {"total": 0, "found": 0, "not_found": 0, "synonyms_added": 0, "lemmas_created": 0}
        self.total_words = 0
        self.processed_count = 0
        self.start_time = None
        self.thread = None
        self.log = deque(maxlen=200)
        self._lock = threading.Lock()

    def _add_log(self, msg):
        with self._lock:
            self.log.append({"time": time.strftime("%H:%M:%S"), "msg": msg})

    def start(self, conn, lemma_lookup, words, workers, delay, dry_run):
        if self.running:
            return
        self.target_workers = workers
        self.current_workers = workers
        self.target_delay = delay
        self.current_delay = delay
        self.dry_run = dry_run
        self.stats = {"total": 0, "found": 0, "not_found": 0, "synonyms_added": 0, "lemmas_created": 0}
        self.start_time = time.time()
        self.running = True

        # Load state for resume
        state = self.load_state_fn()
        already = set(state.get("processed", []))
        self.words_to_process = [w for w in words if w not in already]
        self.total_words = len(self.words_to_process)
        self.processed_count = 0

        self._add_log(f"Avvio {'(dry-run) ' if dry_run else ''}— {self.total_words} parole, {workers} worker, delay {delay}s")

        # Dry-run file
        self.dry_run_file = None
        if dry_run:
            path = os.path.join(SCRIPT_DIR, f"{self.source}_dry_run.txt")
            self.dry_run_file = open(path, "w", encoding="utf-8")

        self.thread = threading.Thread(
            target=self._run_loop, args=(conn, lemma_lookup, state), daemon=True
        )
        self.thread.start()

    def stop(self):
        self.running = False
        self._add_log("Stop richiesto...")

    def _run_loop(self, conn, lemma_lookup, state):
        words = self.words_to_process
        batch_size = 10 if self.source == "treccani" else 20
        i = 0

        try:
            while self.running and i < len(words):
                # Aggiorna workers/delay se cambiati
                self.current_workers = self.target_workers
                self.current_delay = self.target_delay

                batch = words[i:i + batch_size]

                with ThreadPoolExecutor(max_workers=self.current_workers) as executor:
                    results = list(executor.map(
                        lambda w: self.fetch_fn(w, self.current_delay), batch
                    ))

                if self.dry_run:
                    for word, synonyms in results:
                        self.stats["total"] += 1
                        if synonyms:
                            self.stats["found"] += 1
                            in_db = [s for s in synonyms if s.lower() in lemma_lookup]
                            self.stats["synonyms_added"] += len(in_db)
                            line = f"{word} -> {', '.join(in_db)}"
                            if len(synonyms) > len(in_db):
                                not_in_db = [s for s in synonyms if s.lower() not in lemma_lookup]
                                line += f"  (non in DB: {', '.join(not_in_db)})"
                            self.dry_run_file.write(line + "\n")
                            self.dry_run_file.flush()
                            self._add_log(line)
                        else:
                            self.stats["not_found"] += 1
                else:
                    for word, synonyms in results:
                        self.stats["total"] += 1
                        if synonyms is None or not synonyms:
                            self.stats["not_found"] += 1
                            continue
                        self.stats["found"] += 1
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
                                self.stats["lemmas_created"] = self.stats.get("lemmas_created", 0) + 1
                            matched = False
                            for src_id, src_pos in source_entries:
                                for tgt_id, tgt_pos in target_entries:
                                    if src_pos == tgt_pos:
                                        lid1, lid2 = min(src_id, tgt_id), max(src_id, tgt_id)
                                        if lid1 != lid2:
                                            try:
                                                conn.execute(
                                                    "INSERT OR IGNORE INTO synonyms (lemma_id_1, lemma_id_2, type, source) VALUES (?, ?, 'synonym', ?)",
                                                    (lid1, lid2, self.source)
                                                )
                                                self.stats["synonyms_added"] += 1
                                            except Exception:
                                                pass
                                        matched = True
                            if not matched and source_entries and target_entries:
                                lid1, lid2 = min(source_entries[0][0], target_entries[0][0]), max(source_entries[0][0], target_entries[0][0])
                                if lid1 != lid2:
                                    try:
                                        conn.execute(
                                            "INSERT OR IGNORE INTO synonyms (lemma_id_1, lemma_id_2, type, source) VALUES (?, ?, 'synonym', ?)",
                                            (lid1, lid2, self.source)
                                        )
                                        self.stats["synonyms_added"] += 1
                                    except Exception:
                                        pass
                        in_db = [s for s in synonyms if s.lower() in lemma_lookup]
                        if in_db:
                            self._add_log(f"{word} -> {', '.join(in_db[:5])}")

                    conn.commit()
                    state["processed"].extend(batch)
                    self.save_state_fn(state)

                i += len(batch)
                self.processed_count = i

        except Exception as e:
            self._add_log(f"ERRORE: {e}")
        finally:
            self.running = False
            if self.dry_run_file:
                self.dry_run_file.close()
                self.dry_run_file = None
            self._add_log(f"Completato. Trovati: {self.stats['found']}, Sinonimi: {self.stats['synonyms_added']}")

    def get_status(self):
        elapsed = time.time() - self.start_time if self.start_time else 0
        rate = self.processed_count / elapsed if elapsed > 0 and self.running else 0
        remaining = self.total_words - self.processed_count
        eta = remaining / rate if rate > 0 else 0

        return {
            "source": self.source,
            "running": self.running,
            "dry_run": self.dry_run,
            "workers": self.current_workers,
            "target_workers": self.target_workers,
            "delay": self.current_delay,
            "stats": dict(self.stats),
            "total_words": self.total_words,
            "processed": self.processed_count,
            "elapsed": int(elapsed),
            "eta": int(eta),
            "rate": round(rate, 1),
        }

    def get_log(self, last=30):
        with self._lock:
            return list(self.log)[-last:]


class ScraperManager:
    """Gestisce entrambi gli scraper."""

    def __init__(self):
        self.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.lemma_lookup = build_lemma_lookup(self.conn)
        self.gap_words = get_lemmas_without_synonyms(self.conn)
        self.all_words = get_all_lemmas(self.conn)
        self._process = psutil.Process() if HAS_PSUTIL else None

        self.scrapers = {
            "wiktionary": ScraperInstance(
                "wiktionary", wiktionary_fetch, wiktionary_process,
                wiktionary_load_state, wiktionary_save_state,
                WIKTIONARY_STATE_PATH, wiktionary_insert,
            ),
            "treccani": ScraperInstance(
                "treccani", treccani_fetch, treccani_process,
                treccani_load_state, treccani_save_state,
                TRECCANI_STATE_PATH, treccani_insert,
            ),
        }

    def start(self, source, workers, delay, dry_run, all_words=False):
        s = self.scrapers.get(source)
        if not s or s.running:
            return
        words = self.all_words if all_words else self.gap_words
        s.start(self.conn, self.lemma_lookup, words, workers, delay, dry_run)

    def stop(self, source):
        s = self.scrapers.get(source)
        if s:
            s.stop()

    def set_workers(self, source, n):
        s = self.scrapers.get(source)
        if s:
            s.target_workers = max(1, min(10, n))

    def set_delay(self, source, d):
        s = self.scrapers.get(source)
        if s:
            s.target_delay = max(0.1, min(5.0, d))

    def get_status(self):
        cpu = 0
        if self._process:
            try:
                cpu = self._process.cpu_percent(interval=0)
            except Exception:
                pass

        return {
            "wiktionary": self.scrapers["wiktionary"].get_status(),
            "treccani": self.scrapers["treccani"].get_status(),
            "cpu": round(cpu, 1),
            "db": self.get_db_stats(),
            "gap_words": len(self.gap_words),
            "all_words": len(self.all_words),
        }

    def get_log(self, source, last=30):
        s = self.scrapers.get(source)
        return s.get_log(last) if s else []

    def test_word(self, source, word):
        if source == "wiktionary":
            return wiktionary_fetch(word, delay=0)
        elif source == "treccani":
            return treccani_fetch(word, delay=0)
        return word, []

    def get_db_stats(self):
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT COUNT(*) FROM lemmas")
            lemmas = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM synonyms")
            synonyms = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM synonyms WHERE source='wiktionary'")
            wikt = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM synonyms WHERE source='treccani'")
            trec = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM synonyms WHERE source='virgilio'")
            virg = cur.fetchone()[0]
            return {
                "lemmas": lemmas, "synonyms": synonyms,
                "wiktionary": wikt, "treccani": trec, "virgilio": virg,
            }
        except Exception:
            return {}

    def get_dry_run_content(self, source):
        path = os.path.join(SCRIPT_DIR, f"{source}_dry_run.txt")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
        return ""


# ─── HTTP Server ──────────────────────────────────────────────────────────────

manager = None

class DashboardHandler(http.server.BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path in ("/", "/index.html"):
            self._send_html(HTML_PAGE)
        elif path == "/api/status":
            self._send_json(manager.get_status())
        elif path == "/api/log":
            source = qs.get("source", ["wiktionary"])[0]
            last = int(qs.get("last", [30])[0])
            self._send_json(manager.get_log(source, last))
        elif path == "/api/dry-run":
            source = qs.get("source", ["wiktionary"])[0]
            content = manager.get_dry_run_content(source)
            self._send_text(content)
        else:
            self.send_error(404)

    def do_POST(self):
        path = urlparse(self.path).path
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length)) if length else {}

        if path == "/api/start":
            source = body.get("source", "wiktionary")
            workers = int(body.get("workers", 3))
            delay = float(body.get("delay", 0.5))
            dry_run = body.get("dry_run", False)
            all_words = body.get("all_words", False)
            manager.start(source, workers, delay, dry_run, all_words=all_words)
            self._send_json({"ok": True})

        elif path == "/api/stop":
            source = body.get("source", "wiktionary")
            manager.stop(source)
            self._send_json({"ok": True})

        elif path == "/api/workers":
            source = body.get("source")
            workers = int(body.get("workers", 3))
            manager.set_workers(source, workers)
            self._send_json({"ok": True})

        elif path == "/api/delay":
            source = body.get("source")
            delay = float(body.get("delay", 0.5))
            manager.set_delay(source, delay)
            self._send_json({"ok": True})

        elif path == "/api/test":
            source = body.get("source", "wiktionary")
            word = body.get("word", "")
            if word:
                w, syns = manager.test_word(source, word)
                in_db = [s for s in (syns or []) if s.lower() in manager.lemma_lookup]
                not_in_db = [s for s in (syns or []) if s.lower() not in manager.lemma_lookup]
                self._send_json({
                    "word": w, "synonyms": syns or [],
                    "in_db": in_db, "not_in_db": not_in_db,
                })
            else:
                self._send_json({"error": "word required"})
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

    def _send_text(self, text):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(text.encode("utf-8"))


# ─── HTML Frontend ────────────────────────────────────────────────────────────

HTML_PAGE = r"""<!DOCTYPE html>
<html lang="it">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Scraper Dashboard</title>
<style>
:root{--bg:#1a1a2e;--card:#16213e;--accent:#0f3460;--hl:#e94560;--text:#eee;--dim:#888;--ok:#4ecca3;--warn:#f0a500}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text);min-height:100vh}
header{background:var(--card);padding:12px 24px;display:flex;align-items:center;gap:20px;border-bottom:1px solid var(--accent)}
header h1{font-size:18px;font-weight:600;letter-spacing:1px}
.cpu-meter{margin-left:auto;display:flex;align-items:center;gap:8px;font-size:13px}
.cpu-bar{width:120px;height:8px;background:#333;border-radius:4px;overflow:hidden}
.cpu-fill{height:100%;background:var(--ok);transition:width .5s,background .5s;border-radius:4px}
.db-stats{display:flex;gap:16px;font-size:12px;color:var(--dim)}
.db-stats strong{color:var(--text)}

.main{display:grid;grid-template-columns:1fr 1fr;gap:16px;padding:16px 24px;max-width:1400px;margin:0 auto}
@media(max-width:900px){.main{grid-template-columns:1fr}}

.card{background:var(--card);border-radius:8px;padding:16px;border:1px solid var(--accent)}
.card h2{font-size:15px;margin-bottom:12px;display:flex;align-items:center;gap:8px}
.dot{width:10px;height:10px;border-radius:50%;display:inline-block}
.dot.on{background:var(--ok);box-shadow:0 0 6px var(--ok)}
.dot.off{background:#555}

.controls{display:flex;flex-direction:column;gap:8px;margin-bottom:12px}
.row{display:flex;align-items:center;gap:10px}
.row label{width:70px;font-size:13px;color:var(--dim)}
.row input[type=range]{flex:1;accent-color:var(--hl)}
.row .val{min-width:40px;text-align:right;font-size:13px;font-weight:600;color:var(--hl)}
.row input[type=checkbox]{accent-color:var(--hl)}

.btns{display:flex;gap:8px;margin-bottom:12px}
.btn{padding:7px 18px;border:none;border-radius:4px;font-size:13px;font-weight:600;cursor:pointer;transition:.2s}
.btn-start{background:var(--ok);color:#111}.btn-start:hover{filter:brightness(1.15)}
.btn-stop{background:var(--hl);color:#fff}.btn-stop:hover{filter:brightness(1.15)}
.btn:disabled{opacity:.4;cursor:default}

.stats-grid{display:grid;grid-template-columns:1fr 1fr;gap:4px 16px;font-size:13px;margin-bottom:12px}
.stats-grid .label{color:var(--dim)}.stats-grid .value{font-weight:600;text-align:right}

.progress{height:6px;background:#333;border-radius:3px;overflow:hidden;margin-bottom:12px}
.progress-fill{height:100%;background:var(--ok);transition:width .5s;border-radius:3px}

.log-box{background:#111;border-radius:4px;padding:8px;font-family:'Cascadia Code','Fira Code',monospace;font-size:11px;line-height:1.5;max-height:180px;overflow-y:auto;color:var(--dim);white-space:pre-wrap;word-break:break-all}
.log-box:empty::before{content:'Nessun log...';color:#444}

.test-area{grid-column:1/-1;background:var(--card);border-radius:8px;padding:16px;border:1px solid var(--accent)}
.test-area h2{font-size:15px;margin-bottom:12px}
.test-row{display:flex;gap:8px;align-items:center;margin-bottom:8px}
.test-row select,.test-row input[type=text]{padding:7px 12px;border:1px solid var(--accent);border-radius:4px;background:#111;color:var(--text);font-size:13px}
.test-row input[type=text]{flex:1}
.btn-test{background:var(--accent);color:var(--text);border:1px solid var(--hl)}
.test-result{font-size:13px;line-height:1.6}
.test-result .found{color:var(--ok);font-weight:600}
.test-result .missing{color:var(--dim);font-style:italic}
.test-result .loading{color:var(--warn)}
</style>
</head>
<body>
<header>
  <h1>Scraper Dashboard</h1>
  <div class="db-stats" id="dbStats">Caricamento...</div>
  <div class="cpu-meter">
    <span>CPU</span>
    <div class="cpu-bar"><div class="cpu-fill" id="cpuFill" style="width:0"></div></div>
    <span id="cpuVal">0%</span>
  </div>
</header>

<div class="main">
  <!-- WIKTIONARY -->
  <div class="card" id="cardWikt">
    <h2><span class="dot off" id="dotWikt"></span> Wiktionary</h2>
    <div class="controls">
      <div class="row"><label>Workers</label><input type="range" min="1" max="10" value="3" id="wWikt" oninput="setWorkers('wiktionary',this.value)"><span class="val" id="wWiktVal">3</span></div>
      <div class="row"><label>Delay (s)</label><input type="range" min="1" max="50" value="5" id="dWikt" oninput="setDelay('wiktionary',this.value/10)"><span class="val" id="dWiktVal">0.5</span></div>
      <div class="row"><label>Dry Run</label><input type="checkbox" id="drWikt"></div>
      <div class="row"><label>Tutte</label><input type="checkbox" id="allWikt" checked><span style="font-size:11px;color:var(--dim)">Tutte le parole (non solo gap)</span></div>
    </div>
    <div class="btns">
      <button class="btn btn-start" id="startWikt" onclick="startScraper('wiktionary')">Avvia</button>
      <button class="btn btn-stop" id="stopWikt" onclick="stopScraper('wiktionary')" disabled>Ferma</button>
    </div>
    <div class="stats-grid" id="statsWikt">
      <span class="label">Processate</span><span class="value" id="procWikt">0 / 0</span>
      <span class="label">Trovate</span><span class="value" id="foundWikt">0</span>
      <span class="label">Sinonimi</span><span class="value" id="synWikt">0</span>
      <span class="label">Lemmi creati</span><span class="value" id="newWikt">0</span>
      <span class="label">Velocit&agrave;</span><span class="value" id="rateWikt">-</span>
      <span class="label">ETA</span><span class="value" id="etaWikt">-</span>
    </div>
    <div class="progress"><div class="progress-fill" id="progWikt" style="width:0"></div></div>
    <div class="log-box" id="logWikt"></div>
  </div>

  <!-- TRECCANI -->
  <div class="card" id="cardTrec">
    <h2><span class="dot off" id="dotTrec"></span> Treccani</h2>
    <div class="controls">
      <div class="row"><label>Workers</label><input type="range" min="1" max="10" value="2" id="wTrec" oninput="setWorkers('treccani',this.value)"><span class="val" id="wTrecVal">2</span></div>
      <div class="row"><label>Delay (s)</label><input type="range" min="1" max="50" value="10" id="dTrec" oninput="setDelay('treccani',this.value/10)"><span class="val" id="dTrecVal">1.0</span></div>
      <div class="row"><label>Dry Run</label><input type="checkbox" id="drTrec"></div>
      <div class="row"><label>Tutte</label><input type="checkbox" id="allTrec" checked><span style="font-size:11px;color:var(--dim)">Tutte le parole (non solo gap)</span></div>
    </div>
    <div class="btns">
      <button class="btn btn-start" id="startTrec" onclick="startScraper('treccani')">Avvia</button>
      <button class="btn btn-stop" id="stopTrec" onclick="stopScraper('treccani')" disabled>Ferma</button>
    </div>
    <div class="stats-grid" id="statsTrec">
      <span class="label">Processate</span><span class="value" id="procTrec">0 / 0</span>
      <span class="label">Trovate</span><span class="value" id="foundTrec">0</span>
      <span class="label">Sinonimi</span><span class="value" id="synTrec">0</span>
      <span class="label">Lemmi creati</span><span class="value" id="newTrec">0</span>
      <span class="label">Velocit&agrave;</span><span class="value" id="rateTrec">-</span>
      <span class="label">ETA</span><span class="value" id="etaTrec">-</span>
    </div>
    <div class="progress"><div class="progress-fill" id="progTrec" style="width:0"></div></div>
    <div class="log-box" id="logTrec"></div>
  </div>

  <!-- TEST PAROLA -->
  <div class="test-area">
    <h2>Test Parola</h2>
    <div class="test-row">
      <select id="testSource"><option value="wiktionary">Wiktionary</option><option value="treccani">Treccani</option></select>
      <input type="text" id="testWord" placeholder="Scrivi una parola..." onkeydown="if(event.key==='Enter')testWord()">
      <button class="btn btn-test" onclick="testWord()">Testa</button>
    </div>
    <div class="test-result" id="testResult"></div>
  </div>
</div>

<script>
const $ = id => document.getElementById(id);

function fmtTime(s){
  if(!s||s<=0) return '-';
  const h=Math.floor(s/3600),m=Math.floor(s%3600/60),ss=s%60;
  return h>0?`${h}h ${m}m`:`${m}m ${ss}s`;
}

function api(method,path,body){
  const opts={method,headers:{'Content-Type':'application/json'}};
  if(body) opts.body=JSON.stringify(body);
  return fetch(path,opts).then(r=>r.json());
}

function startScraper(src){
  const s=src==='wiktionary'?'Wikt':'Trec';
  const workers=parseInt($('w'+s).value);
  const delay=parseInt($('d'+s).value)/10;
  const dry_run=$('dr'+s).checked;
  const all_words=$('all'+s).checked;
  api('POST','/api/start',{source:src,workers,delay,dry_run,all_words});
}
function stopScraper(src){api('POST','/api/stop',{source:src})}

function setWorkers(src,v){
  const s=src==='wiktionary'?'Wikt':'Trec';
  $('w'+s+'Val').textContent=v;
  api('POST','/api/workers',{source:src,workers:parseInt(v)});
}
function setDelay(src,v){
  const s=src==='wiktionary'?'Wikt':'Trec';
  $('d'+s+'Val').textContent=parseFloat(v).toFixed(1);
  api('POST','/api/delay',{source:src,delay:parseFloat(v)});
}

function updateCard(src,data){
  const s=src==='wiktionary'?'Wikt':'Trec';
  const dot=$('dot'+s);
  dot.className=data.running?'dot on':'dot off';
  $('start'+s).disabled=data.running;
  $('stop'+s).disabled=!data.running;
  $('proc'+s).textContent=`${data.processed.toLocaleString('it')} / ${data.total_words.toLocaleString('it')}`;
  $('found'+s).textContent=data.stats.found.toLocaleString('it');
  $('syn'+s).textContent=data.stats.synonyms_added.toLocaleString('it');
  $('new'+s).textContent=(data.stats.lemmas_created||0).toLocaleString('it');
  $('rate'+s).textContent=data.rate>0?data.rate.toFixed(1)+' /s':'-';
  $('eta'+s).textContent=fmtTime(data.eta);
  const pct=data.total_words>0?100*data.processed/data.total_words:0;
  $('prog'+s).style.width=pct+'%';
}

async function updateLogs(){
  for(const src of ['wiktionary','treccani']){
    const s=src==='wiktionary'?'Wikt':'Trec';
    try{
      const logs=await api('GET',`/api/log?source=${src}&last=30`);
      const box=$('log'+s);
      box.textContent=logs.map(l=>`[${l.time}] ${l.msg}`).join('\n');
      box.scrollTop=box.scrollHeight;
    }catch(e){}
  }
}

async function poll(){
  try{
    const st=await api('GET','/api/status');
    updateCard('wiktionary',st.wiktionary);
    updateCard('treccani',st.treccani);
    // CPU
    const cpu=st.cpu||0;
    $('cpuFill').style.width=cpu+'%';
    $('cpuFill').style.background=cpu>80?'var(--hl)':cpu>50?'var(--warn)':'var(--ok)';
    $('cpuVal').textContent=cpu+'%';
    // DB stats
    const db=st.db||{};
    $('dbStats').innerHTML=`<strong>${(db.lemmas||0).toLocaleString('it')}</strong> lemmi &middot; <strong>${(db.synonyms||0).toLocaleString('it')}</strong> sinonimi &middot; <strong>${(st.gap_words||0).toLocaleString('it')}</strong> gap / <strong>${(st.all_words||0).toLocaleString('it')}</strong> totali &middot; Virgilio: ${(db.virgilio||0).toLocaleString('it')} &middot; Wikt: ${(db.wiktionary||0).toLocaleString('it')} &middot; Trec: ${(db.treccani||0).toLocaleString('it')}`;
  }catch(e){}
  updateLogs();
}

async function testWord(){
  const src=$('testSource').value;
  const word=$('testWord').value.trim();
  if(!word) return;
  $('testResult').innerHTML='<span class="loading">Cercando...</span>';
  try{
    const r=await api('POST','/api/test',{source:src,word});
    if(!r.synonyms||r.synonyms.length===0){
      $('testResult').innerHTML=`<b>${r.word}</b>: nessun sinonimo trovato su ${src}`;
    } else {
      let html=`<b>${r.word}</b> (${src}): `;
      if(r.in_db.length) html+=`<span class="found">${r.in_db.join(', ')}</span>`;
      if(r.not_in_db.length) html+=` <span class="missing">(non in DB: ${r.not_in_db.join(', ')})</span>`;
      $('testResult').innerHTML=html;
    }
  }catch(e){$('testResult').textContent='Errore: '+e.message}
}

setInterval(poll,2000);
poll();
</script>
</body>
</html>"""


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    global manager

    parser = argparse.ArgumentParser(description="Scraper Dashboard")
    parser.add_argument("--port", type=int, default=8060)
    args = parser.parse_args()

    print("Scraper Dashboard")
    print("=" * 40)
    print("  Inizializzazione...")
    manager = ScraperManager()

    stats = manager.get_db_stats()
    print(f"  DB: {stats.get('lemmas',0):,} lemmi | {stats.get('synonyms',0):,} sinonimi")
    print(f"  Gap: {len(manager.gap_words):,} lemmi senza sinonimi")
    if not HAS_PSUTIL:
        print("  NOTA: installa psutil per monitoraggio CPU (pip install psutil)")

    server = http.server.ThreadingHTTPServer(("127.0.0.1", args.port), DashboardHandler)
    print(f"\n  Apri: http://localhost:{args.port}")
    print("  Premi Ctrl+C per chiudere.\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nChiuso.")
        server.server_close()


if __name__ == "__main__":
    main()
