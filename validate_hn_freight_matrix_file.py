from __future__ import annotations
import os, csv, json, math, platform, subprocess
from collections import defaultdict
from statistics import mean
from datetime import datetime
from typing import Any, Mapping, Optional
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from ttkbootstrap import Style
from ttkbootstrap.constants import PRIMARY, INFO, SUCCESS
APP_DIR = os.path.join(os.path.expanduser("~"), ".csvjson_app")
CONFIG_PATH = os.path.join(APP_DIR, "validate_hn_freight_matrix_app_settings.json")
DEFAULT_SETTINGS = {
    "export": {
        "folder": os.path.abspath("export"),
        "open_folder_after": True,
        "filename_pattern": "{base}_{batch}_{group}_{ts}.{ext}",
        "formats": {"csv": False, "json": True},
    },
    "batch": {
        "enabled": True,
        "mode": "rows",
        "rows_per_file": 100000,
        "group_column": "postcode"
    }
}
CSV_FIELD_ALIASES = {
    "sku": ["sku", "productcode", "productCode", "product_code", "product id", "productid"],
    "postCode": ["postcode", "postCode", "post_code", "post code", "zip", "zip_code"],
    "price": ["price", "unit_price", "unitPrice", "unitprice", "unit price", "amount"],
}
def _ensure_app_dir(): os.makedirs(APP_DIR, exist_ok=True)
def load_settings() -> dict:
    _ensure_app_dir()
    if not os.path.exists(CONFIG_PATH): return json.loads(json.dumps(DEFAULT_SETTINGS))
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f: data = json.load(f)
        def deep_merge(d, default):
            for k, v in default.items():
                if k not in d: d[k] = v
                elif isinstance(v, dict) and isinstance(d[k], dict): deep_merge(d[k], v)
            return d
        return deep_merge(data, json.loads(json.dumps(DEFAULT_SETTINGS)))
    except Exception:
        return json.loads(json.dumps(DEFAULT_SETTINGS))
def save_settings(cfg: dict) -> None:
    _ensure_app_dir()
    with open(CONFIG_PATH, "w", encoding="utf-8") as f: json.dump(cfg, f, indent=2)
def normalize_str(v: Any) -> str:
    if v is None: return ""
    if isinstance(v, (int, float)): return str(v).strip()
    return str(v).strip().strip('"').strip()
def _lower_keys(d: Mapping[str, Any]) -> dict[str, Any]: return {(k or "").strip().lower(): v for k, v in d.items()}
def field_from_row(row: Mapping[str, Any], key: str) -> Any:
    for alias in CSV_FIELD_ALIASES.get(key, []):
        a = alias.lower()
        if a in row: return row.get(a)
    return None
def is_valid_sku(s: str) -> tuple[bool, str]:
    s = normalize_str(s)
    if not s: return False, "sku empty"
    for ch in s:
        if not (ch.isalnum() or ch in "-_./"): return False, "sku has invalid characters"
    if len(s) > 64: return False, "sku too long"
    return True, ""
def is_valid_postcode(pc: str) -> tuple[bool, str]:
    pc = normalize_str(pc)
    if not pc: return False, "postCode empty"
    if not pc.isdigit() or len(pc) != 4: return False, "postCode must be 4 digits"
    return True, ""
def normalize_price(p: str) -> tuple[bool, float, str]:
    s = normalize_str(p)
    if s == "": return False, 0.0, "price empty"
    clean = s.replace(",", "")
    for sym in "$€£AUDaud ": clean = clean.replace(sym, "")
    try: val = float(clean)
    except Exception: return False, 0.0, "price not a number"
    if val < 0: return False, 0.0, "price negative"
    if math.isinf(val) or math.isnan(val): return False, 0.0, "price not finite"
    return True, round(val, 2), ""
def build_doc(raw_sku: str, raw_pc: str, price_val: float) -> dict[str, Any]:
    return {"sku": normalize_str(raw_sku), "postCode": normalize_str(raw_pc), "price": float(price_val)}
def _validate_from_reader(reader: csv.DictReader) -> tuple[list[dict], list[dict], list[str]]:
    valid_docs, errors, warnings, seen_ids = [], [], [], set()
    raw_fieldnames = reader.fieldnames or []
    fieldnames_lc = [(h or "").strip().lower() for h in raw_fieldnames]
    if not fieldnames_lc:
        errors.append({"row": 1, "context": "header", "error": "Missing header row"})
        return valid_docs, errors, warnings
    missing_min = []
    for key in ["sku", "postCode", "price"]:
        if not any(alias.lower() in fieldnames_lc for alias in CSV_FIELD_ALIASES[key]): missing_min.append(key)
    if missing_min:
        errors.append({"row": 1, "context": "header", "error": f"Missing required columns: {', '.join(missing_min)}"})
        return valid_docs, errors, warnings
    for idx, row in enumerate(reader, start=2):
        row = _lower_keys(row)
        raw_sku = normalize_str(field_from_row(row, "sku"))
        raw_pc = normalize_str(field_from_row(row, "postCode"))
        raw_price = normalize_str(field_from_row(row, "price"))
        if not raw_sku and not raw_pc and not raw_price: continue
        ok_sku, sku_err = is_valid_sku(raw_sku)
        ok_pc, pc_err = is_valid_postcode(raw_pc)
        ok_price, norm_price, price_err = normalize_price(raw_price)
        errs = []
        if not raw_sku: errs.append("sku missing")
        if not raw_pc: errs.append("postCode missing")
        if not raw_price: errs.append("price missing")
        if raw_sku and not ok_sku: errs.append(sku_err)
        if raw_pc and not ok_pc: errs.append(pc_err)
        if raw_price and not ok_price: errs.append(price_err)
        if errs:
            errors.append({"row": idx, "context": f"sku={raw_sku}, postCode={raw_pc}", "error": "; ".join(errs)})
            continue
        doc_id = f"{raw_sku}|{raw_pc}"
        if doc_id in seen_ids:
            errors.append({"row": idx, "context": f"sku={raw_sku}, postCode={raw_pc}", "error": "Duplicate id within file"})
            continue
        seen_ids.add(doc_id)
        valid_docs.append(build_doc(raw_sku, raw_pc, norm_price))
    return valid_docs, errors, warnings
def validate_csv(file_path: str) -> tuple[list[dict], list[dict], list[str]]:
    try:
        with open(file_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            return _validate_from_reader(reader)
    except Exception as e:
        return [], [{"row": 0, "context": "file", "error": f"Read error: {e}"}], []
def validate_json(file_path: str) -> tuple[list[dict], list[dict], list[str]]:
    valid_docs, errors, warnings, seen_ids = [], [], [], set()
    def validate_obj(obj: dict, idx_for_report: int) -> None:
        obj_lc = _lower_keys(obj)
        raw_sku = normalize_str(field_from_row(obj_lc, "sku"))
        raw_pc = normalize_str(field_from_row(obj_lc, "postCode"))
        raw_price = normalize_str(field_from_row(obj_lc, "price"))
        ok_sku, sku_err = is_valid_sku(raw_sku)
        ok_pc, pc_err = is_valid_postcode(raw_pc)
        ok_price, norm_price, price_err = normalize_price(raw_price)
        errs = []
        if not raw_sku: errs.append("sku missing")
        if not raw_pc: errs.append("postCode missing")
        if raw_price == "": errs.append("price missing")
        if raw_sku and not ok_sku: errs.append(sku_err)
        if raw_pc and not ok_pc: errs.append(pc_err)
        if raw_price != "" and not ok_price: errs.append(price_err)
        if errs:
            errors.append({"row": idx_for_report, "context": f"sku={raw_sku}, postCode={raw_pc}", "error": "; ".join(errs)})
            return
        doc_id = f"{raw_sku}|{raw_pc}"
        if doc_id in seen_ids:
            errors.append({"row": idx_for_report, "context": f"sku={raw_sku}, postCode={raw_pc}", "error": "Duplicate id within file"})
            return
        seen_ids.add(doc_id)
        valid_docs.append(build_doc(raw_sku, raw_pc, norm_price))
    try:
        with open(file_path, encoding="utf-8") as f: data = json.load(f)
        if isinstance(data, list):
            for i, obj in enumerate(data, start=1):
                if not isinstance(obj, dict):
                    errors.append({"row": i, "context": "", "error": "Each item must be a JSON object"})
                    continue
                validate_obj(obj, i)
            return valid_docs, errors, warnings
        else:
            warnings.append("Top-level JSON is not an array; falling back to NDJSON parser.")
    except json.JSONDecodeError:
        warnings.append("JSON is not an array; attempting NDJSON (one JSON object per line).")
    try:
        with open(file_path, encoding="utf-8") as f:
            for i, line in enumerate(f, start=1):
                line = line.strip()
                if not line: continue
                try:
                    obj = json.loads(line)
                    if not isinstance(obj, dict):
                        errors.append({"row": i, "context": "", "error": "Line is not a JSON object"})
                        continue
                    validate_obj(obj, i)
                except json.JSONDecodeError as e:
                    errors.append({"row": i, "context": "", "error": f"Invalid JSON: {e}"})
    except Exception as e:
        errors.append({"row": 0, "context": "", "error": f"Error reading file line-by-line: {e}"})
    return valid_docs, errors, warnings
def validate_pasted_csv_text(text: str) -> tuple[list[dict], list[dict], list[str]]:
    lines = [ln for ln in text.splitlines() if ln.strip() != ""]
    if not lines: return [], [{"row": 1, "context": "header", "error": "No content"}], []
    reader = csv.DictReader(lines)
    return _validate_from_reader(reader)
class App:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Validate HN Freight Matrix CSV/JSON → Clean CSV & JSON")
        self.root.geometry("555x360")
        self.root.resizable(True, True)
        self.style = Style(theme="darkly")
        self.settings = load_settings()
        self.file_path: Optional[str] = None
        self.headers: list[str] = []
        self.last_valid_docs: list[dict] = []
        self.last_errors: list[dict] = []
        self.last_warnings: list[str] = []
        self.cached_stats: dict[str, Any] = {}
        self.output_folder_var = tk.StringVar(value=self.settings["export"]["folder"])
        self.open_folder_after_var = tk.BooleanVar(value=self.settings["export"]["open_folder_after"])
        self.filename_pattern_var = tk.StringVar(value=self.settings["export"]["filename_pattern"])
        self.export_csv_var = tk.BooleanVar(value=self.settings["export"]["formats"]["csv"])
        self.export_json_var = tk.BooleanVar(value=self.settings["export"]["formats"]["json"])
        self.enable_batch_var = tk.BooleanVar(value=self.settings["batch"]["enabled"])
        self.batch_mode_var = tk.StringVar(value=self.settings["batch"]["mode"])
        self.rows_per_file_var = tk.IntVar(value=self.settings["batch"]["rows_per_file"])
        self.group_column_var = tk.StringVar(value=self.settings["batch"]["group_column"])
        self._ensure_export_dir()
        self._build_ui()
    def _build_ui(self) -> None:
        self.root.columnconfigure(0, weight=1); self.root.rowconfigure(0, weight=1)
        nb = ttk.Notebook(self.root); nb.grid(row=0, column=0, sticky="nsew")
        self.tab_source = ttk.Frame(nb, padding=12); self._tab_source(self.tab_source); nb.add(self.tab_source, text="Source")
        self.tab_preview = ttk.Frame(nb, padding=12); self._tab_preview(self.tab_preview); nb.add(self.tab_preview, text="Preview")
        self.tab_settings = ttk.Frame(nb, padding=0); self._tab_settings(self.tab_settings); nb.add(self.tab_settings, text="Settings")
    def _tab_source(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1); parent.rowconfigure(2, weight=1)
        file_fr = ttk.LabelFrame(parent, text="File", padding=10); file_fr.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        file_fr.columnconfigure(1, weight=1)
        ttk.Label(file_fr, text="Selected:").grid(row=0, column=0, sticky="w")
        self.file_label = ttk.Label(file_fr, text="No file selected", anchor="w"); self.file_label.grid(row=0, column=1, sticky="ew", padx=8)
        ttk.Button(file_fr, text="Select CSV/JSON", command=self.load_file, bootstyle=PRIMARY).grid(row=0, column=2)
        paste_fr = ttk.LabelFrame(parent, text="Paste CSV Content", padding=10); paste_fr.grid(row=1, column=0, sticky="nsew")
        paste_fr.columnconfigure(0, weight=1); paste_fr.rowconfigure(0, weight=1)
        self.paste_box = tk.Text(paste_fr, wrap=tk.NONE, height=8); self.paste_box.grid(row=0, column=0, sticky="nsew")
        sy = ttk.Scrollbar(paste_fr, orient="vertical", command=self.paste_box.yview); sy.grid(row=0, column=1, sticky="ns")
        sx = ttk.Scrollbar(paste_fr, orient="horizontal", command=self.paste_box.xview); sx.grid(row=1, column=0, sticky="ew")
        self.paste_box.config(yscrollcommand=sy.set, xscrollcommand=sx.set)
        ttk.Button(parent, text="Validate & Preview", command=self.preview_data, bootstyle=INFO).grid(row=3, column=0, sticky="w", pady=(10, 0))
    def _tab_preview(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=3); parent.columnconfigure(1, weight=2); parent.rowconfigure(1, weight=1)
        top = ttk.Frame(parent); top.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))
        ttk.Label(top, text="Preview shows first 100 validated rows.").pack(side="left")
        ttk.Button(top, text="Export Files", command=self.export_files, bootstyle=SUCCESS).pack(side="right", padx=(8, 0))
        pv_fr = ttk.LabelFrame(parent, text="Preview", padding=10); pv_fr.grid(row=1, column=0, sticky="nsew", padx=(0, 8))
        pv_fr.columnconfigure(0, weight=1); pv_fr.rowconfigure(0, weight=1)
        self.preview_box = tk.Text(pv_fr, wrap=tk.NONE, state="disabled"); self.preview_box.grid(row=0, column=0, sticky="nsew")
        self.preview_box.tag_configure("good", foreground="#5bd75b"); self.preview_box.tag_configure("head", foreground="#9ecbff")
        sy = ttk.Scrollbar(pv_fr, orient="vertical", command=self.preview_box.yview); sy.grid(row=0, column=1, sticky="ns")
        sx = ttk.Scrollbar(pv_fr, orient="horizontal", command=self.preview_box.xview); sx.grid(row=1, column=0, sticky="ew")
        self.preview_box.config(yscrollcommand=sy.set, xscrollcommand=sx.set)
        st_fr = ttk.LabelFrame(parent, text="Data Quality / Stats", padding=10); st_fr.grid(row=1, column=1, sticky="nsew")
        st_fr.columnconfigure(0, weight=1); st_fr.rowconfigure(0, weight=1)
        self.stats_box = tk.Text(st_fr, height=12, wrap=tk.WORD, state="normal"); self.stats_box.grid(row=0, column=0, sticky="nsew")
        self.stats_box.tag_configure("good", foreground="#5bd75b"); self.stats_box.tag_configure("bad", foreground="#ff6b6b")
        sy2 = ttk.Scrollbar(st_fr, orient="vertical", command=self.stats_box.yview); sy2.grid(row=0, column=1, sticky="ns")
        self.stats_box.config(yscrollcommand=sy2.set)
    def _tab_settings(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(0, weight=1)
        canvas = tk.Canvas(parent, highlightthickness=0)
        vsb = ttk.Scrollbar(parent, orient="vertical", command=canvas.yview)
        scroll = ttk.Frame(canvas, padding=8)
        scroll.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=scroll, anchor="nw")
        canvas.configure(yscrollcommand=vsb.set)
        canvas.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        def _on_mousewheel(event):
            if platform.system() == "Darwin":
                canvas.yview_scroll(int(-1 * (event.delta)), "units")
            else:
                canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        canvas.bind_all("<MouseWheel>", _on_mousewheel)
        canvas.bind_all("<Button-4>", lambda e: canvas.yview_scroll(-1, "units"))
        canvas.bind_all("<Button-5>", lambda e: canvas.yview_scroll(1, "units"))
        fm = ttk.LabelFrame(scroll, text="Export Formats", padding=6)
        fm.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        fm.columnconfigure(0, weight=1)
        ttk.Checkbutton(fm, text="CSV", variable=self.export_csv_var).grid(row=0, column=0, sticky="w")
        ttk.Checkbutton(fm, text="JSON", variable=self.export_json_var).grid(row=1, column=0, sticky="w")
        out = ttk.LabelFrame(scroll, text="Output Settings", padding=6)
        out.grid(row=2, column=0, sticky="ew", pady=(0, 8))
        for c in range(4): out.columnconfigure(c, weight=1 if c == 1 else 0)
        ttk.Label(out, text="Folder:").grid(row=0, column=0, sticky="w")
        self.out_label = ttk.Label(out, text=self.output_folder_var.get(), anchor="w")
        self.out_label.grid(row=0, column=1, sticky="ew")
        ttk.Button(out, text="Browse", command=self._choose_output_folder, bootstyle=PRIMARY).grid(row=0, column=2, sticky="w")
        ttk.Label(out, text="Filename Pattern:").grid(row=1, column=0, sticky="w")
        self.pattern_entry = ttk.Entry(out, textvariable=self.filename_pattern_var)
        self.pattern_entry.grid(row=1, column=1, columnspan=2, sticky="ew")
        ttk.Label(out, text="Tokens: {base}{batch}{group}{ts}{ext}").grid(row=2, column=1, sticky="w")
        ttk.Checkbutton(out, text="Open folder after export", variable=self.open_folder_after_var).grid(row=3, column=1, sticky="w")
        lf = ttk.LabelFrame(scroll, text="Batch Export", padding=6)
        lf.grid(row=3, column=0, sticky="ew", pady=(0, 8))
        for c in range(4): lf.columnconfigure(c, weight=1 if c == 1 else 0)
        ttk.Checkbutton(lf, text="Enable batch export", variable=self.enable_batch_var, command=self._toggle_batch_controls).grid(row=0, column=0, columnspan=2, sticky="w")
        ttk.Label(lf, text="Mode:").grid(row=1, column=0, sticky="w")
        row_mode = ttk.Frame(lf)
        row_mode.grid(row=1, column=1, sticky="w")
        ttk.Radiobutton(row_mode, text="Rows per file", value="rows", variable=self.batch_mode_var, command=self._on_mode_change).pack(side="left")
        ttk.Radiobutton(row_mode, text="Group by column", value="group", variable=self.batch_mode_var, command=self._on_mode_change).pack(side="left", padx=(10, 0))
        ttk.Label(lf, text="Rows per file:").grid(row=2, column=0, sticky="w")
        self.rows_entry = ttk.Entry(lf, width=10, textvariable=self.rows_per_file_var)
        self.rows_entry.grid(row=2, column=1, sticky="w")
        ttk.Label(lf, text="Group column:").grid(row=3, column=0, sticky="w")
        self.group_combo = ttk.Combobox(lf, width=16, state="readonly", textvariable=self.group_column_var, values=["postcode", "productcode", "sku", "state"])
        self.group_combo.grid(row=3, column=1, sticky="w")
        self._toggle_batch_controls()
        self._on_mode_change()
        act = ttk.Frame(scroll)
        act.grid(row=7, column=0, sticky="w", pady=(12, 0))
        ttk.Button(act, text="Save Settings", command=self._save_all_settings, bootstyle=SUCCESS).pack(side="left", padx=(0, 8))
        ttk.Button(act, text="Open Config Folder", command=self._open_config_folder, bootstyle=PRIMARY).pack(side="left")
    def _ensure_export_dir(self) -> None:
        folder = self.output_folder_var.get().strip() or os.path.abspath("export")
        self.output_folder_var.set(folder)
        os.makedirs(folder, exist_ok=True)
    def _choose_output_folder(self) -> None:
        path = filedialog.askdirectory(initialdir=self.output_folder_var.get())
        if path:
            self.output_folder_var.set(path); self.out_label.config(text=path)
    def _toggle_batch_controls(self) -> None:
        enabled = self.enable_batch_var.get()
        rows_state = "normal" if (enabled and self.batch_mode_var.get() == "rows") else "disabled"
        group_state = "readonly" if (enabled and self.batch_mode_var.get() == "group") else "disabled"
        self.rows_entry.configure(state=rows_state); self.group_combo.configure(state=group_state)
    def _on_mode_change(self) -> None: self._toggle_batch_controls()
    def _update_group_columns(self, headers: list[str]) -> None:
        if headers:
            self.group_combo.configure(values=headers)
            cur = self.group_column_var.get()
            if cur not in headers: self.group_column_var.set("postcode" if "postcode" in headers else headers[0])
    def load_file(self) -> None:
        file_path = filedialog.askopenfilename(filetypes=[("CSV/JSON files", "*.csv *.json")])
        if not file_path: return
        self.file_path = file_path; self.file_label.config(text=os.path.basename(file_path))
        if file_path.lower().endswith(".csv"):
            try:
                with open(file_path, newline="", encoding="utf-8-sig") as f:
                    rdr = csv.DictReader(f)
                    self.headers = [(h or "").strip().lower() for h in (rdr.fieldnames or [])]
                    self._update_group_columns(self.headers)
            except Exception: pass
        else:
            self.headers = ["sku","postCode","price"]; self._update_group_columns(self.headers)
    def preview_data(self) -> None:
        """Preview data from file or pasted text, validate, and display stats."""
        if self.file_path:
            ext = os.path.splitext(self.file_path)[1].lower()
            if ext == ".csv":
                valid_docs, errors, warnings = validate_csv(self.file_path)
            elif ext == ".json":
                valid_docs, errors, warnings = validate_json(self.file_path)
            else:
                messagebox.showerror("Error", "Unsupported file type.")
                return
        else:
            txt = self.paste_box.get("1.0", tk.END).strip()
            if not txt:
                messagebox.showerror("Error", "No file selected or pasted content.")
                return
            valid_docs, errors, warnings = validate_pasted_csv_text(txt)
            first_line = next((ln for ln in txt.splitlines() if ln.strip()), None)
            if first_line:
                self.headers = [h.strip().lower() for h in next(csv.reader([first_line]))]
            else:
                self.headers = ["sku", "postCode", "price"]
            self._update_group_columns(self.headers)
        self.last_valid_docs, self.last_errors, self.last_warnings = valid_docs, errors, warnings
        self.preview_box.config(state="normal")
        self.preview_box.delete("1.0", tk.END)
        self.preview_box.insert(tk.END, "postCode,sku,price,\n", ("head",))
        for doc in valid_docs[:100]:
            line = f"{doc['postCode']},{doc['sku']},{doc['price']},\n"
            self.preview_box.insert(tk.END, line, ("good",))
        self.preview_box.config(state="disabled")
        total_rows_est = len(valid_docs) + len(errors)
        dup_count = sum(1 for e in errors if "Duplicate id" in e.get("error", ""))
        uniq_skus = len({d["sku"] for d in valid_docs})
        prices = [d["price"] for d in valid_docs]
        pmin = min(prices) if prices else None
        pmax = max(prices) if prices else None
        pavg = round(mean(prices), 6) if prices else None
        warn_count = len(warnings)
        self.cached_stats = {
            "rows_total": total_rows_est,
            "rows_valid": len(valid_docs),
            "rows_invalid": len(errors),
            "duplicates": dup_count,
            "unique_skus": uniq_skus,
            "price_min": pmin,
            "price_max": pmax,
            "price_avg": pavg,
            "warnings": warn_count,
        }
        self.stats_box.config(state="normal")
        self.stats_box.delete("1.0", tk.END)
        def put(line: str, tag: Optional[str] = None):
            """Insert a line into stats_box with optional tag."""
            self.stats_box.insert(tk.END, f"{line}\n", (tag,) if tag else ())
        fmt = lambda n: f"{n:,}"
        put("Rows:", "head")
        put(f"  Estimated: {fmt(total_rows_est)}", "good" if total_rows_est > 0 else "bad")
        put(f"  Valid: {fmt(len(valid_docs))}", "good" if len(valid_docs) > 0 else "bad")
        put(f"  Invalid: {fmt(len(errors))}", "bad" if len(errors) > 0 else "good")
        put("\nData Quality:", "head")
        put(f"  Duplicates: {fmt(dup_count)}", "bad" if dup_count > 0 else "good")
        put(f"  Unique SKUs: {fmt(uniq_skus)}", "good" if uniq_skus > 0 else "bad")
        put("\nPrice Statistics:", "head")
        if pmin is not None:
            put(f"  Min: {pmin}", "good")
            put(f"  Max: {pmax}", "good")
            put(f"  Avg: {pavg}", "good")
        else:
            put("  N/A", "bad")
        put("\nWarnings:", "head")
        put(f"  Count: {fmt(warn_count)}", "bad" if warn_count > 0 else "good")
        if errors:
            self.stats_box.insert(tk.END, "\nIssues (first 50):\n", ("bad",))
            for e in errors[:50]:
                row = e.get("row", "N/A")
                context = e.get("context", "")
                error_msg = e.get("error", "")
                self.stats_box.insert(tk.END, f"Row {row}: {context} -> {error_msg}\n", ("bad",))
        self.stats_box.config(state="disabled")
    def export_files(self) -> None:
        if not self.last_valid_docs and not self.last_errors: self.preview_data()
        if not self.last_valid_docs and self.last_errors:
            messagebox.showerror("Error", "No valid rows to export (all invalid)."); return
        csv_rows = [{"postCode": d["postCode"].lstrip("0") if d["postCode"] else "","sku": d["sku"], "price": d["price"]} for d in self.last_valid_docs]
        json_rows = [{"postCode": d["postCode"], "sku": d["sku"], "price": d["price"]} for d in self.last_valid_docs]
        base_name = os.path.splitext(os.path.basename(self.file_path or 'pasted'))[0]
        base_name_snake = base_name.lower().replace("-", "_").replace(" ", "_")
        export_folder = self.output_folder_var.get().strip() or os.path.abspath("export")
        os.makedirs(export_folder, exist_ok=True)
        if self.enable_batch_var.get():
            mode = self.batch_mode_var.get()
            if mode == "rows":
                try: chunk = max(1, int(self.rows_per_file_var.get()))
                except Exception: chunk = 1000
                self._export_by_rows(export_folder, base_name_snake, csv_rows, json_rows, chunk)
            else:
                group_col = (self.group_column_var.get() or "").strip()
                self._export_by_group(export_folder, base_name_snake, json_rows, csv_rows, group_col)
        else:
            self._export_single(export_folder, base_name_snake, csv_rows, json_rows)
        if self.last_errors:
            error_path = os.path.join(export_folder, f"{base_name_snake}_errors.csv")
            try:
                with open(error_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=["row", "context", "error"])
                    writer.writeheader(); writer.writerows(self.last_errors)
            except Exception as e:
                messagebox.showwarning("Warning", f"Failed to write error file:\n{e}")
        if self.open_folder_after_var.get():
            try: self._open_folder(export_folder)
            except Exception: pass
        messagebox.showinfo("Success", f"Export completed.\nFiles saved in:\n{export_folder}")
    def _export_single(self, folder: str, base: str, csv_rows: list[dict], json_rows: list[dict]) -> None:
        ts = self._ts()
        if self.export_csv_var.get():
            csv_path = self._render_path(folder, base, batch="all", group="all", ts=ts, ext="csv")
            fields = ["postCode", "sku", "price"] 
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(csv_rows)
        if self.export_json_var.get():
            json_path = self._render_path(folder, base, batch="all", group="all", ts=ts, ext="json")
            with open(json_path, 'w', encoding='utf-8') as f: json.dump(json_rows, f, indent=4)
    def _export_by_rows(self, folder: str, base: str, csv_rows: list[dict], json_rows: list[dict], chunk_size: int) -> None:
        total = len(csv_rows)
        if total == 0: return
        ts = self._ts(); parts = (total + chunk_size - 1) // chunk_size
        fields = ["postCode", "sku", "price"]
        for i in range(parts):
            start, end = i * chunk_size, min((i+1) * chunk_size, total)
            batch_id = f"part{(i+1):03d}"
            if self.export_csv_var.get():
                path = self._render_path(folder, base, batch=batch_id, group="all", ts=ts, ext="csv")
                with open(path, 'w', newline='', encoding='utf-8') as f:
                    w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(csv_rows[start:end])
            if self.export_json_var.get():
                path = self._render_path(folder, base, batch=batch_id, group="all", ts=ts, ext="json")
                with open(path, 'w', encoding='utf-8') as f: json.dump(json_rows[start:end], f, indent=4)
    def _export_by_group(self, folder: str, base: str, json_rows: list[dict], csv_rows: list[dict], group_col: str) -> None:
        ts = self._ts(); key_lower = (group_col or "").lower()
        def key_for_json(d: dict) -> str:
            if key_lower in ("postcode", "post_code", "post code"): return d.get("postCode", "") or "UNK"
            if key_lower == "sku": return d.get("sku", "") or "UNK"
            if key_lower == "price": return str(d.get("price", "")) or "UNK"
            return str(d.get(key_lower, "") or "UNK")
        groups_json, groups_csv = defaultdict(list), defaultdict(list)
        for j, c in zip(json_rows, csv_rows):
            g = (key_for_json(j) or "UNK").strip() or "UNK"
            groups_json[g].append(j); groups_csv[g].append(c)
        fields = ["postCode", "sku", "price"]
        for gval, rows_csv in groups_csv.items():
            safe_group = self._sanitize_group(gval)
            if self.export_csv_var.get():
                path = self._render_path(folder, base, batch="group", group=safe_group, ts=ts, ext="csv")
                with open(path, 'w', newline='', encoding='utf-8') as f:
                    w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(rows_csv)
            if self.export_json_var.get():
                rows_json = groups_json[gval]
                path = self._render_path(folder, base, batch="group", group=safe_group, ts=ts, ext="json")
                with open(path, 'w', encoding='utf-8') as f: json.dump(rows_json, f, indent=4)
    def _save_all_settings(self, silent: bool = False) -> None:
        self.settings["export"]["folder"] = self.output_folder_var.get().strip()
        self.settings["export"]["open_folder_after"] = bool(self.open_folder_after_var.get())
        self.settings["export"]["filename_pattern"] = self.filename_pattern_var.get().strip()
        self.settings["export"]["formats"]["csv"] = bool(self.export_csv_var.get())
        self.settings["export"]["formats"]["json"] = bool(self.export_json_var.get())
        self.settings["batch"]["enabled"] = bool(self.enable_batch_var.get())
        self.settings["batch"]["mode"] = self.batch_mode_var.get()
        self.settings["batch"]["rows_per_file"] = int(self.rows_per_file_var.get() or 1000)
        self.settings["batch"]["group_column"] = self.group_column_var.get().strip()
        self._ensure_export_dir()
        save_settings(self.settings)
        if not silent: messagebox.showinfo("Settings", "Settings saved.")
    def _render_path(self, folder: str, base: str, batch: str, group: str, ts: str, ext: str) -> str:
        pattern = (self.filename_pattern_var.get() or "{base}_{batch}_{group}_{ts}.{ext}").strip()
        vals = {"base": base, "batch": batch, "group": group if group else "all", "ts": ts, "ext": ext.lstrip(".")}
        return os.path.join(folder, pattern.format(**vals))
    def _ts(self) -> str: return datetime.now().strftime("%Y%m%d_%H%M%S")
    def _sanitize_group(self, s: str) -> str:
        out = "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in s)
        return out[:80] if out else "UNK"
    def _open_folder(self, path: str) -> None:
        sys = platform.system()
        if sys == "Windows": subprocess.Popen(f'explorer "{path}"', shell=True)
        elif sys == "Darwin": subprocess.Popen(["open", path])
        else: subprocess.Popen(["xdg-open", path])
    def _choose_db_path(self) -> None:
        path = filedialog.asksaveasfilename(defaultextension=".sqlite", filetypes=[("SQLite DB", "*.sqlite *.db")], initialfile=os.path.basename(self.sqlite_path_var.get() or "export_runs.sqlite"))
        if path: self.sqlite_path_var.set(path)
    def _open_config_folder(self) -> None:
        try:
            if platform.system() == "Windows": os.startfile(APP_DIR)
            elif platform.system() == "Darwin": subprocess.Popen(["open", APP_DIR])
            else: subprocess.Popen(["xdg-open", APP_DIR])
        except Exception: messagebox.showwarning("Open", f"Folder: {APP_DIR}")
if __name__ == "__main__":
    root = tk.Tk()
    App(root)
    root.mainloop()
