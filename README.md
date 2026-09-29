# SQL Importer Pro (Python + Modern Web UI)

A high-performance streaming tool to import massive `.sql` dump files (100+ tables, gigabytes in size) into MySQL with real-time live progress.

---

## ⚡ Quick Start (Mac / Linux)

Just run the one-click startup script from your terminal:

```bash
./run.sh
```

*(This automatically detects Python 3, activates/creates the virtual environment, checks dependencies, and launches the app!)*

Then open: **[http://127.0.0.1:5000](http://127.0.0.1:5000)**

---

## 🛠 Manual Setup & Run

### macOS / Linux:
> **Note on macOS:** macOS does not include `python` by default (only `python3`). Use `python3` or activate the virtualenv first:

```bash
# 1. Activate the included virtual environment
source venv/bin/activate

# 2. (Optional) Install dependencies if needed
pip install -r requirements.txt

# 3. Run the application
python app.py
# (Or without activating venv: ./venv/bin/python app.py)
```

### Windows:
Double-click `run.bat` or run in Command Prompt / PowerShell:
```cmd
run.bat
```
Or manually:
```cmd
venv\Scripts\activate
python app.py
```

---

## Features

- **⚡ Memory-Safe Streaming**: Uses constant memory by streaming statements one by one. Handles multi-gigabyte SQL files smoothly without loading the entire dump into RAM.
- ** Real-Time SSE Progress**: Live percentage progress bar, statement counter, execution speed (stmts/sec), live table creation feedback, and elapsed timer.
- ** Test Connection & DB Discovery**: Verify MySQL credentials with one click, inspect the MySQL version, and select from existing databases.
- ** Smart Dump Sanitization**:
  - Automatically overrides dump-specific `USE database;` statements to target your chosen database.
  - Automatically strips external database qualifiers (e.g. `CREATE TABLE other_db.users` &rarr; `users`).
  - Supports `CREATE TABLE`, `CREATE VIEW`, `DELIMITER` blocks, triggers, and stored procedures.
  - Temporarily disables `FOREIGN_KEY_CHECKS` and `UNIQUE_CHECKS` during import so foreign key table order doesn't cause errors.
- ** Table Browser & Error Inspector**:
  - Filter and search through all created tables and views.
  - Click any table badge to quickly copy its name.
  - Detailed error inspector with SQL snippet preview and "Copy All" button.
  - Export a complete import summary report (`.txt`).
