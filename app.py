"""
SQL Importer - High-Performance Streaming MySQL .sql Import Tool.

Streams statement-by-statement, keeping memory usage constant regardless of file size.
Includes real-time progress streaming (SSE), connection testing, database creation,
and robust sanitization for dump-specific quirks.
"""
import json
import os
import re
import tempfile
import time
from typing import Generator

import pymysql
from flask import Flask, Response, jsonify, render_template, request, stream_with_context

app = Flask(__name__)
# 4 GB upload limit
app.config["MAX_CONTENT_LENGTH"] = 4 * 1024 * 1024 * 1024

CREATE_TABLE_RE = re.compile(
    r'^\s*CREATE\s+(?:TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[`"]?[\w$]+[`"]?\.)?[`"]?([\w$]+)[`"]?',
    re.IGNORECASE,
)
CREATE_VIEW_RE = re.compile(
    r'^\s*CREATE\s+(?:OR\s+REPLACE\s+)?(?:ALGORITHM\s*=\s*\w+\s+)?(?:DEFINER\s*=\s*`?[^`]+`?@`?[^`]+`?\s+)?(?:SQL\s+SECURITY\s+\w+\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[`"]?[\w$]+[`"]?\.)?[`"]?([\w$]+)[`"]?',
    re.IGNORECASE,
)
USE_RE = re.compile(r'^\s*USE\s+[`"]?([\w$]+)[`"]?\s*;?$', re.IGNORECASE)
DELIMITER_RE = re.compile(r"^DELIMITER\s+(\S+)", re.IGNORECASE)
ALTER_FK_RE = re.compile(
    r'ALTER\s+TABLE\s+(?:[`"]?[\w$]+[`"]?\.)?[`"]?([\w$]+)[`"]?\s+ADD\s+(?:CONSTRAINT\s+[`"]?[\w$]+[`"]?\s+)?FOREIGN\s+KEY\s*\([`"]?([\w$]+)[`"]?\)\s+REFERENCES\s+(?:[`"]?[\w$]+[`"]?\.)?[`"]?([\w$]+)[`"]?\s*\([`"]?([\w$]+)[`"]?\)',
    re.IGNORECASE,
)


def parse_create_table(sql: str) -> dict:
    """Extract table name, columns, and foreign keys from CREATE TABLE statement."""
    table_match = re.search(
        r'CREATE\s+(?:TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[`"]?[\w$]+[`"]?\.)?[`"]?([\w$]+)[`"]?\s*\((.*)\)[^)]*$',
        sql,
        re.IGNORECASE | re.DOTALL,
    )
    if not table_match:
        m = CREATE_TABLE_RE.match(sql)
        tbl_name = m.group(1) if m else "table"
        return {"name": tbl_name, "columns": [], "foreign_keys": []}

    table_name = table_match.group(1)
    body = table_match.group(2)

    lines: list[str] = []
    buf: list[str] = []
    depth = 0
    in_quote = None
    for c in body:
        if in_quote:
            buf.append(c)
            if c == in_quote:
                in_quote = None
        elif c in ("'", '"', '`'):
            in_quote = c
            buf.append(c)
        elif c == '(':
            depth += 1
            buf.append(c)
        elif c == ')':
            depth -= 1
            buf.append(c)
        elif c == ',' and depth == 0:
            lines.append("".join(buf).strip())
            buf = []
        else:
            buf.append(c)
    if buf:
        lines.append("".join(buf).strip())

    columns: list[dict] = []
    primary_keys: set[str] = set()
    foreign_keys: list[dict] = []

    for line in lines:
        pk_m = re.match(r'^\s*PRIMARY\s+KEY\s*\(([^)]+)\)', line, re.IGNORECASE)
        if pk_m:
            cols = [re.sub(r'[`"\s]', '', c) for c in pk_m.group(1).split(',')]
            primary_keys.update(cols)
            continue

        fk_m = re.match(
            r'^\s*(?:CONSTRAINT\s+[`"]?[\w$]+[`"]?\s+)?FOREIGN\s+KEY\s*\([`"]?([\w$]+)[`"]?\)\s+REFERENCES\s+(?:[`"]?[\w$]+[`"]?\.)?[`"]?([\w$]+)[`"]?\s*\([`"]?([\w$]+)[`"]?\)',
            line,
            re.IGNORECASE,
        )
        if fk_m:
            foreign_keys.append({
                "from_col": fk_m.group(1),
                "to_table": fk_m.group(2),
                "to_col": fk_m.group(3),
            })
            continue

    for line in lines:
        line_clean = line.strip()
        if not line_clean:
            continue
        if re.match(r'^(?:PRIMARY\s+KEY|KEY|INDEX|UNIQUE|CONSTRAINT|FULLTEXT|SPATIAL|CHECK)\b', line_clean, re.IGNORECASE):
            continue

        col_m = re.match(r'^[`"]?([\w$]+)[`"]?\s+([A-Za-z0-9_]+(?:\([^)]*\))?)', line_clean)
        if col_m:
            col_name = col_m.group(1)
            col_type = col_m.group(2).lower()
            is_pk = (col_name in primary_keys) or bool(re.search(r'\bPRIMARY\s+KEY\b', line_clean, re.IGNORECASE))
            is_nullable = not bool(re.search(r'\bNOT\s+NULL\b', line_clean, re.IGNORECASE))
            columns.append({
                "name": col_name,
                "type": col_type,
                "pk": is_pk,
                "nullable": is_nullable,
            })

    return {
        "name": table_name,
        "columns": columns,
        "foreign_keys": foreign_keys,
    }



def iter_statements(fh) -> Generator[tuple[str, int], None, None]:
    """Stream a SQL file and yield (statement, approximate_byte_offset).

    Handles quotes, backslash escapes, -- / # / /* */ comments,
    executable /*! ... */ blocks and the DELIMITER command.
    Memory use stays flat even on multi-gigabyte dumps.
    """
    delimiter = ";"
    buf: list[str] = []
    has_content = False
    quote = None
    in_block = False

    while True:
        line = fh.readline()
        if not line:
            break

        # Whole-line shortcuts, only when not in middle of a statement or comment
        if not has_content and quote is None and not in_block:
            stripped = line.strip()
            if not stripped or stripped.startswith("--") or stripped.startswith("#"):
                continue
            m = DELIMITER_RE.match(stripped)
            if m:
                delimiter = m.group(1)
                buf = []
                continue

        i, n = 0, len(line)
        while i < n:
            c = line[i]

            if in_block:
                if line.startswith("*/", i):
                    in_block = False
                    i += 2
                else:
                    i += 1
                continue

            if quote:
                buf.append(c)
                if c == "\\" and quote != "`":
                    if i + 1 < n:
                        buf.append(line[i + 1])
                        i += 1
                elif c == quote:
                    quote = None
                i += 1
                continue

            # Line comments: "-- " or "#"
            if c == "#" or (
                line.startswith("--", i) and (i + 2 >= n or line[i + 2] in " \t\r\n")
            ):
                buf.append("\n")
                break

            # Block comment (keep executable /*! ... */ blocks)
            if line.startswith("/*", i) and not line.startswith("/*!", i):
                in_block = True
                i += 2
                continue

            if c in "'\"`":
                quote = c
                buf.append(c)
                has_content = True
                i += 1
                continue

            if line.startswith(delimiter, i):
                sql = "".join(buf).strip()
                buf = []
                has_content = False
                i += len(delimiter)
                if sql:
                    yield sql, fh.tell()
                continue

            buf.append(c)
            if not c.isspace():
                has_content = True
            i += 1

    sql = "".join(buf).strip()
    if sql:
        yield sql, fh.tell()


def qid(name: str) -> str:
    """Quote a MySQL identifier safely."""
    return "`" + name.replace("`", "``") + "`"


def sanitize_statement(sql: str, target_db: str, force_target_db: bool = True) -> str:
    """Rewrite statements that hardcode external database names or USE commands."""
    if force_target_db:
        # Override USE `database`;
        if USE_RE.match(sql.strip()):
            return f"USE {qid(target_db)}"

        # Strip database qualifier on CREATE TABLE `dbname`.`tablename`
        sql = re.sub(
            r'^\s*(CREATE\s+(?:TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?)[`"]?[\w$]+[`"]?\.',
            r"\1",
            sql,
            flags=re.IGNORECASE,
        )
        # Strip database qualifier on CREATE VIEW `dbname`.`viewname`
        sql = re.sub(
            r'^\s*(CREATE\s+(?:OR\s+REPLACE\s+)?(?:ALGORITHM\s*=\s*\w+\s+)?(?:DEFINER\s*=\s*`?[^`]+`?@`?[^`]+`?\s+)?(?:SQL\s+SECURITY\s+\w+\s+)?VIEW\s+(?:IF\s+NOT\s+EXISTS\s+)?)[`"]?[\w$]+[`"]?\.',
            r"\1",
            sql,
            flags=re.IGNORECASE,
        )
    return sql


def sse_event(event_type: str, data: dict) -> str:
    """Format Server-Sent Event payload."""
    payload = {"type": event_type, **data}
    return f"data: {json.dumps(payload)}\n\n"


@app.get("/")
def index():
    return render_template("index.html")


@app.post("/test-connection")
def test_connection():
    """Verify MySQL credentials and return server version and accessible databases."""
    data = request.get_json(silent=True) or request.form
    host = data.get("host", "127.0.0.1").strip() or "127.0.0.1"
    port_str = data.get("port", "3306").strip()
    try:
        port = int(port_str or 3306)
    except ValueError:
        return jsonify(ok=False, message=f"Invalid port: {port_str}"), 400

    user = data.get("user", "root").strip()
    password = data.get("password", "")

    try:
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            charset="utf8mb4",
            connect_timeout=6,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT VERSION()")
            ver_row = cur.fetchone()
            version = ver_row[0] if ver_row else "Unknown"

            cur.execute("SHOW DATABASES")
            system_dbs = {"information_schema", "mysql", "performance_schema", "sys"}
            databases = [
                row[0] for row in cur.fetchall() if row[0].lower() not in system_dbs
            ]
        conn.close()
        return jsonify(ok=True, version=version, databases=databases)
    except pymysql.MySQLError as exc:
        return jsonify(ok=False, message=f"MySQL Error: {exc}"), 400
    except Exception as exc:
        return jsonify(ok=False, message=f"Connection Error: {exc}"), 400


@app.post("/import")
def import_sql():
    """Stream import execution with real-time SSE progress."""
    uploaded_file = request.files.get("sqlfile")
    if not uploaded_file or not uploaded_file.filename:
        return jsonify(ok=False, message="Please choose a valid .sql file."), 400

    cfg = {
        "host": request.form.get("host", "127.0.0.1").strip() or "127.0.0.1",
        "port": request.form.get("port", "3306").strip() or "3306",
        "user": request.form.get("user", "root").strip() or "root",
        "password": request.form.get("password", ""),
        "database": request.form.get("database", "").strip(),
    }
    if not cfg["database"]:
        base_name = os.path.splitext(uploaded_file.filename)[0]
        cfg["database"] = re.sub(r'[^a-zA-Z0-9_]', '_', base_name).lower() or "schema_diagram"

    fresh = request.form.get("fresh") == "on"
    keep_going = request.form.get("keep_going") == "on"
    create_db = request.form.get("create_db") == "on"
    force_target_db = request.form.get("force_target_db", "on") == "on"

    fd, tmp_path = tempfile.mkstemp(suffix=".sql")
    os.close(fd)
    uploaded_file.save(tmp_path)
    total_bytes = os.path.getsize(tmp_path)

    def generate():
        started = time.time()
        last_progress_time = started
        executed = 0
        tables = []
        views = []
        errors = []

        conn = None
        try:
            yield sse_event("init", {
                "message": "Connecting to MySQL server...",
                "file_size": total_bytes,
            })

            conn_args = dict(
                host=cfg["host"],
                port=int(cfg["port"] or 3306),
                user=cfg["user"],
                password=cfg["password"],
                charset="utf8mb4",
                autocommit=True,
                max_allowed_packet=128 * 1024 * 1024,
                connect_timeout=12,
            )

            # Create database if requested
            if create_db:
                yield sse_event("log", {"message": f"Ensuring database {cfg['database']} exists..."})
                tmp_args = dict(conn_args)
                tmp = pymysql.connect(**tmp_args)
                try:
                    with tmp.cursor() as cur:
                        cur.execute(
                            f"CREATE DATABASE IF NOT EXISTS {qid(cfg['database'])} "
                            "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                        )
                finally:
                    tmp.close()

            # Connect to target database
            conn = pymysql.connect(database=cfg["database"], **conn_args)

            with conn.cursor() as cur:
                cur.execute("SET FOREIGN_KEY_CHECKS=0")
                cur.execute("SET UNIQUE_CHECKS=0")
                cur.execute("SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO'")

                if fresh:
                    yield sse_event("log", {"message": "Dropping existing tables and views..."})
                    cur.execute("SHOW FULL TABLES")
                    all_tables = cur.fetchall()
                    for name, kind in all_tables:
                        obj_type = "VIEW" if kind == "VIEW" else "TABLE"
                        cur.execute(f"DROP {obj_type} IF EXISTS {qid(name)}")

                yield sse_event("log", {"message": "Executing SQL statements..."})

                with open(tmp_path, "r", encoding="utf-8-sig", errors="replace") as fh:
                    for sql, byte_pos in iter_statements(fh):
                        sql = sanitize_statement(sql, cfg["database"], force_target_db)
                        current_table_name = None

                        try:
                            cur.execute(sql)
                            executed += 1

                            table_match = CREATE_TABLE_RE.match(sql)
                            if table_match:
                                tbl = table_match.group(1)
                                tables.append(tbl)
                                current_table_name = tbl
                                schema = parse_create_table(sql)
                                yield sse_event("table_created", {
                                    "table": tbl,
                                    "is_view": False,
                                    "columns": schema.get("columns", []),
                                    "foreign_keys": schema.get("foreign_keys", []),
                                    "table_count": len(tables),
                                    "statements": executed,
                                })

                            view_match = CREATE_VIEW_RE.match(sql)
                            if view_match:
                                vw = view_match.group(1)
                                views.append(vw)
                                if not current_table_name:
                                    current_table_name = f"{vw} (view)"
                                yield sse_event("table_created", {
                                    "table": vw,
                                    "is_view": True,
                                    "columns": [],
                                    "foreign_keys": [],
                                    "table_count": len(tables) + len(views),
                                    "statements": executed,
                                })

                            alter_fk = ALTER_FK_RE.search(sql)
                            if alter_fk:
                                yield sse_event("relation_created", {
                                    "from_table": alter_fk.group(1),
                                    "from_col": alter_fk.group(2),
                                    "to_table": alter_fk.group(3),
                                    "to_col": alter_fk.group(4),
                                })


                        except Exception as exc:
                            err_entry = {
                                "sql": re.sub(r"\s+", " ", sql)[:180],
                                "error": str(exc),
                            }
                            errors.append(err_entry)
                            yield sse_event("error_item", {
                                "sql": err_entry["sql"],
                                "error": err_entry["error"],
                                "error_count": len(errors),
                            })
                            if not keep_going:
                                yield sse_event("log", {"message": "Import halted due to error."})
                                break

                        now = time.time()
                        # Send periodic progress updates (every 25 statements or 0.2s or when a table is created)
                        if current_table_name or executed % 25 == 0 or (now - last_progress_time) > 0.25:
                            pct = min(99, int((byte_pos / total_bytes) * 100)) if total_bytes > 0 else 0
                            yield sse_event("progress", {
                                "percent": pct,
                                "statements": executed,
                                "table_count": len(tables),
                                "view_count": len(views),
                                "latest_table": current_table_name,
                                "tables": tables,
                                "views": views,
                                "error_count": len(errors),
                                "seconds": round(now - started, 1),
                            })
                            last_progress_time = now

                cur.execute("SET FOREIGN_KEY_CHECKS=1")
                cur.execute("SET UNIQUE_CHECKS=1")

            total_seconds = round(time.time() - started, 2)
            yield sse_event("done", {
                "ok": not errors,
                "percent": 100,
                "statements": executed,
                "tables": tables,
                "table_count": len(tables),
                "views": views,
                "view_count": len(views),
                "errors": errors[:100],
                "error_count": len(errors),
                "seconds": total_seconds,
            })

        except pymysql.MySQLError as exc:
            yield sse_event("fatal", {"message": f"MySQL Error: {exc}"})
        except Exception as exc:
            yield sse_event("fatal", {"message": f"Execution Error: {exc}"})
        finally:
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            try:
                os.remove(tmp_path)
            except OSError:
                pass

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.get("/favicon.ico")
def favicon():
    """Serve a lightweight SVG database favicon."""
    svg = (
        '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="#6366f1">'
        '<ellipse cx="12" cy="5" rx="9" ry="3"/>'
        '<path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/>'
        '<path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>'
        '</svg>'
    )
    return Response(svg, mimetype="image/svg+xml")


# Suppress Werkzeug's development server warning banner cleanly if werkzeug is used
import logging


class _NoDevWarningFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return "development server" not in record.getMessage()


logging.getLogger("werkzeug").addFilter(_NoDevWarningFilter())

if __name__ == "__main__":
    host = "127.0.0.1"
    port = 5000
    print(f"🚀 SQL Importer server running on http://{host}:{port}")
    try:
        from waitress import serve
        serve(app, host=host, port=port, threads=8)
    except ImportError:
        app.run(host=host, port=port, debug=False, threaded=True)



