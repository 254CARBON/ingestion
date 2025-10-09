#!/usr/bin/env python3
"""
Apply ClickHouse schema DDLs for the 254Carbon ingestion pipeline.

This helper script loads the Bronze / Silver / Gold schema definitions via the
ClickHouse HTTP interface so developers can stand up the exemplar pipeline
without manually copying statements into the database.
"""

from __future__ import annotations

import argparse
import base64
import http.client
import sys
import urllib.parse
from pathlib import Path
from typing import Iterable, List


DEFAULT_SCHEMA_FILES = [
    "configs/clickhouse/schemas/bronze.sql",
    "configs/clickhouse/schemas/silver.sql",
    "configs/clickhouse/schemas/gold.sql",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply ClickHouse schema DDLs for the ingestion pipeline."
    )
    parser.add_argument("--host", default="localhost", help="ClickHouse HTTP host (default: localhost)")
    parser.add_argument("--port", type=int, default=8123, help="ClickHouse HTTP port (default: 8123)")
    parser.add_argument("--user", default="default", help="ClickHouse username (default: default)")
    parser.add_argument("--password", default="", help="ClickHouse password (default: blank)")
    parser.add_argument("--database", default="carbon_ingestion", help="Target database (default: carbon_ingestion)")
    parser.add_argument("--timeout", type=int, default=30, help="Request timeout in seconds (default: 30)")
    parser.add_argument(
        "--schema-dir",
        default=None,
        help="Directory containing schema SQL files (overrides repo defaults)",
    )
    parser.add_argument(
        "--files",
        nargs="+",
        default=None,
        help="Specific schema files to apply (absolute or relative paths)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the files that would be applied without executing statements",
    )
    return parser.parse_args()


def resolve_paths(files: Iterable[str], schema_dir: Path | None, repo_root: Path) -> List[Path]:
    resolved: List[Path] = []
    for entry in files:
        candidate = Path(entry)
        if candidate.is_file():
            resolved.append(candidate.resolve())
            continue

        if schema_dir:
            dir_candidate = (schema_dir / candidate).resolve()
            if dir_candidate.is_file():
                resolved.append(dir_candidate)
                continue

        repo_candidate = (repo_root / candidate).resolve()
        if repo_candidate.is_file():
            resolved.append(repo_candidate)
            continue

        raise FileNotFoundError(f"Schema file not found: {entry}")

    return resolved


def build_basic_auth_header(user: str, password: str) -> str | None:
    if not user and not password:
        return None
    token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("utf-8")
    return f"Basic {token}"


def execute_sql(
    *,
    host: str,
    port: int,
    database: str,
    sql: str,
    timeout: int,
    auth_header: str | None,
) -> str:
    connection = http.client.HTTPConnection(host, port, timeout=timeout)
    path = f"/?database={urllib.parse.quote(database)}"
    headers = {
        "Content-Type": "text/plain; charset=utf-8",
        "User-Agent": "254Carbon-DDL/1.0",
    }
    if auth_header:
        headers["Authorization"] = auth_header

    connection.request("POST", path, body=sql.encode("utf-8"), headers=headers)
    response = connection.getresponse()
    body = response.read().decode("utf-8", errors="ignore")
    connection.close()

    if response.status >= 400:
        raise RuntimeError(
            f"ClickHouse error {response.status} {response.reason}: {body.strip()}"
        )

    return body.strip()


def main() -> int:
    args = parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    schema_dir = Path(args.schema_dir).resolve() if args.schema_dir else None

    schema_files = args.files or DEFAULT_SCHEMA_FILES
    try:
        resolved_files = resolve_paths(schema_files, schema_dir, repo_root)
    except FileNotFoundError as exc:
        print(f"[error] {exc}", file=sys.stderr)
        return 1

    if not resolved_files:
        print("[warn] No schema files resolved; nothing to do.")
        return 0

    auth_header = build_basic_auth_header(args.user, args.password)

    for path in resolved_files:
        sql = path.read_text(encoding="utf-8").strip()
        if not sql:
            print(f"[skip] {path} is empty, skipping.")
            continue

        print(f"[apply] {path}")
        if args.dry_run:
            continue

        try:
            execute_sql(
                host=args.host,
                port=args.port,
                database=args.database,
                sql=sql,
                timeout=args.timeout,
                auth_header=auth_header,
            )
        except Exception as exc:  # pragma: no cover - network failure path
            print(f"[error] Failed to apply {path}: {exc}", file=sys.stderr)
            return 1

    print("[ok] ClickHouse schemas applied successfully.")
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
