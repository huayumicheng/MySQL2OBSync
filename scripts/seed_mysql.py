import argparse
import os
import random
import string
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pymysql


@dataclass(frozen=True)
class DBConfig:
    host: str
    port: int
    user: str
    password: str
    database: str


def env_str(name: str, default: str | None = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise SystemExit(f"missing env {name}")
    return v


def env_int(name: str, default: int | None = None) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        if default is None:
            raise SystemExit(f"missing env {name}")
        return default
    return int(v)


def load_db_config(args: argparse.Namespace) -> DBConfig:
    host = args.host or os.getenv("MYSQL_HOST") or "127.0.0.1"
    port = args.port or env_int("MYSQL_PORT", 3306)
    user = args.user or env_str("MYSQL_USER", "root")
    password = args.password if args.password is not None else os.getenv("MYSQL_PASSWORD", "")
    database = args.database or env_str("MYSQL_DB", "sync_check")
    return DBConfig(host=host, port=port, user=user, password=password, database=database)


def rand_str(n: int) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choice(alphabet) for _ in range(n))


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def to_mysql_naive(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def unix_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def gen_rows_sync_test_ai(count: int, base: datetime) -> list[tuple]:
    rows: list[tuple] = []
    for i in range(count):
        ct = base + timedelta(milliseconds=i * 10)
        biz_id = f"biz_{unix_ms(ct)}_{i}"
        amount = round(random.uniform(1, 9999), 2)
        rows.append((biz_id, amount, to_mysql_naive(ct)))
    return rows


def gen_rows_sync_test_pk(count: int, base: datetime) -> list[tuple]:
    rows: list[tuple] = []
    for i in range(count):
        ct = base + timedelta(milliseconds=i * 10)
        pk_id = unix_ms(ct)
        payload = f"p_{rand_str(24)}"
        status = i % 4
        rows.append((pk_id, to_mysql_naive(ct), payload, status))
    return rows


def chunked(seq: list[tuple], size: int) -> list[list[tuple]]:
    if size <= 0:
        raise SystemExit("batch-size must be > 0")
    return [seq[i : i + size] for i in range(0, len(seq), size)]


def insert_rows(conn: pymysql.connections.Connection, table: str, rows: list[tuple], batch_size: int) -> int:
    if table == "sync_test_ai":
        sql = "INSERT INTO sync_test_ai (biz_id, amount, create_time) VALUES (%s, %s, %s)"
    elif table == "sync_test_pk":
        sql = "INSERT INTO sync_test_pk (pk_id, create_time, payload, status) VALUES (%s, %s, %s, %s)"
    else:
        raise SystemExit("unsupported table, use sync_test_ai or sync_test_pk")

    total = 0
    with conn.cursor() as cur:
        for batch in chunked(rows, batch_size):
            cur.executemany(sql, batch)
            total += len(batch)
    return total


def main() -> int:
    parser = argparse.ArgumentParser(prog="seed_mysql.py")
    parser.add_argument("--table", required=True, choices=["sync_test_ai", "sync_test_pk"])
    parser.add_argument("--count", required=True, type=int)
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--user")
    parser.add_argument("--password")
    parser.add_argument("--database")
    args = parser.parse_args()

    if args.count <= 0:
        raise SystemExit("--count must be > 0")

    cfg = load_db_config(args)
    base = utc_now() - timedelta(seconds=args.count)

    if args.table == "sync_test_ai":
        rows = gen_rows_sync_test_ai(args.count, base)
    else:
        rows = gen_rows_sync_test_pk(args.count, base)

    conn = pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        charset="utf8mb4",
        autocommit=False,
    )
    try:
        inserted = insert_rows(conn, args.table, rows, args.batch_size)
        conn.commit()
        print(f"inserted={inserted} table={args.table} db={cfg.database} host={cfg.host}:{cfg.port}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
