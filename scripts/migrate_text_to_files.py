"""
One-shot migration: move chapters.chinese_text / english_text from the DB
to .zh.txt / .en.txt files on disk, then drop the columns and VACUUM.

Safe to re-run — skips rows whose files already exist.

Usage:
    python scripts/migrate_text_to_files.py            # dry-run
    python scripts/migrate_text_to_files.py --apply    # write files only
    python scripts/migrate_text_to_files.py --apply --drop-columns
                                                       # write files, drop
                                                       # columns, VACUUM
"""
import argparse
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import get_database_path
from app.pipeline.chapter_storage import (
    write_zh, write_en, zh_path, en_path,
)


def main(apply: bool, drop_columns: bool) -> int:
    db_path = str(get_database_path())
    print(f"Database: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    cols = {r[1] for r in conn.execute("PRAGMA table_info(chapters)").fetchall()}
    has_zh_col = "chinese_text" in cols
    has_en_col = "english_text" in cols

    if not has_zh_col and not has_en_col:
        print("Columns already dropped — nothing to migrate.")
        conn.close()
        return 0

    select_cols = ["novel_id", "chapter_number"]
    if has_zh_col:
        select_cols.append("chinese_text")
    if has_en_col:
        select_cols.append("english_text")
    rows = conn.execute(
        f"SELECT {', '.join(select_cols)} FROM chapters"
    ).fetchall()

    zh_written = en_written = zh_skipped = en_skipped = 0

    for r in rows:
        nid, n = r["novel_id"], r["chapter_number"]
        zh = r["chinese_text"] if has_zh_col else None
        en = r["english_text"] if has_en_col else None

        if zh:
            if zh_path(nid, n).exists():
                zh_skipped += 1
            elif apply:
                write_zh(nid, n, zh)
                zh_written += 1
            else:
                zh_written += 1
        if en:
            if en_path(nid, n).exists():
                en_skipped += 1
            elif apply:
                write_en(nid, n, en)
                en_written += 1
            else:
                en_written += 1

    verb = "would write" if not apply else "wrote"
    print(f"{verb} {zh_written} .zh.txt files ({zh_skipped} already present)")
    print(f"{verb} {en_written} .en.txt files ({en_skipped} already present)")

    if not apply:
        print("\nDry run. Re-run with --apply to actually write files.")
        conn.close()
        return 0

    if drop_columns:
        print("\nDropping columns chinese_text / english_text...")
        if has_zh_col:
            conn.execute("ALTER TABLE chapters DROP COLUMN chinese_text")
        if has_en_col:
            conn.execute("ALTER TABLE chapters DROP COLUMN english_text")
        conn.commit()

        print("Running VACUUM to shrink the DB file...")
        conn.execute("VACUUM")
        conn.commit()
        print("Done.")
    else:
        print("\nFiles written. Re-run with --drop-columns to remove the")
        print("columns from the DB (make sure the app is stopped first).")

    conn.close()
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true",
                    help="Actually write files (default is dry-run)")
    ap.add_argument("--drop-columns", action="store_true",
                    help="After writing files, drop the DB columns and VACUUM")
    args = ap.parse_args()
    if args.drop_columns and not args.apply:
        print("--drop-columns requires --apply", file=sys.stderr)
        sys.exit(2)
    sys.exit(main(args.apply, args.drop_columns))
