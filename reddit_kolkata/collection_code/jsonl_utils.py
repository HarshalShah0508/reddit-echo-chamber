"""Read/write helpers for JSONL raw data with ID-based deduplication."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Callable, Iterator


def read_jsonl(path: Path) -> Iterator[dict]:
    if not path.exists():
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def append_jsonl_dedup(path: Path, records: list[dict], id_fn: Callable[[dict], str]) -> int:
    """Append records to path, skipping any whose id_fn(record) already exists in the file.

    Returns the number of records actually appended.
    """
    existing_ids = {id_fn(r) for r in read_jsonl(path)}
    path.parent.mkdir(parents=True, exist_ok=True)
    appended = 0
    with open(path, "a", encoding="utf-8") as f:
        for record in records:
            rid = id_fn(record)
            if rid in existing_ids:
                continue
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            existing_ids.add(rid)
            appended += 1
    return appended


def write_json_atomic(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
    os.replace(tmp_path, path)


def read_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
