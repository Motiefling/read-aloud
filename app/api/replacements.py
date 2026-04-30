"""
Find/replace rule management API.

Two flat tables back two user-facing popups:

* ``pre_translation_replacements`` -- substitutions applied to raw Chinese
  text before it is sent to Qwen.
* ``post_translation_replacements`` -- substitutions applied to Qwen's
  English output before it is sent to Kokoro.

Each row is either novel-scoped (``novel_id`` set) or global (``novel_id``
NULL).  Listing for a specific novel returns the union of that novel's rows
and all global rows so the worker only ever asks about one novel.
"""

from __future__ import annotations

import uuid
from typing import Literal

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.database import get_db


router = APIRouter()

Kind = Literal["pre", "post"]
_TABLE = {
    "pre": "pre_translation_replacements",
    "post": "post_translation_replacements",
}


# ===================== Schemas =====================

class ReplacementCreate(BaseModel):
    find_text: str = Field(..., min_length=1)
    replace_text: str = ""
    is_global: bool = False


class ReplacementUpdate(BaseModel):
    find_text: str | None = Field(default=None, min_length=1)
    replace_text: str | None = None
    is_global: bool | None = None


class ReplacementResponse(BaseModel):
    id: str
    novel_id: str | None
    is_global: bool
    find_text: str
    replace_text: str


# ===================== Helpers =====================

def _kind_or_404(kind: str) -> Kind:
    if kind not in _TABLE:
        raise HTTPException(404, f"Unknown replacement kind '{kind}'")
    return kind  # type: ignore[return-value]


def _row_to_response(row) -> ReplacementResponse:
    return ReplacementResponse(
        id=row["id"],
        novel_id=row["novel_id"],
        is_global=row["novel_id"] is None,
        find_text=row["find_text"],
        replace_text=row["replace_text"] or "",
    )


# ===================== Routes =====================

@router.get(
    "/novels/{novel_id}/replacements/{kind}",
    response_model=list[ReplacementResponse],
)
async def list_replacements(novel_id: str, kind: str):
    """Return rows that apply to ``novel_id`` -- novel-scoped + global."""
    kind = _kind_or_404(kind)
    table = _TABLE[kind]
    db = await get_db()
    try:
        cursor = await db.execute(
            f"SELECT id, novel_id, find_text, replace_text FROM {table} "
            "WHERE novel_id = ? OR novel_id IS NULL "
            "ORDER BY (novel_id IS NULL), LENGTH(find_text) DESC, find_text",
            (novel_id,),
        )
        rows = await cursor.fetchall()
    finally:
        await db.close()
    return [_row_to_response(r) for r in rows]


@router.post(
    "/novels/{novel_id}/replacements/{kind}",
    response_model=ReplacementResponse,
)
async def create_replacement(novel_id: str, kind: str, body: ReplacementCreate):
    """Create a new replacement row, scoped to this novel or global."""
    kind = _kind_or_404(kind)
    table = _TABLE[kind]
    new_id = str(uuid.uuid4())
    scope = None if body.is_global else novel_id
    db = await get_db()
    try:
        await db.execute(
            f"INSERT INTO {table} (id, novel_id, find_text, replace_text) "
            "VALUES (?, ?, ?, ?)",
            (new_id, scope, body.find_text, body.replace_text or ""),
        )
        await db.commit()
        cursor = await db.execute(
            f"SELECT id, novel_id, find_text, replace_text FROM {table} WHERE id = ?",
            (new_id,),
        )
        row = await cursor.fetchone()
    finally:
        await db.close()
    return _row_to_response(row)


@router.patch(
    "/replacements/{kind}/{rule_id}",
    response_model=ReplacementResponse,
)
async def update_replacement(kind: str, rule_id: str, body: ReplacementUpdate):
    """Update one replacement row.  Toggling ``is_global`` flips ``novel_id``."""
    kind = _kind_or_404(kind)
    table = _TABLE[kind]

    db = await get_db()
    try:
        cursor = await db.execute(
            f"SELECT id, novel_id, find_text, replace_text FROM {table} WHERE id = ?",
            (rule_id,),
        )
        row = await cursor.fetchone()
        if row is None:
            raise HTTPException(404, "Replacement not found")

        sets = []
        params: list = []
        if body.find_text is not None:
            sets.append("find_text = ?")
            params.append(body.find_text)
        if body.replace_text is not None:
            sets.append("replace_text = ?")
            params.append(body.replace_text)
        if body.is_global is not None:
            # Promote/demote: clearing novel_id makes a row global; setting it
            # back to the original novel scope re-attaches the row.  We need
            # the original novel_id to demote, so it must already be set or
            # the row must have been novel-scoped before.
            if body.is_global:
                sets.append("novel_id = NULL")
            elif row["novel_id"] is None:
                raise HTTPException(
                    400,
                    "Cannot demote a global rule without a target novel; "
                    "delete and recreate it scoped to the novel instead.",
                )
            # If is_global is False and novel_id was already set, nothing to change

        if not sets:
            return _row_to_response(row)

        sets.append("updated_at = CURRENT_TIMESTAMP")
        await db.execute(
            f"UPDATE {table} SET {', '.join(sets)} WHERE id = ?",
            (*params, rule_id),
        )
        await db.commit()

        cursor = await db.execute(
            f"SELECT id, novel_id, find_text, replace_text FROM {table} WHERE id = ?",
            (rule_id,),
        )
        row = await cursor.fetchone()
    finally:
        await db.close()
    return _row_to_response(row)


@router.delete("/replacements/{kind}/{rule_id}")
async def delete_replacement(kind: str, rule_id: str):
    kind = _kind_or_404(kind)
    table = _TABLE[kind]
    db = await get_db()
    try:
        cursor = await db.execute(
            f"DELETE FROM {table} WHERE id = ?",
            (rule_id,),
        )
        if cursor.rowcount == 0:
            raise HTTPException(404, "Replacement not found")
        await db.commit()
    finally:
        await db.close()
    return {"status": "deleted"}


# ===================== Shared loader (used by chapter responses) =====================

async def fetch_rules(db, kind: Kind, novel_id: str) -> list[tuple[str, str]]:
    """Return [(find, replace), ...] for the novel + globals.  Raw rule list."""
    table = _TABLE[kind]
    cursor = await db.execute(
        f"SELECT find_text, replace_text FROM {table} "
        "WHERE novel_id = ? OR novel_id IS NULL",
        (novel_id,),
    )
    rows = await cursor.fetchall()
    return [(r["find_text"], r["replace_text"] or "") for r in rows]
