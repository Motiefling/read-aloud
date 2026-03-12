from fastapi import APIRouter, HTTPException

from app.models import TermDictionaryResponse, TermDictionaryUpdate

router = APIRouter()


@router.get("", response_model=list[TermDictionaryResponse])
async def list_dictionaries():
    """List all term dictionaries."""
    # TODO: Query all dictionaries from DB
    raise HTTPException(501, "Not implemented")


@router.get("/{novel_id}", response_model=TermDictionaryResponse)
async def get_dictionary(novel_id: str):
    """Get the term dictionary for a specific novel."""
    # TODO: Query dictionary by novel_id from DB
    raise HTTPException(501, "Not implemented")


@router.put("/{novel_id}")
async def update_dictionary(novel_id: str, dictionary: TermDictionaryUpdate):
    """Create or update a term dictionary for a novel."""
    # TODO: Upsert dictionary in DB
    raise HTTPException(501, "Not implemented")
