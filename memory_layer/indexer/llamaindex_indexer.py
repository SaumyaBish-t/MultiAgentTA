"""
memory_layer.indexer.llamaindex_indexer
=======================================

Build a searchable vector index over the Obsidian vault using
LlamaIndex. Optional dependency — if ``llama-index`` isn't installed
the module exposes ``available()`` returning False so callers can skip
indexing without breaking.

Embeddings: NVIDIA NIM ``nv-embedqa-e5-v5`` (per LLM stack), but the
embedding provider is plug-and-play — anything LlamaIndex supports works.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from loguru import logger

from config.settings import settings

try:
    from llama_index.core import (  # type: ignore[import-untyped]
        Settings,
        SimpleDirectoryReader,
        VectorStoreIndex,
        StorageContext,
        load_index_from_storage,
    )
    from llama_index.embeddings.nvidia import NVIDIAEmbedding  # type: ignore[import-untyped]
    _HAS_LLI = True
except Exception:  # pragma: no cover
    _HAS_LLI = False


INDEX_DIR_NAME = ".forge_vault_index"


def available() -> bool:
    return _HAS_LLI


def _configure_embeddings() -> bool:
    """Pin the embedding model to NVIDIA NIM ``nv-embedqa-e5-v5``.

    LlamaIndex defaults to OpenAI embeddings; FORGE has no OpenAI key, so
    this MUST be set before building or querying the index. Retrieval is
    embedding-only — no LLM is needed, so ``Settings.llm`` is left alone
    (the retriever path never resolves it).
    """
    if not _HAS_LLI:
        return False
    api_key = settings.llm.nvidia_api_key
    if not api_key:
        logger.warning("NVIDIA_API_KEY missing — cannot configure embeddings")
        return False
    try:
        Settings.embed_model = NVIDIAEmbedding(
            model=settings.llm.nvidia_embedding_model,
            api_key=api_key,
        )
        return True
    except Exception as exc:
        logger.warning("failed to configure NVIDIA embeddings: {}", exc)
        return False


def _vault_path() -> Path | None:
    if not settings.obsidian_vault_path:
        return None
    p = Path(settings.obsidian_vault_path)
    return p if p.exists() else None


def build_index(force_rebuild: bool = False) -> dict[str, Any]:
    """Build (or load) a LlamaIndex VectorStoreIndex from the vault.

    Returns a small status dict. Safe to call when the vault is empty
    or llama-index isn't installed.
    """
    if not _HAS_LLI:
        return {"skipped": "llama-index not installed"}
    if not settings.memory_layer_enabled:
        return {"skipped": "memory_layer disabled"}

    vault = _vault_path()
    if vault is None:
        return {"skipped": "vault path not configured or missing"}

    if not _configure_embeddings():
        return {"error": "embedding model not configured (check NVIDIA_API_KEY)"}

    index_dir = vault / INDEX_DIR_NAME
    if index_dir.exists() and not force_rebuild:
        try:
            sc = StorageContext.from_defaults(persist_dir=str(index_dir))
            load_index_from_storage(sc)
            return {"loaded": True, "path": str(index_dir)}
        except Exception as exc:
            logger.warning("failed to load existing index, rebuilding: {}", exc)

    try:
        docs = SimpleDirectoryReader(input_dir=str(vault), recursive=True).load_data()
    except Exception as exc:
        return {"error": f"reader failed: {exc}"}

    if not docs:
        return {"skipped": "vault contains no documents"}

    try:
        index = VectorStoreIndex.from_documents(docs)
        index_dir.mkdir(exist_ok=True)
        index.storage_context.persist(persist_dir=str(index_dir))
    except Exception as exc:
        return {"error": f"index build failed: {exc}"}

    return {"built": True, "docs": len(docs), "path": str(index_dir)}


def query(question: str, top_k: int = 5) -> list[dict[str, Any]]:
    """Run a semantic query against the vault index. Returns ranked hits."""
    if not _HAS_LLI or not settings.memory_layer_enabled:
        return []
    vault = _vault_path()
    if vault is None:
        return []
    index_dir = vault / INDEX_DIR_NAME
    if not index_dir.exists():
        return []
    if not _configure_embeddings():
        return []
    try:
        sc = StorageContext.from_defaults(persist_dir=str(index_dir))
        index = load_index_from_storage(sc)
        retriever = index.as_retriever(similarity_top_k=top_k)
        nodes = retriever.retrieve(question)
        return [
            {"text": n.get_content()[:500], "score": getattr(n, "score", None)}
            for n in nodes
        ]
    except Exception as exc:
        logger.warning("vault query failed: {}", exc)
        return []
