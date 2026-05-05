from langchain_groq import ChatGroq
from langchain_mistralai import ChatMistralAI
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_core.embeddings import Embeddings
from config.settings import settings
from loguru import logger
import httpx
from typing import List

class NIMEmbeddings(Embeddings):
    """
    Custom wrapper for NVIDIA NIM embeddings to avoid dependency conflicts
    and handle specific payload requirements like input_type.
    """
    def __init__(self, model: str, api_key: str, base_url: str):
        self.model = model
        self.api_key = api_key
        self.base_url = base_url

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        url = f"{self.base_url}/embeddings"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "input": texts,
            "model": self.model,
            "input_type": "passage",
            "encoding_format": "float"
        }
        with httpx.Client() as client:
            r = client.post(url, headers=headers, json=payload, timeout=30.0)
            r.raise_for_status()
            data = r.json()
            # Sort by index to ensure order if the API doesn't guarantee it
            items = data["data"]
            items.sort(key=lambda x: x["index"])
            return [item["embedding"] for item in items]

    def embed_query(self, text: str) -> List[float]:
        url = f"{self.base_url}/embeddings"
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "input": [text],
            "model": self.model,
            "input_type": "query",
            "encoding_format": "float"
        }
        with httpx.Client() as client:
            r = client.post(url, headers=headers, json=payload, timeout=30.0)
            r.raise_for_status()
            data = r.json()
            return data["data"][0]["embedding"]

class LLMFactory:
    """
    Central factory for all LLM instances in the trading system.
    
    Provider assignment by agent role:
    ┌─────────────────────────┬────────────┬──────────────────────────┐
    │ Agent                   │ Provider   │ Reason                   │
    ├─────────────────────────┼────────────┼──────────────────────────┤
    │ Master Orchestrator     │ Groq 70B   │ Fast, 14.4k/day          │
    │ Signal Generation       │ Groq 70B   │ Fast, real-time          │
    │ Risk Agent              │ Groq 70B   │ Critical speed needed    │
    │ Compliance Agent        │ Groq 8B    │ Simple rules, save quota │
    │ Data Quality Agent      │ Groq 8B    │ Simple checks            │
    │ Research Agent          │ Cerebras qwen-3-235b  │ 235B, best quality      │
    │ Document Agent          │ Cerebras qwen-3-235b  │ Long docs, token heavy  │
    │ Agentic Tasks           │ Cerebras zai-glm-4.7  │ Built for tool use      │
    │ Fast Cerebras           │ Cerebras llama3.1-8b  │ Quick, lightweight      │
    │ Hypothesis Generator    │ OpenRouter │ DeepSeek R1, best reason │
    │ Fundamental Analysis    │ OpenRouter │ Complex reasoning        │
    │ Embeddings (ChromaDB)   │ NVIDIA NIM │ RAG only, not chat       │
    │ Fallback (any agent)    │ Mistral    │ When limits hit          │
    └─────────────────────────┴────────────┴──────────────────────────┘
    """

    # ── Groq LLMs ────────────────────────────────────────
    
    _groq_key_index = 0

    @classmethod
    def _get_groq_key(cls) -> str:
        """Rotates through available Groq API keys to distribute load."""
        keys_str = settings.llm.groq_api_keys
        keys = [k.strip() for k in keys_str.split(",") if k.strip()]
        
        if not keys:
            return settings.llm.groq_api_key
        
        key = keys[cls._groq_key_index % len(keys)]
        cls._groq_key_index += 1
        return key

    @staticmethod
    def get_orchestrator_llm() -> ChatGroq:
        """Master orchestrator — fast routing decisions."""
        return ChatGroq(
            model=settings.llm.groq_model_fast,
            api_key=LLMFactory._get_groq_key(),
            temperature=0.1,
            max_tokens=2048
        )

    @staticmethod
    def get_signal_llm() -> ChatGroq:
        """Signal generation — needs speed and reliability."""
        return ChatGroq(
            model=settings.llm.groq_model_fast,
            api_key=LLMFactory._get_groq_key(),
            temperature=0.1,
            max_tokens=2048
        )

    @staticmethod
    def get_risk_llm() -> ChatGroq:
        """Risk management — critical path, must be fast."""
        return ChatGroq(
            model=settings.llm.groq_model_fast,
            api_key=LLMFactory._get_groq_key(),
            temperature=0,
            max_tokens=1024
        )

    @staticmethod
    def get_simple_llm() -> ChatGroq:
        """
        Simple tasks: compliance checks, data quality flags.
        Uses 8B model to preserve 70B quota.
        """
        return ChatGroq(
            model=settings.llm.groq_model_simple,
            api_key=LLMFactory._get_groq_key(),
            temperature=0,
            max_tokens=512
        )

    # ── Cerebras LLMs ────────────────────────────────────

    @staticmethod
    def get_research_llm() -> ChatGroq:
        """
        Research & document agents.
        Moved to Groq for stability.
        """
        return ChatGroq(
            model=settings.llm.groq_model_fast,
            api_key=LLMFactory._get_groq_key(),
            temperature=0.2,
            max_tokens=4096
        )

    @staticmethod
    def get_document_llm() -> ChatGroq:
        """
        Document intelligence / RAG agent.
        Moved to Groq for stability.
        """
        return ChatGroq(
            model=settings.llm.groq_model_fast,
            api_key=LLMFactory._get_groq_key(),
            temperature=0.1,
            max_tokens=4096
        )

    @staticmethod
    def get_agent_llm() -> ChatGroq:
        """
        For agentic tasks specifically.
        Moved to Groq for stability.
        """
        return ChatGroq(
            model=settings.llm.groq_model_fast,
            api_key=LLMFactory._get_groq_key(),
            temperature=0.1,
            max_tokens=4096
        )

    @staticmethod
    def get_fast_cerebras_llm() -> ChatOpenAI:
        """
        Fastest Cerebras model for quick tasks.
        Uses llama3.1-8b — smallest and fastest.
        """
        return ChatOpenAI(
            model=settings.llm.cerebras_model_fast,
            base_url=settings.llm.cerebras_base_url,
            api_key=settings.llm.cerebras_api_key,
            temperature=0,
            max_tokens=1024
        )

    # ── OpenRouter LLMs ──────────────────────────────────

    _openrouter_key_index = 0

    @classmethod
    def _get_openrouter_key(cls) -> str:
        """Rotates through available OpenRouter API keys to distribute load."""
        keys_str = settings.llm.openrouter_api_keys
        keys = [k.strip() for k in keys_str.split(",") if k.strip()]
        
        if not keys:
            return settings.llm.openrouter_api_key
        
        key = keys[cls._openrouter_key_index % len(keys)]
        cls._openrouter_key_index += 1
        return key

    @staticmethod
    def get_reasoning_llm() -> ChatGroq:
        """
        Deep reasoning: hypothesis generation, 
        fundamental analysis. Switched to Groq for stability.
        """
        return ChatGroq(
            model=settings.llm.groq_model_fast,
            api_key=LLMFactory._get_groq_key(),
            temperature=0.3,
            max_tokens=4096
        )

    # ── NVIDIA NIM Embeddings ─────────────────────────────

    @staticmethod
    def get_embeddings() -> NIMEmbeddings:
        """
        Embeddings for ChromaDB vector store.
        NVIDIA NIM: 40 RPM free forever.
        """
        return NIMEmbeddings(
            model=settings.llm.nvidia_embedding_model,
            api_key=settings.llm.nvidia_api_key,
            base_url=settings.llm.nvidia_base_url,
        )

    # ── Mistral Fallback ─────────────────────────────────

    @staticmethod
    def get_fallback_llm() -> ChatMistralAI:
        """
        Fallback when Groq or Cerebras hit daily limits.
        Mistral: 1B tokens/month free.
        """
        return ChatMistralAI(
            model=settings.llm.mistral_model,
            api_key=settings.llm.mistral_api_key,
            temperature=0.1,
            max_tokens=2048
        )

    # ── Smart Fallback Wrapper ───────────────────────────

    @staticmethod
    def get_llm_with_fallback(primary_llm, fallback_llm=None):
        """
        Wraps any LLM with automatic fallback to Mistral.
        Use this for critical agents that cannot fail.
        """
        if fallback_llm is None:
            fallback_llm = LLMFactory.get_fallback_llm()
        
        from langchain_core.runnables import RunnableWithFallbacks
        return primary_llm.with_fallbacks([fallback_llm])

class LLMProxy:
    """
    A proxy that fetches a fresh LLM instance from the factory 
    every time a method or attribute is accessed.
    This enables true rotation for 'singleton' instances.
    """
    def __init__(self, factory_method):
        self._factory_method = factory_method
    
    def __getattr__(self, name):
        # Get a fresh LLM instance (this triggers the rotation logic)
        llm = self._factory_method()
        return getattr(llm, name)
    
    def __call__(self, *args, **kwargs):
        # Handles cases where the proxy itself might be called
        return self._factory_method()(*args, **kwargs)

# Convenience dynamic instances
# Import these directly in agent files to get automatic key rotation:
orchestrator_llm = LLMProxy(LLMFactory.get_orchestrator_llm)
signal_llm       = LLMProxy(LLMFactory.get_signal_llm)
risk_llm         = LLMProxy(LLMFactory.get_risk_llm)
simple_llm       = LLMProxy(LLMFactory.get_simple_llm)
research_llm     = LLMProxy(LLMFactory.get_research_llm)
document_llm     = LLMProxy(LLMFactory.get_document_llm)
agent_llm        = LLMProxy(LLMFactory.get_agent_llm)
fast_cerebras_llm = LLMProxy(LLMFactory.get_fast_cerebras_llm)
reasoning_llm    = LLMProxy(LLMFactory.get_reasoning_llm)
fallback_llm     = LLMProxy(LLMFactory.get_fallback_llm)

# Note: Embeddings don't rotate as they use a single NVIDIA key
embeddings = LLMFactory.get_embeddings()
