import os
import sys

# Add project root to sys.path if needed
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from loguru import logger
from langchain_core.messages import HumanMessage
from config.llm_config import (
    orchestrator_llm,
    simple_llm,
    research_llm,
    reasoning_llm,
    embeddings,
    fallback_llm
)
from config.settings import settings

def run_tests():
    logger.info("Starting LLM Provider Tests...")
    
    # Initialize settings validation (forces parsing .env)
    settings.validate()
    
    results = {}
    
    # 1. Test Groq 70B
    try:
        response = orchestrator_llm.invoke([HumanMessage(content="Reply with only the word GROQ")])
        if "GROQ" in response.content.upper():
            logger.info("✅ Groq 70B working")
            results["Groq 70B"] = "✅ WORKING"
        else:
            logger.error(f"❌ Groq 70B FAILED: Unexpected response: {response.content}")
            results["Groq 70B"] = "❌ FAILED"
    except Exception as e:
        logger.error(f"❌ Groq 70B FAILED: {e}")
        results["Groq 70B"] = "❌ FAILED"

    # 2. Test Groq 8B
    try:
        response = simple_llm.invoke([HumanMessage(content="Reply with only the word SIMPLE")])
        if "SIMPLE" in response.content.upper():
            logger.info("✅ Groq 8B working")
            results["Groq 8B"] = "✅ WORKING"
        else:
            logger.error(f"❌ Groq 8B FAILED: Unexpected response: {response.content}")
            results["Groq 8B"] = "❌ FAILED"
    except Exception as e:
        logger.error(f"❌ Groq 8B FAILED: {e}")
        results["Groq 8B"] = "❌ FAILED"

    # 3. Test Cerebras
    try:
        response = research_llm.invoke([HumanMessage(content="Reply with only the word CEREBRAS")])
        if "CEREBRAS" in response.content.upper():
            logger.info("✅ Cerebras working")
            results["Cerebras"] = "✅ WORKING"
        else:
            logger.error(f"❌ Cerebras FAILED: Unexpected response: {response.content}")
            results["Cerebras"] = "❌ FAILED"
    except Exception as e:
        logger.error(f"❌ Cerebras FAILED: {e}")
        results["Cerebras"] = "❌ FAILED"

    # 4. Test OpenRouter (DeepSeek R1)
    try:
        response = reasoning_llm.invoke([HumanMessage(content="Reply with only the word OPENROUTER")])
        if "OPENROUTER" in response.content.upper():
            logger.info("✅ OpenRouter working")
            results["OpenRouter"] = "✅ WORKING"
        else:
            logger.error(f"❌ OpenRouter FAILED: Unexpected response: {response.content}")
            results["OpenRouter"] = "❌ FAILED"
    except Exception as e:
        logger.error(f"❌ OpenRouter FAILED: {e}")
        results["OpenRouter"] = "❌ FAILED"

    # 5. Test NVIDIA Embeddings
    try:
        embeds = embeddings.embed_query("test embedding for trading system")
        if isinstance(embeds, list) and len(embeds) > 100:
            logger.info("✅ NVIDIA embeddings working")
            results["NVIDIA Embed"] = "✅ WORKING"
        else:
            logger.error(f"❌ NVIDIA FAILED: Invalid embedding structure")
            results["NVIDIA Embed"] = "❌ FAILED"
    except Exception as e:
        logger.error(f"❌ NVIDIA FAILED: {e}")
        results["NVIDIA Embed"] = "❌ FAILED"

    # 6. Test Mistral
    try:
        response = fallback_llm.invoke([HumanMessage(content="Reply with only the word MISTRAL")])
        if "MISTRAL" in response.content.upper():
            logger.info("✅ Mistral working")
            results["Mistral"] = "✅ WORKING"
        else:
            logger.error(f"❌ Mistral FAILED: Unexpected response: {response.content}")
            results["Mistral"] = "❌ FAILED"
    except Exception as e:
        logger.error(f"❌ Mistral FAILED: {e}")
        results["Mistral"] = "❌ FAILED"

    # 7. Print Final Summary
    print("\n" + "="*30)
    print("LLM PROVIDER STATUS SUMMARY")
    print("="*30)
    all_passed = True
    for name, status in results.items():
        # Strip emojis for clean print in Windows terminal
        clean_status = status.replace("✅ ", "").replace("❌ ", "")
        display_status = "WORKING" if "WORKING" in status else "FAILED"
        print(f"{name.ljust(14)}: {display_status}")
        if "FAILED" in status:
            all_passed = False
    print("-" * 30)
    print(f"Ready to run trading system: {'YES' if all_passed else 'NO'}")
    print("="*30 + "\n")

if __name__ == "__main__":
    run_tests()
