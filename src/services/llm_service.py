import os
import json
import re
from typing import Dict, Any, Optional

from openai import OpenAI


def load_llm_config(config_path: str) -> Dict[str, Any]:
    """Loads LLM configuration JSON."""
    with open(config_path, "r") as f:
        return json.load(f)


def _extract_json_block(text: str) -> Optional[Dict[str, Any]]:
    """Try to extract a ```json ... ``` block and parse it.

    Falls back to first {...} JSON object if fenced block is absent.
    """
    # Prefer fenced json block
    match = re.search(r"```json\s*(.*?)\s*```", text, re.DOTALL | re.IGNORECASE)
    if match:
        try:
            return json.loads(match.group(1))
        except Exception:
            pass

    # Fallback: try to find first JSON object in text
    brace_start = text.find("{")
    brace_end = text.rfind("}")
    if brace_start != -1 and brace_end != -1 and brace_end > brace_start:
        cand = text[brace_start:brace_end + 1]
        try:
            return json.loads(cand)
        except Exception:
            pass
    return None


def call_llm(
    prompt: str,
    llm_config: Dict[str, Any],
    max_tokens: int = 2048,
    temperature: float = 0.1,
    stop: Optional[list] = None,
) -> Dict[str, Any]:
    """
    Calls LLM (OpenAI's GPT models) with the given prompt and tries to return parsed JSON.
    """
    api_key = llm_config.get("api_key") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("Missing OpenAI API key in config or OPENAI_API_KEY env variable.")

    model = llm_config.get("model", "gpt-4-0125-preview")
    system_message = llm_config.get(
        "system_message",
        "You are a skilled BigQuery SQL optimizer. Always answer ONLY using a JSON block inside Markdown fences with all required fields as described.",
    )

    client = OpenAI(api_key=api_key)

    messages = [
        {"role": "system", "content": system_message},
        {"role": "user", "content": prompt},
    ]

    try:
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            stop=stop,
        )
        d= response.model_dump()
        print(d)
        text = response.choices[0].message.content or ""

        parsed = _extract_json_block(text)
        if parsed is not None:
            return parsed
        return {"raw_response": text}

    except Exception as e:
        return {"error": str(e)}
