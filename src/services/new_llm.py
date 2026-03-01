import os
from typing import Dict, Any, Optional, List
from openai import OpenAI, APIError, RateLimitError, APIConnectionError

# Best Practice: Instantiate the client once and reuse it.
# This avoids creating a new connection on every function call.
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

import json
from typing import Dict, Any, Optional

def _extract_json_block(text: str) -> Optional[Dict[str, Any]]:
    """A placeholder for your JSON extraction logic."""
    try:
        # Find the start of the JSON code block
        json_start = text.find("```json")
        if json_start == -1:
            return None
        # Adjust to the position right after "```
        json_start += len("```json")
        
        # Find the end of the JSON code block
        json_end = text.find("```", json_start)
        if json_end == -1:
            return None
        
        # Extract the JSON string and parse it
        json_str = text[json_start:json_end].strip()
        return json.loads(json_str)
    except (json.JSONDecodeError, AttributeError):
        return None



def call_llm(
    prompt: str,
    llm_config: Dict[str, Any],
    max_tokens: int = 2048,
    temperature: float = 0.1,
    stop: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Calls LLM (OpenAI's GPT models) with the given prompt and tries to return parsed JSON.
    """
    # The API key is now handled when the client is initialized outside the function.
    if not client.api_key:
        raise ValueError("Missing OpenAI API key. Set the OPENAI_API_KEY environment variable.")

    # You mentioned using gpt-4o-mini, so we can make it the default.
    model = llm_config.get("model", "gpt-4o-mini")
    system_message = llm_config.get(
        "system_message",
        "You are a skilled BigQuery SQL optimizer. Always answer ONLY using a JSON block inside Markdown fences with all required fields as described.",
    )

    messages = [
        {"role": "system", "content": system_message},
        {"role": "user", "content": prompt},
    ]

    try:
        # The client automatically handles retries for transient errors
        # like rate limits or connection issues.
        response = client.chat.completions.create(
            model=model,
            messages=messages,
            max_tokens=max_tokens,
            temperature=temperature,
            stop=stop,
        )
        text = response.choices[0].message.content or ""

        parsed = _extract_json_block(text)
        if parsed is not None:
            return parsed
        
        # Return the raw response if JSON parsing fails
        return {"raw_response": text}

    # Catch specific, non-retriable API errors for explicit handling
    except APIError as e:
        # Handles errors like invalid requests, authentication issues, etc.
        return {"error": f"OpenAI API Error: {e}"}
    except Exception as e:
        # Catch any other unexpected errors
        return {"error": f"An unexpected error occurred: {str(e)}"}

