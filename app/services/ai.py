import json
from typing import Optional, Dict, Any
from app.core.config import settings
import httpx

class AIService:
    """
    Expert Digital Advertising Analyst powered by LLMs.
    Ports the logic from ScriptBot v5.0 Aiengine.gs.
    """
    
    SYSTEM_PROMPT = """
    You are an expert digital advertising analyst for an AI-powered ad management system.
    Analyze the entity performance data and provide ONE specific, actionable optimization suggestion.
    You MUST respond with valid JSON only.

    Response schema:
    {
      "action_type": "BUDGET_INCREASE|BUDGET_DECREASE|PAUSE|RESUME|BID_ADJUST|REVIEW_CREATIVE|STRUCTURAL_REVIEW",
      "category": "EXECUTABLE|ADVISORY",
      "direction": "UP|DOWN|PAUSE",
      "magnitude": "10|15|20|25|30",
      "suggestion_text": "one clear sentence",
      "reasoning": "2-3 sentences explaining the data signals",
      "confidence": 0.0 to 1.0,
      "risk_level": "LOW|MEDIUM|HIGH|CRITICAL",
      "data_points": {}
    }

    HARD RULES:
    - magnitude MUST NOT exceed 30
    - EXECUTABLE only if confidence >= 0.65
    """

    async def get_suggestion_narrative(self, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Calls OpenAI (primary) or Gemini (fallback) to generate a performance narrative.
        """
        prompt = f"{self.SYSTEM_PROMPT}\n\nData Payload:\n{json.dumps(payload)}\n\nResponse (JSON ONLY):"

        # Primary: Gemini
        if settings.GEMINI_API_KEY:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={settings.GEMINI_API_KEY}"
            async with httpx.AsyncClient() as client:
                try:
                    response = await client.post(url, json={
                        "contents": [{"parts": [{"text": prompt}]}],
                        "generationConfig": {"response_mime_type": "application/json"},
                    }, timeout=30.0)
                    if response.status_code == 200:
                        text = response.json()["candidates"][0]["content"]["parts"][0]["text"]
                        return json.loads(text)
                    print(f"Gemini error {response.status_code}: {response.text}")
                except Exception as e:
                    print(f"Gemini narrative error: {e}")

        # Fallback: OpenAI
        if settings.OPENAI_API_KEY:
            async with httpx.AsyncClient() as client:
                try:
                    response = await client.post(
                        "https://api.openai.com/v1/chat/completions",
                        headers={"Authorization": f"Bearer {settings.OPENAI_API_KEY}"},
                        json={
                            "model": "gpt-4o-mini",
                            "messages": [{"role": "user", "content": prompt}],
                            "response_format": {"type": "json_object"},
                        },
                        timeout=30.0,
                    )
                    if response.status_code == 200:
                        text = response.json()["choices"][0]["message"]["content"]
                        return json.loads(text)
                    print(f"OpenAI error {response.status_code}: {response.text}")
                except Exception as e:
                    print(f"OpenAI narrative error: {e}")

        return None

# Global Instance
ai_service = AIService()
