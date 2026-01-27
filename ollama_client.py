import base64
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests


@dataclass
class OllamaConfig:
    enabled: bool
    base_url: str
    model: str
    prompt_template: str
    max_response_chars: int = 512


class OllamaClient:
    def __init__(self, config: OllamaConfig) -> None:
        self.config = config

    def summarize(self, metrics: dict) -> Optional[str]:
        if not self.config.enabled:
            return None

        prompt = self.config.prompt_template.format(metrics=json.dumps(metrics, sort_keys=True))
        payload = {
            "model": self.config.model,
            "prompt": prompt,
            "stream": False,
        }
        try:
            response = requests.post(
                f"{self.config.base_url}/api/generate",
                json=payload,
                timeout=30,
            )
            response.raise_for_status()
            text = response.json().get("response", "").strip()
            if len(text) > self.config.max_response_chars:
                return text[: self.config.max_response_chars]
            return text
        except requests.RequestException:
            return None

    def analyze_image(self, image_path: str, prompt: Optional[str] = None) -> Optional[str]:
        if not self.config.enabled:
            return None

        prompt_text = prompt or "Describe the image in one sentence."
        image_data = _read_image_base64(image_path)
        if not image_data:
            return None

        payload = {
            "model": self.config.model,
            "prompt": prompt_text,
            "images": [image_data],
            "stream": False,
        }
        try:
            response = requests.post(
                f"{self.config.base_url}/api/generate",
                json=payload,
                timeout=60,
            )
            response.raise_for_status()
            text = response.json().get("response", "").strip()
            if len(text) > self.config.max_response_chars:
                return text[: self.config.max_response_chars]
            return text
        except requests.RequestException:
            return None


def _read_image_base64(path: str) -> Optional[str]:
    try:
        data = Path(path).read_bytes()
        return base64.b64encode(data).decode("utf-8")
    except Exception:
        return None
