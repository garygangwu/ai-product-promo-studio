import base64
import os

from product_promo.common import build_client, response_text


class GoogleLLMGateway:
    def __init__(self, project_id, location):
        self.client = build_client(project_id, location)

    def generate_content(self, *, model, contents, config=None):
        return self.client.models.generate_content(model=model, contents=contents, config=config)

    @staticmethod
    def extract_text(response):
        return response_text(response)


class OpenAILLMGateway:
    def __init__(self, project_id, location):
        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY is not set.")
        try:
            from openai import OpenAI
        except Exception as exc:
            raise RuntimeError("OpenAI Python SDK is not installed. Install `openai` to use llm_provider=openai.") from exc
        self.client = OpenAI(api_key=api_key)
        self.google_fallback = GoogleLLMGateway(project_id, location)

    @staticmethod
    def _is_google_model(model):
        return isinstance(model, str) and model.startswith("publishers/google/models/")

    @staticmethod
    def _has_image_output_config(config):
        if config is None:
            return False
        modalities = getattr(config, "response_modalities", None)
        if modalities:
            return "IMAGE" in modalities
        if isinstance(config, dict):
            values = config.get("response_modalities") or config.get("responseModalities") or []
            return "IMAGE" in values
        return False

    @staticmethod
    def _content_part_from_item(item):
        if isinstance(item, str):
            return {"type": "input_text", "text": item}
        inline_data = getattr(item, "inline_data", None)
        if inline_data is not None:
            data = getattr(inline_data, "data", None)
            mime_type = getattr(inline_data, "mime_type", "image/jpeg")
            if data:
                b64 = base64.b64encode(data).decode("ascii")
                return {"type": "input_image", "image_url": f"data:{mime_type};base64,{b64}"}
        return {"type": "input_text", "text": str(item)}

    @staticmethod
    def _normalize_contents(contents):
        if contents is None:
            return []
        if isinstance(contents, str):
            return [contents]
        return list(contents)

    def generate_content(self, *, model, contents, config=None):
        if self._is_google_model(model) or self._has_image_output_config(config):
            return self.google_fallback.generate_content(model=model, contents=contents, config=config)
        content = [self._content_part_from_item(item) for item in self._normalize_contents(contents)]
        kwargs = {}
        want_json = isinstance(config, dict) and config.get("response_mime_type") == "application/json"
        if want_json:
            kwargs["text"] = {"format": {"type": "json_object"}}
        try:
            return self.client.responses.create(
                model=model,
                input=[{"role": "user", "content": content}],
                **kwargs,
            )
        except TypeError:
            return self.client.responses.create(
                model=model,
                input=[{"role": "user", "content": content}],
            )

    def extract_text(self, response):
        output_text = getattr(response, "output_text", None)
        if output_text:
            return output_text
        if isinstance(response, dict):
            value = response.get("output_text") or response.get("text")
            if value:
                return value
        return self.google_fallback.extract_text(response)


def build_llm_gateway(config, *, provider=None):
    provider = str(provider or config.get("llm_provider", "google")).strip().lower()
    if provider == "openai":
        return OpenAILLMGateway(config["project_id"], config["location"])
    return GoogleLLMGateway(config["project_id"], config["location"])
