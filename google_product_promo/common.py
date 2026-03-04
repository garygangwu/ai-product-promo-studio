import json
import mimetypes
import os
import re
import subprocess
import time
from datetime import datetime
from pathlib import Path

from google import genai
from google.genai import errors, types


ROOT_DIR = Path(__file__).resolve().parent
RUNS_DIR = ROOT_DIR / "runs"


def _load_local_dotenv():
    dotenv_path = ROOT_DIR.parent / ".env"
    if not dotenv_path.is_file():
        return
    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


_load_local_dotenv()

DEFAULT_PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT", "")
DEFAULT_LOCATION = "us-central1"
DEFAULT_ASPECT_RATIO = "9:16"
DEFAULT_AUDIO_THEME = "Deep bass, cinematic pulse, high-end tech atmosphere"

PROMPT_MODEL_CANDIDATES = [
    "publishers/google/models/gemini-2.5-pro",
    "publishers/google/models/gemini-3.1-pro-preview",
]
IMAGE_MODEL_CANDIDATES = [
    "publishers/google/models/gemini-2.5-flash-image",
    "publishers/google/models/gemini-2.5-flash-image-preview",
    "publishers/google/models/gemini-3.1-flash-image-preview",
]
VIDEO_MODEL_CANDIDATES = [
    "publishers/google/models/veo-3.1-generate-001",
    "publishers/google/models/veo-3.1-generate-preview",
]


def sanitize_for_json(value):
    if isinstance(value, (bytes, bytearray)):
        return f"<{len(value)} bytes>"
    if isinstance(value, dict):
        return {str(k): sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [sanitize_for_json(item) for item in value]
    return value


def append_transcript(run_dir, event_type, payload):
    record = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "event": event_type,
        "payload": sanitize_for_json(payload),
    }
    log_path = Path(run_dir) / "transcript.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def make_run_id(prefix="run"):
    return f"{prefix}_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}"


def require_file(path):
    if not Path(path).is_file():
        raise FileNotFoundError(f"Required file not found: {path}")


def ensure_run_dir(run_dir):
    path = Path(run_dir)
    path.mkdir(parents=True, exist_ok=True)
    return path


def build_client(project_id, location):
    os.environ["GOOGLE_GENAI_USE_VERTEXAI"] = "True"
    os.environ["GOOGLE_CLOUD_PROJECT"] = project_id
    os.environ["GOOGLE_CLOUD_LOCATION"] = location
    return genai.Client(vertexai=True, project=project_id, location=location)


def list_available_models(project_id, location):
    client = build_client(project_id, location)
    return [getattr(model, "name", "") for model in client.models.list()]


def resolve_model_name(available_names, candidates, label):
    for candidate in candidates:
        if candidate in available_names:
            return candidate
        suffix = candidate.split("/")[-1]
        for name in available_names:
            if name.endswith("/" + suffix):
                return name
    raise RuntimeError(f"No usable {label} model found from candidates: {candidates}")


def mime_for_path(path):
    mime, _ = mimetypes.guess_type(path)
    return mime or "image/jpeg"


def build_image(path):
    return types.Image(image_bytes=Path(path).read_bytes(), mime_type=mime_for_path(path))


def build_inline_image_part(path):
    return types.Part(
        inline_data=types.Blob(
            data=Path(path).read_bytes(),
            mime_type=mime_for_path(path),
        )
    )


def response_text(response):
    text = getattr(response, "text", None)
    if text:
        return text
    candidates = getattr(response, "candidates", None) or []
    if candidates and getattr(candidates[0], "content", None):
        parts = getattr(candidates[0].content, "parts", [])
        joined = "\n".join(
            [getattr(part, "text", "") for part in parts if getattr(part, "text", None)]
        ).strip()
        if joined:
            return joined
    return None


def parse_prompt_list(raw_text, expected_count):
    if not raw_text:
        raise ValueError("Model returned an empty prompt response.")
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw_text.strip())
    cleaned = re.sub(r"\s*```$", "", cleaned)
    data = json.loads(cleaned)
    if not isinstance(data, list):
        raise ValueError("Model did not return a JSON array.")
    prompts = [str(item).strip() for item in data if str(item).strip()]
    if len(prompts) != expected_count:
        raise ValueError(f"Expected {expected_count} prompts, got {len(prompts)}.")
    return prompts


def load_json(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def save_inline_image(response, output_path):
    candidates = getattr(response, "candidates", None) or []
    for candidate in candidates:
        content = getattr(candidate, "content", None)
        if not content:
            continue
        parts = getattr(content, "parts", []) or []
        for part in parts:
            inline_data = getattr(part, "inline_data", None)
            if inline_data and getattr(inline_data, "data", None):
                Path(output_path).write_bytes(inline_data.data)
                return
    raise RuntimeError("Image generation returned no inline image data.")


def extract_video_output(operation):
    response = getattr(operation, "response", None)
    if response is None and isinstance(operation, dict):
        response = operation.get("response")
    if response is None:
        raise RuntimeError("Generation finished but no response payload was returned.")
    generated = getattr(response, "generated_videos", None)
    if generated is None and isinstance(response, dict):
        generated = response.get("generated_videos") or response.get("generatedVideos")
    if not generated:
        raise RuntimeError("Generation finished but no videos were returned.")
    first = generated[0]
    video = getattr(first, "video", None)
    if video is None and isinstance(first, dict):
        video = first.get("video")
    if video is None:
        raise RuntimeError("Generation returned no video payload.")
    return video


def save_video_payload(video_payload, output_path):
    video_bytes = getattr(video_payload, "video_bytes", None)
    if video_bytes is None and isinstance(video_payload, dict):
        video_bytes = video_payload.get("video_bytes") or video_payload.get("videoBytes")
    if not video_bytes:
        raise RuntimeError("Expected inline video bytes in Veo response.")
    Path(output_path).write_bytes(video_bytes)


def poll_operation(client, operation, poll_interval=10.0):
    while not getattr(operation, "done", False):
        time.sleep(poll_interval)
        operation = client.operations.get(operation)
        print("Waiting for video generation...")
    return operation


def call_with_retry(func, retries=5, initial_delay=15.0, retry_statuses=(429,)):
    delay = initial_delay
    for attempt in range(1, retries + 1):
        try:
            return func()
        except errors.ClientError as exc:
            if exc.code not in retry_statuses or attempt == retries:
                raise
            print(f"Retryable error {exc.code}; sleeping {delay:.0f}s before retry {attempt + 1}/{retries}")
            time.sleep(delay)
            delay *= 2


def should_skip_output(path, force):
    return Path(path).exists() and not force


def ffmpeg_concat(video_paths, output_path):
    concat_file = Path(output_path).with_suffix(".concat.txt")
    concat_file.write_text(
        "\n".join([f"file '{Path(path).resolve()}'" for path in video_paths]) + "\n",
        encoding="utf-8",
    )
    cmd = [
        "ffmpeg",
        "-y",
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        str(concat_file),
        "-c",
        "copy",
        str(Path(output_path).resolve()),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg concat failed:\n{result.stderr}")
    return cmd


def ffprobe_duration(path):
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(Path(path).resolve()),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"ffprobe duration failed:\n{result.stderr}")
    return float(result.stdout.strip())


def ffprobe_has_audio(path):
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "a",
        "-show_entries",
        "stream=index",
        "-of",
        "csv=p=0",
        str(Path(path).resolve()),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"ffprobe audio probe failed:\n{result.stderr}")
    return bool(result.stdout.strip())
