import base64
import json
import re
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

import google.auth
from google.auth.transport.requests import Request
from google.genai import types

from google_product_promo.common import (
    DEFAULT_ASPECT_RATIO,
    DEFAULT_AUDIO_THEME,
    DEFAULT_LOCATION,
    DEFAULT_PROJECT_ID,
    IMAGE_MODEL_CANDIDATES,
    PROMPT_MODEL_CANDIDATES,
    RUNS_DIR,
    VIDEO_MODEL_CANDIDATES,
    append_transcript,
    build_client,
    build_image,
    build_inline_image_part,
    call_with_retry,
    ensure_run_dir,
    extract_video_output,
    ffmpeg_concat,
    ffprobe_duration,
    ffprobe_has_audio,
    list_available_models,
    load_json,
    make_run_id,
    poll_operation,
    require_file,
    resolve_model_name,
    response_text,
    save_inline_image,
    save_video_payload,
    should_skip_output,
    write_json,
)


LYRIA_MODEL = "lyria-002"
TTS_MODEL = "gemini-2.5-flash-tts"
RUN_STATE_FILE = "run_state.json"
FINAL_STATES = {
    "promo_create_run": "CREATED",
    "promo_generate_anchor_plan": "ANCHOR_PLAN_READY",
    "promo_generate_anchor_images": "ANCHOR_IMAGES_READY",
    "promo_generate_bridge_videos": "SEGMENTS_READY",
    "promo_concat_visual": "VISUAL_FINAL_READY",
    "promo_generate_audio_plan": "AUDIO_PLAN_READY",
    "promo_generate_audio_assets": "AUDIO_ASSETS_READY",
    "promo_merge_audio": "FINAL_READY",
}


def _run_state_path(run_dir):
    return Path(run_dir) / RUN_STATE_FILE


def _artifact_snapshot(run_dir):
    run_dir = Path(run_dir)
    return {
        "config_json": str(run_dir / "config.json") if (run_dir / "config.json").is_file() else None,
        "anchor_plan_json": str(run_dir / "anchor_plan.json") if (run_dir / "anchor_plan.json").is_file() else None,
        "anchor_images": [str(path) for path in sorted(run_dir.glob("anchor_*.png"))],
        "segment_videos": [str(path) for path in sorted(run_dir.glob("segment_*.mp4"))],
        "final_mp4": str(run_dir / "final.mp4") if (run_dir / "final.mp4").is_file() else None,
        "audio_plan_json": str(run_dir / "audio_plan.json") if (run_dir / "audio_plan.json").is_file() else None,
        "audio_assets_json": str(run_dir / "audio_assets.json") if (run_dir / "audio_assets.json").is_file() else None,
        "final_with_audio_mp4": str(run_dir / "final_with_audio.mp4") if (run_dir / "final_with_audio.mp4").is_file() else None,
        "alt_scenario_brief_json": str(run_dir / "alt_scenario_brief.json") if (run_dir / "alt_scenario_brief.json").is_file() else None,
        "alt_image_prompts_json": str(run_dir / "alt_image_prompts.json") if (run_dir / "alt_image_prompts.json").is_file() else None,
        "alt_candidates": [str(path) for path in sorted((run_dir / "alt_candidates").glob("candidate_*.png"))]
        if (run_dir / "alt_candidates").is_dir()
        else [],
        "alt_image_rankings_json": str(run_dir / "alt_image_rankings.json") if (run_dir / "alt_image_rankings.json").is_file() else None,
        "alt_selected_top3_json": str(run_dir / "alt_selected_top3.json") if (run_dir / "alt_selected_top3.json").is_file() else None,
        "alt_video_prompt_txt": str(run_dir / "alt_video_prompt.txt") if (run_dir / "alt_video_prompt.txt").is_file() else None,
        "alt_video_prompt_review_json": str(run_dir / "alt_video_prompt_review.json") if (run_dir / "alt_video_prompt_review.json").is_file() else None,
        "alt_final_8s_mp4": str(run_dir / "alt_final_8s.mp4") if (run_dir / "alt_final_8s.mp4").is_file() else None,
        "transcript_jsonl": str(run_dir / "transcript.jsonl") if (run_dir / "transcript.jsonl").is_file() else None,
    }


def _detect_state_from_artifacts(run_dir):
    run_dir = Path(run_dir)
    anchor_count = sum((run_dir / f"anchor_{idx}.png").is_file() for idx in range(1, 5))
    segment_count = sum((run_dir / f"segment_{label}.mp4").is_file() for label in ("a", "b", "c"))
    has_audio_plan = (run_dir / "audio_plan.json").is_file()
    has_final = (run_dir / "final.mp4").is_file()
    has_final_with_audio = (run_dir / "final_with_audio.mp4").is_file()
    has_anchor_plan = (run_dir / "anchor_plan.json").is_file()
    has_music = (run_dir / "soundtrack.wav").is_file()
    has_narration = (run_dir / "narration.wav").is_file()

    if has_final_with_audio:
        return "FINAL_READY"
    if has_music or has_narration:
        return "AUDIO_ASSETS_READY"
    if has_audio_plan:
        return "AUDIO_PLAN_READY"
    if has_final:
        return "VISUAL_FINAL_READY"
    if segment_count == 3:
        return "SEGMENTS_READY"
    if segment_count > 0:
        return "SEGMENTS_PARTIAL"
    if anchor_count == 4:
        return "ANCHOR_IMAGES_READY"
    if anchor_count > 0:
        return "ANCHOR_IMAGES_PARTIAL"
    if has_anchor_plan:
        return "ANCHOR_PLAN_READY"
    if (run_dir / "config.json").is_file():
        return "CREATED"
    return "MISSING"


def _next_tool_for_state(state):
    mapping = {
        "CREATED": "promo_generate_anchor_plan",
        "ANCHOR_PLAN_READY": "promo_generate_anchor_images",
        "ANCHOR_IMAGES_PARTIAL": "promo_generate_anchor_images",
        "ANCHOR_IMAGES_READY": "promo_generate_bridge_videos",
        "SEGMENTS_PARTIAL": "promo_generate_bridge_videos",
        "SEGMENTS_READY": "promo_concat_visual",
        "VISUAL_FINAL_READY": "promo_generate_audio_plan",
        "AUDIO_PLAN_READY": "promo_generate_audio_assets",
        "AUDIO_ASSETS_PARTIAL": "promo_generate_audio_assets",
        "AUDIO_ASSETS_READY": "promo_merge_audio",
        "ALT_SCENARIO_READY": "promo_alt_generate_candidate_images",
        "ALT_IMAGES_READY": "promo_alt_rank_and_select_images",
        "ALT_TOP3_READY": "promo_alt_generate_video_prompt",
        "ALT_VIDEO_PROMPT_READY": "promo_alt_generate_8s_video",
        "FINAL_READY": None,
        "ALT_VIDEO_READY": None,
    }
    return mapping.get(state)


def update_run_state(run_dir, *, state=None, last_completed_step=None, last_error=None):
    run_dir = Path(run_dir)
    path = _run_state_path(run_dir)
    existing = load_json(path) if path.is_file() else {}
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    computed_state = state or _detect_state_from_artifacts(run_dir)
    payload = {
        "run_id": existing.get("run_id") or run_dir.name,
        "run_dir": str(run_dir.resolve()),
        "state": computed_state,
        "created_at": existing.get("created_at") or now,
        "updated_at": now,
        "last_completed_step": last_completed_step if last_completed_step is not None else existing.get("last_completed_step"),
        "last_error": last_error if last_error is not None else existing.get(
            "last_error",
            {
                "failed_step": None,
                "error_type": None,
                "error_message": None,
                "retryable": False,
            },
        ),
        "artifacts": _artifact_snapshot(run_dir),
        "next_recommended_tool": _next_tool_for_state(computed_state),
    }
    write_json(path, payload)
    return payload


def _record_step_success(run_dir, step_name):
    return update_run_state(run_dir, state=FINAL_STATES[step_name], last_completed_step=step_name, last_error={
        "failed_step": None,
        "error_type": None,
        "error_message": None,
        "retryable": False,
    })


def _record_step_failure(run_dir, step_name, exc):
    message = str(exc)
    lower = message.lower()
    error_type = "runtime_error"
    retryable = False
    if "429" in lower or "resource exhausted" in lower:
        error_type = "quota_exhausted"
        retryable = True
    elif "high load" in lower or "try again later" in lower or "temporarily unavailable" in lower:
        error_type = "service_overloaded"
        retryable = True
    elif "recitation" in lower:
        error_type = "music_generation_blocked"
        retryable = True
    elif "not found" in lower or "missing" in lower:
        error_type = "missing_input_artifact"
    elif "ffmpeg" in lower:
        error_type = "ffmpeg_failure"
    elif "http 400" in lower or "invalid_argument" in lower:
        error_type = "validation_error"
    current_state = _detect_state_from_artifacts(run_dir)
    return update_run_state(
        run_dir,
        state="FAILED",
        last_error={
            "failed_step": step_name,
            "error_type": error_type,
            "error_message": message,
            "retryable": retryable,
            "last_known_state": current_state,
        },
    )


def get_run_status(run_dir):
    run_dir = Path(run_dir)
    state_path = _run_state_path(run_dir)
    if state_path.is_file():
        state = load_json(state_path)
        state["artifacts"] = _artifact_snapshot(run_dir)
        state["next_recommended_tool"] = _next_tool_for_state(state["state"]) if state["state"] != "FAILED" else None
        return state
    return update_run_state(run_dir)


def list_runs(runs_dir=RUNS_DIR):
    runs_dir = Path(runs_dir)
    results = []
    if not runs_dir.is_dir():
        return results
    for run_dir in sorted([path for path in runs_dir.iterdir() if path.is_dir()]):
        status = get_run_status(run_dir)
        results.append(
            {
                "run_id": status["run_id"],
                "run_dir": status["run_dir"],
                "state": status["state"],
                "created_at": status["created_at"],
                "updated_at": status["updated_at"],
                "next_recommended_tool": status.get("next_recommended_tool"),
            }
        )
    return results


def create_run(
    *,
    product_images,
    logo_image,
    description,
    run_id=None,
    project_id=DEFAULT_PROJECT_ID,
    location=DEFAULT_LOCATION,
    aspect_ratio=DEFAULT_ASPECT_RATIO,
    audio_theme=DEFAULT_AUDIO_THEME,
):
    for path in product_images:
        require_file(path)
    require_file(logo_image)

    resolved_run_id = run_id or make_run_id("promo")
    run_dir = ensure_run_dir(RUNS_DIR / resolved_run_id)
    try:
        available_models = list_available_models(project_id, location)
        config = {
            "run_id": resolved_run_id,
            "run_dir": str(run_dir.resolve()),
            "project_id": project_id,
            "location": location,
            "product_images": [str(Path(p).resolve()) for p in product_images],
            "logo_image": str(Path(logo_image).resolve()),
            "description": description,
            "aspect_ratio": aspect_ratio,
            "audio_theme": audio_theme,
            "prompt_model": resolve_model_name(available_models, PROMPT_MODEL_CANDIDATES, "prompt"),
            "image_model": resolve_model_name(available_models, IMAGE_MODEL_CANDIDATES, "image"),
            "video_model": resolve_model_name(available_models, VIDEO_MODEL_CANDIDATES, "video"),
        }
        write_json(run_dir / "available_models.json", {"models": available_models})
        write_json(run_dir / "config.json", config)
        append_transcript(run_dir, "run_initialized", config)
        state = _record_step_success(run_dir, "promo_create_run")
        return {
            "run_id": resolved_run_id,
            "run_dir": str(run_dir),
            "config": config,
            "run_state": state,
        }
    except Exception as exc:
        _record_step_failure(run_dir, "promo_create_run", exc)
        raise


def generate_anchor_plan(run_dir, *, force=False):
    run_dir = Path(run_dir)
    try:
        config = load_json(run_dir / "config.json")
        output_path = run_dir / "anchor_plan.json"
        if should_skip_output(output_path, force):
            state = update_run_state(run_dir)
            return {"status": "skipped", "path": str(output_path), "run_state": state}

        client = build_client(config["project_id"], config["location"])
        instruction = """
You are planning a four-image product promo campaign.

Inputs:
- A user description of the intended campaign
- Three product reference images
- One logo image

Your job:
1. Infer the product's visual identity from the reference images.
2. Combine that with the user description.
3. Produce a coherent four-anchor campaign plan for image generation.

Requirements:
- Preserve the exact same product identity across all four anchors.
- Ground your understanding in the reference images, not just the text.
- Do not redesign, simplify, or invent a different version of the product.
- Keep one unified campaign world across all anchors:
  same brand mood, compatible environment, compatible lighting direction, and compatible camera language.
- Make the four anchors distinct in purpose and composition:
  1. Hook: premium, high-energy opening image
  2. Detail: macro or detail-focused image that highlights a real product feature visible in the references
  3. Action: the product in a plausible use context
  4. Finale: hero composition that leaves clear space for the logo and brand finish
- Use the logo image only to inform the finale anchor.
- Do not ask the image model to render text overlays except where logo placement is relevant in the finale.
- Keep prompts visually specific and concrete.
- Avoid generic ad-language filler.
- The prompts should be suitable as direct inputs to an image generation model.

Return strict JSON only with this exact structure:

{
  "product_profile": {
    "category": "string",
    "summary": "string",
    "key_visual_traits": ["string"],
    "materials": ["string"],
    "color_palette": ["string"],
    "usage_contexts": ["string"],
    "brand_mood": "string",
    "logo_notes": "string"
  },
  "campaign_direction": {
    "theme": "string",
    "environment": "string",
    "lighting": "string",
    "camera_language": "string"
  },
  "anchor_prompts": [
    {
      "anchor_id": 1,
      "name": "hook",
      "goal": "premium high-energy opening image",
      "prompt": "string"
    },
    {
      "anchor_id": 2,
      "name": "detail",
      "goal": "macro or detail-focused feature image",
      "prompt": "string"
    },
    {
      "anchor_id": 3,
      "name": "action",
      "goal": "plausible usage-context image",
      "prompt": "string"
    },
    {
      "anchor_id": 4,
      "name": "finale",
      "goal": "hero composition with clear logo placement",
      "prompt": "string"
    }
  ]
}

Additional prompt-writing rules:
- Every prompt must explicitly preserve the same product identity from the references.
- Each prompt must specify composition, environment, lighting, and camera feel.
- The detail anchor must focus on real visible product traits from the references.
- The action anchor must be physically plausible for the product category.
- The finale anchor must mention clean negative space or intentional placement for the logo.
- Do not include markdown fences.
- Do not include commentary outside the JSON.
""".strip()
        contents = [
            instruction,
            f"User campaign description:\n{config['description']}",
            "Product reference image 1:",
            build_inline_image_part(config["product_images"][0]),
            "Product reference image 2:",
            build_inline_image_part(config["product_images"][1]),
            "Product reference image 3:",
            build_inline_image_part(config["product_images"][2]),
            "Logo reference image:",
            build_inline_image_part(config["logo_image"]),
        ]
        append_transcript(
            run_dir,
            "llm_request",
            {
                "model": config["prompt_model"],
                "kind": "generate_anchor_plan",
                "text": instruction,
                "description": config["description"],
                "product_images": config["product_images"],
                "logo_image": config["logo_image"],
            },
        )
        response = client.models.generate_content(
            model=config["prompt_model"],
            contents=contents,
            config={"response_mime_type": "application/json"},
        )
        raw_text = response_text(response)
        cleaned = (raw_text or "").strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
            cleaned = re.sub(r"\s*```$", "", cleaned)
        if cleaned and not cleaned.startswith("{"):
            match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
            if match:
                cleaned = match.group(0)
        if not cleaned:
            append_transcript(run_dir, "anchor_plan_parse_error", {"model": config["prompt_model"], "raw_text": raw_text, "response_repr": str(response)})
            raise RuntimeError("Prompt model returned empty or non-text output. Check transcript.jsonl for details.")
        try:
            anchor_plan = json.loads(cleaned)
        except json.JSONDecodeError as exc:
            append_transcript(run_dir, "anchor_plan_parse_error", {"model": config["prompt_model"], "raw_text": raw_text, "cleaned_text": cleaned, "error": str(exc)})
            raise RuntimeError("Prompt model returned invalid JSON. Check transcript.jsonl for the raw output.") from exc
        write_json(output_path, anchor_plan)
        append_transcript(run_dir, "llm_response", {"model": config["prompt_model"], "kind": "generate_anchor_plan", "text": raw_text})
        state = _record_step_success(run_dir, "promo_generate_anchor_plan")
        return {"status": "generated", "path": str(output_path), "anchor_plan": anchor_plan, "run_state": state}
    except Exception as exc:
        _record_step_failure(run_dir, "promo_generate_anchor_plan", exc)
        raise


def _generate_anchor_image(run_dir, config, prompt, reference_paths, output_path, seed):
    client = build_client(config["project_id"], config["location"])
    contents = [build_inline_image_part(path) for path in reference_paths]
    contents.append(
        f"Use all reference images to preserve the exact same product identity. {prompt} "
        "Keep the product visually identical to the references. Do not add text overlays."
    )
    append_transcript(
        run_dir,
        "llm_request",
        {"model": config["image_model"], "kind": "generate_anchor_image", "text": prompt, "reference_images": reference_paths, "output_path": str(output_path), "seed": seed},
    )

    def _call():
        return client.models.generate_content(
            model=config["image_model"],
            contents=contents,
            config=types.GenerateContentConfig(
                response_modalities=["IMAGE"],
                seed=seed,
                image_config=types.ImageConfig(aspect_ratio=config["aspect_ratio"], person_generation="ALLOW_ADULT"),
            ),
        )

    response = call_with_retry(_call, retries=5, initial_delay=20.0)
    save_inline_image(response, output_path)
    append_transcript(run_dir, "llm_response", {"model": config["image_model"], "kind": "generate_anchor_image", "output_path": str(output_path), "text": response_text(response)})


def generate_anchor_images(run_dir, *, force=False):
    run_dir = Path(run_dir)
    try:
        config = load_json(run_dir / "config.json")
        anchor_plan = load_json(run_dir / "anchor_plan.json")
        prompts = [item["prompt"] for item in anchor_plan["anchor_prompts"]]
        anchor_paths = [run_dir / f"anchor_{idx}.png" for idx in range(1, 5)]
        seeds = [701, 702, 703, 704]
        product_images = config["product_images"]
        logo_image = config["logo_image"]
        reference_sets = [
            product_images,
            product_images + [str(anchor_paths[0])],
            product_images + [str(anchor_paths[1])],
            product_images + [logo_image, str(anchor_paths[2])],
        ]
        results = []
        for prompt, refs, output_path, seed in zip(prompts, reference_sets, anchor_paths, seeds):
            if should_skip_output(output_path, force):
                results.append({"path": str(output_path), "status": "skipped"})
                continue
            for ref in refs:
                if not Path(ref).exists():
                    raise FileNotFoundError(f"Missing reference image for anchor generation: {ref}")
            _generate_anchor_image(run_dir, config, prompt, refs, output_path, seed)
            results.append({"path": str(output_path), "status": "generated"})
        state_name = "ANCHOR_IMAGES_READY" if all((run_dir / f"anchor_{idx}.png").is_file() for idx in range(1, 5)) else "ANCHOR_IMAGES_PARTIAL"
        state = update_run_state(run_dir, state=state_name, last_completed_step="promo_generate_anchor_images", last_error={
            "failed_step": None,
            "error_type": None,
            "error_message": None,
            "retryable": False,
        })
        return {"results": results, "run_state": state}
    except Exception as exc:
        _record_step_failure(run_dir, "promo_generate_anchor_images", exc)
        raise


def _generate_bridge_video(run_dir, config, prompt, start_image_path, end_image_path, output_path):
    client = build_client(config["project_id"], config["location"])
    full_prompt = (
        f"{prompt} Keep the exact same product identity and cinematic look throughout the segment. "
        f"Audio: {config['audio_theme']}."
    )
    append_transcript(run_dir, "llm_request", {"model": config["video_model"], "kind": "generate_bridge_video", "text": full_prompt, "start_image_path": str(start_image_path), "end_image_path": str(end_image_path), "output_path": str(output_path)})

    def _call():
        return client.models.generate_videos(
            model=config["video_model"],
            prompt=full_prompt,
            image=build_image(start_image_path),
            config=types.GenerateVideosConfig(
                last_frame=build_image(end_image_path),
                duration_seconds=8,
                generate_audio=False,
                aspect_ratio=config["aspect_ratio"],
                resolution="1080p",
                number_of_videos=1,
            ),
        )

    operation = call_with_retry(_call, retries=4, initial_delay=20.0)
    operation = poll_operation(client, operation)
    error_obj = getattr(operation, "error", None)
    if error_obj:
        message = getattr(error_obj, "message", str(error_obj))
        append_transcript(run_dir, "llm_response", {"model": config["video_model"], "kind": "generate_bridge_video", "error": message})
        raise RuntimeError(f"Veo generation failed: {message}")
    video_payload = extract_video_output(operation)
    save_video_payload(video_payload, output_path)
    append_transcript(run_dir, "llm_response", {"model": config["video_model"], "kind": "generate_bridge_video", "output_path": str(output_path)})


def generate_bridge_videos(run_dir, *, force=False):
    run_dir = Path(run_dir)
    try:
        config = load_json(run_dir / "config.json")
        anchor_paths = [run_dir / f"anchor_{idx}.png" for idx in range(1, 5)]
        for path in anchor_paths:
            if not path.exists():
                raise FileNotFoundError(f"Missing anchor image: {path}")
        bridge_prompts = [
            "A smooth premium cinematic transition from the hook image into the detail image.",
            "A smooth premium cinematic transition from the detail image into the action image.",
            "A smooth premium cinematic transition from the action image into the final branded hero image.",
        ]
        segments = [
            (anchor_paths[0], anchor_paths[1], run_dir / "segment_a.mp4"),
            (anchor_paths[1], anchor_paths[2], run_dir / "segment_b.mp4"),
            (anchor_paths[2], anchor_paths[3], run_dir / "segment_c.mp4"),
        ]
        results = []
        for prompt, (start_path, end_path, output_path) in zip(bridge_prompts, segments):
            if should_skip_output(output_path, force):
                results.append({"path": str(output_path), "status": "skipped"})
                continue
            _generate_bridge_video(run_dir, config, prompt, start_path, end_path, output_path)
            results.append({"path": str(output_path), "status": "generated"})
        state_name = "SEGMENTS_READY" if all((run_dir / f"segment_{label}.mp4").is_file() for label in ("a", "b", "c")) else "SEGMENTS_PARTIAL"
        state = update_run_state(run_dir, state=state_name, last_completed_step="promo_generate_bridge_videos", last_error={
            "failed_step": None,
            "error_type": None,
            "error_message": None,
            "retryable": False,
        })
        return {"results": results, "run_state": state}
    except Exception as exc:
        _record_step_failure(run_dir, "promo_generate_bridge_videos", exc)
        raise


def concat_visual(run_dir, *, force=False):
    run_dir = Path(run_dir)
    try:
        output_path = run_dir / "final.mp4"
        if should_skip_output(output_path, force):
            state = update_run_state(run_dir)
            return {"status": "skipped", "path": str(output_path), "run_state": state}
        segment_paths = [run_dir / "segment_a.mp4", run_dir / "segment_b.mp4", run_dir / "segment_c.mp4"]
        for path in segment_paths:
            if not path.exists():
                raise FileNotFoundError(f"Missing segment video: {path}")
        cmd = ffmpeg_concat(segment_paths, output_path)
        append_transcript(run_dir, "ffmpeg_command", {"cmd": cmd})
        append_transcript(run_dir, "run_complete", {"segments": [str(p) for p in segment_paths], "final_video": str(output_path)})
        state = _record_step_success(run_dir, "promo_concat_visual")
        return {"status": "generated", "path": str(output_path), "duration_seconds": ffprobe_duration(output_path), "run_state": state}
    except Exception as exc:
        _record_step_failure(run_dir, "promo_concat_visual", exc)
        raise


def generate_audio_plan(run_dir, *, force=False):
    run_dir = Path(run_dir)
    try:
        config = load_json(run_dir / "config.json")
        anchor_plan = load_json(run_dir / "anchor_plan.json")
        final_video = run_dir / "final.mp4"
        if not final_video.is_file():
            raise FileNotFoundError(f"Missing final visual video: {final_video}")
        duration_seconds = round(ffprobe_duration(final_video), 2)
        output_json = run_dir / "audio_plan.json"
        legacy_narration_json = run_dir / "narration_script.json"
        output_txt = run_dir / "narration_script.txt"
        music_prompt_txt = run_dir / "music_prompt.txt"
        if all(should_skip_output(path, force) for path in [output_json, legacy_narration_json, output_txt, music_prompt_txt]):
            state = update_run_state(run_dir)
            return {"status": "skipped", "path": str(output_json), "run_state": state}

        client = build_client(config["project_id"], config["location"])
        instruction = f"""
You are planning the audio for a product promo video.

Inputs:
- A product campaign description
- A structured anchor plan for four visual anchor images
- The final video duration

Your job:
1. Write one short narration script for the full final video.
2. Write one coherent background music brief and one direct music-generation prompt for the whole video.
3. Keep everything premium, concise, natural, and aligned with the campaign tone.
4. Keep the narration wording short enough to fit comfortably within the final runtime.
5. Make the narration and music arc flow across three visual segments:
   - opening / hook
   - detail / action
   - final brand finish
6. Do not mention camera directions.
7. Do not invent product claims not supported by the brief.
8. The background music should work as one continuous track for the full video.
9. Music prompts must be high-level, original, and non-referential.
10. Do not imitate artists, songs, ad music, or copyrighted styles.
11. Prefer abstract descriptors: mood, tempo, instrumentation, energy, and mix.
12. The music generation prompt must request original instrumental music with no vocals and no resemblance to known songs.

Return strict JSON only with this exact structure:
{{
  "narration": {{
    "style": "string",
    "total_duration_hint_seconds": {duration_seconds},
    "segments": [
      {{"segment_id": 1, "timing": "0-8s", "text": "string"}},
      {{"segment_id": 2, "timing": "8-16s", "text": "string"}},
      {{"segment_id": 3, "timing": "16-24s", "text": "string"}}
    ],
    "full_script": "string"
  }},
  "music": {{
    "duration_hint_seconds": {duration_seconds},
    "style": "string",
    "tempo_bpm": 0,
    "mood_arc": [
      {{"range": "0-8s", "energy": "string", "notes": "string"}},
      {{"range": "8-16s", "energy": "string", "notes": "string"}},
      {{"range": "16-24s", "energy": "string", "notes": "string"}}
    ],
    "instrumentation": ["string"],
    "mix_notes": "string",
    "generation_prompt": "string",
    "generation_prompt_safe": "string"
  }}
}}

Campaign description:
{config["description"]}

Final video duration seconds:
{duration_seconds}

Anchor plan JSON:
{json.dumps(anchor_plan, ensure_ascii=False)}
""".strip()
        append_transcript(run_dir, "llm_request", {"model": config["prompt_model"], "kind": "generate_audio_plan", "text": instruction})
        response = client.models.generate_content(model=config["prompt_model"], contents=instruction, config={"response_mime_type": "application/json"})
        raw_text = response_text(response)
        cleaned = (raw_text or "").strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.removeprefix("```json").removeprefix("```").strip()
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3].strip()
        script_payload = json.loads(cleaned)
        if not script_payload["music"].get("generation_prompt_safe"):
            music = script_payload["music"]
            safe_prompt = (
                f"Original instrumental music, {music['style']}, around {music['tempo_bpm']} BPM, "
                f"instrumentation: {', '.join(music['instrumentation'])}, "
                f"mix: {music['mix_notes']}. No vocals, no lyrics, no speech, no resemblance to known songs or artists."
            )
            script_payload["music"]["generation_prompt_safe"] = safe_prompt
        write_json(output_json, script_payload)
        write_json(legacy_narration_json, script_payload["narration"])
        output_txt.write_text(script_payload["narration"]["full_script"].strip() + "\n", encoding="utf-8")
        music_prompt_txt.write_text(script_payload["music"]["generation_prompt_safe"].strip() + "\n", encoding="utf-8")
        append_transcript(run_dir, "llm_response", {"model": config["prompt_model"], "kind": "generate_audio_plan", "text": raw_text})
        state = _record_step_success(run_dir, "promo_generate_audio_plan")
        return {"status": "generated", "path": str(output_json), "audio_plan": script_payload, "run_state": state}
    except Exception as exc:
        _record_step_failure(run_dir, "promo_generate_audio_plan", exc)
        raise


def _get_access_token():
    credentials, _project = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    credentials.refresh(Request())
    return credentials.token


def _post_json(url, payload, project_id, retries=5, initial_delay=10.0):
    body = json.dumps(payload).encode("utf-8")
    delay = initial_delay
    for attempt in range(1, retries + 1):
        token = _get_access_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json", "x-goog-user-project": project_id}
        request = urllib.request.Request(url=url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=300) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            if exc.code in (429, 500, 503) and attempt < retries:
                time.sleep(delay)
                delay *= 2
                continue
            raise RuntimeError(f"HTTP {exc.code} for {url}: {raw}") from exc


def _generate_music(run_dir, config, audio_plan):
    music = audio_plan["music"]
    output_path = Path(run_dir) / "soundtrack.wav"
    prompt = music.get("generation_prompt_safe") or music["generation_prompt"]
    payload = {"instances": [{"prompt": prompt, "negative_prompt": "vocals, singing, speech, dialogue", "seed": 24680}], "parameters": {}}
    url = f"https://{config['location']}-aiplatform.googleapis.com/v1/projects/{config['project_id']}/locations/{config['location']}/publishers/google/models/{LYRIA_MODEL}:predict"
    append_transcript(run_dir, "audio_asset_request", {"kind": "music", "model": LYRIA_MODEL, "url": url, "payload": payload})
    response = _post_json(url, payload, project_id=config["project_id"])
    predictions = response.get("predictions") or []
    if not predictions:
        raise RuntimeError("Lyria response did not include predictions.")
    first = predictions[0]
    audio_content = first.get("audioContent") or first.get("bytesBase64Encoded")
    if not audio_content:
        raise RuntimeError(f"Lyria response missing audioContent: {response}")
    output_path.write_bytes(base64.b64decode(audio_content))
    append_transcript(run_dir, "audio_asset_response", {"kind": "music", "model": LYRIA_MODEL, "output_path": str(output_path), "response_keys": list(response.keys())})
    return {"status": "generated", "provider": "vertex_lyria", "model": LYRIA_MODEL, "path": str(output_path), "prompt": prompt}


def _generate_narration(run_dir, config, audio_plan, voice):
    narration = audio_plan["narration"]
    output_path = Path(run_dir) / "narration.wav"
    payload = {
        "input": {"prompt": "Read this as a premium, confident product commercial voiceover.", "text": narration["full_script"]},
        "voice": {"languageCode": "en-us", "name": voice, "modelName": TTS_MODEL},
        "audioConfig": {"audioEncoding": "LINEAR16"},
    }
    url = "https://texttospeech.googleapis.com/v1/text:synthesize"
    append_transcript(run_dir, "audio_asset_request", {"kind": "narration", "model": TTS_MODEL, "url": url, "payload": payload})
    response = _post_json(url, payload, project_id=config["project_id"])
    audio_content = response.get("audioContent")
    if not audio_content:
        raise RuntimeError(f"TTS response missing audioContent: {response}")
    output_path.write_bytes(base64.b64decode(audio_content))
    append_transcript(run_dir, "audio_asset_response", {"kind": "narration", "model": TTS_MODEL, "output_path": str(output_path)})
    return {"status": "generated", "provider": "google_tts", "model": TTS_MODEL, "path": str(output_path), "voice": voice, "text": narration["full_script"]}


def generate_audio_assets(run_dir, *, music_only=False, narration_only=False, voice="Kore", force=False):
    run_dir = Path(run_dir)
    if music_only and narration_only:
        raise ValueError("Use at most one of music_only or narration_only.")
    try:
        config = load_json(run_dir / "config.json")
        audio_plan = load_json(run_dir / "audio_plan.json")
        assets_path = run_dir / "audio_assets.json"
        soundtrack_path = run_dir / "soundtrack.wav"
        narration_path = run_dir / "narration.wav"
        assets = {"music": {"status": "skipped"}, "narration": {"status": "skipped"}}
        if not narration_only:
            if should_skip_output(soundtrack_path, force):
                assets["music"] = {"status": "existing", "provider": "vertex_lyria", "model": LYRIA_MODEL, "path": str(soundtrack_path), "prompt": audio_plan["music"].get("generation_prompt_safe") or audio_plan["music"]["generation_prompt"]}
            else:
                assets["music"] = _generate_music(run_dir, config, audio_plan)
        if not music_only:
            if should_skip_output(narration_path, force):
                assets["narration"] = {"status": "existing", "provider": "google_tts", "model": TTS_MODEL, "path": str(narration_path), "voice": voice, "text": audio_plan["narration"]["full_script"]}
            else:
                assets["narration"] = _generate_narration(run_dir, config, audio_plan, voice)
        write_json(assets_path, assets)
        append_transcript(run_dir, "audio_assets_complete", assets)
        state_name = "AUDIO_ASSETS_READY" if soundtrack_path.is_file() or narration_path.is_file() else "AUDIO_ASSETS_PARTIAL"
        state = update_run_state(run_dir, state=state_name, last_completed_step="promo_generate_audio_assets", last_error={
            "failed_step": None,
            "error_type": None,
            "error_message": None,
            "retryable": False,
        })
        return {"status": "generated", "path": str(assets_path), "assets": assets, "run_state": state}
    except Exception as exc:
        _record_step_failure(run_dir, "promo_generate_audio_assets", exc)
        raise


def _choose_soundtrack_source(run_dir, explicit_music_file=None):
    if explicit_music_file:
        path = Path(explicit_music_file).resolve()
        if not path.is_file():
            raise FileNotFoundError(f"Music file not found: {path}")
        return path, "explicit"
    assets_path = Path(run_dir) / "audio_assets.json"
    if assets_path.is_file():
        assets = load_json(assets_path)
        music_path = (assets.get("music") or {}).get("path")
        if music_path and Path(music_path).is_file():
            return Path(music_path), "audio_assets"
    segment_a = Path(run_dir) / "segment_a.mp4"
    if segment_a.is_file() and ffprobe_has_audio(segment_a):
        return segment_a, "segment_a_audio"
    music_prompt = Path(run_dir) / "music_prompt.txt"
    if music_prompt.is_file():
        raise RuntimeError(
            "No soundtrack source available. step_06 generated a music prompt, but not audio. "
            f"Use that prompt to create a music file, then rerun with --music-file. Prompt file: {music_prompt}"
        )
    raise RuntimeError("No soundtrack source available. Provide --music-file, or use a segment file that contains audio.")


def _build_ffmpeg_audio_command(final_video, soundtrack_source, output_path, duration, music_volume, narration_file=None, narration_volume=1.0, music_fade_out_seconds=2.5):
    cmd = ["ffmpeg", "-y", "-i", str(final_video.resolve()), "-stream_loop", "-1", "-i", str(Path(soundtrack_source).resolve())]
    fade_seconds = max(0.0, min(music_fade_out_seconds, duration))
    fade_start = max(0.0, duration - fade_seconds)
    bg_filter = f"[1:a]volume={music_volume},atrim=duration={duration}"
    if fade_seconds > 0:
        bg_filter += f",afade=t=out:st={fade_start}:d={fade_seconds}"
    bg_filter += ",asetpts=N/SR/TB[bg]"
    filter_parts = [bg_filter]
    map_audio = "[bg]"
    if narration_file:
        cmd.extend(["-i", str(Path(narration_file).resolve())])
        filter_parts.append(f"[2:a]volume={narration_volume},atrim=duration={duration},asetpts=N/SR/TB[vo]")
        filter_parts.append("[bg][vo]amix=inputs=2:duration=longest:dropout_transition=2[aout]")
        map_audio = "[aout]"
    cmd.extend(["-filter_complex", ";".join(filter_parts), "-map", "0:v:0", "-map", map_audio, "-c:v", "copy", "-c:a", "aac", "-shortest", str(output_path.resolve())])
    return cmd


def merge_audio(run_dir, *, music_file=None, narration_file=None, music_volume=0.35, narration_volume=1.0, music_fade_out_seconds=2.5, force=False):
    run_dir = Path(run_dir)
    try:
        config = load_json(run_dir / "config.json")
        final_video = run_dir / "final.mp4"
        output_path = run_dir / "final_with_audio.mp4"
        if should_skip_output(output_path, force):
            state = update_run_state(run_dir)
            return {"status": "skipped", "path": str(output_path), "run_state": state}
        if not final_video.is_file():
            raise FileNotFoundError(f"Missing visual-only final video: {final_video}")
        resolved_narration = narration_file
        assets_path = run_dir / "audio_assets.json"
        if not resolved_narration and assets_path.is_file():
            assets = load_json(assets_path)
            narration_path = (assets.get("narration") or {}).get("path")
            if narration_path and Path(narration_path).is_file():
                resolved_narration = narration_path
        if resolved_narration and not Path(resolved_narration).is_file():
            raise FileNotFoundError(f"Narration file not found: {resolved_narration}")
        duration = ffprobe_duration(final_video)
        soundtrack_source, source_kind = _choose_soundtrack_source(run_dir, music_file)
        cmd = _build_ffmpeg_audio_command(final_video, soundtrack_source, output_path, duration, music_volume, narration_file=resolved_narration, narration_volume=narration_volume, music_fade_out_seconds=music_fade_out_seconds)
        append_transcript(run_dir, "audio_step_start", {"final_video": str(final_video), "output_path": str(output_path), "soundtrack_source": str(soundtrack_source), "soundtrack_source_kind": source_kind, "narration_file": resolved_narration, "duration_seconds": duration, "music_fade_out_seconds": music_fade_out_seconds, "audio_theme": config.get("audio_theme")})
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            append_transcript(run_dir, "audio_step_error", {"cmd": cmd, "stderr": result.stderr})
            raise RuntimeError(f"ffmpeg audio mux failed:\n{result.stderr}")
        append_transcript(run_dir, "audio_step_complete", {"cmd": cmd, "output_path": str(output_path)})
        state = _record_step_success(run_dir, "promo_merge_audio")
        return {"status": "generated", "path": str(output_path), "run_state": state}
    except Exception as exc:
        _record_step_failure(run_dir, "promo_merge_audio", exc)
        raise


def generate_alt_scenario_and_image_prompts(run_dir, *, force=False):
    run_dir = Path(run_dir)
    config = load_json(run_dir / "config.json")
    scenario_path = run_dir / "alt_scenario_brief.json"
    prompts_path = run_dir / "alt_image_prompts.json"
    if all(should_skip_output(path, force) for path in [scenario_path, prompts_path]):
        state = update_run_state(run_dir)
        return {"status": "skipped", "scenario_path": str(scenario_path), "prompts_path": str(prompts_path), "run_state": state}

    client = build_client(config["project_id"], config["location"])
    instruction = """
You are creating an image-generation plan for a product promo.

Inputs:
- User campaign description
- Three product reference images

Tasks:
1) Propose one coherent campaign scenario that best fits the product and user description.
2) Generate 6 high-impact image prompts that preserve exact product identity.

Rules:
- Keep product identity exact: shape, material, logo placement, color, and signature details.
- Prompts must be visually diverse but still belong to one coherent campaign world.
- Prompts must be suitable for image generation input.
- No text overlay instructions.
- No markdown.

Return strict JSON only:
{
  "scenario_brief": {
    "theme": "string",
    "setting": "string",
    "lighting": "string",
    "mood": "string",
    "usage_context": "string",
    "product_identity_constraints": ["string"],
    "forbidden_drift": ["string"]
  },
  "image_prompts": [
    {"id": 1, "intent": "hero", "prompt": "string"},
    {"id": 2, "intent": "detail", "prompt": "string"},
    {"id": 3, "intent": "lifestyle", "prompt": "string"},
    {"id": 4, "intent": "action", "prompt": "string"},
    {"id": 5, "intent": "dramatic", "prompt": "string"},
    {"id": 6, "intent": "premium_close", "prompt": "string"}
  ]
}
""".strip()
    contents = [
        instruction,
        f"User description:\n{config['description']}",
        "Product reference image 1:",
        build_inline_image_part(config["product_images"][0]),
        "Product reference image 2:",
        build_inline_image_part(config["product_images"][1]),
        "Product reference image 3:",
        build_inline_image_part(config["product_images"][2]),
    ]
    append_transcript(run_dir, "llm_request", {"model": config["prompt_model"], "kind": "generate_alt_scenario_and_image_prompts"})
    response = client.models.generate_content(
        model=config["prompt_model"],
        contents=contents,
        config={"response_mime_type": "application/json"},
    )
    raw_text = response_text(response)
    if not raw_text:
        raise RuntimeError("Scenario/prompt generation returned empty text.")
    data = json.loads(raw_text)
    scenario = data.get("scenario_brief")
    prompts = data.get("image_prompts")
    if not scenario or not isinstance(prompts, list) or len(prompts) != 6:
        raise RuntimeError("Scenario/prompt JSON must include scenario_brief and 6 image_prompts.")
    write_json(scenario_path, scenario)
    write_json(prompts_path, {"prompts": prompts})
    append_transcript(run_dir, "llm_response", {"model": config["prompt_model"], "kind": "generate_alt_scenario_and_image_prompts"})
    state = update_run_state(
        run_dir,
        state="ALT_SCENARIO_READY",
        last_completed_step="promo_alt_generate_scenario_and_image_prompts",
        last_error={"failed_step": None, "error_type": None, "error_message": None, "retryable": False},
    )
    return {"status": "generated", "scenario_path": str(scenario_path), "prompts_path": str(prompts_path), "run_state": state}


def generate_alt_candidate_images(run_dir, *, force=False):
    run_dir = Path(run_dir)
    config = load_json(run_dir / "config.json")
    prompts_payload = load_json(run_dir / "alt_image_prompts.json")
    prompts = prompts_payload["prompts"]
    candidates_dir = run_dir / "alt_candidates"
    candidates_dir.mkdir(parents=True, exist_ok=True)
    seeds = [9101, 9102, 9103, 9104, 9105, 9106]
    manifest = {"candidates": []}
    for idx, prompt_item in enumerate(prompts, start=1):
        output_path = candidates_dir / f"candidate_{idx}.png"
        if should_skip_output(output_path, force):
            manifest["candidates"].append(
                {
                    "id": idx,
                    "intent": prompt_item.get("intent"),
                    "prompt": prompt_item.get("prompt"),
                    "path": str(output_path),
                    "seed": seeds[idx - 1],
                    "status": "skipped",
                }
            )
            continue
        _generate_anchor_image(run_dir, config, prompt_item["prompt"], config["product_images"], output_path, seeds[idx - 1])
        manifest["candidates"].append(
            {
                "id": idx,
                "intent": prompt_item.get("intent"),
                "prompt": prompt_item.get("prompt"),
                "path": str(output_path),
                "seed": seeds[idx - 1],
                "status": "generated",
            }
        )
    manifest_path = run_dir / "alt_candidates_manifest.json"
    write_json(manifest_path, manifest)
    state = update_run_state(
        run_dir,
        state="ALT_IMAGES_READY",
        last_completed_step="promo_alt_generate_candidate_images",
        last_error={"failed_step": None, "error_type": None, "error_message": None, "retryable": False},
    )
    return {"status": "generated", "manifest_path": str(manifest_path), "results": manifest["candidates"], "run_state": state}


def _score_alt_candidate(run_dir, config, candidate_path):
    client = build_client(config["project_id"], config["location"])
    instruction = """
Evaluate this generated product promo image against product reference images.

Score each field from 0 to 100:
- product_match
- impact
- clarity
- visual_quality

Return strict JSON only:
{
  "product_match": 0,
  "impact": 0,
  "clarity": 0,
  "visual_quality": 0,
  "reason": "string"
}
""".strip()
    contents = [
        instruction,
        f"User description:\n{config['description']}",
        "Product reference image 1:",
        build_inline_image_part(config["product_images"][0]),
        "Product reference image 2:",
        build_inline_image_part(config["product_images"][1]),
        "Product reference image 3:",
        build_inline_image_part(config["product_images"][2]),
        "Candidate image:",
        build_inline_image_part(candidate_path),
    ]
    response = client.models.generate_content(
        model=config["prompt_model"],
        contents=contents,
        config={"response_mime_type": "application/json"},
    )
    raw_text = response_text(response)
    if not raw_text:
        raise RuntimeError(f"Empty scoring response for {candidate_path}.")
    data = json.loads(raw_text)
    for key in ["product_match", "impact", "clarity", "visual_quality"]:
        data[key] = max(0, min(100, int(data.get(key, 0))))
    data["reason"] = str(data.get("reason", "")).strip()
    return data


def rank_and_select_alt_images(run_dir, *, force=False):
    run_dir = Path(run_dir)
    config = load_json(run_dir / "config.json")
    rankings_path = run_dir / "alt_image_rankings.json"
    selected_path = run_dir / "alt_selected_top3.json"
    if all(should_skip_output(path, force) for path in [rankings_path, selected_path]):
        state = update_run_state(run_dir)
        return {"status": "skipped", "rankings_path": str(rankings_path), "selected_path": str(selected_path), "run_state": state}

    manifest = load_json(run_dir / "alt_candidates_manifest.json")
    weights = {
        "product_match": 0.45,
        "impact": 0.25,
        "clarity": 0.20,
        "visual_quality": 0.10,
    }
    scored = []
    for item in manifest["candidates"]:
        candidate_path = item["path"]
        if not Path(candidate_path).is_file():
            raise FileNotFoundError(f"Candidate image missing: {candidate_path}")
        score = _score_alt_candidate(run_dir, config, candidate_path)
        total = round(
            score["product_match"] * weights["product_match"]
            + score["impact"] * weights["impact"]
            + score["clarity"] * weights["clarity"]
            + score["visual_quality"] * weights["visual_quality"],
            2,
        )
        scored.append(
            {
                "id": item["id"],
                "path": candidate_path,
                "intent": item.get("intent"),
                "product_match": score["product_match"],
                "impact": score["impact"],
                "clarity": score["clarity"],
                "visual_quality": score["visual_quality"],
                "total": total,
                "reason": score["reason"],
            }
        )
    scored.sort(key=lambda x: x["total"], reverse=True)
    top3 = [{"id": item["id"], "path": item["path"], "intent": item.get("intent"), "total": item["total"]} for item in scored[:3]]
    write_json(rankings_path, {"weights": weights, "scores": scored})
    write_json(selected_path, {"selected": top3})
    state = update_run_state(
        run_dir,
        state="ALT_TOP3_READY",
        last_completed_step="promo_alt_rank_and_select_images",
        last_error={"failed_step": None, "error_type": None, "error_message": None, "retryable": False},
    )
    return {"status": "generated", "rankings_path": str(rankings_path), "selected_path": str(selected_path), "selected": top3, "run_state": state}


def generate_alt_video_prompt(run_dir, *, force=False):
    run_dir = Path(run_dir)
    config = load_json(run_dir / "config.json")
    scenario = load_json(run_dir / "alt_scenario_brief.json")
    selected = load_json(run_dir / "alt_selected_top3.json")
    prompt_path = run_dir / "alt_video_prompt.txt"
    meta_path = run_dir / "alt_video_prompt_meta.json"
    review_path = run_dir / "alt_video_prompt_review.json"
    if all(should_skip_output(path, force) for path in [prompt_path, meta_path, review_path]):
        state = update_run_state(run_dir)
        return {
            "status": "skipped",
            "prompt_path": str(prompt_path),
            "meta_path": str(meta_path),
            "review_path": str(review_path),
            "run_state": state,
        }

    client = build_client(config["project_id"], config["location"])
    draft_instruction = (
        "Write one concise Veo prompt for an 8-second vertical product showcase video. "
        "The prompt must maximize product impact and preserve exact product identity. "
        "Use scenario and selected top images for style alignment. "
        "Avoid product operation or manipulation actions. Focus on cinematic showcase only. "
        "Return plain text only."
    )
    draft_contents = [
        draft_instruction,
        f"User description:\n{config['description']}",
        f"Scenario brief JSON:\n{json.dumps(scenario, ensure_ascii=False)}",
        f"Selected top-3 JSON:\n{json.dumps(selected, ensure_ascii=False)}",
    ]
    append_transcript(run_dir, "llm_request", {"model": config["prompt_model"], "kind": "generate_alt_video_prompt_draft"})
    response = client.models.generate_content(model=config["prompt_model"], contents=draft_contents)
    draft_text = response_text(response)
    if not draft_text:
        raise RuntimeError("Empty video prompt response.")
    current_prompt = draft_text.strip()

    critique_rounds = []
    accepted_round = None
    risk_threshold = 35

    for round_idx in range(1, 4):
        critique_instruction = """
You are reviewing a product video prompt for operation-risk.

Goal:
- Keep the prompt product-agnostic and showcase-first.
- Avoid any operation/manipulation/state-change actions on the product.

Score operation risk from 0-100:
- 0 means safe showcase-only wording.
- 100 means high risk that the model will fail on product operation actions.

Return strict JSON only:
{
  "operation_risk": 0,
  "issues": ["string"],
  "safe_rewrite": "string"
}
""".strip()
        critique_contents = [
            critique_instruction,
            f"User description:\n{config['description']}",
            f"Scenario brief JSON:\n{json.dumps(scenario, ensure_ascii=False)}",
            f"Selected top-3 JSON:\n{json.dumps(selected, ensure_ascii=False)}",
            f"Current prompt:\n{current_prompt}",
        ]
        append_transcript(
            run_dir,
            "llm_request",
            {"model": config["prompt_model"], "kind": "generate_alt_video_prompt_critique", "round": round_idx, "prompt": current_prompt},
        )
        critique_response = client.models.generate_content(
            model=config["prompt_model"],
            contents=critique_contents,
            config={"response_mime_type": "application/json"},
        )
        critique_raw = response_text(critique_response)
        if not critique_raw:
            raise RuntimeError("Prompt critique returned empty response.")
        critique_data = json.loads(critique_raw)
        operation_risk = max(0, min(100, int(critique_data.get("operation_risk", 100))))
        issues = critique_data.get("issues") or []
        if not isinstance(issues, list):
            issues = [str(issues)]
        safe_rewrite = str(critique_data.get("safe_rewrite", "")).strip()

        critique_rounds.append(
            {
                "round": round_idx,
                "operation_risk": operation_risk,
                "issues": [str(item) for item in issues],
                "prompt_in": current_prompt,
                "safe_rewrite": safe_rewrite,
            }
        )

        if operation_risk <= risk_threshold and current_prompt:
            accepted_round = round_idx
            break

        if safe_rewrite:
            current_prompt = safe_rewrite
        else:
            rewrite_instruction = """
Rewrite this product video prompt to be showcase-only and product-agnostic.

Rules:
- No operation/manipulation/state-change actions on product.
- Keep cinematic showcase style: framing, camera movement, lighting, detail emphasis.
- Preserve product identity and premium impact.
- Return plain text only.
""".strip()
            rewrite_contents = [
                rewrite_instruction,
                f"Current prompt:\n{current_prompt}",
                f"Critique issues:\n{json.dumps(issues, ensure_ascii=False)}",
            ]
            rewrite_response = client.models.generate_content(model=config["prompt_model"], contents=rewrite_contents)
            rewrite_text = response_text(rewrite_response)
            if rewrite_text:
                current_prompt = rewrite_text.strip()

    final_prompt = current_prompt.strip()
    if not final_prompt:
        raise RuntimeError("Final alt video prompt is empty after critique loop.")

    write_json(
        review_path,
        {
            "model": config["prompt_model"],
            "risk_threshold": risk_threshold,
            "accepted_round": accepted_round,
            "rounds": critique_rounds,
            "final_prompt": final_prompt,
        },
    )
    prompt_path.write_text(final_prompt + "\n", encoding="utf-8")
    write_json(
        meta_path,
        {
            "model": config["prompt_model"],
            "prompt": final_prompt,
            "review_path": str(review_path),
            "accepted_round": accepted_round,
        },
    )
    append_transcript(
        run_dir,
        "llm_response",
        {
            "model": config["prompt_model"],
            "kind": "generate_alt_video_prompt",
            "text": final_prompt,
            "accepted_round": accepted_round,
            "operation_risk_final": critique_rounds[-1]["operation_risk"] if critique_rounds else None,
        },
    )
    state = update_run_state(
        run_dir,
        state="ALT_VIDEO_PROMPT_READY",
        last_completed_step="promo_alt_generate_video_prompt",
        last_error={"failed_step": None, "error_type": None, "error_message": None, "retryable": False},
    )
    return {
        "status": "generated",
        "prompt_path": str(prompt_path),
        "meta_path": str(meta_path),
        "review_path": str(review_path),
        "prompt": final_prompt,
        "run_state": state,
    }


def _build_video_reference_image(path):
    return types.VideoGenerationReferenceImage(image=build_image(path), reference_type="asset")


def generate_alt_8s_video(run_dir, *, force=False):
    run_dir = Path(run_dir)
    config = load_json(run_dir / "config.json")
    selected = load_json(run_dir / "alt_selected_top3.json")
    prompt_path = run_dir / "alt_video_prompt.txt"
    output_path = run_dir / "alt_final_8s.mp4"
    if should_skip_output(output_path, force):
        state = update_run_state(run_dir)
        return {"status": "skipped", "path": str(output_path), "run_state": state}
    if not prompt_path.is_file():
        raise FileNotFoundError(f"Missing alt video prompt: {prompt_path}")
    prompt = prompt_path.read_text(encoding="utf-8").strip()
    selected_images = [item["path"] for item in selected.get("selected", [])]
    if len(selected_images) != 3:
        raise RuntimeError("alt_selected_top3.json must contain exactly 3 selected images.")
    for path in selected_images:
        require_file(path)

    client = build_client(config["project_id"], config["location"])
    append_transcript(run_dir, "llm_request", {"model": config["video_model"], "kind": "generate_alt_8s_video", "reference_images": selected_images})

    def _call():
        return client.models.generate_videos(
            model=config["video_model"],
            prompt=prompt,
            config=types.GenerateVideosConfig(
                reference_images=[_build_video_reference_image(path) for path in selected_images],
                duration_seconds=8,
                aspect_ratio=config["aspect_ratio"],
                number_of_videos=1,
                generate_audio=False,
                resolution="1080p",
            ),
        )

    operation = call_with_retry(_call, retries=4, initial_delay=20.0)
    operation = poll_operation(client, operation)
    error_obj = getattr(operation, "error", None)
    if error_obj:
        message = getattr(error_obj, "message", str(error_obj))
        append_transcript(run_dir, "llm_response", {"model": config["video_model"], "kind": "generate_alt_8s_video", "error": message})
        raise RuntimeError(f"Veo generation failed: {message}")
    video_payload = extract_video_output(operation)
    save_video_payload(video_payload, output_path)
    append_transcript(run_dir, "llm_response", {"model": config["video_model"], "kind": "generate_alt_8s_video", "output_path": str(output_path)})
    state = update_run_state(
        run_dir,
        state="ALT_VIDEO_READY",
        last_completed_step="promo_alt_generate_8s_video",
        last_error={"failed_step": None, "error_type": None, "error_message": None, "retryable": False},
    )
    return {"status": "generated", "path": str(output_path), "duration_seconds": ffprobe_duration(output_path), "run_state": state}
