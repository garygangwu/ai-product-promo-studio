import json
from pathlib import Path

from product_promo import workflow


def _resolve_run_dir(arguments):
    run_dir = arguments.get("run_dir")
    if run_dir:
        path = Path(run_dir).resolve()
        if not path.is_dir():
            raise FileNotFoundError(f"Run directory not found: {path}")
        return path
    run_id = arguments.get("run_id")
    if not run_id:
        raise ValueError("Provide run_id or run_dir.")
    path = (workflow.RUNS_DIR / run_id).resolve()
    if not path.is_dir():
        raise FileNotFoundError(f"Run directory not found for run_id={run_id}: {path}")
    return path


def _base_response(run_dir, message, **extra):
    status = workflow.get_run_status(run_dir)
    payload = {
        "ok": True,
        "run_id": status["run_id"],
        "run_dir": status["run_dir"],
        "state": status["state"],
        "message": message,
    }
    payload.update(extra)
    return payload


def _error_response(run_dir, exc):
    status = workflow.get_run_status(run_dir)
    last_error = status.get("last_error") or {}
    return {
        "ok": False,
        "run_id": status["run_id"],
        "run_dir": status["run_dir"],
        "state": status["state"],
        "message": str(exc),
        "error": last_error,
    }


def handle_create_run(arguments):
    result = workflow.create_run(
        product_images=arguments["product_images"],
        logo_image=arguments["logo_image"],
        description=arguments["description"],
        run_id=arguments.get("run_id"),
        project_id=arguments.get("project_id", workflow.DEFAULT_PROJECT_ID),
        location=arguments.get("location", workflow.DEFAULT_LOCATION),
        aspect_ratio=arguments.get("aspect_ratio", workflow.DEFAULT_ASPECT_RATIO),
        audio_theme=arguments.get("audio_theme", workflow.DEFAULT_AUDIO_THEME),
        llm_provider=arguments.get("llm_provider", "openai"),
        llm_model_prompt=arguments.get("llm_model_prompt", "gpt-5"),
        llm_provider_prompt=arguments.get("llm_provider_prompt"),
        llm_provider_qa=arguments.get("llm_provider_qa"),
        llm_model_qa=arguments.get("llm_model_qa"),
    )
    return _base_response(
        result["run_dir"],
        "Run initialized.",
        config=result["config"],
        artifacts=workflow.get_run_status(result["run_dir"])["artifacts"],
    )


def handle_get_run(arguments):
    run_dir = _resolve_run_dir(arguments)
    status = workflow.get_run_status(run_dir)
    return {
        "ok": True,
        "run_id": status["run_id"],
        "run_dir": status["run_dir"],
        "state": status["state"],
        "message": "Run loaded.",
        "artifacts": status["artifacts"],
        "last_error": status["last_error"],
        "next_recommended_tool": status.get("next_recommended_tool"),
        "last_completed_step": status.get("last_completed_step"),
    }


def handle_list_runs(arguments):
    desired_state = arguments.get("status")
    runs = workflow.list_runs()
    if desired_state:
        runs = [item for item in runs if item["state"] == desired_state]
    return {
        "ok": True,
        "message": "Runs listed.",
        "runs": runs,
    }


def _run_step(arguments, step_name, func, success_message, extra_builder=None):
    run_dir = _resolve_run_dir(arguments)
    try:
        result = func(run_dir, force=arguments.get("force", False))
        payload = _base_response(run_dir, success_message, result=result)
        if extra_builder:
            payload.update(extra_builder(run_dir, result))
        return payload
    except Exception as exc:
        return _error_response(run_dir, exc)


def handle_generate_anchor_plan(arguments):
    return _run_step(arguments, "promo_generate_anchor_plan", workflow.generate_anchor_plan, "Anchor plan generated.")


def handle_generate_anchor_images(arguments):
    return _run_step(arguments, "promo_generate_anchor_images", workflow.generate_anchor_images, "Anchor image generation complete.")


def handle_generate_bridge_videos(arguments):
    return _run_step(arguments, "promo_generate_bridge_videos", workflow.generate_bridge_videos, "Bridge video generation complete.")


def handle_concat_visual(arguments):
    return _run_step(arguments, "promo_concat_visual", workflow.concat_visual, "Visual final video created.")


def handle_generate_audio_plan(arguments):
    return _run_step(arguments, "promo_generate_audio_plan", workflow.generate_audio_plan, "Audio plan generated.")


def handle_generate_audio_assets(arguments):
    run_dir = _resolve_run_dir(arguments)
    try:
        result = workflow.generate_audio_assets(
            run_dir,
            music_only=arguments.get("music_only", False),
            narration_only=arguments.get("narration_only", False),
            voice=arguments.get("voice", "Kore"),
            force=arguments.get("force", False),
        )
        return _base_response(run_dir, "Audio assets generated.", result=result)
    except Exception as exc:
        return _error_response(run_dir, exc)


def handle_merge_audio(arguments):
    run_dir = _resolve_run_dir(arguments)
    try:
        result = workflow.merge_audio(
            run_dir,
            music_file=arguments.get("music_file"),
            narration_file=arguments.get("narration_file"),
            music_volume=arguments.get("music_volume", 0.35),
            narration_volume=arguments.get("narration_volume", 1.0),
            music_fade_out_seconds=arguments.get("music_fade_out_seconds", 2.5),
            force=arguments.get("force", False),
        )
        return _base_response(run_dir, "Final video with audio created.", result=result)
    except Exception as exc:
        return _error_response(run_dir, exc)


def handle_retry_failed_step(arguments):
    run_dir = _resolve_run_dir(arguments)
    status = workflow.get_run_status(run_dir)
    failed_step = arguments.get("step") or (status.get("last_error") or {}).get("failed_step")
    if not failed_step:
        return _base_response(run_dir, "Run has no failed step to retry.")
    step_args = {"run_dir": str(run_dir), "force": arguments.get("force", False)}
    if failed_step == "promo_generate_audio_assets":
        step_args["music_only"] = arguments.get("music_only", False)
        step_args["narration_only"] = arguments.get("narration_only", False)
        step_args["voice"] = arguments.get("voice", "Kore")
    if failed_step == "promo_merge_audio":
        step_args["music_file"] = arguments.get("music_file")
        step_args["narration_file"] = arguments.get("narration_file")
        step_args["music_volume"] = arguments.get("music_volume", 0.35)
        step_args["narration_volume"] = arguments.get("narration_volume", 1.0)
        step_args["music_fade_out_seconds"] = arguments.get("music_fade_out_seconds", 2.5)
    handler = TOOL_HANDLERS.get(failed_step)
    if handler is None:
        raise ValueError(f"Unsupported retry target: {failed_step}")
    return handler(step_args)


TOOL_DEFINITIONS = [
    {
        "name": "promo_create_run",
        "description": "Initialize a new product promo run and resolve models.",
        "inputSchema": {
            "type": "object",
            "required": ["product_images", "logo_image", "description"],
            "properties": {
                "run_id": {"type": "string"},
                "project_id": {"type": "string"},
                "location": {"type": "string"},
                "product_images": {"type": "array", "minItems": 3, "maxItems": 3, "items": {"type": "string"}},
                "logo_image": {"type": "string"},
                "description": {"type": "string"},
                "aspect_ratio": {"type": "string"},
                "audio_theme": {"type": "string"},
                "llm_provider": {"type": "string", "enum": ["google", "openai"]},
                "llm_provider_prompt": {"type": "string", "enum": ["google", "openai"]},
                "llm_provider_qa": {"type": "string", "enum": ["google", "openai"]},
                "llm_model_prompt": {"type": "string"},
                "llm_model_qa": {"type": "string"},
            },
        },
    },
    {
        "name": "promo_get_run",
        "description": "Load current run state and artifacts.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "run_dir": {"type": "string"},
            },
        },
    },
    {
        "name": "promo_list_runs",
        "description": "List promo runs on disk.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "status": {"type": "string"},
            },
        },
    },
    {
        "name": "promo_generate_anchor_plan",
        "description": "Generate anchor_plan.json from description and reference images.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "run_dir": {"type": "string"},
                "force": {"type": "boolean"},
            },
        },
    },
    {
        "name": "promo_generate_anchor_images",
        "description": "Generate anchor PNG images for the run.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "run_dir": {"type": "string"},
                "force": {"type": "boolean"},
            },
        },
    },
    {
        "name": "promo_generate_bridge_videos",
        "description": "Generate bridge video segments between anchor images.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "run_dir": {"type": "string"},
                "force": {"type": "boolean"},
            },
        },
    },
    {
        "name": "promo_concat_visual",
        "description": "Concatenate visual segments into final.mp4.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "run_dir": {"type": "string"},
                "force": {"type": "boolean"},
            },
        },
    },
    {
        "name": "promo_generate_audio_plan",
        "description": "Generate audio plan, narration script, and music prompt files.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "run_dir": {"type": "string"},
                "force": {"type": "boolean"},
            },
        },
    },
    {
        "name": "promo_generate_audio_assets",
        "description": "Generate soundtrack and narration audio assets.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "run_dir": {"type": "string"},
                "music_only": {"type": "boolean"},
                "narration_only": {"type": "boolean"},
                "voice": {"type": "string"},
                "force": {"type": "boolean"},
            },
        },
    },
    {
        "name": "promo_merge_audio",
        "description": "Merge soundtrack and optional narration into final_with_audio.mp4.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "run_dir": {"type": "string"},
                "music_file": {"type": "string"},
                "narration_file": {"type": "string"},
                "music_volume": {"type": "number"},
                "narration_volume": {"type": "number"},
                "music_fade_out_seconds": {"type": "number"},
                "force": {"type": "boolean"},
            },
        },
    },
    {
        "name": "promo_retry_failed_step",
        "description": "Retry the last failed workflow step for a run.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "run_id": {"type": "string"},
                "run_dir": {"type": "string"},
                "step": {"type": "string"},
                "music_only": {"type": "boolean"},
                "narration_only": {"type": "boolean"},
                "voice": {"type": "string"},
                "music_file": {"type": "string"},
                "narration_file": {"type": "string"},
                "music_volume": {"type": "number"},
                "narration_volume": {"type": "number"},
                "music_fade_out_seconds": {"type": "number"},
                "force": {"type": "boolean"},
            },
        },
    },
]


TOOL_HANDLERS = {
    "promo_create_run": handle_create_run,
    "promo_get_run": handle_get_run,
    "promo_list_runs": handle_list_runs,
    "promo_generate_anchor_plan": handle_generate_anchor_plan,
    "promo_generate_anchor_images": handle_generate_anchor_images,
    "promo_generate_bridge_videos": handle_generate_bridge_videos,
    "promo_concat_visual": handle_concat_visual,
    "promo_generate_audio_plan": handle_generate_audio_plan,
    "promo_generate_audio_assets": handle_generate_audio_assets,
    "promo_merge_audio": handle_merge_audio,
    "promo_retry_failed_step": handle_retry_failed_step,
}


def list_tools():
    return TOOL_DEFINITIONS


def call_tool(name, arguments=None):
    arguments = arguments or {}
    handler = TOOL_HANDLERS.get(name)
    if handler is None:
        raise ValueError(f"Unknown tool: {name}")
    return handler(arguments)


def render_tools_json():
    return json.dumps({"tools": list_tools()}, indent=2)
