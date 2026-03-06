---
name: product-promo-workflow
description: Operate the product promo workflow through MCP tools. Use this when creating, resuming, inspecting, retrying, or repairing multi-step product promo runs that generate anchor plans, anchor images, bridge videos, audio plans, audio assets, and final merged videos.
---

# Product Promo Workflow

Use this skill when the user wants to run or recover the promo pipeline through MCP tools instead of manually invoking step scripts.

## Core Policy

- Prefer MCP tools over shelling into `product_promo/step_*.py`.
- Treat each run directory under `product_promo/runs/` as the source of truth.
- Always inspect an existing run with `promo_get_run` before choosing the next action.
- Prefer resuming a run over recreating it.
- Do not rerun expensive completed steps unless the user explicitly asks or the artifact is missing or invalid.
- For retries, target the failed step only.

## Default Workflow

For a new run, use this order:
1. `promo_create_run`
2. `promo_generate_anchor_plan`
3. `promo_generate_anchor_images`
4. `promo_generate_bridge_videos`
5. `promo_concat_visual`
6. `promo_generate_audio_plan`
7. `promo_generate_audio_assets`
8. `promo_merge_audio`

For an existing run:
1. Call `promo_get_run`
2. Read `state`, `last_error`, and `next_recommended_tool`
3. Execute only the next missing or failed step

## Run States

Expected state progression:
- `CREATED`
- `ANCHOR_PLAN_READY`
- `ANCHOR_IMAGES_PARTIAL` or `ANCHOR_IMAGES_READY`
- `SEGMENTS_PARTIAL` or `SEGMENTS_READY`
- `VISUAL_FINAL_READY`
- `AUDIO_PLAN_READY`
- `AUDIO_ASSETS_PARTIAL` or `AUDIO_ASSETS_READY`
- `FINAL_READY`

If the run is `FAILED`, inspect:
- `failed_step`
- `error_type`
- `retryable`

Then either retry the failed step or repair its inputs first.

## Recovery Rules

### Quota Or Overload

If `error_type` is `quota_exhausted` or `service_overloaded`:
- retry the same step with backoff
- do not regenerate earlier successful artifacts

### Music Generation Blocked

If `error_type` is `music_generation_blocked`:
- regenerate the audio plan with a safer music prompt
- keep the music prompt abstract, instrumental, and non-referential
- avoid artist, song, or ad-music imitation
- rerun `promo_generate_audio_assets` only

### Missing Inputs

If `error_type` is `missing_input_artifact`:
- regenerate the missing prerequisite artifact
- then continue forward from that point

### Merge Failures

If `error_type` is `ffmpeg_failure`:
- do not regenerate visual or audio assets by default
- retry only `promo_merge_audio` after validating input file paths

## Audio Policy

- Prefer one global soundtrack for the final video.
- Do not rely on per-segment Veo audio for final delivery.
- Use `promo_generate_audio_plan` to create narration and music guidance.
- Use `promo_generate_audio_assets` for Lyria music and Google TTS narration.
- Use `promo_merge_audio` to build the final deliverable.
- Keep background music faded out near the end so the cut is not abrupt.

## Output Expectations

When using this skill, report:
- current run state
- action taken
- whether artifacts were generated or reused
- next recommended step if the workflow is not complete

Keep responses concise and operational.
