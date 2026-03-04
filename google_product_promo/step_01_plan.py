import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from google_product_promo.common import (
    DEFAULT_ASPECT_RATIO,
    DEFAULT_AUDIO_THEME,
    DEFAULT_LOCATION,
    DEFAULT_PROJECT_ID,
)
from google_product_promo.workflow import create_run


def parse_args():
    parser = argparse.ArgumentParser(description="Initialize a resumable Google product promo run.")
    parser.add_argument("--run-id", default=None, help="Optional run ID. Default: generated timestamp.")
    parser.add_argument("--project-id", default=DEFAULT_PROJECT_ID)
    parser.add_argument("--location", default=DEFAULT_LOCATION)
    parser.add_argument("--product-images", nargs=3, required=True)
    parser.add_argument("--logo-image", required=True)
    parser.add_argument("--description", required=True)
    parser.add_argument("--aspect-ratio", default=DEFAULT_ASPECT_RATIO)
    parser.add_argument("--audio-theme", default=DEFAULT_AUDIO_THEME)
    return parser.parse_args()


def main():
    args = parse_args()
    result = create_run(
        run_id=args.run_id,
        project_id=args.project_id,
        location=args.location,
        product_images=args.product_images,
        logo_image=args.logo_image,
        description=args.description,
        aspect_ratio=args.aspect_ratio,
        audio_theme=args.audio_theme,
    )
    print(f"Run dir: {result['run_dir']}")
    print(f"Prompt model: {result['config']['prompt_model']}")
    print(f"Image model: {result['config']['image_model']}")
    print(f"Video model: {result['config']['video_model']}")


if __name__ == "__main__":
    main()
