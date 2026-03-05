import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from google_product_promo.workflow import generate_alt_video_prompt


def parse_args():
    parser = argparse.ArgumentParser(description="Generate alt 8-second Veo prompt from selected top-3 images.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    result = generate_alt_video_prompt(args.run_dir, force=args.force)
    if result["status"] == "skipped":
        print(f"Skipping existing {result['prompt_path']}")
        print(f"Skipping existing {result['meta_path']}")
        print(f"Skipping existing {result['review_path']}")
    else:
        print(f"Wrote {result['prompt_path']}")
        print(f"Wrote {result['meta_path']}")
        print(f"Wrote {result['review_path']}")


if __name__ == "__main__":
    main()
