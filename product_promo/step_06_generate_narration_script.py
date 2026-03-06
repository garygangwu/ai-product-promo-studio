import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from product_promo.workflow import generate_audio_plan


def parse_args():
    parser = argparse.ArgumentParser(description="Generate an audio plan and narration script for the final promo video.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    result = generate_audio_plan(args.run_dir, force=args.force)
    prefix = "Skipping existing" if result["status"] == "skipped" else "Wrote"
    print(f"{prefix} {result['path']}")


if __name__ == "__main__":
    main()
