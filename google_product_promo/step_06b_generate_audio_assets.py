import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from google_product_promo.workflow import generate_audio_assets


def parse_args():
    parser = argparse.ArgumentParser(description="Generate soundtrack and narration audio assets from audio_plan.json.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--music-only", action="store_true")
    parser.add_argument("--narration-only", action="store_true")
    parser.add_argument("--voice", default="Kore")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    result = generate_audio_assets(
        args.run_dir,
        music_only=args.music_only,
        narration_only=args.narration_only,
        voice=args.voice,
        force=args.force,
    )
    print(f"Wrote {result['path']}")


if __name__ == "__main__":
    main()
