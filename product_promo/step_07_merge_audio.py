import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from product_promo.workflow import merge_audio


def parse_args():
    parser = argparse.ArgumentParser(description="Merge one global soundtrack and optional narration into the final promo video.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--music-file", default=None, help="Optional audio file to use as the global soundtrack.")
    parser.add_argument("--narration-file", default=None, help="Optional narration audio file to mix over the soundtrack.")
    parser.add_argument("--music-volume", type=float, default=0.35)
    parser.add_argument("--narration-volume", type=float, default=1.0)
    parser.add_argument("--music-fade-out-seconds", type=float, default=2.5)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    result = merge_audio(
        args.run_dir,
        music_file=args.music_file,
        narration_file=args.narration_file,
        music_volume=args.music_volume,
        narration_volume=args.narration_volume,
        music_fade_out_seconds=args.music_fade_out_seconds,
        force=args.force,
    )
    print(f"{'Skipping existing' if result['status']=='skipped' else 'Wrote'} {result['path']}")


if __name__ == "__main__":
    main()
