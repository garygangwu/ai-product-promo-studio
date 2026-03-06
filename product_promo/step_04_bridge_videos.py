import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from product_promo.workflow import generate_bridge_videos


def parse_args():
    parser = argparse.ArgumentParser(description="Generate bridge videos for a run.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    result = generate_bridge_videos(args.run_dir, force=args.force)
    for item in result["results"]:
        print(f"{'Skipping existing' if item['status']=='skipped' else 'Wrote'} {item['path']}")


if __name__ == "__main__":
    main()
