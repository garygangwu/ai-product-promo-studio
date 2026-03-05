import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from google_product_promo.workflow import generate_alt_8s_video


def parse_args():
    parser = argparse.ArgumentParser(description="Generate one 8-second alt video from selected top-3 reference images.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    result = generate_alt_8s_video(args.run_dir, force=args.force)
    print(f"{'Skipping existing' if result['status']=='skipped' else 'Wrote'} {result['path']}")
    if result["status"] != "skipped":
        print(f"Duration: {result['duration_seconds']:.2f}s")


if __name__ == "__main__":
    main()
