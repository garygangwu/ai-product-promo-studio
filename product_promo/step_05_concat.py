import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from product_promo.workflow import concat_visual


def parse_args():
    parser = argparse.ArgumentParser(description="Concatenate generated bridge videos.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    result = concat_visual(args.run_dir, force=args.force)
    print(f"{'Skipping existing' if result['status']=='skipped' else 'Wrote'} {result['path']}")


if __name__ == "__main__":
    main()
