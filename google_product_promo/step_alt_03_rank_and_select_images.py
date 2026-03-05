import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from google_product_promo.workflow import rank_and_select_alt_images


def parse_args():
    parser = argparse.ArgumentParser(description="Rank six alt candidate images and select top three.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    result = rank_and_select_alt_images(args.run_dir, force=args.force)
    print(f"{'Skipping existing' if result['status']=='skipped' else 'Wrote'} {result['rankings_path']}")
    print(f"{'Skipping existing' if result['status']=='skipped' else 'Wrote'} {result['selected_path']}")
    if result["status"] != "skipped":
        for item in result["selected"]:
            print(f"Selected: {item['path']} (score={item['total']})")


if __name__ == "__main__":
    main()
