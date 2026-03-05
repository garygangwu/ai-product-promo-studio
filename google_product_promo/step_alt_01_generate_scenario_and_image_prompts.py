import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from google_product_promo.workflow import generate_alt_scenario_and_image_prompts


def parse_args():
    parser = argparse.ArgumentParser(description="Generate alt scenario brief and six image prompts.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    result = generate_alt_scenario_and_image_prompts(args.run_dir, force=args.force)
    if result["status"] == "skipped":
        print(f"Skipping existing {result['scenario_path']}")
        print(f"Skipping existing {result['prompts_path']}")
    else:
        print(f"Wrote {result['scenario_path']}")
        print(f"Wrote {result['prompts_path']}")


if __name__ == "__main__":
    main()
