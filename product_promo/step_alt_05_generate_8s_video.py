import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from product_promo.workflow import generate_alt_8s_video


def parse_args():
    parser = argparse.ArgumentParser(description="Generate multiple 8-second alt video candidates, QA-rank, and output the best clip.")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--candidates", type=int, default=3, help="Number of video candidates to generate before QA selection.")
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()
    result = generate_alt_8s_video(args.run_dir, force=args.force, candidate_count=args.candidates)
    print(f"{'Skipping existing' if result['status']=='skipped' else 'Wrote'} {result['path']}")
    if result["status"] != "skipped":
        print(f"Duration: {result['duration_seconds']:.2f}s")
        print(f"Selected candidate: {result['selected_candidate']}")
        print(f"QA report: {result['qa_path']}")


if __name__ == "__main__":
    main()
