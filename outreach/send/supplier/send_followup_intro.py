import argparse
from outreach.send.shared.send_engine import run_engine

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--advance-state", action="store_true")
    args = ap.parse_args()

    run_engine(
        campaign_name="supplier_intro",
        limit=args.limit,
        advance_state=args.advance_state
    )

if __name__ == "__main__":
    main()
