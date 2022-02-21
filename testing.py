from src.gateway.stats import Stats
from pathlib import Path


def main():
    stats = Stats(Path("./summaries.db"))
    last_hash, _ = stats.fetch_last_hash()
    print(stats.fetch_build_success_pct(last_hash))


if __name__ == "__main__":
    main()
