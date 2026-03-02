import sys
import argparse
import asyncio
from crawlers.X_crawler import main as run_x_crawler
from crawlers.otofun_crawler import OtofunCrawlerV2

def main():
    parser = argparse.ArgumentParser(description="Automated Data Crawler CLI")
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # X Crawler
    x_parser = subparsers.add_parser("x", help="Run X (Twitter) crawler")
    
    # Otofun Crawler
    otofun_parser = subparsers.add_parser("otofun", help="Run Otofun crawler")
    otofun_parser.add_argument("--url", required=True, help="Search URL for Otofun")
    otofun_parser.add_argument("--output", default="otofun_results.csv", help="Output CSV file")

    args = parser.parse_args()

    if args.command == "x":
        asyncio.run(run_x_crawler())
    elif args.command == "otofun":
        crawler = OtofunCrawlerV2(auto_save_file=args.output)
        crawler.crawl_search_results(args.url)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
