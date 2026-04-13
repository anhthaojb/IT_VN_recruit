# run_scrapy.py
import sys
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "daily"

    settings = get_project_settings()
    settings.set("CRAWL_MODE", mode)

    process = CrawlerProcess(settings)

    SPIDERS = [
        "topcv",
        "careerlink",
        "careerviet",
        "joboko",
         "jobsgo",
        "timviec365",
        "vieclam24h",
        "vietnamwork",
    ]

    for spider_name in SPIDERS:
        process.crawl(spider_name)

    process.start()  # block cho đến khi tất cả xong


if __name__ == "__main__":
    main()