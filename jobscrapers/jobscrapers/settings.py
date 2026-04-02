# Scrapy settings for jobscrapers project

BOT_NAME = "jobscrapers"
SPIDER_MODULES = ["jobscrapers.spiders"]
NEWSPIDER_MODULE = "jobscrapers.spiders"

ADDONS = {}

# =========================================================
#  ScrapeOps — fake user agent
# =========================================================
SCRAPEOPS_API_KEY                    = '53cd5666-1098-4bef-b063-461899aa5e2c'
SCRAPEOPS_FAKE_USER_AGENT_ENDPOINT   = True
SCRAPEOPS_NUM_RESULTS                = 50

# =========================================================
#  Crawl mode
#  Mặc định: "daily" — chỉ lấy job mới trong 24h
#  Ghi đè:   scrapy crawl <spider> -s CRAWL_MODE=full
# =========================================================
CRAWL_MODE = "daily"   # "full" | "daily"

# =========================================================
#  Giới hạn tự động dừng
#
#  daily : CLOSESPIDER_ITEMCOUNT = 0 (không giới hạn số item,
#          dừng theo job_posted_at trong spider)
#  full  : ghi đè bằng -s CLOSESPIDER_ITEMCOUNT=0 khi chạy
#
#  => Để 0 ở đây, mỗi spider tự kiểm soát dừng theo mode
# =========================================================
CLOSESPIDER_ITEMCOUNT  = 0   # 0 = không giới hạn
CLOSESPIDER_PAGECOUNT  = 0
CLOSESPIDER_TIMEOUT    = 0

# =========================================================
#  Tốc độ & concurrency
# =========================================================
ROBOTSTXT_OBEY               = False
CONCURRENT_REQUESTS          = 1
CONCURRENT_REQUESTS_PER_DOMAIN = 1
DOWNLOAD_DELAY               = 2
RANDOMIZE_DOWNLOAD_DELAY     = True   # delay thực tế = 1~3s, tránh bị ban

# =========================================================
#  Headers mặc định
# =========================================================
DEFAULT_REQUEST_HEADERS = {
    "Accept"                   : "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language"          : "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding"          : "gzip, deflate",
    "Connection"               : "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

# =========================================================
#  Middlewares
# =========================================================
DOWNLOADER_MIDDLEWARES = {
    "scrapy.downloadermiddlewares.useragent.UserAgentMiddleware": None,
    "jobscrapers.middlewares.RotateUserAgentMiddleware"         : 400,
    "jobscrapers.middlewares.JobscrapersDownloaderMiddleware"   : 543,
}

# =========================================================
#  Playwright
# =========================================================
DOWNLOAD_HANDLERS = {
    "http" : "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}
TWISTED_REACTOR          = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
PLAYWRIGHT_BROWSER_TYPE  = "chromium"
PLAYWRIGHT_LAUNCH_OPTIONS = {"headless": True}

# =========================================================
#  Pipelines
# =========================================================
ITEM_PIPELINES = {
    "jobscrapers.pipelines.CleaningPipeline"   : 300,
    "jobscrapers.pipelines.SaveToMySQLPipeline": 400,
}

# =========================================================
#  Encoding
# =========================================================
FEED_EXPORT_ENCODING = "utf-8"