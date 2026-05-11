# 📊 Phân Tích Thị Trường Tuyển Dụng Việt Nam 2025–2026

> Crawl · ETL · Data Warehouse · Dashboard · Insight

Dự án thu thập **54.532 nhu cầu tuyển dụng** từ **10.107 doanh nghiệp** trên **10 nền tảng** trong khoảng thời gian Q2/2025 – Q1/2026, xây dựng pipeline ETL hoàn chỉnh từ dữ liệu thô đến kho dữ liệu sạch, và phân tích cấu trúc thị trường lao động kỹ thuật số Việt Nam.

---

## 📌 Số liệu tổng quan

| Chỉ số | Giá trị |
|---|---|
| Tin đăng tuyển (unique) | 29.909 |
| Nhu cầu tuyển (tổng số lượng) | 54.532 |
| Doanh nghiệp | 10.107 |
| Lương trung bình | 23,84 triệu VND/tháng |
| Nền tảng thu thập | 10 |
| Thời gian | 4/2025 – 2/2026 |

---

## 🗂️ Mục lục

1. [Thu thập dữ liệu](#1-thu-thập-dữ-liệu)
2. [Xử lý dữ liệu](#2-xử-lý-dữ-liệu)
3. [Pipeline ETL](#3-pipeline-etl)
4. [Kho dữ liệu](#4-kho-dữ-liệu)
5. [Kết quả & Dashboard](#5-kết-quả--dashboard)
6. [Insight chính](#6-insight-chính)
7. [Kết luận](#7-kết-luận)
8. [Cấu trúc project](#8-cấu-trúc-project)

---

## 1. Thu thập dữ liệu

### 1.1 Nguồn dữ liệu

Dữ liệu được thu thập từ 10 nền tảng tuyển dụng tại Việt Nam bao gồm cả trang tổng hợp lẫn chuyên ngành IT:

- TopCV, VietnamWorks, CareerLink, TimViecNhanh, JobStreet
- ITviec, LinkedIn (VN), và một số nền tảng khác

### 1.2 Phương pháp thu thập

**Scrapy** — crawler song song cho các trang có cấu trúc HTML ổn định.

**Selenium** — xử lý các trang render JavaScript (LinkedIn, ITviec) yêu cầu trình duyệt thực.

Mỗi tin tuyển thu thập 24 trường thô bao gồm: `job_title`, `company_title`, `location`, `compensation`, `experience`, `level`, `job_description`, `job_requirement`, `job_type`, `work_mode`, `education_level`, `company_size`, `company_industry`, `job_category`, `number_recruit`, `job_posted_at`, `job_deadline`, và các metadata khác.

### 1.3 Lịch chạy

Pipeline chạy tự động hàng ngày qua file `.bat` theo 2 chế độ:

- `MODE=daily` — crawl và ETL dữ liệu trong ngày
- `MODE=full` — reindex toàn bộ Typesense + ETL toàn bộ bảng

---

## 2. Xử lý dữ liệu

### 2.1 Dữ liệu có cấu trúc

Các trường như `website`, `scraped_at`, `job_type`, `work_mode` tương đối sạch, chỉ cần chuẩn hoá giá trị theo lookup table.

### 2.2 Dữ liệu phi cấu trúc

Đây là phần phức tạp nhất của dự án. Hầu hết trường quan trọng đều ở dạng text tự do:

**Lương (`compensation`)** — cực kỳ đa dạng format:
```
"15 - 20 triệu"  |  "Up to $2000"  |  "Thỏa thuận"
"1.500 USD/tháng"  |  "từ 500$/tháng trở lên"
```
Logic xử lý: detect currency → normalize separator → extract số → apply suffix (triệu/k/M) → convert về VND/tháng → sanity check theo ngưỡng từng currency.

**Kinh nghiệm (`experience`)** — nhiều cách diễn đạt:
```
"2 năm"  |  "Trên 3 năm"  |  "Không yêu cầu kinh nghiệm"
"1-3 years of experience"  |  "Fresher"
```
Logic xử lý: regex extract số năm/tháng → phân biệt min/max/exact → fallback tìm trong `job_description` nếu trường rỗng.

**Địa điểm (`location`)** — một tin có thể có nhiều tỉnh:
```
"Hà Nội, TP. HCM, Đà Nẵng"  |  "Hồ Chí Minh (Quận 1)"
"KCN Bắc Ninh"  |  "Toàn quốc"
```
Logic xử lý: split theo dấu phân cách → map về tỉnh canonical → gán region → nếu nhiều tỉnh thì **fan-out thành nhiều row** (1 tin = nhiều dòng fact).

**Tên công ty (`company_title`)** — nhiễu loạn nhất:
```
"Cty TNHH ABC"  |  "CTCP XYZ Việt Nam"  |  "Công ty Cổ Phần ABC"
```
Logic xử lý: normalize prefix pháp lý → build canonical key → fuzzy match với Typesense (bidirectional score: ratio + partial_ratio + token_sort) → threshold 85 thì cập nhật tên chuẩn, 65–84 giữ để review, dưới 65 coi là tên mới.

**Skills, Education, Industry** — extract từ toàn bộ JD bằng keyword matching với lookup table lớn.

---

## 3. Pipeline ETL

```
pipeline.bat
│
├── [Startup]  Docker Desktop → Typesense container → health check
│
├── [Phase 0]  Typesense reindex (chỉ MODE=full)
│              └── typesense.py: load company.csv → collection 'companies'
│
├── [Phase 1]  Selenium scrapers (song song)
│              ├── linkedin_selenium.py
│              └── itviec_selenium.py
│
├── [Phase 2]  Scrapy (tất cả spider song song)
│              └── run_spiders.py → bảng `jobs` (MySQL)
│
├── [Phase 3]  ETL  (transform.py)
│              ├── Load từ bảng `jobs`
│              ├── Transform: parse 15+ trường phi cấu trúc
│              ├── Company dedup (fuzzy union-find, threshold 88)
│              ├── Save → `fact_jobs_etl`
│              └── Company match với Typesense
│
└── [Phase 4]  Load DW
               └── CALL sp_ETL_Load_DW() → Star Schema
```

### ETL chi tiết (`transform.py`)

| Bước | Mô tả |
|---|---|
| Load | Đọc từ bảng `jobs` theo mode (today / date / all) |
| Transform | Parse 15+ trường, fan-out theo location |
| Company Dedup | Union-Find trên canonical key, gộp tên trùng |
| Save Fact | INSERT IGNORE + DELETE-reinsert cho updated rows |
| Company Match | Typesense 2-pass (prefix → infix), bidirectional fuzzy score |
| Save Errors | Ghi chi tiết lỗi từng trường vào `fact_etl_error` |

### Output bảng staging

- `fact_jobs_etl` — toàn bộ cột gốc + cột đã xử lý (60+ cột)
- `fact_etl_log` — log mỗi lần chạy (input/output/new/updated/error rows)
- `fact_etl_error` — chi tiết lỗi từng trường để debug

---

## 4. Kho dữ liệu

### 4.1 Kiến trúc Star Schema

```
                    Dim_Nguon
                    Dim_CapBac
                    Dim_HinhThuc
Dim_DiaDiem ──────► Fact_JobPostings ◄────── Dim_CongTy
                         │
                    Dim_Nganh          Dim_DanhMucCongViec
                         │
                    bridge_jobrequire
                         │
                    Dim_Require
                    (hard_skill, soft_skill,
                     certification, language, major)
```

### 4.2 Bảng chính

**`Fact_JobPostings`** — bảng fact trung tâm:

| Nhóm cột | Nội dung |
|---|---|
| Keys | `etl_id`, `posting_id`, FK đến tất cả Dim |
| Thời gian | `ngay_dang`, `ngay_deadline`, `ngay_crawl` |
| Lương | `salary_min`, `salary_max`, `salary_avg`, `salary_currency`, `conversion_rate`, `is_negotiable` |
| Kinh nghiệm | `exp_min_yr`, `exp_max_yr`, `is_exp_required` |
| Phân loại | `is_it`, `job_title_clean`, `job_title_detect` |

**Date hierarchy** được tạo trực tiếp trong Power BI từ cột `ngay_dang` (kiểu DATE) — không cần `Dim_ThoiGian`.

### 4.3 Logic load DW (`sp_ETL_Load_DW`)

Stored procedure chạy theo 3 mode: `today`, `date`, `all`.

Thứ tự load đảm bảo referential integrity: Dim_Nguon → Dim_CapBac → Dim_HinhThuc → Dim_Nganh → Dim_DiaDiem → Dim_CongTy → Dim_DanhMucCongViec → **Fact_JobPostings** → Dim_Require → bridge_jobrequire.

`Dim_CongTy` dùng `ON DUPLICATE KEY UPDATE` để cập nhật metadata khi có thay đổi. `Fact_JobPostings` dùng `ON DUPLICATE KEY UPDATE` trên `etl_id` để upsert.

Skills (hard/soft), certifications, languages, major được parse từ text bằng `JSON_TABLE` + `REGEXP_REPLACE` trực tiếp trong SQL khi load vào `Dim_Require` và `bridge_jobrequire`.

---

## 5. Kết quả & Dashboard

### 5.1 Kho dữ liệu sạch

Sau toàn bộ pipeline:

- **29.909 tin tuyển** unique được làm sạch và chuẩn hoá
- **10.107 doanh nghiệp** sau dedup (giảm từ ~14.900 tên thô)
- **9 danh mục IT** được phân loại tự động
- Lương có giá trị thực (không phải "thỏa thuận") cho ~40% tin tuyển
- Tỷ lệ lỗi parse < 0,02% (9 lỗi / 48.211 rows)

### 5.2 Dashboard (Power BI)

**Overview Dashboard** — bức tranh toàn cảnh thị trường:
- Phân bố tin tuyển theo tỉnh/vùng/thời gian
- Top ngành, top vị trí, xu hướng tuyển dụng theo tháng
- Đỉnh tuyển dụng tháng 11/2025: ~4.846 nhu cầu/ngày

**Skill Dashboard** — bản đồ kỹ năng:
- Top 15 kỹ năng hard/soft được yêu cầu
- Phân tích theo danh mục IT
- Ngôn ngữ lập trình và tech stack phổ biến

**Role Dashboard** — phân tích theo vai trò:
- Lương theo vai trò × kinh nghiệm
- Career trajectory của 4 Data roles
- Phân tích Data Analyst / Data Engineer / Data Scientist / BI Analyst

---

## 6. Insight chính

### Thị trường tổng quan

**Tập trung địa lý cực độ** — Hà Nội (49%) và TP.HCM (34%) chiếm 83% tổng nhu cầu. Hà Nội dẫn đầu bất ngờ do tập trung outsourcing IT, FDI sản xuất điện tử, và chuyển đổi số doanh nghiệp nhà nước.

**Đa dạng ngành** — Công nghệ (10.691 tin) chỉ nhỉnh hơn Tài chính (9.726) và Sản xuất (9.404), thị trường không "IT-only" như nhiều người nghĩ.

**IT Sales > Backend Developer** — 12.067 vs 10.459 vị trí. Turnover Sales cao hoặc áp lực tăng trưởng doanh thu đang lớn hơn áp lực xây sản phẩm.

### Kỹ năng

**Soft skill áp đảo hard skill** — Giao tiếp (15K) và Tiếng Anh (11K) vượt xa SQL (5,5K) và Python (3K). Thị trường cần người kết hợp technical + communication hơn chuyên gia kỹ thuật thuần túy.

**SQL là ngôn ngữ chung** — 5,5K vị trí yêu cầu SQL, vượt Python (3K), JavaScript (2K), Java (1K). Từ BA đến DS đều cần.

**MLOps đang nổi lên** — CI/CD và Docker xuất hiện trong Data Analytics JD, tín hiệu DE cấp cao cần hiểu ML pipeline và automation.

### Ngành Data (phân tích chuyên sâu)

| Vai trò | Số vị trí | Entry | Senior (5–7 năm) |
|---|---|---|---|
| Data Analyst | 1.389 | 17,5M | 40M (75M với 8+ năm) |
| Data Engineer | 481 | 22,5M | 33,75M |
| Data Scientist | 268 | 15M | 38,25M |
| BI Analyst | ~gộp vào DA | 15,97M | 28M |

**Nghịch lý DA vs DS** — Data Analyst 8+ năm (75M) cao hơn Data Scientist (38,25M). Nguyên nhân khả năng cao: "DA senior" tại VN đang bao gồm Analytics Manager, Head of BI — các leadership position được JD gọi là DA do thiếu chuẩn hóa nghề nghiệp.

**DE ceiling thấp hơn DA** — DE entry cao nhất (22,5M) nhưng ceiling 33,75M thấp hơn DA (40M+). DE tránh bị commodity hóa cần đầu tư vào MLOps, data mesh, feature store.

**DS: thị trường hẹp** — 268 vị trí (0,49% tổng). Mức lương DS thực sự tốt chỉ ở fintech lớn, AI startup, e-commerce tier 1. Employer choice quan trọng hơn kỹ năng kỹ thuật ở segment này.

---

## 7. Kết luận

Dự án xây dựng thành công pipeline end-to-end từ web scraping đến insight, giải quyết các thách thức kỹ thuật thực tế:

**Về kỹ thuật:**
- Xử lý dữ liệu tuyển dụng phi cấu trúc ở quy mô lớn (48K+ rows) với tỷ lệ lỗi cực thấp
- Company matching bằng bidirectional fuzzy score + Typesense search thay vì exact match đơn giản
- Fan-out location cho phép phân tích địa lý chính xác mà không mất tin có nhiều địa điểm
- Star schema linh hoạt, hỗ trợ slicing theo nhiều chiều trong Power BI

**Về thị trường:**
- Thị trường tuyển dụng IT Việt Nam đang trong giai đoạn tăng trưởng nhưng tập trung địa lý cao
- Data roles còn nhỏ về số lượng (< 4% tổng) nhưng có đường cong lương hấp dẫn
- Kỹ năng mềm và khả năng kết nối technical-business đang được định giá cao hơn kỹ thuật thuần túy
- Chuẩn hóa JD và career ladder trong ngành Data tại VN vẫn còn nhiều khoảng trống

**Giới hạn:** Dữ liệu lương chỉ có giá trị thực cho ~40% tin tuyển, phần còn lại là "thỏa thuận". Các phân tích lương cần được đọc với lưu ý này.

---

## 8. Cấu trúc project

```
D:\ITTA\jobscrapers\
│
├── pipeline.bat                 # Orchestrator chạy toàn bộ pipeline
├── run_spiders.py               # Chạy tất cả Scrapy spider song song
├── company.csv                  # Danh sách công ty chuẩn (dùng cho Typesense)
│
└── jobscrapers\
    ├── typesense.py             # Load company.csv → Typesense collection
    ├── transform.py             # ETL chính: parse, dedup, match, save
    ├── lookups.py               # Lookup tables: province, level, skill, ...
    │
    └── spiders\
        ├── linkedin_selenium.py
        ├── itviec_selenium.py
        └── [các spider Scrapy]
```

### Stack kỹ thuật

| Layer | Công nghệ |
|---|---|
| Crawling | Scrapy, Selenium |
| ETL | Python (pandas, rapidfuzz, typesense) |
| Search / Matching | Typesense (Docker) |
| Database | MySQL 9.4 |
| Data Warehouse | MySQL Star Schema + Stored Procedure |
| Visualization | Power BI |
| Orchestration | Windows Task Scheduler + .bat |

---

*Dữ liệu: 29.909 tin đăng tuyển · 10.107 doanh nghiệp · 10 nền tảng · 4/2025 – 2/2026*
