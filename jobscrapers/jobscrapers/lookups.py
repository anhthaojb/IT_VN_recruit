# lookups.py
# ==============================================================================
# BẢNG TRA CỨU TĨNH — khởi tạo 1 lần lúc import
# ==============================================================================

import re

# ==============================================================================
# 1. GEO
# ==============================================================================

PROVINCE_CANONICAL: dict[str, str] = {

    # ── TP. Hồ Chí Minh ──────────────────────────────────────────────────────
    "hồ chí minh":              "TP. Hồ Chí Minh",
    "tp. hồ chí minh":          "TP. Hồ Chí Minh",
    "tp hồ chí minh":           "TP. Hồ Chí Minh",
    "hcm":                      "TP. Hồ Chí Minh",
    "tphcm":                    "TP. Hồ Chí Minh",
    "hcmc":                     "TP. Hồ Chí Minh",
    "sg":                       "TP. Hồ Chí Minh",
    "sài gòn":                  "TP. Hồ Chí Minh",
    "ho chi minh city":         "TP. Hồ Chí Minh",
    "ho chi minh":              "TP. Hồ Chí Minh",
    "bình dương":               "TP. Hồ Chí Minh",
    "binh duong":               "TP. Hồ Chí Minh",
    "bà rịa":                   "TP. Hồ Chí Minh",
    "vũng tàu":                 "TP. Hồ Chí Minh",
    "vung tau":                 "TP. Hồ Chí Minh",
    "bà rịa - vũng tàu":       "TP. Hồ Chí Minh",

    # ── TP. Hà Nội ───────────────────────────────────────────────────────────
    "hà nội":                   "TP. Hà Nội",
    "hn":                       "TP. Hà Nội",
    "tp. hà nội":               "TP. Hà Nội",
    "tp hà nội":                "TP. Hà Nội",
    "hanoi":                    "TP. Hà Nội",
    "ha noi":                   "TP. Hà Nội",

    # ── TP. Hải Phòng ────────────────────────────────────────────────────────
    "hải phòng":                "TP. Hải Phòng",
    "tp. hải phòng":            "TP. Hải Phòng",
    "tp hải phòng":             "TP. Hải Phòng",
    "hai phong":                "TP. Hải Phòng",
    "hải dương":                "TP. Hải Phòng",
    "hai duong":                "TP. Hải Phòng",

    # ── TP. Đà Nẵng ──────────────────────────────────────────────────────────
    "đà nẵng":                  "TP. Đà Nẵng",
    "đà nang":                  "TP. Đà Nẵng",
    "tp. đà nẵng":              "TP. Đà Nẵng",
    "tp đà nẵng":               "TP. Đà Nẵng",
    "da nang":                  "TP. Đà Nẵng",
    "da nang city":             "TP. Đà Nẵng",
    "quảng nam":                "TP. Đà Nẵng",

    # ── TP. Cần Thơ ──────────────────────────────────────────────────────────
    "cần thơ":                  "TP. Cần Thơ",
    "tp. cần thơ":              "TP. Cần Thơ",
    "tp cần thơ":               "TP. Cần Thơ",
    "can tho":                  "TP. Cần Thơ",
    "sóc trăng":                "TP. Cần Thơ",
    "hậu giang":                "TP. Cần Thơ",

    # ── TP. Huế ──────────────────────────────────────────────────────────────
    "huế":                      "TP. Huế",
    "tp. huế":                  "TP. Huế",
    "tp huế":                   "TP. Huế",
    "hue":                      "TP. Huế",
    "thừa thiên huế":           "TP. Huế",

    # ── Miền Bắc ─────────────────────────────────────────────────────────────
    "bắc ninh":                 "Bắc Ninh",
    "bac ninh":                 "Bắc Ninh",
    "bắc giang":                "Bắc Ninh",

    "hưng yên":                 "Hưng Yên",
    "hung yen":                 "Hưng Yên",
    "thái bình":                "Hưng Yên",

    "lào cai":                  "Lào Cai",
    "lao cai":                  "Lào Cai",
    "yên bái":                  "Lào Cai",

    "ninh bình":                "Ninh Bình",
    "hà nam":                   "Ninh Bình",
    "nam định":                 "Ninh Bình",

    "phú thọ":                  "Phú Thọ",
    "hòa bình":                 "Phú Thọ",
    "vĩnh phúc":                "Phú Thọ",
    "vinh phuc":                "Phú Thọ",

    "thái nguyên":              "Thái Nguyên",
    "thai nguyen":              "Thái Nguyên",
    "bắc kạn":                  "Thái Nguyên",
    "bắc cạn":                  "Thái Nguyên",

    "tuyên quang":              "Tuyên Quang",
    "hà giang":                 "Tuyên Quang",

    "quảng ninh":               "Quảng Ninh",
    "quang ninh":               "Quảng Ninh",

    "cao bằng":                 "Cao Bằng",
    "lạng sơn":                 "Lạng Sơn",
    "sơn la":                   "Sơn La",
    "điện biên":                "Điện Biên",
    "lai châu":                 "Lai Châu",

    # ── Miền Trung ───────────────────────────────────────────────────────────
    "thanh hóa":                "Thanh Hóa",
    "thanh hoa":                "Thanh Hóa",
    "nghệ an":                  "Nghệ An",
    "nghe an":                  "Nghệ An",
    "hà tĩnh":                  "Hà Tĩnh",
    "ha tinh":                  "Hà Tĩnh",

    "quảng bình":               "Quảng Bình",
    "quảng trị":                "Quảng Bình",

    "quảng ngãi":               "Quảng Ngãi",
    "kon tum":                  "Quảng Ngãi",

    "bình định":                "Gia Lai",
    "gia lai":                  "Gia Lai",

    "đắk lắk":                  "Đắk Lắk",
    "dak lak":                  "Đắk Lắk",
    "phú yên":                  "Đắk Lắk",

    "khánh hòa":                "Khánh Hoà",
    "khánh hoà":                "Khánh Hoà",
    "khanh hoa":                "Khánh Hoà",
    "ninh thuận":               "Khánh Hoà",
    "nha trang":                "Khánh Hoà",

    "lâm đồng":                 "Lâm Đồng",
    "lam dong":                 "Lâm Đồng",
    "đắk nông":                 "Lâm Đồng",
    "dak nong":                 "Lâm Đồng",
    "bình thuận":               "Lâm Đồng",
    "đà lạt":                   "Lâm Đồng",
    "da lat":                   "Lâm Đồng",

    # ── Miền Nam ─────────────────────────────────────────────────────────────
    "đồng nai":                 "Đồng Nai",
    "dong nai":                 "Đồng Nai",
    "bình phước":               "Đồng Nai",

    "đồng tháp":                "Đồng Tháp",
    "tiền giang":               "Đồng Tháp",

    "tây ninh":                 "Tây Ninh",
    "long an":                  "Tây Ninh",

    "vĩnh long":                "Vĩnh Long",
    "bến tre":                  "Vĩnh Long",
    "trà vinh":                 "Vĩnh Long",

    "an giang":                 "An Giang",
    "kiên giang":               "An Giang",
    "kien giang":               "An Giang",
    "phú quốc":                 "An Giang",
    "phu quoc":                 "An Giang",

    "cà mau":                   "Cà Mau",
    "bạc liêu":                 "Cà Mau",
}
GEO_KEYS_SORTED: list[str] = sorted(PROVINCE_CANONICAL.keys(), key=len, reverse=True)

REGION_MAP: dict[str, str] = {
    "TP. Hà Nội":       "Bắc",
    "TP. Hải Phòng":    "Bắc",
    "Bắc Ninh":         "Bắc",
    "Hưng Yên":         "Bắc",
    "Lào Cai":          "Bắc",
    "Ninh Bình":        "Bắc",
    "Phú Thọ":          "Bắc",
    "Thái Nguyên":      "Bắc",
    "Tuyên Quang":      "Bắc",
    "Quảng Ninh":       "Bắc",
    "Cao Bằng":         "Bắc",
    "Lạng Sơn":         "Bắc",
    "Sơn La":           "Bắc",
    "Điện Biên":        "Bắc",
    "Lai Châu":         "Bắc",

    "Thanh Hóa":        "Trung",
    "Nghệ An":          "Trung",
    "Hà Tĩnh":          "Trung",
    "Quảng Bình":       "Trung",
    "TP. Huế":          "Trung",
    "TP. Đà Nẵng":      "Trung",
    "Quảng Ngãi":       "Trung",
    "Gia Lai":          "Trung",
    "Đắk Lắk":          "Trung",
    "Khánh Hoà":        "Trung",
    "Lâm Đồng":         "Trung",

    "TP. Hồ Chí Minh":  "Nam",
    "TP. Cần Thơ":      "Nam",
    "Đồng Nai":         "Nam",
    "Đồng Tháp":        "Nam",
    "Tây Ninh":         "Nam",
    "Vĩnh Long":        "Nam",
    "An Giang":         "Nam",
    "Cà Mau":           "Nam",
}

FOREIGN_KW: list[str] = [
    "singapore", "japan", "korea", "taiwan", "china", "germany", "france",
    "usa", "united states", "australia", "uk", "canada", "malaysia",
    "thailand", "indonesia", "philippines", "india",
    "nhật bản", "hàn quốc", "đài loan", "trung quốc", "đức", "pháp",
    "mỹ", "úc", "anh",
]


# ==============================================================================
# 2. SALARY
# ==============================================================================

# FIX: conversion_rate = hệ số để đổi sang NGHÌN VND (đơn vị lưu trữ)
# Ví dụ:
#   1 USD  × 25000 = 25.000.000 đồng = 25.000 nghìn đồng  → rate = 25000
#   1 triệu VND × 1000 = 1.000.000 đồng = 1000 nghìn đồng → rate = 1000
#   1 nghìn VND × 1    = 1000 đồng = 1 nghìn đồng          → rate = 1
#   1 VND × 0.001                                           → rate = 0.001
CURRENCY_RULES: list[tuple[str, str, float]] = [
    (r"usd|us\$|\$(?!\d*vnd)",    "USD",  25_000.0),   # 1 USD = 25000 nghìn đồng
    (r"sgd|s\$",                   "SGD",  18_500.0),   # 1 SGD ≈ 18500 nghìn đồng
    (r"jpy|¥",                     "JPY",      168.0),  # 1 JPY ≈ 168 nghìn đồng
    (r"eur|€",                     "EUR",  27_000.0),   # 1 EUR ≈ 27000 nghìn đồng
    (r"triệu|tr\b|million|\bm\b",  "VND",   1_000.0),  # 1 triệu = 1000 nghìn
    (r"nghìn|ngàn|\bk\b",          "VND",       1.0),  # 1 nghìn = 1 nghìn
    (r"đồng|\bvnd\b",              "VND",       0.001), # 1 đồng = 0.001 nghìn
]

NEGOTIABLE_KW: frozenset[str] = frozenset([
    "thỏa thuận", "thoả thuận", "thương lượng", "cạnh tranh",
    "mới cập nhật", "negotiate", "negotiable", "competitive",
    "you'll love it", "hấp dẫn", "theo năng lực",
    "trao đổi thêm", "theo kinh nghiệm", "xem thêm",
    "lương cứng", "attractive", "based on",
])

SALARY_BINS: list[tuple[float, float, str]] = [
    (0,           3_000_000,    "Dưới 3 triệu"),
    (3_000_000,   10_000_000,   "3 – 10 triệu"),
    (10_000_000,  15_000_000,   "10 – 15 triệu"),
    (15_000_000,  25_000_000,   "15 – 25 triệu"),
    (25_000_000,  35_000_000,   "25 – 35 triệu"),
    (35_000_000,  50_000_000,   "35 – 50 triệu"),
    (50_000_000,  float("inf"), "Hơn 50 triệu"),
]


# ==============================================================================
# 3. EXPERIENCE
# ==============================================================================

NO_EXP_KW: frozenset[str] = frozenset([
    "không yêu cầu", "no experience", "chưa có kinh nghiệm",
    "fresher", "không cần kinh nghiệm", "sinh viên mới ra trường",
    "chưa có", "không cần", "no experience required",
    "sinh viên năm cuối", "mới ra trường", "entry level",
    "0 năm kinh nghiệm",
])

EXP_BINS: list[tuple[float, float, str]] = [
    (0.0, 0.0,          "Không yêu cầu"),
    (0.0, 1.0,          "Dưới 1 năm"),
    (1.0, 3.0,          "1 – 3 năm"),
    (3.0, 5.0,          "3 – 5 năm"),
    (5.0, float("inf"), "Trên 5 năm"),
]


# ==============================================================================
# 4. JOB CLASSIFICATION
# ==============================================================================

LEVEL_MAP: list[tuple[list[str], str]] = [
    (["intern", "thực tập", "internship", "trainee"],
     "Intern"),
    (["fresher", "fresh grad", "mới tốt nghiệp", "entry level", "entry-level"],
     "Fresher"),
    (["junior"],
     "Junior"),
    (["mid-level", "mid level", "middle", "intermediate"],
     "Mid-level"),
    (["senior", "experienced", "chuyên gia", "chuyên viên cao cấp", "principal"],
     "Senior"),
    (["leader", "tech lead", "team lead", "trưởng nhóm"],
     "Leader"),
    (["manager", "quản lý", "trưởng phòng", "giám đốc", "director",
      "head of", "cto", "ceo", r"\bvp\b", "vice president",
      "c-level", "phó giám đốc", "project manager", r"\bpm\b"],
     "Manager"),
]

EXP_TO_LEVEL: list[tuple[float, float, str]] = [
    (0.0,  0.001,        "Intern"),
    (0.0,  1.0,          "Fresher"),
    (1.0,  3.0,          "Junior"),
    (3.0,  5.0,          "Mid-level"),
    (5.0,  8.0,          "Senior"),
    (8.0,  float("inf"), "Leader"),
]

EDUCATION_MAP: dict[str, list[str]] = {
    "Tiến sĩ":       ["tiến sĩ", "phd", "doctorate"],
    "Thạc sĩ":       ["thạc sĩ", "master", "mba", "m.s.", "m.a."],
    "Đại học":       ["đại học", "cử nhân", "kỹ sư", "bachelor", "university", "college"],
    "Cao đẳng":      ["cao đẳng", "associate"],
    "Trung cấp":     ["trung cấp", "vocational"],
    "THCS/THPT":     ["thpt", "thcs", "trung học", "high school"],
    "Không yêu cầu": [],
}

INDUSTRY_TREE: list[dict] = [

# ======================= CÔNG NGHỆ =======================

{"l1": "Công nghệ", "l2": "IT", "l3": "Phần mềm / CNTT",
 "kw": ["phần mềm", "cntt", "công nghệ thông tin", "lập trình",
        "software", "developer", "backend", "frontend", "fullstack",
        "web developer", "mobile developer", "app developer",
        "saas", "system", "api", "microservices","e-commerce", "erp", "crm"]},

{"l1": "Công nghệ", "l2": "DATA", "l3": "Data / BI",
 "kw": ["data analyst", "data engineer", "data scientist",
        "business intelligence", "bi", "etl", "data warehouse",
        "dashboard", "power bi", "tableau", "sql", "big data"]},

{"l1": "Công nghệ", "l2": "AI", "l3": "AI / Machine Learning",
 "kw": ["machine learning", "deep learning", "ai", "trí tuệ nhân tạo",
        "nlp", "computer vision", "llm", "model training"]},

{"l1": "Công nghệ", "l2": "SEC", "l3": "An ninh mạng",
 "kw": ["an ninh mạng", "bảo mật", "cyber security",
        "information security", "soc analyst", "penetration testing"]},

{"l1": "Công nghệ", "l2": "HW", "l3": "Phần cứng / Viễn thông",
 "kw": ["phần cứng", "viễn thông", "network", "telecom",
        "hạ tầng mạng", "it infrastructure"]},

{"l1": "Công nghệ", "l2": "EMB", "l3": "Nhúng / IoT",
 "kw": ["embedded", "nhúng", "firmware", "iot",
        "vi mạch", "plc", "scada", "tự động hóa"]},

{"l1": "Công nghệ", "l2": "GAME", "l3": "Game",
 "kw": ["game", "unity", "unreal engine", "game developer"]},

{"l1": "Công nghệ", "l2": "QA", "l3": "QA / Testing",
 "kw": ["tester", "qa engineer", "quality assurance",
        "automation test", "manual test"]},

{"l1": "Công nghệ", "l2": "DES", "l3": "Thiết kế (UI/UX/Graphic)",
 "kw": ["ui", "ux", "designer", "graphic designer",
        "figma", "photoshop", "illustrator"]},


# ======================= TÀI CHÍNH & KINH DOANH =======================

{"l1": "Tài chính & Kinh doanh", "l2": "FIN", "l3": "Ngân hàng / Tài chính",
 "kw": ["ngân hàng", "banking", "finance", "financial services",
        "tài chính", "đầu tư", "quỹ", "asset management",
        "wealth management", "forex"]},

{"l1": "Tài chính & Kinh doanh", "l2": "ACC", "l3": "Kế toán / Kiểm toán",
 "kw": ["kế toán", "kiểm toán", "thuế", "accounting", "audit", "tax"]},

{"l1": "Tài chính & Kinh doanh", "l2": "SALE", "l3": "Kinh doanh / Sales",
 "kw": ["bán hàng", "sales", "kinh doanh",
        "business development", "account manager"]},

{"l1": "Tài chính & Kinh doanh", "l2": "MKT", "l3": "Marketing & Truyền thông",
 "kw": ["marketing", "digital marketing", "seo", "sem",
        "content", "branding", "pr", "quảng cáo"]},

{"l1": "Tài chính & Kinh doanh", "l2": "CONS", "l3": "Tư vấn / Consulting",
 "kw": ["consulting", "management consulting",
        "business consulting", "advisory services",
        "chiến lược", "tư vấn doanh nghiệp"]},


# ======================= SẢN XUẤT & CÔNG NGHIỆP =======================

{"l1": "Sản xuất & Công nghiệp", "l2": "MFG", "l3": "Sản xuất / Chế biến",
 "kw": ["sản xuất", "manufacturing", "chế biến",
        "cơ khí", "điện tử", "ô tô",
        "qa/qc sản xuất", "quản lý chất lượng"]},

{"l1": "Sản xuất & Công nghiệp", "l2": "ENE", "l3": "Điện / Năng lượng",
 "kw": ["điện lực", "năng lượng", "điện mặt trời",
        "điện gió", "renewable energy"]},

{"l1": "Sản xuất & Công nghiệp", "l2": "MIN", "l3": "Khai khoáng / Dầu khí",
 "kw": ["dầu khí", "oil & gas", "khai khoáng", "mỏ"]},

{"l1": "Sản xuất & Công nghiệp", "l2": "AGR", "l3": "Nông - Lâm - Thủy sản",
 "kw": ["nông nghiệp", "chăn nuôi", "thủy sản", "agriculture"]},

{"l1": "Sản xuất & Công nghiệp", "l2": "ENV", "l3": "Môi trường",
 "kw": ["môi trường", "xử lý nước", "waste treatment"]},


# ======================= XÂY DỰNG & BẤT ĐỘNG SẢN =======================

{"l1": "Xây dựng & BĐS", "l2": "CON", "l3": "Xây dựng / Kiến trúc",
 "kw": ["xây dựng", "construction", "kiến trúc",
        "thi công", "nội thất"]},

{"l1": "Xây dựng & BĐS", "l2": "RE", "l3": "Bất động sản",
 "kw": ["bất động sản", "real estate",
        "môi giới", "property"]},


# ======================= THƯƠNG MẠI & DỊCH VỤ =======================

{"l1": "Thương mại & Dịch vụ", "l2": "RET", "l3": "Bán lẻ / E-commerce",
 "kw": ["bán lẻ", "retail", "ecommerce",
        "thương mại điện tử", "fmcg", "phân phối"]},

{"l1": "Thương mại & Dịch vụ", "l2": "HOS", "l3": "Nhà hàng / Du lịch",
 "kw": ["nhà hàng", "khách sạn", "du lịch",
        "hospitality", "f&b", "food and beverage"]},

{"l1": "Thương mại & Dịch vụ", "l2": "SVC", "l3": "Dịch vụ tiêu dùng",
 "kw": ["spa", "làm đẹp", "thẩm mỹ",
        "dịch vụ cá nhân", "chăm sóc khách hàng"]},


# ======================= THỂ THAO & GIẢI TRÍ =======================

{"l1": "Thể thao & Giải trí", "l2": "SPORT", "l3": "Thể thao / Fitness / Gym",
 "kw": ["gym", "fitness", "thể hình",
        "huấn luyện viên", "pt", "yoga",
        "sports", "personal trainer"]},

{"l1": "Thể thao & Giải trí", "l2": "ENT", "l3": "Giải trí / Media",
 "kw": ["media", "entertainment", "game streaming",
        "âm nhạc", "phim", "sự kiện"]},


# ======================= VẬN TẢI & LOGISTICS =======================

{"l1": "Vận tải & Logistics", "l2": "LOG", "l3": "Logistics / Vận tải",
 "kw": ["logistics", "vận tải", "supply chain",
        "kho bãi", "xuất nhập khẩu", "freight"]},


# ======================= CÔNG & XÃ HỘI =======================

{"l1": "Công & Xã hội", "l2": "MED", "l3": "Y tế / Dược",
 "kw": ["y tế", "bệnh viện", "pharma",
        "healthcare", "medical"]},

{"l1": "Công & Xã hội", "l2": "EDU", "l3": "Giáo dục / Đào tạo",
 "kw": ["giáo dục", "đào tạo", "trường học",
        "education", "teaching"]},

{"l1": "Công & Xã hội", "l2": "ADM", "l3": "Hành chính / Nhân sự",
 "kw": ["nhân sự", "hr", "human resources",
        "hành chính", "administration"]},

{"l1": "Công & Xã hội", "l2": "LEGAL", "l3": "Pháp lý",
 "kw": ["luật", "pháp lý", "legal", "compliance"]},

{"l1": "Công & Xã hội", "l2": "GOV", "l3": "Nhà nước / NGO",
 "kw": ["chính phủ", "nhà nước",
        "ngo", "tổ chức phi chính phủ",
        "government"]},

]

COMPANY_TYPE_PATTERNS = [
    (r"trách nhiệm hữu hạn một thành viên|trách nhiệm hữu hạn 1 thành viên|trách nhiệm hữu hạn mtv|trách nhiệm hữu hạn|tnhh", "TNHH"),
    (r"cổ phần|\bjsc\b", "Cổ phần"),
    (r"hợp danh", "Hợp danh"),
    (r"doanh nghiệp tư nhân|dntn", "DNTN"),
    (r"hợp tác xã|htx|cooperative|co-op", "Hợp tác xã"),
    (r"doanh nghiệp nhà nước|state[- ]owned|so e", "Nhà nước"),
    (r"doanh nghiệp xã hội|social enterprise", "Xã hội"),
    (r"\bllc\b", "LLC"),
    (r"\bltd\.?\b|limited", "Ltd"),
    (r"co\.?\s*,?\s*ltd\.?", "Co., Ltd"),
    (r"\binc\.?\b|incorporated", "Inc"),
    (r"\bcorp\.?\b|corporation", "Corporation"),
    (r"\bplc\b|public limited company", "PLC"),
    (r"\bllp\b|limited liability partnership", "LLP"),
    (r"\blp\b|limited partnership", "LP"),
    (r"\bgmbh\b", "GmbH"),
    (r"\bag\b", "AG"),
    (r"pte\.?\s*ltd\.?", "Pte Ltd"),
    (r"tập đoàn|group", "Tập đoàn"),
    (r"holdings?|holding company", "Holding"),
]

# FIX: COMPANY_TYPE_STRIP giữ nguyên (dùng riêng trong parse_company_title bằng _COMPANY_NOISE)
COMPANY_TYPE_STRIP = re.compile(
    r"(công ty\s*)?(trách nhiệm hữu hạn|tnhh|cổ phần|\bcp\b|hợp danh|\bhd\b"
    r"|doanh nghiệp tư nhân|dntn|tập đoàn|\bcorp\.?\b|\bcorporation\b"
    r"|\bllc\b|\binc\.?\b|\bincorporated\b|\bco\.,?\s*ltd\.?\b"
    r"|\bltd\.?\b|\blimited\b|\bjsc\b)",
    re.IGNORECASE,
)

JOB_CATEGORY_MAP: dict[str, str] = {
    "Project Manager":      "Management",
    "Project Leader":       "Management",
    "IT Manager":           "Management",
    "Tech Lead":            "Management",

    "IT Consultant":        "Consulting",

    "Product Owner":        "Product",
    "Product Manager":      "Product",
    "Product Executive":    "Product",

   

    "Full-stack Developer": "Development",
    "Back-end Developer":   "Development",
    "Front-end Developer":  "Development",
    "Mobile Developer":     "Development",
    "Game Developer":       "Development",
    "Embedded Engineer":    "Development",

    "Tester":               "QA/Testing",
    "QA - QC":              "QA/Testing",

    "DevOps/DevSecOps":     "Infrastructure",
    "Cloud Engineer":       "Infrastructure",
    "System Engineer":      "Infrastructure",
    "System Admin":         "Infrastructure",

    "Data Engineer":        "Data",
    "Data Analyst":         "Data",
    "Data Scientist":       "Data",
    "BI Analyst":           "Data",
    "Database Engineer":    "Data",

    "AI Engineer":          "AI/ML",
    "Blockchain Engineer":  "Blockchain",

    "Designer":             "Design",

    "ERP/CRM Engineer":     "ERP/CRM",
    "Solution Architect":   "Architecture",

    "IT Support":           "Support",

    "IT Sales":             "Business",
    "Business Analyst":     "Business",
}

IT_TITLES: frozenset[str] = frozenset(JOB_CATEGORY_MAP.keys()) - {"Designer"}

JOB_TITLE_MAP: dict[str, list[str]] = {

    "Project Manager": [
        "project manager", "pm ", "pm)", "pmo",
        "quản lý dự án", "điều phối dự án", "quản trị dự án"
    ],
    "Project Leader": [
        "project leader", "trưởng dự án"
    ],
    "IT Manager": [
        "it manager", "trưởng phòng it", "giám đốc công nghệ",
        "cto", "cio", "trưởng bộ phận it", "trưởng phòng công nghệ",
        "it section manager"
    ],
    "Tech Lead": [
        "tech lead", "technical lead", "trưởng nhóm kỹ thuật",
        "team lead"
    ],
    "IT Consultant": [
        "it consultant", "tư vấn giải pháp", "tư vấn công nghệ",
        "tư vấn kỹ thuật", "triển khai phần mềm",
        "technical consultant", "giải pháp phần mềm",
        "solution consultant", "presales consultant"
    ],
    "Product Owner": [
        "product owner", "po ", "po)", "quản lý sản phẩm",
        "giám đốc sản phẩm", "phát triển sản phẩm"
    ],
    "Product Manager": [
        "product manager", "quản trị sản phẩm", "product lead"
    ],
    "Product Executive": [
        "product executive", "chuyên viên sản phẩm"
    ],
    "Business Analyst": [
        "business analyst", "ba ", "ba)",
        "phân tích nghiệp vụ", "phân tích kinh doanh",
        "business data analyst"
    ],
    "Full-stack Developer": [
        "fullstack", "full-stack", "full stack"
    ],
    "Back-end Developer": [
        "backend", "back-end", "back end",
        "python developer", "java developer",
        "node developer", "api developer",
        "software engineer", "developer", "it developer"
    ],
    "Front-end Developer": [
        "frontend", "front-end", "front end", "web developer"
    ],
    "Mobile Developer": [
        "mobile", "android", "ios", "flutter",
        "react native", "swift", "kotlin", "xamarin"
    ],
    "Game Developer": [
        "game developer", "unity", "unreal"
    ],
    "Embedded Engineer": [
        "embedded", "nhúng", "firmware", "iot", "vi mạch",
        "plc", "scada", "automation", "tự động hóa"
    ],
    "Tester": [
        "tester", "kiểm thử", "manual test",
        "test engineer", "qa engineer", "automation test"
    ],
    "QA - QC": [
        "qa", "qc", "quality assurance",
        "quality control", "pqa", "đảm bảo chất lượng"
    ],
    "DevOps/DevSecOps": [
        "devops", "sre", "devsecops",
        "ci/cd", "site reliability", "mlops"
    ],
    "Cloud Engineer": [
        "cloud engineer", "aws engineer", "azure engineer",
        "gcp engineer", "kỹ sư đám mây", "cloud architect"
    ],
    "System Engineer": [
        "system engineer", "network engineer",
        "kỹ sư mạng", "an ninh mạng",
        "security engineer", "bảo mật",
        "hạ tầng", "infrastructure engineer",
        "triển khai hệ thống"
    ],
    "System Admin": [
        "system admin", "sysadmin",
        "quản trị hệ thống", "it administrator"
    ],
    "Data Engineer": [
        "data engineer", "kỹ sư dữ liệu", "kĩ sư dữ liệu",
        "big data", "etl engineer", "data pipeline"
    ],
    "Data Analyst": [
        "data analyst", "phân tích dữ liệu",
        "chuyên viên phân tích dữ liệu", "anlatics", "da", "analytics"
    ],
    "Data Scientist": [
        "data scientist", "khoa học dữ liệu", "ds", "data science"
    ],
    "BI Analyst": [
        "business intelligence", "bi analyst",
        "bi executive", "bi developer", "bi"
    ],
    "Database Engineer": [
        "dba", "database", "cơ sở dữ liệu",
        "sql developer", "quản trị csdl","quản lý dữ liệu",
        "database administrator"
    ],
    "AI Engineer": [
        "ai engineer", "trí tuệ nhân tạo",
        "machine learning", "deep learning",
        "llm", "generative ai", "gen ai",
        "computer vision", "nlp", "ml engineer",
        "ai researcher", "research engineer",
        "applied scientist", "mlops engineer",
        "prompt engineer", "rag engineer",
    ],
    "Blockchain Engineer": [
        "blockchain", "smart contract",
        "web3", "solidity", "nft",
        "defi", "crypto"
    ],
        "Solution Architect": [
        "solution architect", "kiến trúc sư",
        "kiến trúc hệ thống", "enterprise architect",
        "triển khai giải pháp", "chief architect","solution designer"
    ],
    "Designer": [
        "designer", "thiết kế", "ui/ux",
        "ux designer", "ui designer",
        "graphic", "đồ họa",
        "art director", "artist",
        "motion graphic", "photoshop"
    ],
    "ERP/CRM Engineer": [
        "erp", "sap", "odoo",
        "salesforce", "crm developer",
        "microsoft dynamics", "servicenow"
    ],

    "IT Support": [
        "it support", "helpdesk",
        "hỗ trợ kỹ thuật",
        "nhân viên kỹ thuật",
        "nhân viên vận hành","chuyên viên vận hành hệ thống","chuyên viên hỗ trợ kỹ thuật",
        "kỹ thuật máy tính",
        "sửa chữa", "lắp ráp",
        "kỹ thuật viên",
        "it phần cứng",
        "it helpdesk",
        "nhân viên it","chuyên viên it",
        "it officer",
        "it executive", "technology services","techinical architect"
    ],
    "IT Sales": [
        "kinh doanh", "sale", "sales", "bán hàng",
        "account manager", "business development",
        "bd", "telesales",
        "sales executive",
        "solution sales",
        "project sales",
        "sales engineer",
        "pre sales", "presales"
    ],
}

ROLE_WORDS: dict[str, str] = {
    "developer":    "dev",  "engineer":     "dev",
    "lập trình":    "dev",  "programmer":   "dev",
    "coding":       "dev",  "software":     "dev",
    "analyst":      "analyst", "analysis":  "analyst",
    "phân tích":    "analyst",
    "tester":       "qa",   "testing":      "qa",
    "kiểm thử":     "qa",   "qa":           "qa",
    "qc":           "qa",
    "devops":       "infra", "sre":         "infra",
    "cloud":        "infra", "system":      "infra",
    "network":      "infra", "admin":       "infra",
    "manager":      "mgr",  "director":     "mgr",
    "head":         "mgr",  "lead":         "mgr",
    "cto":          "mgr",  "ceo":          "mgr",
    "trưởng":       "mgr",  "giám đốc":     "mgr",
    "support":      "support", "helpdesk":  "support",
    "hỗ trợ":       "support",
}

TECH_DOMAIN: dict[str, str] = {
    "react": "fe",  "vue": "fe",    "angular": "fe",
    "frontend": "fe", "front-end": "fe", "html": "fe",
    "css": "fe",    "nextjs": "fe",
    "python": "be", "java": "be",   "golang": "be",
    "node": "be",   "php": "be",    "backend": "be",
    "back-end": "be", "api": "be",  "django": "be",
    "spring": "be", "cobol": "be",  ".net": "be",
    "ruby": "be",   "kotlin": "be",
    "android": "mob", "ios": "mob", "flutter": "mob",
    "react native": "mob", "swift": "mob",
    "sql": "data",  "etl": "data",  "pipeline": "data",
    "spark": "data", "kafka": "data", "airflow": "data",
    "power bi": "data", "tableau": "data",
    "machine learning": "ai", "deep learning": "ai",
    "llm": "ai",    "nlp": "ai",    "pytorch": "ai",
    "tensorflow": "ai", "ai ": "ai",
    "fullstack": "fs", "full-stack": "fs", "full stack": "fs",
}

ROLE_DOMAIN_TO_TITLE: dict[tuple, str] = {
    ("dev", "fe"):   "Front-end Developer",
    ("dev", "be"):   "Back-end Developer",
    ("dev", "mob"):  "Mobile Developer",
    ("dev", "fs"):   "Full-stack Developer",
    ("dev", "ai"):   "AI Engineer",
    ("dev", "data"): "Data Engineer",
    ("dev", None):   "Back-end Developer",
    ("analyst", "data"): "Data Analyst",
    ("analyst", "ai"):   "Data Scientist",
    ("analyst", None):   "Business Analyst",
    ("qa",   None):  "Tester",
    ("infra", None): "DevOps/DevSecOps",
    ("mgr",  None):  "IT Manager",
    ("support", None): "IT Support",
}

# ==============================================================================
# 5. CERTIFICATIONS & SKILLS
# ==============================================================================

CERT_KW: list[str] = [
    # Cloud
    "aws certified", "aws solutions architect", "aws developer",
    "aws sysops", "aws cloud practitioner",
    "azure certified", "az-900", "az-104", "az-204", "az-305",
    "gcp certified", "google cloud professional",
    # Security
    "cissp", "cism", "cisa", "ceh", "oscp", "comptia security+",
    "comptia network+", "comptia a+", "ccna", "ccnp", "ccsp",
    # Project / Agile
    "pmp", "prince2", "pmi-acp", "scrum master", "csm", "psm",
    "safe", "kanban", "itil", "cobit",
    # Data / AI
    "google data analytics", "ibm data science", "databricks",
    "tensorflow developer", "pytorch",
    # Dev / Language
    "oracle certified", "java certified", "oca", "ocp",
    "microsoft certified", "mcsa", "mcse", "mcsd", "mta",
    "red hat certified", "rhcsa", "rhce",
    "linux foundation", "lfcs", "ckad", "cka",
    # Quality
    "six sigma", "iso 9001", "iso 27001",
    # Language certs
    "toeic", "ielts", "toefl", "toefl ibt",
    "jlpt", "n1", "n2", "n3", "n4", "n5",
    "hsk", "hsk1", "hsk2", "hsk3", "hsk4", "hsk5", "hsk6",
    "topik", "topik i", "topik ii",
    "delf", "dalf",
]

LANG_CERT_KW: frozenset[str] = frozenset([
    "toeic", "ielts", "toefl", "toefl ibt",
    "jlpt", "n1", "n2", "n3", "n4", "n5",
    "hsk", "hsk1", "hsk2", "hsk3", "hsk4", "hsk5", "hsk6",
    "topik", "topik i", "topik ii",
    "delf", "dalf",
])

LANG_CERT_TO_LANG: dict[str, str] = {
    "toeic":     "Tiếng Anh",
    "ielts":     "Tiếng Anh",
    "toefl":     "Tiếng Anh",
    "toefl ibt": "Tiếng Anh",
    "jlpt":      "Tiếng Nhật",
    "n1":        "Tiếng Nhật",
    "n2":        "Tiếng Nhật",
    "n3":        "Tiếng Nhật",
    "n4":        "Tiếng Nhật",
    "n5":        "Tiếng Nhật",
    "hsk":       "Tiếng Trung",
    "hsk1":      "Tiếng Trung",
    "hsk2":      "Tiếng Trung",
    "hsk3":      "Tiếng Trung",
    "hsk4":      "Tiếng Trung",
    "hsk5":      "Tiếng Trung",
    "hsk6":      "Tiếng Trung",
    "topik":     "Tiếng Hàn",
    "topik i":   "Tiếng Hàn",
    "topik ii":  "Tiếng Hàn",
    "delf":      "Tiếng Pháp",
    "dalf":      "Tiếng Pháp",
}

WORK_TYPE_MAP: dict[str, list[str]] = {
    "Freelance": ["freelance", "freelancer", "tự do", "cộng tác viên", "ctv",
                  "project base", "theo dự án", "thời vụ"],
    "Part-time": ["part time", "part-time", "bán thời gian", "ca gãy",
                  "4 tiếng", "parttime", "internship", "thực tập", "trainee"],
    "Full-time": ["full time", "full-time", "toàn thời gian", "chính thức",
                  "hành chính", "hợp đồng chính thức"],
}

WORK_MODE_MAP: dict[str, list[str]] = {
    "Hybrid": ["hybrid", "linh hoạt", "xen kẽ", "flexible", "kết hợp", "mix",
               "bán từ xa", "semi-remote", "ngày lên văn phòng", "days at office"],
    "Remote": ["remote", "từ xa", "wfh", "work from home", "tại nhà",
               "không cần lên văn phòng", "làm việc tại nhà", "làm từ xa",
               "work remotely"],
    "Onsite": ["onsite", "tại văn phòng", "office", "trực tiếp", "offline",
               "văn phòng", "làm việc tại văn phòng"],
}

SKILL_MAP: dict[str, dict[str, list[str]]] = {
    "hard": {
        # Ngôn ngữ lập trình
        "Python":        ["python"],
        "Java":          [r"java ", r"java,"],
        "Go/Golang":     ["golang", "go lang"],
        "JavaScript":    ["javascript", r"js ", r"js,", r"js\."],
        "TypeScript":    ["typescript"],
        "C++":           [r"c\+\+"],
        "C#":            ["c#", ".net", "dotnet", "asp.net"],
        "PHP":           ["php", "laravel", "symfony"],
        "Ruby":          ["ruby", "rails"],
        "Swift":         ["swift"],
        "Kotlin":        ["kotlin"],
        "Dart/Flutter":  ["dart", "flutter"],
        "SQL":           ["sql", "mysql", "postgresql", "postgres",
                          "sql server", "mssql", "nosql", "mongodb",
                          "redis", "elasticsearch", "cassandra"],
        "HTML/CSS":      ["html", "css", "sass", "scss", "tailwind"],
        "Rust":          ["rust"],
        "Scala":         ["scala"],
        "Bash/Shell":    ["bash", "shell script", "linux", "unix"],
        "VBA":           ["vba", "excel macro"],
        "R":             ["r lang", "r programming"],
        "MATLAB":        ["matlab"],
        "C/C++":         [r"c/c\+\+", r"\bc lang\b"],

        # Framework / Lib
        "React":         ["react", "reactjs", "react.js", "react native",
                          "next.js", "nextjs"],
        "Angular":       ["angular", "angularjs"],
        "Vue":           ["vue", "vuejs", "nuxt"],
        "NodeJS":        ["node", "nodejs", "node.js", "express.js", "nestjs"],
        "Spring":        ["spring boot", "spring mvc", "spring cloud"],
        "Django/Flask":  ["django", "flask", "fastapi"],

        # Cloud & DevOps
        "AWS":           ["aws", "amazon web services", "ec2", "s3", "lambda",
                          "eks", "ecs"],
        "Azure":         ["azure", "microsoft azure"],
        "GCP":           ["gcp", "google cloud", "bigquery", "cloud run"],
        "Docker":        ["docker", "dockerfile", "docker compose"],
        "Kubernetes":    ["k8s", "kubernetes", "helm"],
        "Terraform":     ["terraform", "infrastructure as code", "iac"],
        "CI/CD":         ["ci/cd", "jenkins", "github actions", "gitlab ci",
                          "circleci", "argocd"],
        "Git":           ["git", "github", "gitlab", "bitbucket", "svn"],

        # Data & BI
        "Excel":         ["excel", "spreadsheet", "google sheet", "google sheets",
                          "vlookup", "pivot table"],
        "Power BI":      ["power bi", "powerbi", "dax", "power query"],
        "Tableau":       ["tableau"],
        "Looker":        ["looker", "google data studio", "looker studio"],
        "Qlik":          ["qlik", "qlikview", "qliksense"],
        "SAS/SPSS":      ["sas", "spss"],
        "Apache Spark":  ["spark", "apache spark", "pyspark"],
        "Kafka":         ["kafka", "apache kafka"],
        "Airflow":       ["airflow", "apache airflow"],

        # AI / ML — MẢNG MỞ RỘNG
        "TensorFlow":       ["tensorflow", "tf", "tensorflow 2", "tf2"],
        "PyTorch":          ["pytorch", "torch"],
        "Scikit-learn":     ["scikit-learn", "sklearn"],
        "LangChain":        ["langchain"],
        "LLM/RAG":          ["llm", "rag", "retrieval augmented", "vector database",
                             "vector db", "prompt engineering", "few-shot",
                             "chain-of-thought", "cot", "in-context learning",
                             "context window"],
        "Generative AI":    ["generative ai", "gen ai", "genai", "stable diffusion",
                             "diffusion model", "midjourney", "dall-e",
                             "imagen", "text-to-image", "text to image"],
        "OpenAI API":       ["openai", "gpt-4", "gpt-3", "chatgpt api",
                             "whisper", "dall-e", "openai api"],
        "Hugging Face":     ["hugging face", "huggingface", "transformers library",
                             "bert", "roberta", "t5", "gpt2", "llama",
                             "mistral", "falcon", "gemma"],
        "Computer Vision":  ["computer vision", "cv ", "object detection",
                             "image classification", "image segmentation",
                             "yolo", "yolov", "resnet", "vgg", "efficientnet",
                             "mediapipe", "opencv", "open-cv"],
        "NLP":              ["nlp", "natural language processing",
                             "text classification", "ner", "named entity",
                             "sentiment analysis", "information extraction",
                             "question answering", "summarization",
                             "word embedding", "word2vec", "fasttext",
                             "spacy", "nltk"],
        "MLOps":            ["mlops", "ml pipeline", "model serving",
                             "model deployment", "model monitoring",
                             "feature store", "mlflow", "kubeflow",
                             "bentoml", "seldon", "triton inference",
                             "ray serve", "sagemaker", "vertex ai",
                             "azure ml", "weight & biases", "wandb",
                             "dvc", "data version control"],
        "Deep Learning":    ["deep learning", "neural network", "cnn",
                             "convolutional neural", "rnn", "lstm",
                             "transformer", "attention mechanism",
                             "self-attention", "encoder decoder",
                             "gan", "generative adversarial",
                             "vae", "variational autoencoder",
                             "graph neural", "gnn", "reinforcement learning",
                             "rl ", "drl", "ppo", "dqn"],
        "Data Science":     ["data science", "exploratory data analysis", "eda",
                             "feature engineering", "feature selection",
                             "model evaluation", "cross-validation",
                             "hyperparameter tuning", "a/b testing",
                             "statistical modeling", "regression",
                             "classification", "clustering", "anomaly detection",
                             "time series", "forecasting"],
        "Vector DB":        ["vector database", "vector db", "pinecone",
                             "weaviate", "qdrant", "milvus", "chroma",
                             "faiss", "pgvector", "embedding"],
        "AutoML":           ["automl", "auto ml", "auto-sklearn",
                             "h2o.ai", "pycaret", "tpot", "optuna",
                             "hyperopt", "ray tune"],

        # Tools & Collaboration
        "Jira/Confluence":  ["jira", "confluence", "atlassian"],
        "Trello/Asana":     ["trello", "asana", "monday.com", "notion"],
        "Office":           ["word", "powerpoint", "ms office",
                             "tin học văn phòng", "google workspace"],
        "Design Tool":      ["figma", "photoshop", "adobe xd", "sketch",
                             "illustrator", "after effects", "canva"],
    },
    "soft": {
        "Giao tiếp":             ["giao tiếp", "communication", "trình bày",
                                  "thuyết trình", "presentation"],
        "Lãnh đạo":              ["lãnh đạo", "leadership", "dẫn dắt",
                                  "quản lý nhóm", "team lead"],
        "Thương lượng":          ["thương lượng", "đàm phán", "negotiation"],
        "Giải quyết vấn đề":     ["giải quyết vấn đề", "problem solving",
                                  "xử lý tình huống"],
        "Tư duy phản biện":      ["phản biện", "critical thinking", "tư duy logic",
                                  "analytical thinking"],
        "Sáng tạo":              ["sáng tạo", "creative", "innovation"],
        "Quản lý thời gian":     ["quản lý thời gian", "time management",
                                  "sắp xếp công việc"],
        "Làm việc nhóm":         ["làm việc nhóm", "teamwork", "team work",
                                  "hòa đồng", "collaboration"],
        "Chịu áp lực":           ["chịu được áp lực", "work under pressure",
                                  "áp lực cao", "high pressure"],
        "Tự học":                ["tự học", "self-learning", "thích nghi",
                                  "ham học hỏi", "continuous learning"],
        "Tiếng Anh":             ["tiếng anh", "english", "toeic", "ielts",
                                  "toefl", "fluent english"],
        "Tiếng Nhật":            ["tiếng nhật", "japanese", "n1", "n2", "n3",
                                  "jlpt"],
        "Tiếng Trung":           ["tiếng trung", "chinese", "hsk", "mandarin"],
        "Tiếng Hàn":             ["tiếng hàn", "korean", "topik"],
    },
}

MAJOR_MAP: list[tuple[str, list[str]]] = [
    ("Công nghệ thông tin",
     ["công nghệ thông tin", "khoa học máy tính", "computer science",
      "kỹ thuật phần mềm", "software engineering", "hệ thống thông tin"]),
    ("Kỹ thuật điện - điện tử",
     ["điện", "điện tử", "viễn thông", "tự động hóa", "cơ điện tử"]),
    ("Cơ khí - Chế tạo",
     ["cơ khí", "chế tạo máy", "kỹ thuật cơ khí", "vật liệu"]),
    ("Kinh tế - Tài chính",
     ["kinh tế", "tài chính", "ngân hàng", "kế toán", "kiểm toán",
      "quản trị kinh doanh", "mba", "thương mại"]),
    ("Marketing - Truyền thông",
     ["marketing", "truyền thông", "báo chí", "quan hệ công chúng"]),
    ("Thiết kế",
     ["thiết kế", "mỹ thuật", "kiến trúc", "graphic", "ui/ux"]),
    ("Khoa học tự nhiên",
     ["toán", "vật lý", "hóa học", "sinh học", "thống kê"]),
    ("Y - Dược",
     ["y khoa", "dược", "điều dưỡng", "y tế công cộng"]),
    ("Luật",
     ["luật", "pháp lý", "law"]),
    ("Ngoại ngữ",
     ["ngoại ngữ", "tiếng anh", "tiếng nhật", "tiếng trung", "tiếng hàn",
      "ngôn ngữ học", "biên phiên dịch"]),
    ("Logistics - Quản lý chuỗi cung ứng",
     ["logistics", "xuất nhập khẩu", "quản lý chuỗi cung ứng"]),
]

# ==============================================================================
# 6. VIETNAMWORKS — ID → Text
# ==============================================================================

VW_JOB_TYPE: dict[str, str] = {
    "1": "Toàn thời gian",
    "2": "Bán thời gian",
    "3": "Hợp đồng",
    "4": "Thực tập",
    "5": "Tạm thời",
}

VW_EDUCATION: dict[str, str] = {
    "0":  "Bất kỳ",
    "2":  "Trung học",
    "3":  "Trung cấp",
    "4":  "Cử nhân",
    "5":  "Thạc sĩ",
    "6":  "Tiến sĩ",
    "11": "Khác",
    "12": "Cao đẳng",
    "-1": "Không xác định",
}

VW_JOB_LEVEL: dict[str, str] = {
    "1": "Mới Tốt Nghiệp",
    "3": "Giám Đốc và Cấp Cao Hơn",
    "5": "Nhân viên",
    "7": "Trưởng phòng",
    "8": "Thực tập sinh/Sinh viên",
}

VW_COMPANY_SIZE: dict[str, str] = {
    "1":  "Ít hơn 10",
    "2":  "10-24",
    "3":  "25-99",
    "4":  "100-499",
    "5":  "500-999",
    "6":  "1.000-4.999",
    "7":  "5.000-9.999",
    "8":  "10.000-19.999",
    "9":  "20.000-49.999",
    "10": "Hơn 50.000",
}


# ==============================================================================
# 7. ITVIEC
# ==============================================================================

ITVIEC_WORK_MODE_MAP: dict[str, str] = {
    "at office": "Onsite",
    "hybrid":    "Hybrid",
    "remote":    "Remote",
}

ITVIEC_VALID_OUTPUTS:  frozenset[str] = frozenset(ITVIEC_WORK_MODE_MAP.values())
ITVIEC_WORK_MODE_INPUTS: frozenset[str] = frozenset(ITVIEC_WORK_MODE_MAP.keys())