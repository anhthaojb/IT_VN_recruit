
CREATE OR REPLACE FUNCTION public.sp_etl_load_dw(
	p_mode character varying,
	p_run_id character varying DEFAULT NULL::character varying)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
BEGIN

    
    -- 0. TRUNCATE KHI MODE = 'all'
    
    RAISE NOTICE 'Bước 0: TRUNCATE';
    IF p_mode = 'all' THEN
        SET session_replication_role = 'replica';
        TRUNCATE TABLE
            bridge_jobrequire,
            fact_jobpostings,
            fact_pipeline_snapshot,
            fact_error_detail
        RESTART IDENTITY;
        SET session_replication_role = 'origin';
    END IF;

    
    -- 1. DIM_NGUON
    
    RAISE NOTICE 'Bước 1: dim_nguon';
    INSERT INTO dim_nguon (ten_nguon)
    SELECT DISTINCT TRIM(website_clean)
    FROM   fact_jobs_etl
    WHERE  website_clean IS NOT NULL
      AND  TRIM(website_clean) <> ''
      AND  is_valid = TRUE
    ON CONFLICT (ten_nguon) DO NOTHING;

    
    -- 2. DIM_CAPBAC
    
    RAISE NOTICE 'Bước 2: dim_capbac';
    INSERT INTO dim_capbac (ten_cap_bac)
    SELECT DISTINCT TRIM(level_clean)
    FROM   fact_jobs_etl
    WHERE  level_clean IS NOT NULL
      AND  TRIM(level_clean) <> ''
      AND  is_valid = TRUE
    ON CONFLICT (ten_cap_bac) DO NOTHING;

    
    -- 3. DIM_HINHTHUC
    
    RAISE NOTICE 'Bước 3: dim_hinhthuc';
    INSERT INTO dim_hinhthuc (job_type, work_mode)
    SELECT DISTINCT
        COALESCE(NULLIF(TRIM(job_type_clean),  ''), 'Full-time'),
        COALESCE(NULLIF(TRIM(work_mode_clean), ''), 'Onsite')
    FROM fact_jobs_etl
    WHERE is_valid = TRUE
    ON CONFLICT (job_type, work_mode) DO NOTHING;

    
    -- 4. DIM_NGANH
    
    RAISE NOTICE 'Bước 4: dim_nganh';
    INSERT INTO dim_nganh (cap_do_1, cap_do_2)
    SELECT DISTINCT
        TRIM(industry_level1),
        NULLIF(TRIM(industry_level2), '')
    FROM  fact_jobs_etl
    WHERE industry_level1 IS NOT NULL
      AND TRIM(industry_level1) <> ''
      AND is_valid = TRUE
    ON CONFLICT (cap_do_1, cap_do_2) DO NOTHING;

    -- FIX: dùng WHERE NOT EXISTS vì ON CONFLICT không bắt được NULL = NULL
INSERT INTO dim_nganh (cap_do_1, cap_do_2)
SELECT 'Không xác định', 'Không xác định'
WHERE NOT EXISTS (
    SELECT 1 FROM dim_nganh
    WHERE cap_do_1 = 'Không xác định' AND cap_do_2 = 'Không xác định'
);

    
    -- 5. DIM_DIADIEM
    
    RAISE NOTICE 'Bước 5: dim_diadiem';
    INSERT INTO dim_diadiem (tinh_thanh, vung, is_vn)
    SELECT DISTINCT
        TRIM(location_province),
        TRIM(location_region),
        is_vn
    FROM  fact_jobs_etl
    WHERE location_province IS NOT NULL
      AND TRIM(location_province) <> ''
      AND is_valid = TRUE
    ON CONFLICT (tinh_thanh) DO NOTHING;

    -- FIX: dùng WHERE NOT EXISTS
    INSERT INTO dim_diadiem (tinh_thanh, vung, is_vn)
    SELECT 'Khác', NULL, TRUE
    WHERE NOT EXISTS (
        SELECT 1 FROM dim_diadiem WHERE tinh_thanh = 'Khác'
    );

    
    -- 6. DIM_CONGTY
    
    RAISE NOTICE 'Bước 6: dim_congty';
    INSERT INTO dim_congty (ten_cong_ty, canonical_key, company_type, quy_mo_min, quy_mo_max)
    SELECT
        COALESCE(NULLIF(TRIM(company_title_clean), ''), 'Unknown') AS ten_cong_ty,
        MAX(company_canonical_key)  AS canonical_key,
        MAX(company_type)           AS company_type,
        MAX(company_size_min)       AS quy_mo_min,
        MAX(company_size_max)       AS quy_mo_max
    FROM  fact_jobs_etl
    WHERE is_valid = TRUE
    GROUP BY COALESCE(NULLIF(TRIM(company_title_clean), ''), 'Unknown')
    ON CONFLICT (ten_cong_ty) DO UPDATE SET
        canonical_key = COALESCE(EXCLUDED.canonical_key, dim_congty.canonical_key),
        company_type  = COALESCE(EXCLUDED.company_type,  dim_congty.company_type),
        quy_mo_min    = COALESCE(EXCLUDED.quy_mo_min,    dim_congty.quy_mo_min),
        quy_mo_max    = COALESCE(EXCLUDED.quy_mo_max,    dim_congty.quy_mo_max);

    
    -- 7. DIM_DANHMUCCONGVIEC
    
    RAISE NOTICE 'Bước 7: dim_danhmuccongviec';
    INSERT INTO dim_danhmuccongviec (ten_danh_muc)
    SELECT DISTINCT TRIM(job_category_clean)
    FROM   fact_jobs_etl
    WHERE  job_category_clean IS NOT NULL
      AND  TRIM(job_category_clean) <> ''
      AND  is_valid = TRUE
    ON CONFLICT (ten_danh_muc) DO NOTHING;

    
    -- 8. FACT_JOBPOSTINGS
    
    RAISE NOTICE 'Bước 8: fact_jobpostings';
    INSERT INTO fact_jobpostings (
        etl_id, job_url, ngay_dang,
        dia_diem_id, cong_ty_id, nganh_id,
        cap_bac_id, hinh_thuc_id, nguon_id, danh_muc_id,
        is_it, is_duplicate, duplicate_of_id, dedup_method,
        job_title_clean, job_title_detect,
        so_luong_tuyen,
        salary_min, salary_max, salary_avg,
        salary_currency, conversion_rate,
        is_negotiable, salary_type,
        exp_min_yr, exp_max_yr, is_exp_required,
        etl_run_id, ngay_crawl, ngay_deadline,
        created_at, updated_at
    )
    WITH deduped AS (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY etl_id::integer
                       ORDER BY scraped_at DESC NULLS LAST
                   ) AS rn
            FROM fact_jobs_etl
            WHERE is_valid = TRUE
              AND (
                   p_mode = 'all'
                OR (p_mode = 'today' AND etl_run_id::text = p_run_id)
              )
        ) t
        WHERE rn = 1
    )
    SELECT
        src.etl_id::integer,
        src.job_url,
        src.job_posted_at_clean::date,
        COALESCE(dd.dia_diem_id, dd_fallback.dia_diem_id),
        ct.cong_ty_id,
        COALESCE(ng.nganh_id, ng_fallback.nganh_id),
        cb.cap_bac_id,
        ht.hinh_thuc_id,
        ns.nguon_id,
        dm.danh_muc_id,
        src.is_it,
        src.is_duplicate,
        src.duplicate_of_id::integer,
        src.dedup_method,
        src.job_title_clean,
        src.job_title_detect,
        COALESCE(src.number_recruit_clean, 1),
        src.salary_min::numeric,
        src.salary_max::numeric,
        CASE
            WHEN src.salary_min IS NOT NULL AND src.salary_max IS NOT NULL
                THEN ROUND((src.salary_min::numeric + src.salary_max::numeric) / 2.0)
            WHEN src.salary_min IS NOT NULL THEN src.salary_min::numeric
            WHEN src.salary_max IS NOT NULL THEN src.salary_max::numeric
            ELSE NULL
        END,
        src.salary_currency,
        src.conversion_rate::real,
        src.is_negotiable,
        COALESCE(
            src.salary_type,
            CASE WHEN src.is_negotiable = FALSE THEN 'monthly' ELSE 'negotiable' END
        ),
        src.exp_min_yr::real,
        src.exp_max_yr::real,
        src.is_exp_required,
        src.etl_run_id::integer,
        CASE
            WHEN src.scraped_at IS NOT NULL AND TRIM(src.scraped_at::text) <> ''
            THEN (TRIM(src.scraped_at::text))::date
            ELSE NULL
        END,
        CASE
            WHEN src.job_deadline_clean IS NOT NULL
            THEN src.job_deadline_clean::date
            ELSE NULL
        END,
        CURRENT_TIMESTAMP,
        CURRENT_TIMESTAMP

    FROM deduped src

    LEFT JOIN dim_diadiem dd
           ON TRIM(src.location_province) = dd.tinh_thanh
    LEFT JOIN dim_diadiem dd_fallback
           ON dd_fallback.tinh_thanh = 'Khác'
    LEFT JOIN dim_congty ct
           ON COALESCE(NULLIF(TRIM(src.company_title_clean), ''), 'Unknown') = ct.ten_cong_ty
    -- FIX: IS NOT DISTINCT FROM tránh NULL fanout
    LEFT JOIN dim_nganh ng
           ON TRIM(src.industry_level1) = ng.cap_do_1
          AND NULLIF(TRIM(src.industry_level2), '') IS NOT DISTINCT FROM ng.cap_do_2
    LEFT JOIN dim_nganh ng_fallback
           ON ng_fallback.cap_do_1 = 'Không xác định'
          AND ng_fallback.cap_do_2 IS NULL
    LEFT JOIN dim_capbac cb
           ON TRIM(src.level_clean) = cb.ten_cap_bac
    LEFT JOIN dim_hinhthuc ht
           ON COALESCE(NULLIF(TRIM(src.job_type_clean),  ''), 'Full-time') = ht.job_type
          AND COALESCE(NULLIF(TRIM(src.work_mode_clean), ''), 'Onsite')    = ht.work_mode
    LEFT JOIN dim_nguon ns
           ON TRIM(src.website_clean) = ns.ten_nguon
    LEFT JOIN dim_danhmuccongviec dm
           ON TRIM(src.job_category_clean) = dm.ten_danh_muc

    ON CONFLICT (etl_id) DO UPDATE SET
        updated_at       = CURRENT_TIMESTAMP,
        is_it            = EXCLUDED.is_it,
        is_duplicate     = EXCLUDED.is_duplicate,
        duplicate_of_id  = EXCLUDED.duplicate_of_id,
        dedup_method     = EXCLUDED.dedup_method,
        ngay_dang        = EXCLUDED.ngay_dang,
        job_title_clean  = EXCLUDED.job_title_clean,
        job_title_detect = EXCLUDED.job_title_detect,
        so_luong_tuyen   = EXCLUDED.so_luong_tuyen,
        salary_min       = EXCLUDED.salary_min,
        salary_max       = EXCLUDED.salary_max,
        salary_avg       = EXCLUDED.salary_avg,
        salary_currency  = EXCLUDED.salary_currency,
        conversion_rate  = EXCLUDED.conversion_rate,
        is_negotiable    = EXCLUDED.is_negotiable,
        salary_type      = EXCLUDED.salary_type,
        exp_min_yr       = EXCLUDED.exp_min_yr,
        exp_max_yr       = EXCLUDED.exp_max_yr,
        is_exp_required  = EXCLUDED.is_exp_required,
        ngay_crawl       = EXCLUDED.ngay_crawl,
        dia_diem_id      = EXCLUDED.dia_diem_id,
        nganh_id         = EXCLUDED.nganh_id,
        cong_ty_id       = EXCLUDED.cong_ty_id;

    
    -- 9. DIM_REQUIRE
    
    RAISE NOTICE 'Bước 9: dim_require';
    INSERT INTO dim_require (require_type, require_value)

    SELECT DISTINCT 'hard_skill', TRIM(val)
    FROM fact_jobs_etl
    CROSS JOIN LATERAL unnest(string_to_array(
        regexp_replace(hard_skills, '\s*,\s*', ',', 'g'), ',')) AS val
    WHERE hard_skills IS NOT NULL AND is_duplicate = FALSE AND is_valid = TRUE AND TRIM(val) <> ''

    UNION ALL

    SELECT DISTINCT 'soft_skill', TRIM(val)
    FROM fact_jobs_etl
    CROSS JOIN LATERAL unnest(string_to_array(
        regexp_replace(soft_skills, '\s*,\s*', ',', 'g'), ',')) AS val
    WHERE soft_skills IS NOT NULL AND is_duplicate = FALSE AND is_valid = TRUE AND TRIM(val) <> ''

    UNION ALL

    SELECT DISTINCT 'certification', TRIM(val)
    FROM fact_jobs_etl
    CROSS JOIN LATERAL unnest(string_to_array(
        regexp_replace(certifications, '\s*,\s*', ',', 'g'), ',')) AS val
    WHERE certifications IS NOT NULL AND is_duplicate = FALSE AND is_valid = TRUE AND TRIM(val) <> ''

    UNION ALL

    SELECT DISTINCT 'language', TRIM(val)
    FROM fact_jobs_etl
    CROSS JOIN LATERAL unnest(string_to_array(
        regexp_replace(languages, '\s*,\s*', ',', 'g'), ',')) AS val
    WHERE languages IS NOT NULL AND is_duplicate = FALSE AND is_valid = TRUE AND TRIM(val) <> ''

    UNION ALL

    SELECT DISTINCT 'major', TRIM(val)
    FROM fact_jobs_etl
    CROSS JOIN LATERAL unnest(string_to_array(
        regexp_replace(major, '\s*,\s*', ',', 'g'), ',')) AS val
    WHERE major IS NOT NULL AND is_duplicate = FALSE AND is_valid = TRUE AND TRIM(val) <> ''

    ON CONFLICT (require_type, require_value) DO NOTHING;

    
    -- 10. BRIDGE_JOBREQUIRE
    
    RAISE NOTICE 'Bước 10: bridge_jobrequire';
    INSERT INTO bridge_jobrequire (fact_id, require_id)

    SELECT f.posting_id, r.require_id
    FROM  fact_jobs_etl src
    JOIN  fact_jobpostings f ON src.etl_id::integer = f.etl_id
    CROSS JOIN LATERAL unnest(string_to_array(
        regexp_replace(src.hard_skills, '\s*,\s*', ',', 'g'), ',')) AS val
    JOIN  dim_require r ON r.require_type = 'hard_skill' AND r.require_value = TRIM(val)
    WHERE src.hard_skills IS NOT NULL AND src.is_duplicate = FALSE AND TRIM(val) <> ''
      AND (p_mode = 'all'
        OR (p_mode = 'today' AND src.etl_run_id::text = p_run_id))

    UNION ALL

    SELECT f.posting_id, r.require_id
    FROM  fact_jobs_etl src
    JOIN  fact_jobpostings f ON src.etl_id::integer = f.etl_id
    CROSS JOIN LATERAL unnest(string_to_array(
        regexp_replace(src.soft_skills, '\s*,\s*', ',', 'g'), ',')) AS val
    JOIN  dim_require r ON r.require_type = 'soft_skill' AND r.require_value = TRIM(val)
    WHERE src.soft_skills IS NOT NULL AND src.is_duplicate = FALSE AND TRIM(val) <> ''
      AND (p_mode = 'all'
        OR (p_mode = 'today' AND src.etl_run_id::text = p_run_id))

    UNION ALL

    SELECT f.posting_id, r.require_id
    FROM  fact_jobs_etl src
    JOIN  fact_jobpostings f ON src.etl_id::integer = f.etl_id
    CROSS JOIN LATERAL unnest(string_to_array(
        regexp_replace(src.certifications, '\s*,\s*', ',', 'g'), ',')) AS val
    JOIN  dim_require r ON r.require_type = 'certification' AND r.require_value = TRIM(val)
    WHERE src.certifications IS NOT NULL AND src.is_duplicate = FALSE AND TRIM(val) <> ''
      AND (p_mode = 'all'
        OR (p_mode = 'today' AND src.etl_run_id::text = p_run_id))

    UNION ALL

    SELECT f.posting_id, r.require_id
    FROM  fact_jobs_etl src
    JOIN  fact_jobpostings f ON src.etl_id::integer = f.etl_id
    CROSS JOIN LATERAL unnest(string_to_array(
        regexp_replace(src.languages, '\s*,\s*', ',', 'g'), ',')) AS val
    JOIN  dim_require r ON r.require_type = 'language' AND r.require_value = TRIM(val)
    WHERE src.languages IS NOT NULL AND src.is_duplicate = FALSE AND TRIM(val) <> ''
      AND (p_mode = 'all'
        OR (p_mode = 'today' AND src.etl_run_id::text = p_run_id))

    UNION ALL

    SELECT f.posting_id, r.require_id
    FROM  fact_jobs_etl src
    JOIN  fact_jobpostings f ON src.etl_id::integer = f.etl_id
    CROSS JOIN LATERAL unnest(string_to_array(
        regexp_replace(src.major, '\s*,\s*', ',', 'g'), ',')) AS val
    JOIN  dim_require r ON r.require_type = 'major' AND r.require_value = TRIM(val)
    WHERE src.major IS NOT NULL AND src.is_duplicate = FALSE AND TRIM(val) <> ''
      AND (p_mode = 'all'
        OR (p_mode = 'today' AND src.etl_run_id::text = p_run_id))

    ON CONFLICT (fact_id, require_id) DO NOTHING;

    RETURN 'ETL DW hoan tat - mode=' || p_mode ||
           COALESCE(' run_id=' || p_run_id, '');

EXCEPTION
    WHEN OTHERS THEN
        SET session_replication_role = 'origin';
        RAISE EXCEPTION 'sp_etl_load_dw FAILED [mode=%] — %: %',
            p_mode, SQLSTATE, SQLERRM;

END;
$BODY$;

ALTER FUNCTION public.sp_etl_load_dw(character varying, character varying)
    OWNER TO postgres;

