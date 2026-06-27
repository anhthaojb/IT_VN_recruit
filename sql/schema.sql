--
-- PostgreSQL database dump
--

\restrict uCK61kN6AOZ2FeNkAMjyuUSTFfVFss4ZiJ6oVN2UhXt8vbGCy1TGDgovxRW5SZn

-- Dumped from database version 18.3
-- Dumped by pg_dump version 18.3

-- Started on 2026-06-27 17:31:53

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 5434 (class 1262 OID 48929)
-- Name: recruitment_dw; Type: DATABASE; Schema: -; Owner: postgres
--

CREATE DATABASE recruitment_dw WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE_PROVIDER = libc LOCALE = 'English_United States.1252';


ALTER DATABASE recruitment_dw OWNER TO postgres;

\unrestrict uCK61kN6AOZ2FeNkAMjyuUSTFfVFss4ZiJ6oVN2UhXt8vbGCy1TGDgovxRW5SZn
\connect recruitment_dw
\restrict uCK61kN6AOZ2FeNkAMjyuUSTFfVFss4ZiJ6oVN2UhXt8vbGCy1TGDgovxRW5SZn

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 7 (class 2615 OID 2200)
-- Name: public; Type: SCHEMA; Schema: -; Owner: pg_database_owner
--

CREATE SCHEMA public;


ALTER SCHEMA public OWNER TO pg_database_owner;

--
-- TOC entry 5435 (class 0 OID 0)
-- Dependencies: 7
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: pg_database_owner
--

COMMENT ON SCHEMA public IS 'standard public schema';


--
-- TOC entry 398 (class 1255 OID 49155)
-- Name: fn_set_updated_at(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.fn_set_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;


ALTER FUNCTION public.fn_set_updated_at() OWNER TO postgres;

--
-- TOC entry 386 (class 1255 OID 143868)
-- Name: get_table_script(text); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.get_table_script(target_table text) RETURNS text
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_table_ddl text;
    v_column_record record;
    v_pk_record record;
BEGIN
    -- 1. Khởi tạo câu lệnh CREATE TABLE
    v_table_ddl := 'CREATE TABLE ' || target_table || ' (' || chr(10);

    -- 2. Quét và nối các cột, kiểu dữ liệu, ràng buộc NULL
    FOR v_column_record IN 
        SELECT 
            column_name, 
            data_type,
            character_maximum_length,
            is_nullable
        FROM information_schema.columns
        WHERE table_name = target_table
        ORDER BY ordinal_position
    LOOP
        v_table_ddl := v_table_ddl || '    ' || v_column_record.column_name || ' ' || v_column_record.data_type;
        
        -- Nếu có độ dài dữ liệu (ví dụ varchar(255))
        IF v_column_record.character_maximum_length IS NOT NULL THEN
            v_table_ddl := v_table_ddl || '(' || v_column_record.character_maximum_length || ')';
        END IF;
        
        -- Thêm NOT NULL nếu có
        IF v_column_record.is_nullable = 'NO' THEN
            v_table_ddl := v_table_ddl || ' NOT NULL';
        END IF;
        
        v_table_ddl := v_table_ddl || ',' || chr(10);
    END LOOP;

    -- 3. Tìm và thêm khóa chính (Primary Key) nếu có
    SELECT a.attname INTO v_pk_record
    FROM pg_index i
    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
    WHERE i.indrelid = target_table::regclass AND i.indisprimary;

    IF FOUND THEN
        v_table_ddl := v_table_ddl || '    CONSTRAINT ' || target_table || '_pkey PRIMARY KEY (' || v_pk_record.attname || ')' || chr(10);
    ELSE
        -- Xóa dấu phẩy thừa ở cột cuối nếu không có PK
        v_table_ddl := rtrim(v_table_ddl, ',' || chr(10)) || chr(10);
    END IF;

    v_table_ddl := v_table_ddl || ');';
    RETURN v_table_ddl;
END;
$$;


ALTER FUNCTION public.get_table_script(target_table text) OWNER TO postgres;

--
-- TOC entry 320 (class 1255 OID 143461)
-- Name: sp_etl_load_dw(character varying, character varying); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.sp_etl_load_dw(p_mode character varying, p_run_id character varying DEFAULT NULL::character varying) RETURNS text
    LANGUAGE plpgsql
    AS $$
BEGIN

    -- =========================================================================
    -- 0. TRUNCATE KHI MODE = 'all'
    -- =========================================================================
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

    -- =========================================================================
    -- 1. DIM_NGUON
    -- =========================================================================
    RAISE NOTICE 'Bước 1: dim_nguon';
    INSERT INTO dim_nguon (ten_nguon)
    SELECT DISTINCT TRIM(website_clean)
    FROM   fact_jobs_etl
    WHERE  website_clean IS NOT NULL
      AND  TRIM(website_clean) <> ''
      AND  is_valid = TRUE
    ON CONFLICT (ten_nguon) DO NOTHING;

    -- =========================================================================
    -- 2. DIM_CAPBAC
    -- =========================================================================
    RAISE NOTICE 'Bước 2: dim_capbac';
    INSERT INTO dim_capbac (ten_cap_bac)
    SELECT DISTINCT TRIM(level_clean)
    FROM   fact_jobs_etl
    WHERE  level_clean IS NOT NULL
      AND  TRIM(level_clean) <> ''
      AND  is_valid = TRUE
    ON CONFLICT (ten_cap_bac) DO NOTHING;

    -- =========================================================================
    -- 3. DIM_HINHTHUC
    -- =========================================================================
    RAISE NOTICE 'Bước 3: dim_hinhthuc';
    INSERT INTO dim_hinhthuc (job_type, work_mode)
    SELECT DISTINCT
        COALESCE(NULLIF(TRIM(job_type_clean),  ''), 'Full-time'),
        COALESCE(NULLIF(TRIM(work_mode_clean), ''), 'Onsite')
    FROM fact_jobs_etl
    WHERE is_valid = TRUE
    ON CONFLICT (job_type, work_mode) DO NOTHING;

    -- =========================================================================
    -- 4. DIM_NGANH
    -- =========================================================================
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

    -- =========================================================================
    -- 5. DIM_DIADIEM
    -- =========================================================================
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

    -- =========================================================================
    -- 6. DIM_CONGTY
    -- =========================================================================
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

    -- =========================================================================
    -- 7. DIM_DANHMUCCONGVIEC
    -- =========================================================================
    RAISE NOTICE 'Bước 7: dim_danhmuccongviec';
    INSERT INTO dim_danhmuccongviec (ten_danh_muc)
    SELECT DISTINCT TRIM(job_category_clean)
    FROM   fact_jobs_etl
    WHERE  job_category_clean IS NOT NULL
      AND  TRIM(job_category_clean) <> ''
      AND  is_valid = TRUE
    ON CONFLICT (ten_danh_muc) DO NOTHING;

    -- =========================================================================
    -- 8. FACT_JOBPOSTINGS
    -- FIX 1: dedup etl_id bằng ROW_NUMBER
    -- FIX 2: JOIN dim_nganh dùng IS NOT DISTINCT FROM để tránh NULL fanout
    -- =========================================================================
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

    -- =========================================================================
    -- 9. DIM_REQUIRE
    -- =========================================================================
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

    -- =========================================================================
    -- 10. BRIDGE_JOBREQUIRE
    -- =========================================================================
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
$$;


ALTER FUNCTION public.sp_etl_load_dw(p_mode character varying, p_run_id character varying) OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 259 (class 1259 OID 49502)
-- Name: bridge_jobrequire; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.bridge_jobrequire (
    bridge_id integer NOT NULL,
    fact_id integer,
    require_id integer
);


ALTER TABLE public.bridge_jobrequire OWNER TO postgres;

--
-- TOC entry 292 (class 1259 OID 88290)
-- Name: bridge_jobrequire_bridge_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.bridge_jobrequire_bridge_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.bridge_jobrequire_bridge_id_seq OWNER TO postgres;

--
-- TOC entry 5436 (class 0 OID 0)
-- Dependencies: 292
-- Name: bridge_jobrequire_bridge_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.bridge_jobrequire_bridge_id_seq OWNED BY public.bridge_jobrequire.bridge_id;


--
-- TOC entry 294 (class 1259 OID 114208)
-- Name: dim_capbac_cap_bac_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.dim_capbac_cap_bac_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_capbac_cap_bac_id_seq OWNER TO postgres;

--
-- TOC entry 260 (class 1259 OID 49505)
-- Name: dim_capbac; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dim_capbac (
    cap_bac_id integer DEFAULT nextval('public.dim_capbac_cap_bac_id_seq'::regclass),
    ten_cap_bac text
);


ALTER TABLE public.dim_capbac OWNER TO postgres;

--
-- TOC entry 295 (class 1259 OID 114210)
-- Name: dim_congty_cong_ty_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.dim_congty_cong_ty_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_congty_cong_ty_id_seq OWNER TO postgres;

--
-- TOC entry 261 (class 1259 OID 49510)
-- Name: dim_congty; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dim_congty (
    cong_ty_id integer DEFAULT nextval('public.dim_congty_cong_ty_id_seq'::regclass),
    ten_cong_ty text,
    canonical_key text,
    company_type text,
    quy_mo_min real,
    quy_mo_max real
);


ALTER TABLE public.dim_congty OWNER TO postgres;

--
-- TOC entry 296 (class 1259 OID 114212)
-- Name: dim_danhmuccongviec_danh_muc_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.dim_danhmuccongviec_danh_muc_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_danhmuccongviec_danh_muc_id_seq OWNER TO postgres;

--
-- TOC entry 262 (class 1259 OID 49515)
-- Name: dim_danhmuccongviec; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dim_danhmuccongviec (
    danh_muc_id integer DEFAULT nextval('public.dim_danhmuccongviec_danh_muc_id_seq'::regclass),
    ten_danh_muc text
);


ALTER TABLE public.dim_danhmuccongviec OWNER TO postgres;

--
-- TOC entry 297 (class 1259 OID 114214)
-- Name: dim_diadiem_dia_diem_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.dim_diadiem_dia_diem_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_diadiem_dia_diem_id_seq OWNER TO postgres;

--
-- TOC entry 263 (class 1259 OID 49520)
-- Name: dim_diadiem; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dim_diadiem (
    dia_diem_id integer DEFAULT nextval('public.dim_diadiem_dia_diem_id_seq'::regclass) NOT NULL,
    tinh_thanh text,
    vung text,
    is_vn boolean
);


ALTER TABLE public.dim_diadiem OWNER TO postgres;

--
-- TOC entry 298 (class 1259 OID 114216)
-- Name: dim_hinhthuc_hinh_thuc_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.dim_hinhthuc_hinh_thuc_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_hinhthuc_hinh_thuc_id_seq OWNER TO postgres;

--
-- TOC entry 264 (class 1259 OID 49525)
-- Name: dim_hinhthuc; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dim_hinhthuc (
    hinh_thuc_id integer DEFAULT nextval('public.dim_hinhthuc_hinh_thuc_id_seq'::regclass),
    job_type text,
    work_mode text
);


ALTER TABLE public.dim_hinhthuc OWNER TO postgres;

--
-- TOC entry 299 (class 1259 OID 114218)
-- Name: dim_nganh_nganh_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.dim_nganh_nganh_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_nganh_nganh_id_seq OWNER TO postgres;

--
-- TOC entry 265 (class 1259 OID 49530)
-- Name: dim_nganh; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dim_nganh (
    nganh_id integer DEFAULT nextval('public.dim_nganh_nganh_id_seq'::regclass),
    cap_do_1 text,
    cap_do_2 text
);


ALTER TABLE public.dim_nganh OWNER TO postgres;

--
-- TOC entry 300 (class 1259 OID 114220)
-- Name: dim_nguon_nguon_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.dim_nguon_nguon_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_nguon_nguon_id_seq OWNER TO postgres;

--
-- TOC entry 266 (class 1259 OID 49535)
-- Name: dim_nguon; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dim_nguon (
    nguon_id integer DEFAULT nextval('public.dim_nguon_nguon_id_seq'::regclass),
    ten_nguon text
);


ALTER TABLE public.dim_nguon OWNER TO postgres;

--
-- TOC entry 301 (class 1259 OID 114222)
-- Name: dim_require_require_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.dim_require_require_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.dim_require_require_id_seq OWNER TO postgres;

--
-- TOC entry 267 (class 1259 OID 49540)
-- Name: dim_require; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.dim_require (
    require_id integer DEFAULT nextval('public.dim_require_require_id_seq'::regclass),
    require_type text,
    require_value text
);


ALTER TABLE public.dim_require OWNER TO postgres;

--
-- TOC entry 268 (class 1259 OID 49545)
-- Name: fact_error_detail; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.fact_error_detail (
    error_id integer NOT NULL,
    run_id integer,
    row_id integer,
    column_name text,
    bad_value text,
    error_type text,
    created_at timestamp with time zone
);


ALTER TABLE public.fact_error_detail OWNER TO postgres;

--
-- TOC entry 304 (class 1259 OID 145476)
-- Name: fact_etl_error_error_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.fact_etl_error_error_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.fact_etl_error_error_id_seq OWNER TO postgres;

--
-- TOC entry 269 (class 1259 OID 49551)
-- Name: fact_etl_error; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.fact_etl_error (
    error_id integer DEFAULT nextval('public.fact_etl_error_error_id_seq'::regclass),
    run_id integer,
    src_id integer,
    job_url text,
    field_name text,
    raw_value text,
    error_type text,
    error_detail text,
    created_at timestamp with time zone
);


ALTER TABLE public.fact_etl_error OWNER TO postgres;

--
-- TOC entry 270 (class 1259 OID 49556)
-- Name: fact_etl_log_run_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.fact_etl_log_run_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.fact_etl_log_run_id_seq OWNER TO postgres;

--
-- TOC entry 271 (class 1259 OID 49557)
-- Name: fact_etl_log; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.fact_etl_log (
    run_id integer DEFAULT nextval('public.fact_etl_log_run_id_seq'::regclass) NOT NULL,
    run_date date,
    mode text,
    target_date date,
    started_at timestamp with time zone,
    finished_at timestamp with time zone,
    duration_sec real,
    total_input integer,
    total_output integer,
    new_rows integer,
    updated_rows integer,
    error_rows integer,
    status text,
    note text
);


ALTER TABLE public.fact_etl_log OWNER TO postgres;

--
-- TOC entry 272 (class 1259 OID 49564)
-- Name: fact_jobpostings; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.fact_jobpostings (
    posting_id integer NOT NULL,
    etl_id integer,
    job_url text,
    dia_diem_id integer,
    cong_ty_id integer,
    nganh_id integer,
    cap_bac_id integer,
    hinh_thuc_id integer,
    nguon_id integer,
    danh_muc_id integer,
    job_title_clean text,
    job_title_detect text,
    so_luong_tuyen numeric,
    salary_min numeric,
    salary_max numeric,
    salary_avg numeric,
    salary_currency text,
    conversion_rate real,
    is_negotiable boolean,
    exp_min_yr real,
    exp_max_yr real,
    is_exp_required boolean,
    is_it boolean,
    is_duplicate boolean,
    duplicate_of_id integer,
    dedup_method text,
    ngay_dang date,
    etl_run_id integer,
    ngay_crawl date,
    ngay_deadline date,
    created_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    salary_type character varying(20)
);


ALTER TABLE public.fact_jobpostings OWNER TO postgres;

--
-- TOC entry 273 (class 1259 OID 49570)
-- Name: fact_jobpostings_posting_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.fact_jobpostings_posting_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.fact_jobpostings_posting_id_seq OWNER TO postgres;

--
-- TOC entry 5437 (class 0 OID 0)
-- Dependencies: 273
-- Name: fact_jobpostings_posting_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.fact_jobpostings_posting_id_seq OWNED BY public.fact_jobpostings.posting_id;


--
-- TOC entry 302 (class 1259 OID 136387)
-- Name: fact_jobs_etl_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.fact_jobs_etl_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.fact_jobs_etl_id_seq OWNER TO postgres;

--
-- TOC entry 289 (class 1259 OID 50145)
-- Name: fact_jobs_etl; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.fact_jobs_etl (
    etl_id bigint DEFAULT nextval('public.fact_jobs_etl_id_seq'::regclass),
    src_id bigint,
    job_url text,
    scraped_at text,
    etl_run_id bigint,
    etl_processed_at timestamp without time zone,
    website text,
    website_clean text,
    job_posted_at text,
    job_deadline text,
    job_posted_at_clean date,
    job_deadline_clean date,
    job_title text,
    job_title_detect text,
    job_title_clean text,
    job_category_clean text,
    is_it boolean,
    company_title text,
    company_title_clean text,
    company_type text,
    company_canonical_key text,
    location text,
    location_province text,
    location_region text,
    is_vn boolean,
    job_type text,
    work_mode text,
    job_type_clean text,
    work_mode_clean text,
    compensation text,
    salary_min double precision,
    salary_max double precision,
    salary_currency text,
    conversion_rate double precision,
    is_negotiable boolean,
    experience text,
    exp_min_yr double precision,
    exp_max_yr double precision,
    is_exp_required boolean,
    level text,
    level_clean text,
    job_description text,
    job_requirement text,
    hard_skills text,
    soft_skills text,
    major text,
    certifications text,
    languages text,
    education_level text,
    education_clean text,
    company_size text,
    company_size_min double precision,
    company_size_max double precision,
    company_industry text,
    industry_level1 text,
    industry_level2 text,
    job_category text,
    number_recruit text,
    number_recruit_clean bigint,
    is_valid boolean,
    is_duplicate boolean,
    duplicate_of_id double precision,
    dedup_method text,
    error_log text,
    salary_type text
);


ALTER TABLE public.fact_jobs_etl OWNER TO postgres;

--
-- TOC entry 274 (class 1259 OID 49571)
-- Name: fact_jobs_etl_etl_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.fact_jobs_etl_etl_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.fact_jobs_etl_etl_id_seq OWNER TO postgres;

--
-- TOC entry 293 (class 1259 OID 114206)
-- Name: fact_pipeline_snapshot_run_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.fact_pipeline_snapshot_run_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.fact_pipeline_snapshot_run_id_seq OWNER TO postgres;

--
-- TOC entry 291 (class 1259 OID 88075)
-- Name: fact_pipeline_snapshot; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.fact_pipeline_snapshot (
    run_id bigint DEFAULT nextval('public.fact_pipeline_snapshot_run_id_seq'::regclass),
    website text,
    started_at timestamp without time zone,
    finished_at timestamp without time zone,
    duration_sec bigint,
    total_scraped bigint,
    new_jobs bigint,
    updated_jobs bigint,
    duplicate_jobs bigint,
    invalid_jobs bigint,
    error_jobs bigint,
    status text,
    session_id text,
    triggered_by text
);


ALTER TABLE public.fact_pipeline_snapshot OWNER TO postgres;

--
-- TOC entry 303 (class 1259 OID 136389)
-- Name: staging_jobs_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.staging_jobs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.staging_jobs_id_seq OWNER TO postgres;

--
-- TOC entry 290 (class 1259 OID 77020)
-- Name: staging_jobs; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.staging_jobs (
    id bigint DEFAULT nextval('public.staging_jobs_id_seq'::regclass),
    website text,
    job_title text,
    company_title text,
    location text,
    experience text,
    compensation text,
    job_type text,
    work_mode text,
    level text,
    job_url text,
    company_size text,
    company_industry text,
    job_category text,
    number_recruit text,
    education_level text,
    job_description text,
    job_requirement text,
    job_posted_at text,
    job_deadline text,
    scraped_at text,
    is_valid boolean,
    error_log text,
    ai_processed boolean
);


ALTER TABLE public.staging_jobs OWNER TO postgres;

--
-- TOC entry 275 (class 1259 OID 49594)
-- Name: vw_jobpostings_all; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.vw_jobpostings_all AS
 SELECT posting_id,
    etl_id,
    job_url,
    dia_diem_id,
    cong_ty_id,
    nganh_id,
    cap_bac_id,
    hinh_thuc_id,
    nguon_id,
    danh_muc_id,
    job_title_clean,
    job_title_detect,
    so_luong_tuyen,
    salary_min,
    salary_max,
    salary_avg,
    salary_currency,
    conversion_rate,
    is_negotiable,
    exp_min_yr,
    exp_max_yr,
    is_exp_required,
    is_it,
    is_duplicate,
    duplicate_of_id,
    dedup_method,
    ngay_dang,
    etl_run_id,
    ngay_crawl,
    ngay_deadline,
    created_at,
    updated_at,
    salary_type
   FROM public.fact_jobpostings;


ALTER VIEW public.vw_jobpostings_all OWNER TO postgres;

--
-- TOC entry 276 (class 1259 OID 49599)
-- Name: vw_jobpostings_unique; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.vw_jobpostings_unique AS
 SELECT posting_id,
    etl_id,
    job_url,
    dia_diem_id,
    cong_ty_id,
    nganh_id,
    cap_bac_id,
    hinh_thuc_id,
    nguon_id,
    danh_muc_id,
    job_title_clean,
    job_title_detect,
    so_luong_tuyen,
    salary_min,
    salary_max,
    salary_avg,
    salary_currency,
    conversion_rate,
    is_negotiable,
    exp_min_yr,
    exp_max_yr,
    is_exp_required,
    is_it,
    is_duplicate,
    duplicate_of_id,
    dedup_method,
    ngay_dang,
    etl_run_id,
    ngay_crawl,
    ngay_deadline,
    created_at,
    updated_at,
    salary_type
   FROM public.fact_jobpostings
  WHERE (is_duplicate = false);


ALTER VIEW public.vw_jobpostings_unique OWNER TO postgres;

--
-- TOC entry 5193 (class 2604 OID 88291)
-- Name: bridge_jobrequire bridge_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bridge_jobrequire ALTER COLUMN bridge_id SET DEFAULT nextval('public.bridge_jobrequire_bridge_id_seq'::regclass);


--
-- TOC entry 5204 (class 2604 OID 49745)
-- Name: fact_jobpostings posting_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_jobpostings ALTER COLUMN posting_id SET DEFAULT nextval('public.fact_jobpostings_posting_id_seq'::regclass);


--
-- TOC entry 5234 (class 2606 OID 49815)
-- Name: fact_error_detail fact_error_detail_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_error_detail
    ADD CONSTRAINT fact_error_detail_pkey PRIMARY KEY (error_id);


--
-- TOC entry 5239 (class 2606 OID 49817)
-- Name: fact_etl_log fact_etl_log_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_etl_log
    ADD CONSTRAINT fact_etl_log_pkey PRIMARY KEY (run_id);


--
-- TOC entry 5213 (class 2606 OID 49827)
-- Name: bridge_jobrequire uq_bridge_jobrequire_pair; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.bridge_jobrequire
    ADD CONSTRAINT uq_bridge_jobrequire_pair UNIQUE (fact_id, require_id);


--
-- TOC entry 5215 (class 2606 OID 49829)
-- Name: dim_capbac uq_dim_capbac_ten; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_capbac
    ADD CONSTRAINT uq_dim_capbac_ten UNIQUE (ten_cap_bac);


--
-- TOC entry 5218 (class 2606 OID 49831)
-- Name: dim_congty uq_dim_congty; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_congty
    ADD CONSTRAINT uq_dim_congty UNIQUE (ten_cong_ty);


--
-- TOC entry 5220 (class 2606 OID 49833)
-- Name: dim_danhmuccongviec uq_dim_danhmuc_ten; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_danhmuccongviec
    ADD CONSTRAINT uq_dim_danhmuc_ten UNIQUE (ten_danh_muc);


--
-- TOC entry 5222 (class 2606 OID 49835)
-- Name: dim_diadiem uq_dim_diadiem; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_diadiem
    ADD CONSTRAINT uq_dim_diadiem UNIQUE (tinh_thanh);


--
-- TOC entry 5224 (class 2606 OID 49837)
-- Name: dim_hinhthuc uq_dim_hinhthuc; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_hinhthuc
    ADD CONSTRAINT uq_dim_hinhthuc UNIQUE (job_type, work_mode);


--
-- TOC entry 5226 (class 2606 OID 49839)
-- Name: dim_nganh uq_dim_nganh; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_nganh
    ADD CONSTRAINT uq_dim_nganh UNIQUE (cap_do_1, cap_do_2);


--
-- TOC entry 5228 (class 2606 OID 88262)
-- Name: dim_nganh uq_dim_nganh_cap_do; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_nganh
    ADD CONSTRAINT uq_dim_nganh_cap_do UNIQUE (cap_do_1, cap_do_2);


--
-- TOC entry 5230 (class 2606 OID 49841)
-- Name: dim_nguon uq_dim_nguon_ten; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_nguon
    ADD CONSTRAINT uq_dim_nguon_ten UNIQUE (ten_nguon);


--
-- TOC entry 5232 (class 2606 OID 49843)
-- Name: dim_require uq_dim_require; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.dim_require
    ADD CONSTRAINT uq_dim_require UNIQUE (require_type, require_value);


--
-- TOC entry 5247 (class 2606 OID 49845)
-- Name: fact_jobpostings uq_fact_jobpostings_etl_id; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fact_jobpostings
    ADD CONSTRAINT uq_fact_jobpostings_etl_id UNIQUE (etl_id);


--
-- TOC entry 5248 (class 1259 OID 114229)
-- Name: fact_jobs_etl_etl_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX fact_jobs_etl_etl_id_idx ON public.fact_jobs_etl USING btree (etl_id);


--
-- TOC entry 5249 (class 1259 OID 114228)
-- Name: fact_jobs_etl_is_valid_etl_run_id_idx; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX fact_jobs_etl_is_valid_etl_run_id_idx ON public.fact_jobs_etl USING btree (is_valid, etl_run_id);


--
-- TOC entry 5210 (class 1259 OID 49928)
-- Name: idx_bridge_fact; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_bridge_fact ON public.bridge_jobrequire USING btree (fact_id);


--
-- TOC entry 5211 (class 1259 OID 49929)
-- Name: idx_bridge_require; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_bridge_require ON public.bridge_jobrequire USING btree (require_id);


--
-- TOC entry 5216 (class 1259 OID 49930)
-- Name: idx_congty_canonical; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_congty_canonical ON public.dim_congty USING btree (canonical_key);


--
-- TOC entry 5235 (class 1259 OID 49935)
-- Name: idx_etl_err_field; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_etl_err_field ON public.fact_etl_error USING btree (field_name);


--
-- TOC entry 5236 (class 1259 OID 49936)
-- Name: idx_etl_err_run; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_etl_err_run ON public.fact_etl_error USING btree (run_id);


--
-- TOC entry 5237 (class 1259 OID 49937)
-- Name: idx_etl_err_src; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_etl_err_src ON public.fact_etl_error USING btree (src_id);


--
-- TOC entry 5240 (class 1259 OID 49949)
-- Name: idx_fp_cap_bac; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_fp_cap_bac ON public.fact_jobpostings USING btree (cap_bac_id);


--
-- TOC entry 5241 (class 1259 OID 49950)
-- Name: idx_fp_cong_ty; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_fp_cong_ty ON public.fact_jobpostings USING btree (cong_ty_id);


--
-- TOC entry 5242 (class 1259 OID 49951)
-- Name: idx_fp_danh_muc; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_fp_danh_muc ON public.fact_jobpostings USING btree (danh_muc_id);


--
-- TOC entry 5243 (class 1259 OID 49952)
-- Name: idx_fp_nganh; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_fp_nganh ON public.fact_jobpostings USING btree (nganh_id);


--
-- TOC entry 5244 (class 1259 OID 49953)
-- Name: idx_fp_nguon; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_fp_nguon ON public.fact_jobpostings USING btree (nguon_id);


--
-- TOC entry 5245 (class 1259 OID 49954)
-- Name: idx_fp_title_clean; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX idx_fp_title_clean ON public.fact_jobpostings USING btree (job_title_clean);


--
-- TOC entry 5250 (class 1259 OID 113416)
-- Name: uix_fact_jobs_etl_url_province; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX uix_fact_jobs_etl_url_province ON public.fact_jobs_etl USING btree (job_url, location_province);


--
-- TOC entry 5251 (class 1259 OID 136106)
-- Name: uq_staging_jobs_job_url; Type: INDEX; Schema: public; Owner: postgres
--

CREATE UNIQUE INDEX uq_staging_jobs_job_url ON public.staging_jobs USING btree (job_url);


--
-- TOC entry 5252 (class 2620 OID 49972)
-- Name: fact_jobpostings trg_fp_updated_at; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER trg_fp_updated_at BEFORE UPDATE ON public.fact_jobpostings FOR EACH ROW EXECUTE FUNCTION public.fn_set_updated_at();


--
-- TOC entry 5417 (class 3256 OID 50093)
-- Name: bridge_jobrequire allow_select; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY allow_select ON public.bridge_jobrequire FOR SELECT USING (true);


--
-- TOC entry 5418 (class 3256 OID 50094)
-- Name: dim_capbac allow_select; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY allow_select ON public.dim_capbac FOR SELECT USING (true);


--
-- TOC entry 5419 (class 3256 OID 50095)
-- Name: dim_congty allow_select; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY allow_select ON public.dim_congty FOR SELECT USING (true);


--
-- TOC entry 5420 (class 3256 OID 50096)
-- Name: dim_danhmuccongviec allow_select; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY allow_select ON public.dim_danhmuccongviec FOR SELECT USING (true);


--
-- TOC entry 5421 (class 3256 OID 50097)
-- Name: dim_diadiem allow_select; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY allow_select ON public.dim_diadiem FOR SELECT USING (true);


--
-- TOC entry 5422 (class 3256 OID 50098)
-- Name: dim_hinhthuc allow_select; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY allow_select ON public.dim_hinhthuc FOR SELECT USING (true);


--
-- TOC entry 5423 (class 3256 OID 50099)
-- Name: dim_nganh allow_select; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY allow_select ON public.dim_nganh FOR SELECT USING (true);


--
-- TOC entry 5424 (class 3256 OID 50100)
-- Name: dim_nguon allow_select; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY allow_select ON public.dim_nguon FOR SELECT USING (true);


--
-- TOC entry 5425 (class 3256 OID 50101)
-- Name: dim_require allow_select; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY allow_select ON public.dim_require FOR SELECT USING (true);


--
-- TOC entry 5426 (class 3256 OID 50102)
-- Name: fact_etl_log allow_select; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY allow_select ON public.fact_etl_log FOR SELECT USING (true);


--
-- TOC entry 5427 (class 3256 OID 50103)
-- Name: fact_jobpostings allow_select; Type: POLICY; Schema: public; Owner: postgres
--

CREATE POLICY allow_select ON public.fact_jobpostings FOR SELECT USING (true);


--
-- TOC entry 5404 (class 0 OID 49502)
-- Dependencies: 259
-- Name: bridge_jobrequire; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.bridge_jobrequire ENABLE ROW LEVEL SECURITY;

--
-- TOC entry 5405 (class 0 OID 49505)
-- Dependencies: 260
-- Name: dim_capbac; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.dim_capbac ENABLE ROW LEVEL SECURITY;

--
-- TOC entry 5406 (class 0 OID 49510)
-- Dependencies: 261
-- Name: dim_congty; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.dim_congty ENABLE ROW LEVEL SECURITY;

--
-- TOC entry 5407 (class 0 OID 49515)
-- Dependencies: 262
-- Name: dim_danhmuccongviec; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.dim_danhmuccongviec ENABLE ROW LEVEL SECURITY;

--
-- TOC entry 5408 (class 0 OID 49520)
-- Dependencies: 263
-- Name: dim_diadiem; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.dim_diadiem ENABLE ROW LEVEL SECURITY;

--
-- TOC entry 5409 (class 0 OID 49525)
-- Dependencies: 264
-- Name: dim_hinhthuc; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.dim_hinhthuc ENABLE ROW LEVEL SECURITY;

--
-- TOC entry 5410 (class 0 OID 49530)
-- Dependencies: 265
-- Name: dim_nganh; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.dim_nganh ENABLE ROW LEVEL SECURITY;

--
-- TOC entry 5411 (class 0 OID 49535)
-- Dependencies: 266
-- Name: dim_nguon; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.dim_nguon ENABLE ROW LEVEL SECURITY;

--
-- TOC entry 5412 (class 0 OID 49540)
-- Dependencies: 267
-- Name: dim_require; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.dim_require ENABLE ROW LEVEL SECURITY;

--
-- TOC entry 5413 (class 0 OID 49545)
-- Dependencies: 268
-- Name: fact_error_detail; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.fact_error_detail ENABLE ROW LEVEL SECURITY;

--
-- TOC entry 5414 (class 0 OID 49551)
-- Dependencies: 269
-- Name: fact_etl_error; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.fact_etl_error ENABLE ROW LEVEL SECURITY;

--
-- TOC entry 5415 (class 0 OID 49557)
-- Dependencies: 271
-- Name: fact_etl_log; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.fact_etl_log ENABLE ROW LEVEL SECURITY;

--
-- TOC entry 5416 (class 0 OID 49564)
-- Dependencies: 272
-- Name: fact_jobpostings; Type: ROW SECURITY; Schema: public; Owner: postgres
--

ALTER TABLE public.fact_jobpostings ENABLE ROW LEVEL SECURITY;

-- Completed on 2026-06-27 17:31:53

--
-- PostgreSQL database dump complete
--

\unrestrict uCK61kN6AOZ2FeNkAMjyuUSTFfVFss4ZiJ6oVN2UhXt8vbGCy1TGDgovxRW5SZn

