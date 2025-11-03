-- Schema Creation

CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS pre_dwh;
CREATE SCHEMA IF NOT EXISTS dwh;



-- Raw tables
CREATE TABLE IF NOT EXISTS stg.abr_raw_companies
(
    id integer,
    abn character varying(20) COLLATE pg_catalog."default",
    entity_name text COLLATE pg_catalog."default",
    entity_type text COLLATE pg_catalog."default",
    entity_status character varying(50) COLLATE pg_catalog."default",
    address text COLLATE pg_catalog."default",
    postcode character varying(20) COLLATE pg_catalog."default",
    state character varying(20) COLLATE pg_catalog."default",
    start_date date,
    created_at timestamp without time zone
)


CREATE TABLE IF NOT EXISTS stg.common_crawl_raw_companies
(
    id integer NOT NULL DEFAULT nextval('stg.common_crawl_raw_companies_id_seq'::regclass),
    url text COLLATE pg_catalog."default" NOT NULL,
    domain text COLLATE pg_catalog."default" NOT NULL,
    company_name text COLLATE pg_catalog."default",
    abn character(20) COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    emails text[] COLLATE pg_catalog."default",
    phones text[] COLLATE pg_catalog."default",
    postcode character(20) COLLATE pg_catalog."default",
    structured_data jsonb,
    snippet text COLLATE pg_catalog."default",
    created_at timestamp without time zone DEFAULT now(),
    CONSTRAINT common_crawl_raw_companies_pkey PRIMARY KEY (id)
)

-- pre dwh layer

CREATE TABLE IF NOT EXISTS pre_dwh.cleaned_abr_companies
(
    id text COLLATE pg_catalog."default",
    abn text COLLATE pg_catalog."default",
    entity_name text COLLATE pg_catalog."default",
    entity_type text COLLATE pg_catalog."default",
    entity_status text COLLATE pg_catalog."default",
    address text COLLATE pg_catalog."default",
    postcode text COLLATE pg_catalog."default",
    state text COLLATE pg_catalog."default",
    start_date text COLLATE pg_catalog."default",
    created_at timestamp without time zone DEFAULT now()
)


CREATE TABLE IF NOT EXISTS pre_dwh.cleaned_commoncrawl_companies
(
    id text COLLATE pg_catalog."default",
    url text COLLATE pg_catalog."default",
    domain text COLLATE pg_catalog."default",
    company_name text COLLATE pg_catalog."default",
    abn text COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    emails text COLLATE pg_catalog."default",
    phones text COLLATE pg_catalog."default",
    postcode text COLLATE pg_catalog."default",
    structured_data text COLLATE pg_catalog."default",
    snippet text COLLATE pg_catalog."default",
    created_at timestamp without time zone DEFAULT now()
)
--DWH 

CREATE TABLE IF NOT EXISTS dwh.dim_entity_match_company_data
(
    crawl_domain text COLLATE pg_catalog."default",
    crawl_company_name text COLLATE pg_catalog."default",
    crawl_abn character(20) COLLATE pg_catalog."default",
    abr_abn character varying(20) COLLATE pg_catalog."default",
    abr_company_name text COLLATE pg_catalog."default",
    abr_entity_type text COLLATE pg_catalog."default",
    abr_state text COLLATE pg_catalog."default",
    abr_postcode character varying(20) COLLATE pg_catalog."default",
    match_method text COLLATE pg_catalog."default",
    match_score numeric,
    match_confidence text COLLATE pg_catalog."default",
    created_at timestamp without time zone DEFAULT now(),
    creation_dt timestamp without time zone DEFAULT now()
)

--- Indexing and optimization
-- Raw Commoncrawl Companies
CREATE INDEX idx_stg_commoncrawl_abn 
ON prd_firmable.stg.raw_commoncrawl_companies(abn);

CREATE INDEX idx_stg_commoncrawl_company_name 
ON prd_firmable.stg.raw_commoncrawl_companies(company_name);

-- Raw ABR Companies
CREATE INDEX idx_stg_abr_abn 
ON prd_firmable.stg.raw_abr_companies(abn);

CREATE INDEX idx_stg_abr_entity_name 
ON prd_firmable.stg.raw_abr_companies(entity_name);

-- Cleaned Commoncrawl Companies
CREATE INDEX idx_pre_commoncrawl_abn 
ON prd_firmable.pre_dwh.cleaned_commoncrawl_companies(abn);

CREATE INDEX idx_pre_commoncrawl_name_postcode 
ON prd_firmable.pre_dwh.cleaned_commoncrawl_companies(company_name, postcode);

-- Cleaned ABR Companies
CREATE INDEX idx_pre_abr_abn 
ON prd_firmable.pre_dwh.cleaned_abr_companies(abn);

CREATE INDEX idx_pre_abr_name_postcode 
ON prd_firmable.pre_dwh.cleaned_abr_companies(entity_name, postcode);
);


--RBAC

-- Grant all privileges on database
GRANT ALL PRIVILEGES ON DATABASE prd_firmable TO etl_user;
-- Grant usage and create on schemas
GRANT USAGE, CREATE ON SCHEMA stg TO etl_user;
GRANT USAGE, CREATE ON SCHEMA pre_dwh TO etl_user;
GRANT USAGE, CREATE ON SCHEMA dwh TO etl_user;
-- Raw layer
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA stg TO etl_user;
-- Cleaned layer
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA pre_dwh TO etl_user;
-- DWH layer
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dwh TO etl_user;


