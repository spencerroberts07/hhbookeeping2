-- =====================================================================
-- BASELINE SCHEMA SNAPSHOT
-- =====================================================================
--
-- *** DO NOT RUN ON EXISTING DB — baseline snapshot only ***
--
-- Generated:        2026-04-28
-- Source:           pg_dump --schema-only of the live Render Postgres
-- Postgres version: 18.3
--
-- Purpose
-- -------
-- This file captures the EXACT state of the live public schema as of the
-- date above, so that a future maintainer can:
--   - Stand up a clone of the live DB on a fresh Postgres for forensic /
--     restore purposes.
--   - Compare migrations going forward against a known baseline.
--   - Diagnose drift without having to read 50+ table definitions
--     scattered across schema.sql + sql/001..013.
--
-- This file is NOT part of the normal migration sequence. The numbered
-- migrations 001..013 in this folder are the source of truth for new
-- environments. This file is reference material only.
--
-- Caveats
-- -------
-- - The \restrict / \unrestrict directives at the top and bottom belong
--   to the live cluster and will not replay on another cluster — strip
--   them if you ever need to actually replay this file.
-- - Owners and privileges have been stripped (--no-owner --no-privileges).
-- - Data is NOT included; only schema.
-- - Some columns are dead weight that the application no longer reads
--   (notably bank_transaction_matches.matched_table / matched_record_id /
--   match_status). See 000_baseline_schema_audit.md for the rationale.
-- =====================================================================
--
-- PostgreSQL database dump
--

\restrict Fah63hMU2y5DPGKgef79vadBPZfGIhcdC0VkqhcwgWKoHoK47KR6bkD87k8Jycq

-- Dumped from database version 18.3 (Debian 18.3-1.pgdg12+1)
-- Dumped by pg_dump version 18.3

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
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA public;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: account_mapping_rules; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.account_mapping_rules (
    id bigint NOT NULL,
    entity_id uuid NOT NULL,
    source_type text NOT NULL,
    source_key text NOT NULL,
    mapped_account_code text NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    posting_direction text
);


--
-- Name: account_mapping_rules_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.account_mapping_rules_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: account_mapping_rules_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.account_mapping_rules_id_seq OWNED BY public.account_mapping_rules.id;


--
-- Name: accounting_periods; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.accounting_periods (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    period_label text NOT NULL,
    period_start date NOT NULL,
    period_end date NOT NULL,
    status text DEFAULT 'draft'::text NOT NULL,
    fiscal_period_number integer,
    fiscal_year integer
);


--
-- Name: accounts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.accounts (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    account_code text NOT NULL,
    account_name text NOT NULL,
    account_class text NOT NULL,
    statement_type text NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    quickbooks_account_id text
);


--
-- Name: audit_log; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.audit_log (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid,
    actor_email text,
    action text NOT NULL,
    payload jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: bank_csv_import_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bank_csv_import_runs (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    file_name text NOT NULL,
    file_checksum_sha256 text,
    file_size_bytes integer,
    mapping_profile text DEFAULT 'generic'::text NOT NULL,
    source_account_code text,
    source_account_name text,
    column_map_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    total_row_count integer DEFAULT 0 NOT NULL,
    parsed_row_count integer DEFAULT 0 NOT NULL,
    inserted_count integer DEFAULT 0 NOT NULL,
    duplicate_count integer DEFAULT 0 NOT NULL,
    skipped_count integer DEFAULT 0 NOT NULL,
    error_count integer DEFAULT 0 NOT NULL,
    earliest_transaction_date date,
    latest_transaction_date date,
    status text DEFAULT 'completed'::text NOT NULL,
    is_preview boolean DEFAULT false NOT NULL,
    error_text text,
    summary_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    actor_email text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: bank_feed_transactions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bank_feed_transactions (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    accounting_period_id uuid,
    bank_account_code text NOT NULL,
    transaction_date date NOT NULL,
    posted_date date,
    memo text NOT NULL,
    amount numeric(14,2) NOT NULL,
    direction text NOT NULL,
    source_file_id uuid,
    quickbooks_txn_id text,
    source_system text,
    source_transaction_id text,
    raw_json jsonb DEFAULT '{}'::jsonb NOT NULL
);


--
-- Name: bank_transaction_matches; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bank_transaction_matches (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    bank_transaction_id uuid NOT NULL,
    entity_id uuid NOT NULL,
    match_type text NOT NULL,
    matched_table text,
    matched_record_id uuid,
    match_status text DEFAULT 'matched'::text NOT NULL,
    matched_amount numeric(14,2),
    note text,
    created_by text,
    raw_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    target_table_name text,
    target_record_id text,
    active boolean DEFAULT true NOT NULL,
    released_by text,
    released_at timestamp with time zone,
    released_note text,
    target_label text,
    payload_json jsonb DEFAULT '{}'::jsonb NOT NULL
);


--
-- Name: bank_transaction_review_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bank_transaction_review_events (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    bank_transaction_id uuid NOT NULL,
    entity_id uuid,
    action text NOT NULL,
    actor_email text,
    from_review_status text,
    to_review_status text,
    note text,
    payload_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: bank_transactions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bank_transactions (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    accounting_period_id uuid,
    source_system text NOT NULL,
    source_connection_id uuid,
    source_account_id text,
    source_account_name text,
    source_account_code text,
    source_transaction_id text NOT NULL,
    source_transaction_type text NOT NULL,
    transaction_date date NOT NULL,
    posted_date date,
    description text NOT NULL,
    reference_number text,
    amount numeric(14,2) NOT NULL,
    currency_code text DEFAULT 'CAD'::text NOT NULL,
    direction text NOT NULL,
    review_status text DEFAULT 'new'::text NOT NULL,
    raw_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    imported_at timestamp with time zone DEFAULT now() NOT NULL,
    last_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    review_note text,
    reviewed_by text,
    reviewed_at timestamp with time zone,
    normalized_description text,
    counterparty_name text,
    last_reviewed_at timestamp with time zone,
    source_import_run_id uuid
);


--
-- Name: card_settlement_batches; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.card_settlement_batches (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    accounting_period_id uuid,
    processor_name text NOT NULL,
    merchant_account text,
    settlement_reference text,
    business_date date NOT NULL,
    deposit_date date,
    currency_code text DEFAULT 'CAD'::text NOT NULL,
    gross_sales_amount numeric(14,2) DEFAULT 0 NOT NULL,
    refunds_amount numeric(14,2) DEFAULT 0 NOT NULL,
    chargebacks_amount numeric(14,2) DEFAULT 0 NOT NULL,
    fees_amount numeric(14,2) DEFAULT 0 NOT NULL,
    tax_on_fees_amount numeric(14,2) DEFAULT 0 NOT NULL,
    net_deposit_amount numeric(14,2) NOT NULL,
    expected_cash_balancing_amount numeric(14,2),
    matched_bank_amount numeric(14,2) DEFAULT 0 NOT NULL,
    reconciliation_status text DEFAULT 'new'::text NOT NULL,
    source_file_name text,
    note text,
    raw_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_by text,
    reviewed_by text,
    reviewed_at timestamp with time zone,
    active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: card_settlement_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.card_settlement_events (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    batch_id uuid NOT NULL,
    action text NOT NULL,
    actor_email text,
    from_status text,
    to_status text,
    note text,
    payload_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: cash_balancing_days; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cash_balancing_days (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    accounting_period_id uuid,
    business_date date NOT NULL,
    tab_name text,
    total_sales numeric(14,2),
    total_hst numeric(14,2),
    raw_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    closing_cash numeric(14,2),
    opening_cash numeric(14,2)
);


--
-- Name: cash_balancing_import_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cash_balancing_import_runs (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    entity_id uuid NOT NULL,
    source_id uuid NOT NULL,
    run_type text DEFAULT 'manual'::text NOT NULL,
    status text DEFAULT 'running'::text NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    tabs_read jsonb DEFAULT '[]'::jsonb NOT NULL,
    summary_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_text text
);


--
-- Name: cash_balancing_lines; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cash_balancing_lines (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    cash_balancing_day_id uuid NOT NULL,
    line_code text,
    line_label text NOT NULL,
    amount numeric(14,2) NOT NULL,
    mapped_account_code text,
    translation_status text DEFAULT 'pending'::text NOT NULL
);


--
-- Name: cash_balancing_rows; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cash_balancing_rows (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    entity_id uuid NOT NULL,
    source_id uuid NOT NULL,
    import_run_id uuid NOT NULL,
    source_tab_name text NOT NULL,
    business_date date,
    row_number integer NOT NULL,
    row_key text NOT NULL,
    row_hash text NOT NULL,
    notes text,
    sales_amount numeric(14,2),
    cash_amount numeric(14,2),
    debit_amount numeric(14,2),
    credit_amount numeric(14,2),
    ecommerce_amount numeric(14,2),
    gift_card_amount numeric(14,2),
    hst_amount numeric(14,2),
    over_short_amount numeric(14,2),
    raw_row_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    imported_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: cash_balancing_sources; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cash_balancing_sources (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    entity_id uuid NOT NULL,
    source_name text NOT NULL,
    provider text DEFAULT 'google_sheets'::text NOT NULL,
    spreadsheet_id text NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    sync_time_local time without time zone DEFAULT '21:00:00'::time without time zone NOT NULL,
    lookback_days integer DEFAULT 56 NOT NULL,
    timezone_name text DEFAULT 'America/Toronto'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: close_checklist_items; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.close_checklist_items (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    accounting_period_id uuid,
    item_code text NOT NULL,
    item_name text NOT NULL,
    status text DEFAULT 'pending'::text NOT NULL,
    notes text
);


--
-- Name: direct_vendor_ap_invoice_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.direct_vendor_ap_invoice_events (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    invoice_id uuid NOT NULL,
    action text NOT NULL,
    actor_email text,
    from_status text,
    to_status text,
    note text,
    payload_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: direct_vendor_ap_invoices; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.direct_vendor_ap_invoices (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    accounting_period_id uuid,
    vendor_name text NOT NULL,
    vendor_code text,
    invoice_number text NOT NULL,
    invoice_date date NOT NULL,
    due_date date,
    received_date date,
    currency_code text DEFAULT 'CAD'::text NOT NULL,
    subtotal_amount numeric(14,2) DEFAULT 0 NOT NULL,
    tax_amount numeric(14,2) DEFAULT 0 NOT NULL,
    total_amount numeric(14,2) NOT NULL,
    paid_amount numeric(14,2) DEFAULT 0 NOT NULL,
    open_amount numeric(14,2) DEFAULT 0 NOT NULL,
    status text DEFAULT 'open'::text NOT NULL,
    payment_status text DEFAULT 'unpaid'::text NOT NULL,
    priority text DEFAULT 'normal'::text NOT NULL,
    source_document_name text,
    note text,
    raw_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_by text,
    approved_by text,
    approved_at timestamp with time zone,
    last_payment_date date,
    active boolean DEFAULT true NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: document_lines; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.document_lines (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    normalized_document_id uuid NOT NULL,
    line_number integer NOT NULL,
    description text NOT NULL,
    quantity numeric(14,4),
    unit_price numeric(14,4),
    line_amount numeric(14,2) NOT NULL,
    tax_amount numeric(14,2),
    suggested_account_code text,
    extracted_json jsonb DEFAULT '{}'::jsonb NOT NULL
);


--
-- Name: ecommerce_payout_cycles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ecommerce_payout_cycles (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    accounting_period_id uuid,
    cycle_start date,
    cycle_end date,
    dealer_amount numeric(14,2) DEFAULT 0 NOT NULL,
    order_fee numeric(14,2) DEFAULT 0 NOT NULL,
    fulfill_fee numeric(14,2) DEFAULT 0 NOT NULL,
    fee_tax numeric(14,2) DEFAULT 0 NOT NULL,
    retail_total numeric(14,2) DEFAULT 0 NOT NULL,
    retail_tax numeric(14,2) DEFAULT 0 NOT NULL,
    payout_amount numeric(14,2) DEFAULT 0 NOT NULL,
    payout_status text DEFAULT 'open'::text NOT NULL,
    source_file_id uuid
);


--
-- Name: entities; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.entities (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    organization_id uuid NOT NULL,
    entity_code text NOT NULL,
    entity_name text NOT NULL,
    fiscal_year_end_month smallint NOT NULL,
    fiscal_year_end_day smallint NOT NULL,
    base_currency text DEFAULT 'CAD'::text NOT NULL,
    quickbooks_company_id text,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: entity_integrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.entity_integrations (
    id bigint NOT NULL,
    entity_id uuid NOT NULL,
    integration_type text NOT NULL,
    integration_name text NOT NULL,
    spreadsheet_id text,
    quickbooks_realm_id text,
    is_active boolean DEFAULT true NOT NULL,
    settings_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: entity_integrations_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.entity_integrations_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: entity_integrations_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.entity_integrations_id_seq OWNED BY public.entity_integrations.id;


--
-- Name: entity_settings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.entity_settings (
    id bigint NOT NULL,
    entity_id uuid NOT NULL,
    fiscal_year_end_month integer DEFAULT 9 NOT NULL,
    fiscal_year_end_day integer DEFAULT 30 NOT NULL,
    timezone_name text DEFAULT 'America/Toronto'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: entity_settings_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.entity_settings_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: entity_settings_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.entity_settings_id_seq OWNED BY public.entity_settings.id;


--
-- Name: exception_queue; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.exception_queue (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    accounting_period_id uuid,
    exception_type text NOT NULL,
    severity text DEFAULT 'medium'::text NOT NULL,
    status text DEFAULT 'open'::text NOT NULL,
    source_ref text,
    summary text NOT NULL,
    details_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: hh_ap_documents; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.hh_ap_documents (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    document_type text NOT NULL,
    source_filename text NOT NULL,
    source_hash text NOT NULL,
    document_date date,
    upload_source text DEFAULT 'manual_upload'::text NOT NULL,
    processing_status text DEFAULT 'uploaded'::text NOT NULL,
    extracted_text text,
    raw_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    content_type text,
    file_size_bytes integer,
    file_bytes bytea
);


--
-- Name: hh_ap_invoice_overrides; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.hh_ap_invoice_overrides (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    invoice_number text NOT NULL,
    invoice_type text NOT NULL,
    override_invoice_date date,
    override_due_date date,
    override_subtotal numeric(14,2),
    override_hst_amount numeric(14,2),
    override_total_amount numeric(14,2),
    override_special_shares_amount numeric(14,2),
    override_five_year_note_amount numeric(14,2),
    override_advertising_amount numeric(14,2),
    reason text NOT NULL,
    review_status text DEFAULT 'approved'::text NOT NULL,
    reviewed_by text,
    is_active boolean DEFAULT true NOT NULL,
    raw_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT hh_ap_invoice_overrides_review_status_chk CHECK ((review_status = ANY (ARRAY['approved'::text, 'pending'::text, 'rejected'::text])))
);


--
-- Name: hh_ap_invoices; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.hh_ap_invoices (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    document_id uuid,
    invoice_number text NOT NULL,
    invoice_type text NOT NULL,
    vendor_name text,
    vendor_invoice_number text,
    po_number text,
    invoice_date date,
    due_date date,
    remittance_due_date date,
    currency_code text DEFAULT 'CAD'::text NOT NULL,
    subtotal numeric(14,2),
    hst_amount numeric(14,2),
    surcharge_amount numeric(14,2),
    advertising_amount numeric(14,2),
    subscribed_shares_amount numeric(14,2),
    five_year_note_amount numeric(14,2),
    total_amount numeric(14,2),
    match_status text DEFAULT 'unmatched'::text NOT NULL,
    is_statement_only boolean DEFAULT false NOT NULL,
    notes text,
    raw_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: hh_ap_invoices_effective; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.hh_ap_invoices_effective AS
 SELECT i.id,
    i.entity_id,
    i.document_id,
    i.invoice_number,
    i.invoice_type,
    COALESCE(o.override_invoice_date, i.invoice_date) AS invoice_date,
    COALESCE(o.override_due_date, i.due_date) AS due_date,
    i.remittance_due_date,
    i.vendor_name,
    i.vendor_invoice_number,
    i.po_number,
    i.currency_code,
    COALESCE(o.override_subtotal, i.subtotal, (0)::numeric) AS subtotal,
    COALESCE(o.override_hst_amount, i.hst_amount, (0)::numeric) AS hst_amount,
    COALESCE(o.override_total_amount, i.total_amount, (0)::numeric) AS total_amount,
    COALESCE(o.override_special_shares_amount, i.subscribed_shares_amount, (0)::numeric) AS subscribed_shares_amount,
    COALESCE(o.override_five_year_note_amount, i.five_year_note_amount, (0)::numeric) AS five_year_note_amount,
    COALESCE(o.override_advertising_amount, i.advertising_amount, (0)::numeric) AS advertising_amount,
    COALESCE(i.surcharge_amount, (0)::numeric) AS surcharge_amount,
    i.match_status,
    i.is_statement_only,
    i.notes,
    i.raw_json,
    o.id AS override_id,
    o.reason AS override_reason,
    o.review_status AS override_review_status,
    o.reviewed_by AS override_reviewed_by,
    o.is_active AS override_is_active,
    o.raw_json AS override_raw_json,
    i.created_at,
    i.updated_at
   FROM (public.hh_ap_invoices i
     LEFT JOIN public.hh_ap_invoice_overrides o ON (((o.entity_id = i.entity_id) AND (o.invoice_number = i.invoice_number) AND (o.invoice_type = i.invoice_type) AND (o.is_active = true))));


--
-- Name: hh_ap_remittance_bank_match_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.hh_ap_remittance_bank_match_events (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    remittance_id uuid NOT NULL,
    bank_transaction_id uuid,
    bank_transaction_match_id uuid,
    action text NOT NULL,
    actor_email text,
    note text,
    payload_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: hh_ap_remittance_lines; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.hh_ap_remittance_lines (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    remittance_id uuid NOT NULL,
    entity_id uuid NOT NULL,
    invoice_number text,
    line_description text,
    due_date date,
    line_amount numeric(14,2) NOT NULL,
    matched_invoice_id uuid,
    match_status text DEFAULT 'unmatched'::text NOT NULL,
    raw_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: hh_ap_remittances; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.hh_ap_remittances (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    document_id uuid,
    remittance_reference text,
    remittance_date date,
    withdrawal_date date,
    total_amount numeric(14,2),
    raw_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: hh_ap_statement_lines; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.hh_ap_statement_lines (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    statement_id uuid NOT NULL,
    entity_id uuid NOT NULL,
    invoice_number text,
    invoice_type text,
    invoice_date date,
    due_date date,
    invoice_amount numeric(14,2),
    open_amount numeric(14,2),
    current_amount numeric(14,2),
    past_due_amount numeric(14,2),
    matched_invoice_id uuid,
    match_status text DEFAULT 'unmatched'::text NOT NULL,
    is_missing_download boolean DEFAULT false NOT NULL,
    raw_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: hh_ap_statements; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.hh_ap_statements (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    document_id uuid,
    statement_date date,
    statement_month_end date,
    total_open_balance numeric(14,2),
    raw_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: hh_statement_lines; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.hh_statement_lines (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    normalized_document_id uuid NOT NULL,
    statement_line_type text,
    invoice_number text,
    amount numeric(14,2) NOT NULL,
    tax_amount numeric(14,2),
    due_date date,
    matched_document_id uuid,
    status text DEFAULT 'unmatched'::text NOT NULL
);


--
-- Name: journal_batch_workflow_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.journal_batch_workflow_events (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    journal_batch_id uuid NOT NULL,
    entity_id uuid NOT NULL,
    accounting_period_id uuid,
    source_module text NOT NULL,
    batch_label text NOT NULL,
    action text NOT NULL,
    from_workflow_status text,
    to_workflow_status text,
    actor_email text,
    note text,
    payload_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: journal_batches; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.journal_batches (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    accounting_period_id uuid NOT NULL,
    source_module text NOT NULL,
    batch_label text NOT NULL,
    status text DEFAULT 'draft'::text NOT NULL,
    total_debits numeric(14,2) DEFAULT 0 NOT NULL,
    total_credits numeric(14,2) DEFAULT 0 NOT NULL,
    summary_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    workflow_status text DEFAULT 'draft_ready'::text NOT NULL,
    submitted_by text,
    submitted_at timestamp with time zone,
    reviewed_by text,
    reviewed_at timestamp with time zone,
    approved_by text,
    approved_at timestamp with time zone,
    approval_note text,
    rejection_note text,
    locked_by text,
    locked_at timestamp with time zone
);


--
-- Name: journal_lines; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.journal_lines (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    journal_batch_id uuid NOT NULL,
    line_number integer NOT NULL,
    account_code text NOT NULL,
    debit_amount numeric(14,2) DEFAULT 0 NOT NULL,
    credit_amount numeric(14,2) DEFAULT 0 NOT NULL,
    memo text,
    source_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: normalized_documents; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.normalized_documents (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    source_file_id uuid NOT NULL,
    document_type text NOT NULL,
    store_code text,
    vendor_name text,
    external_document_number text,
    invoice_number text,
    statement_number text,
    document_date date,
    due_date date,
    subtotal numeric(14,2),
    tax_amount numeric(14,2),
    total_amount numeric(14,2),
    extracted_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    confidence_score numeric(5,2),
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: organizations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.organizations (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    name text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: payroll_batches; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.payroll_batches (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    accounting_period_id uuid,
    pay_period_start date,
    pay_period_end date,
    pay_date date,
    gross_wages numeric(14,2),
    deductions_total numeric(14,2),
    employer_burden_total numeric(14,2),
    net_pay numeric(14,2),
    source_file_id uuid,
    raw_json jsonb DEFAULT '{}'::jsonb NOT NULL
);


--
-- Name: posting_rules; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.posting_rules (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    rule_code text NOT NULL,
    area text NOT NULL,
    source text NOT NULL,
    trigger_match text NOT NULL,
    posting_logic text NOT NULL,
    key_accounts text[] DEFAULT '{}'::text[] NOT NULL,
    auto_level text NOT NULL,
    review_tier text NOT NULL,
    exception_logic text,
    evidence text,
    conditions_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    outputs_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    priority integer DEFAULT 100 NOT NULL
);


--
-- Name: quickbooks_connections; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.quickbooks_connections (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    realm_id text NOT NULL,
    access_token text NOT NULL,
    refresh_token text NOT NULL,
    access_token_expires_at timestamp with time zone,
    refresh_token_expires_at timestamp with time zone,
    connected_at timestamp with time zone DEFAULT now() NOT NULL,
    disconnected_at timestamp with time zone,
    is_active boolean DEFAULT true NOT NULL
);


--
-- Name: quickbooks_sync_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.quickbooks_sync_runs (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    quickbooks_connection_id uuid NOT NULL,
    sync_type text NOT NULL,
    sync_from date,
    sync_to date,
    status text DEFAULT 'running'::text NOT NULL,
    summary_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone
);


--
-- Name: quickbooks_transactions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.quickbooks_transactions (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    accounting_period_id uuid,
    quickbooks_txn_id text,
    txn_type text NOT NULL,
    txn_date date,
    memo text,
    counterparty_name text,
    bank_memo text,
    amount numeric(14,2) NOT NULL,
    source_account_code text,
    source_account_name text,
    imported_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: recurring_month_end_rules; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.recurring_month_end_rules (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    rule_code text NOT NULL,
    entry_name text NOT NULL,
    frequency text NOT NULL,
    debit_account_code text NOT NULL,
    credit_account_code text NOT NULL,
    default_amount numeric(14,2),
    logic text,
    is_active boolean DEFAULT true NOT NULL
);


--
-- Name: rule_runs; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.rule_runs (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    accounting_period_id uuid,
    run_type text NOT NULL,
    started_at timestamp with time zone DEFAULT now() NOT NULL,
    finished_at timestamp with time zone,
    status text DEFAULT 'running'::text NOT NULL,
    summary_json jsonb DEFAULT '{}'::jsonb NOT NULL
);


--
-- Name: source_files; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.source_files (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    accounting_period_id uuid,
    file_name text NOT NULL,
    storage_path text NOT NULL,
    mime_type text,
    source_type text NOT NULL,
    checksum_sha256 text,
    uploaded_at timestamp with time zone DEFAULT now() NOT NULL,
    parser_status text DEFAULT 'pending'::text NOT NULL
);


--
-- Name: suggested_entries; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.suggested_entries (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    accounting_period_id uuid,
    source_rule_code text,
    entry_date date NOT NULL,
    memo text NOT NULL,
    review_status text DEFAULT 'draft'::text NOT NULL,
    confidence_score numeric(5,2),
    source_refs jsonb DEFAULT '[]'::jsonb NOT NULL
);


--
-- Name: suggested_entry_lines; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.suggested_entry_lines (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    suggested_entry_id uuid NOT NULL,
    account_code text NOT NULL,
    description text,
    debit numeric(14,2) DEFAULT 0 NOT NULL,
    credit numeric(14,2) DEFAULT 0 NOT NULL
);


--
-- Name: vendors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.vendors (
    id uuid DEFAULT public.uuid_generate_v4() NOT NULL,
    entity_id uuid NOT NULL,
    vendor_name text NOT NULL,
    vendor_normalized text NOT NULL,
    default_account_code text,
    quickbooks_vendor_id text
);


--
-- Name: account_mapping_rules id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.account_mapping_rules ALTER COLUMN id SET DEFAULT nextval('public.account_mapping_rules_id_seq'::regclass);


--
-- Name: entity_integrations id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entity_integrations ALTER COLUMN id SET DEFAULT nextval('public.entity_integrations_id_seq'::regclass);


--
-- Name: entity_settings id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entity_settings ALTER COLUMN id SET DEFAULT nextval('public.entity_settings_id_seq'::regclass);


--
-- Name: account_mapping_rules account_mapping_rules_entity_id_source_type_source_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.account_mapping_rules
    ADD CONSTRAINT account_mapping_rules_entity_id_source_type_source_key_key UNIQUE (entity_id, source_type, source_key);


--
-- Name: account_mapping_rules account_mapping_rules_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.account_mapping_rules
    ADD CONSTRAINT account_mapping_rules_pkey PRIMARY KEY (id);


--
-- Name: accounting_periods accounting_periods_entity_id_period_start_period_end_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.accounting_periods
    ADD CONSTRAINT accounting_periods_entity_id_period_start_period_end_key UNIQUE (entity_id, period_start, period_end);


--
-- Name: accounting_periods accounting_periods_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.accounting_periods
    ADD CONSTRAINT accounting_periods_pkey PRIMARY KEY (id);


--
-- Name: accounts accounts_entity_id_account_code_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.accounts
    ADD CONSTRAINT accounts_entity_id_account_code_key UNIQUE (entity_id, account_code);


--
-- Name: accounts accounts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.accounts
    ADD CONSTRAINT accounts_pkey PRIMARY KEY (id);


--
-- Name: audit_log audit_log_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.audit_log
    ADD CONSTRAINT audit_log_pkey PRIMARY KEY (id);


--
-- Name: bank_csv_import_runs bank_csv_import_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bank_csv_import_runs
    ADD CONSTRAINT bank_csv_import_runs_pkey PRIMARY KEY (id);


--
-- Name: bank_feed_transactions bank_feed_transactions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bank_feed_transactions
    ADD CONSTRAINT bank_feed_transactions_pkey PRIMARY KEY (id);


--
-- Name: bank_transaction_matches bank_transaction_matches_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bank_transaction_matches
    ADD CONSTRAINT bank_transaction_matches_pkey PRIMARY KEY (id);


--
-- Name: bank_transaction_review_events bank_transaction_review_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bank_transaction_review_events
    ADD CONSTRAINT bank_transaction_review_events_pkey PRIMARY KEY (id);


--
-- Name: bank_transactions bank_transactions_entity_id_source_system_source_transactio_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bank_transactions
    ADD CONSTRAINT bank_transactions_entity_id_source_system_source_transactio_key UNIQUE (entity_id, source_system, source_transaction_id);


--
-- Name: bank_transactions bank_transactions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bank_transactions
    ADD CONSTRAINT bank_transactions_pkey PRIMARY KEY (id);


--
-- Name: card_settlement_batches card_settlement_batches_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.card_settlement_batches
    ADD CONSTRAINT card_settlement_batches_pkey PRIMARY KEY (id);


--
-- Name: card_settlement_events card_settlement_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.card_settlement_events
    ADD CONSTRAINT card_settlement_events_pkey PRIMARY KEY (id);


--
-- Name: cash_balancing_days cash_balancing_days_entity_id_business_date_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cash_balancing_days
    ADD CONSTRAINT cash_balancing_days_entity_id_business_date_key UNIQUE (entity_id, business_date);


--
-- Name: cash_balancing_days cash_balancing_days_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cash_balancing_days
    ADD CONSTRAINT cash_balancing_days_pkey PRIMARY KEY (id);


--
-- Name: cash_balancing_import_runs cash_balancing_import_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cash_balancing_import_runs
    ADD CONSTRAINT cash_balancing_import_runs_pkey PRIMARY KEY (id);


--
-- Name: cash_balancing_lines cash_balancing_lines_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cash_balancing_lines
    ADD CONSTRAINT cash_balancing_lines_pkey PRIMARY KEY (id);


--
-- Name: cash_balancing_rows cash_balancing_rows_entity_id_source_id_source_tab_name_row_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cash_balancing_rows
    ADD CONSTRAINT cash_balancing_rows_entity_id_source_id_source_tab_name_row_key UNIQUE (entity_id, source_id, source_tab_name, row_key);


--
-- Name: cash_balancing_rows cash_balancing_rows_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cash_balancing_rows
    ADD CONSTRAINT cash_balancing_rows_pkey PRIMARY KEY (id);


--
-- Name: cash_balancing_sources cash_balancing_sources_entity_id_source_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cash_balancing_sources
    ADD CONSTRAINT cash_balancing_sources_entity_id_source_name_key UNIQUE (entity_id, source_name);


--
-- Name: cash_balancing_sources cash_balancing_sources_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cash_balancing_sources
    ADD CONSTRAINT cash_balancing_sources_pkey PRIMARY KEY (id);


--
-- Name: close_checklist_items close_checklist_items_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.close_checklist_items
    ADD CONSTRAINT close_checklist_items_pkey PRIMARY KEY (id);


--
-- Name: direct_vendor_ap_invoice_events direct_vendor_ap_invoice_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.direct_vendor_ap_invoice_events
    ADD CONSTRAINT direct_vendor_ap_invoice_events_pkey PRIMARY KEY (id);


--
-- Name: direct_vendor_ap_invoices direct_vendor_ap_invoices_entity_id_vendor_name_invoice_num_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.direct_vendor_ap_invoices
    ADD CONSTRAINT direct_vendor_ap_invoices_entity_id_vendor_name_invoice_num_key UNIQUE (entity_id, vendor_name, invoice_number);


--
-- Name: direct_vendor_ap_invoices direct_vendor_ap_invoices_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.direct_vendor_ap_invoices
    ADD CONSTRAINT direct_vendor_ap_invoices_pkey PRIMARY KEY (id);


--
-- Name: document_lines document_lines_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.document_lines
    ADD CONSTRAINT document_lines_pkey PRIMARY KEY (id);


--
-- Name: ecommerce_payout_cycles ecommerce_payout_cycles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ecommerce_payout_cycles
    ADD CONSTRAINT ecommerce_payout_cycles_pkey PRIMARY KEY (id);


--
-- Name: entities entities_entity_code_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entities
    ADD CONSTRAINT entities_entity_code_key UNIQUE (entity_code);


--
-- Name: entities entities_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entities
    ADD CONSTRAINT entities_pkey PRIMARY KEY (id);


--
-- Name: entity_integrations entity_integrations_entity_id_integration_type_integration__key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entity_integrations
    ADD CONSTRAINT entity_integrations_entity_id_integration_type_integration__key UNIQUE (entity_id, integration_type, integration_name);


--
-- Name: entity_integrations entity_integrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entity_integrations
    ADD CONSTRAINT entity_integrations_pkey PRIMARY KEY (id);


--
-- Name: entity_settings entity_settings_entity_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entity_settings
    ADD CONSTRAINT entity_settings_entity_id_key UNIQUE (entity_id);


--
-- Name: entity_settings entity_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entity_settings
    ADD CONSTRAINT entity_settings_pkey PRIMARY KEY (id);


--
-- Name: exception_queue exception_queue_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.exception_queue
    ADD CONSTRAINT exception_queue_pkey PRIMARY KEY (id);


--
-- Name: hh_ap_documents hh_ap_documents_entity_id_document_type_source_hash_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_documents
    ADD CONSTRAINT hh_ap_documents_entity_id_document_type_source_hash_key UNIQUE (entity_id, document_type, source_hash);


--
-- Name: hh_ap_documents hh_ap_documents_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_documents
    ADD CONSTRAINT hh_ap_documents_pkey PRIMARY KEY (id);


--
-- Name: hh_ap_invoice_overrides hh_ap_invoice_overrides_entity_invoice_type_uk; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_invoice_overrides
    ADD CONSTRAINT hh_ap_invoice_overrides_entity_invoice_type_uk UNIQUE (entity_id, invoice_number, invoice_type);


--
-- Name: hh_ap_invoice_overrides hh_ap_invoice_overrides_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_invoice_overrides
    ADD CONSTRAINT hh_ap_invoice_overrides_pkey PRIMARY KEY (id);


--
-- Name: hh_ap_invoices hh_ap_invoices_entity_id_invoice_number_invoice_type_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_invoices
    ADD CONSTRAINT hh_ap_invoices_entity_id_invoice_number_invoice_type_key UNIQUE (entity_id, invoice_number, invoice_type);


--
-- Name: hh_ap_invoices hh_ap_invoices_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_invoices
    ADD CONSTRAINT hh_ap_invoices_pkey PRIMARY KEY (id);


--
-- Name: hh_ap_remittance_bank_match_events hh_ap_remittance_bank_match_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_remittance_bank_match_events
    ADD CONSTRAINT hh_ap_remittance_bank_match_events_pkey PRIMARY KEY (id);


--
-- Name: hh_ap_remittance_lines hh_ap_remittance_lines_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_remittance_lines
    ADD CONSTRAINT hh_ap_remittance_lines_pkey PRIMARY KEY (id);


--
-- Name: hh_ap_remittances hh_ap_remittances_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_remittances
    ADD CONSTRAINT hh_ap_remittances_pkey PRIMARY KEY (id);


--
-- Name: hh_ap_statement_lines hh_ap_statement_lines_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_statement_lines
    ADD CONSTRAINT hh_ap_statement_lines_pkey PRIMARY KEY (id);


--
-- Name: hh_ap_statements hh_ap_statements_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_statements
    ADD CONSTRAINT hh_ap_statements_pkey PRIMARY KEY (id);


--
-- Name: hh_statement_lines hh_statement_lines_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_statement_lines
    ADD CONSTRAINT hh_statement_lines_pkey PRIMARY KEY (id);


--
-- Name: journal_batch_workflow_events journal_batch_workflow_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.journal_batch_workflow_events
    ADD CONSTRAINT journal_batch_workflow_events_pkey PRIMARY KEY (id);


--
-- Name: journal_batches journal_batches_entity_id_accounting_period_id_source_modul_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.journal_batches
    ADD CONSTRAINT journal_batches_entity_id_accounting_period_id_source_modul_key UNIQUE (entity_id, accounting_period_id, source_module, batch_label);


--
-- Name: journal_batches journal_batches_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.journal_batches
    ADD CONSTRAINT journal_batches_pkey PRIMARY KEY (id);


--
-- Name: journal_lines journal_lines_journal_batch_id_line_number_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.journal_lines
    ADD CONSTRAINT journal_lines_journal_batch_id_line_number_key UNIQUE (journal_batch_id, line_number);


--
-- Name: journal_lines journal_lines_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.journal_lines
    ADD CONSTRAINT journal_lines_pkey PRIMARY KEY (id);


--
-- Name: normalized_documents normalized_documents_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.normalized_documents
    ADD CONSTRAINT normalized_documents_pkey PRIMARY KEY (id);


--
-- Name: organizations organizations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organizations
    ADD CONSTRAINT organizations_pkey PRIMARY KEY (id);


--
-- Name: payroll_batches payroll_batches_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.payroll_batches
    ADD CONSTRAINT payroll_batches_pkey PRIMARY KEY (id);


--
-- Name: posting_rules posting_rules_entity_id_rule_code_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.posting_rules
    ADD CONSTRAINT posting_rules_entity_id_rule_code_key UNIQUE (entity_id, rule_code);


--
-- Name: posting_rules posting_rules_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.posting_rules
    ADD CONSTRAINT posting_rules_pkey PRIMARY KEY (id);


--
-- Name: quickbooks_connections quickbooks_connections_entity_id_realm_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.quickbooks_connections
    ADD CONSTRAINT quickbooks_connections_entity_id_realm_id_key UNIQUE (entity_id, realm_id);


--
-- Name: quickbooks_connections quickbooks_connections_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.quickbooks_connections
    ADD CONSTRAINT quickbooks_connections_pkey PRIMARY KEY (id);


--
-- Name: quickbooks_sync_runs quickbooks_sync_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.quickbooks_sync_runs
    ADD CONSTRAINT quickbooks_sync_runs_pkey PRIMARY KEY (id);


--
-- Name: quickbooks_transactions quickbooks_transactions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.quickbooks_transactions
    ADD CONSTRAINT quickbooks_transactions_pkey PRIMARY KEY (id);


--
-- Name: recurring_month_end_rules recurring_month_end_rules_entity_id_rule_code_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.recurring_month_end_rules
    ADD CONSTRAINT recurring_month_end_rules_entity_id_rule_code_key UNIQUE (entity_id, rule_code);


--
-- Name: recurring_month_end_rules recurring_month_end_rules_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.recurring_month_end_rules
    ADD CONSTRAINT recurring_month_end_rules_pkey PRIMARY KEY (id);


--
-- Name: rule_runs rule_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.rule_runs
    ADD CONSTRAINT rule_runs_pkey PRIMARY KEY (id);


--
-- Name: source_files source_files_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_files
    ADD CONSTRAINT source_files_pkey PRIMARY KEY (id);


--
-- Name: suggested_entries suggested_entries_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.suggested_entries
    ADD CONSTRAINT suggested_entries_pkey PRIMARY KEY (id);


--
-- Name: suggested_entry_lines suggested_entry_lines_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.suggested_entry_lines
    ADD CONSTRAINT suggested_entry_lines_pkey PRIMARY KEY (id);


--
-- Name: vendors vendors_entity_id_vendor_normalized_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vendors
    ADD CONSTRAINT vendors_entity_id_vendor_normalized_key UNIQUE (entity_id, vendor_normalized);


--
-- Name: vendors vendors_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vendors
    ADD CONSTRAINT vendors_pkey PRIMARY KEY (id);


--
-- Name: accounting_periods_entity_fy_period_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX accounting_periods_entity_fy_period_unique ON public.accounting_periods USING btree (entity_id, fiscal_year, fiscal_period_number);


--
-- Name: idx_bank_csv_import_runs_entity_account_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bank_csv_import_runs_entity_account_date ON public.bank_csv_import_runs USING btree (entity_id, source_account_code, latest_transaction_date DESC);


--
-- Name: idx_bank_csv_import_runs_entity_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bank_csv_import_runs_entity_created ON public.bank_csv_import_runs USING btree (entity_id, created_at DESC);


--
-- Name: idx_bank_transaction_matches_bank_txn; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bank_transaction_matches_bank_txn ON public.bank_transaction_matches USING btree (bank_transaction_id, created_at DESC);


--
-- Name: idx_bank_transaction_matches_entity; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bank_transaction_matches_entity ON public.bank_transaction_matches USING btree (entity_id, match_status, created_at);


--
-- Name: idx_bank_transaction_matches_entity_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bank_transaction_matches_entity_active ON public.bank_transaction_matches USING btree (entity_id, active, created_at DESC);


--
-- Name: idx_bank_transaction_matches_hh_remittance_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bank_transaction_matches_hh_remittance_active ON public.bank_transaction_matches USING btree (target_record_id) WHERE ((active = true) AND (target_table_name = 'hh_ap_remittances'::text));


--
-- Name: idx_bank_transaction_matches_hh_remittance_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bank_transaction_matches_hh_remittance_lookup ON public.bank_transaction_matches USING btree (entity_id, target_record_id) WHERE ((active = true) AND (target_table_name = 'hh_ap_remittances'::text));


--
-- Name: idx_bank_transaction_matches_target_label; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bank_transaction_matches_target_label ON public.bank_transaction_matches USING btree (entity_id, target_table_name, target_record_id, target_label) WHERE (active = true);


--
-- Name: idx_bank_transaction_matches_transaction; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bank_transaction_matches_transaction ON public.bank_transaction_matches USING btree (bank_transaction_id, match_status, created_at);


--
-- Name: idx_bank_transaction_review_events_transaction; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bank_transaction_review_events_transaction ON public.bank_transaction_review_events USING btree (bank_transaction_id, created_at);


--
-- Name: idx_bank_transaction_review_events_txn_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bank_transaction_review_events_txn_created ON public.bank_transaction_review_events USING btree (bank_transaction_id, created_at DESC);


--
-- Name: idx_bank_transactions_entity_account; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bank_transactions_entity_account ON public.bank_transactions USING btree (entity_id, source_account_name, transaction_date DESC);


--
-- Name: idx_bank_transactions_entity_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bank_transactions_entity_date ON public.bank_transactions USING btree (entity_id, transaction_date DESC);


--
-- Name: idx_bank_transactions_entity_direction_date_amount; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bank_transactions_entity_direction_date_amount ON public.bank_transactions USING btree (entity_id, direction, transaction_date, amount);


--
-- Name: idx_bank_transactions_entity_review; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bank_transactions_entity_review ON public.bank_transactions USING btree (entity_id, review_status, transaction_date);


--
-- Name: idx_bank_transactions_entity_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bank_transactions_entity_status ON public.bank_transactions USING btree (entity_id, review_status, transaction_date DESC);


--
-- Name: idx_bank_transactions_source_import_run; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bank_transactions_source_import_run ON public.bank_transactions USING btree (source_import_run_id) WHERE (source_import_run_id IS NOT NULL);


--
-- Name: idx_bank_transactions_source_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bank_transactions_source_lookup ON public.bank_transactions USING btree (entity_id, source_system, source_transaction_id);


--
-- Name: idx_card_settlement_batches_entity_business_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_card_settlement_batches_entity_business_date ON public.card_settlement_batches USING btree (entity_id, business_date, processor_name);


--
-- Name: idx_card_settlement_batches_entity_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_card_settlement_batches_entity_status ON public.card_settlement_batches USING btree (entity_id, reconciliation_status, deposit_date, business_date);


--
-- Name: idx_card_settlement_events_batch_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_card_settlement_events_batch_created ON public.card_settlement_events USING btree (batch_id, created_at);


--
-- Name: idx_cash_balancing_rows_entity_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_cash_balancing_rows_entity_date ON public.cash_balancing_rows USING btree (entity_id, business_date);


--
-- Name: idx_direct_vendor_ap_invoice_events_invoice_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_direct_vendor_ap_invoice_events_invoice_created ON public.direct_vendor_ap_invoice_events USING btree (invoice_id, created_at);


--
-- Name: idx_direct_vendor_ap_invoices_entity_due_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_direct_vendor_ap_invoices_entity_due_status ON public.direct_vendor_ap_invoices USING btree (entity_id, due_date, status, payment_status);


--
-- Name: idx_direct_vendor_ap_invoices_entity_invoice_date; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_direct_vendor_ap_invoices_entity_invoice_date ON public.direct_vendor_ap_invoices USING btree (entity_id, invoice_date);


--
-- Name: idx_hh_ap_documents_entity_type; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_hh_ap_documents_entity_type ON public.hh_ap_documents USING btree (entity_id, document_type);


--
-- Name: idx_hh_ap_invoice_overrides_entity_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_hh_ap_invoice_overrides_entity_active ON public.hh_ap_invoice_overrides USING btree (entity_id, is_active);


--
-- Name: idx_hh_ap_invoice_overrides_entity_invoice; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_hh_ap_invoice_overrides_entity_invoice ON public.hh_ap_invoice_overrides USING btree (entity_id, invoice_number, invoice_type);


--
-- Name: idx_hh_ap_invoices_entity_invoice; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_hh_ap_invoices_entity_invoice ON public.hh_ap_invoices USING btree (entity_id, invoice_number);


--
-- Name: idx_hh_ap_invoices_match_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_hh_ap_invoices_match_status ON public.hh_ap_invoices USING btree (entity_id, match_status);


--
-- Name: idx_hh_ap_remittance_bank_match_events_entity_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_hh_ap_remittance_bank_match_events_entity_created ON public.hh_ap_remittance_bank_match_events USING btree (entity_id, created_at);


--
-- Name: idx_hh_ap_remittance_bank_match_events_remittance_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_hh_ap_remittance_bank_match_events_remittance_created ON public.hh_ap_remittance_bank_match_events USING btree (remittance_id, created_at);


--
-- Name: idx_hh_ap_remittance_lines_entity_invoice; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_hh_ap_remittance_lines_entity_invoice ON public.hh_ap_remittance_lines USING btree (entity_id, invoice_number);


--
-- Name: idx_hh_ap_remittances_entity_withdrawal; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_hh_ap_remittances_entity_withdrawal ON public.hh_ap_remittances USING btree (entity_id, withdrawal_date, remittance_date, total_amount);


--
-- Name: idx_hh_ap_statement_lines_entity_invoice; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_hh_ap_statement_lines_entity_invoice ON public.hh_ap_statement_lines USING btree (entity_id, invoice_number);


--
-- Name: idx_hh_ap_statement_lines_missing_download; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_hh_ap_statement_lines_missing_download ON public.hh_ap_statement_lines USING btree (entity_id, is_missing_download);


--
-- Name: idx_journal_batch_workflow_events_batch_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_journal_batch_workflow_events_batch_created ON public.journal_batch_workflow_events USING btree (journal_batch_id, created_at);


--
-- Name: idx_journal_batch_workflow_events_entity_period; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_journal_batch_workflow_events_entity_period ON public.journal_batch_workflow_events USING btree (entity_id, accounting_period_id, created_at);


--
-- Name: idx_qb_connections_entity_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_qb_connections_entity_active ON public.quickbooks_connections USING btree (entity_id, is_active);


--
-- Name: ux_bank_transaction_matches_active_one_per_txn; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_bank_transaction_matches_active_one_per_txn ON public.bank_transaction_matches USING btree (bank_transaction_id) WHERE (active = true);


--
-- Name: account_mapping_rules account_mapping_rules_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.account_mapping_rules
    ADD CONSTRAINT account_mapping_rules_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id) ON DELETE CASCADE;


--
-- Name: accounting_periods accounting_periods_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.accounting_periods
    ADD CONSTRAINT accounting_periods_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: accounts accounts_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.accounts
    ADD CONSTRAINT accounts_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: audit_log audit_log_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.audit_log
    ADD CONSTRAINT audit_log_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: bank_csv_import_runs bank_csv_import_runs_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bank_csv_import_runs
    ADD CONSTRAINT bank_csv_import_runs_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: bank_feed_transactions bank_feed_transactions_accounting_period_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bank_feed_transactions
    ADD CONSTRAINT bank_feed_transactions_accounting_period_id_fkey FOREIGN KEY (accounting_period_id) REFERENCES public.accounting_periods(id);


--
-- Name: bank_feed_transactions bank_feed_transactions_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bank_feed_transactions
    ADD CONSTRAINT bank_feed_transactions_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: bank_feed_transactions bank_feed_transactions_source_file_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bank_feed_transactions
    ADD CONSTRAINT bank_feed_transactions_source_file_id_fkey FOREIGN KEY (source_file_id) REFERENCES public.source_files(id);


--
-- Name: bank_transaction_matches bank_transaction_matches_bank_transaction_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bank_transaction_matches
    ADD CONSTRAINT bank_transaction_matches_bank_transaction_id_fkey FOREIGN KEY (bank_transaction_id) REFERENCES public.bank_transactions(id) ON DELETE CASCADE;


--
-- Name: bank_transaction_matches bank_transaction_matches_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bank_transaction_matches
    ADD CONSTRAINT bank_transaction_matches_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: bank_transaction_review_events bank_transaction_review_events_bank_transaction_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bank_transaction_review_events
    ADD CONSTRAINT bank_transaction_review_events_bank_transaction_id_fkey FOREIGN KEY (bank_transaction_id) REFERENCES public.bank_transactions(id) ON DELETE CASCADE;


--
-- Name: bank_transaction_review_events bank_transaction_review_events_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bank_transaction_review_events
    ADD CONSTRAINT bank_transaction_review_events_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: bank_transactions bank_transactions_accounting_period_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bank_transactions
    ADD CONSTRAINT bank_transactions_accounting_period_id_fkey FOREIGN KEY (accounting_period_id) REFERENCES public.accounting_periods(id);


--
-- Name: bank_transactions bank_transactions_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bank_transactions
    ADD CONSTRAINT bank_transactions_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: bank_transactions bank_transactions_source_connection_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bank_transactions
    ADD CONSTRAINT bank_transactions_source_connection_id_fkey FOREIGN KEY (source_connection_id) REFERENCES public.quickbooks_connections(id);


--
-- Name: bank_transactions bank_transactions_source_import_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bank_transactions
    ADD CONSTRAINT bank_transactions_source_import_run_id_fkey FOREIGN KEY (source_import_run_id) REFERENCES public.bank_csv_import_runs(id);


--
-- Name: card_settlement_batches card_settlement_batches_accounting_period_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.card_settlement_batches
    ADD CONSTRAINT card_settlement_batches_accounting_period_id_fkey FOREIGN KEY (accounting_period_id) REFERENCES public.accounting_periods(id);


--
-- Name: card_settlement_batches card_settlement_batches_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.card_settlement_batches
    ADD CONSTRAINT card_settlement_batches_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: card_settlement_events card_settlement_events_batch_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.card_settlement_events
    ADD CONSTRAINT card_settlement_events_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES public.card_settlement_batches(id) ON DELETE CASCADE;


--
-- Name: card_settlement_events card_settlement_events_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.card_settlement_events
    ADD CONSTRAINT card_settlement_events_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: cash_balancing_days cash_balancing_days_accounting_period_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cash_balancing_days
    ADD CONSTRAINT cash_balancing_days_accounting_period_id_fkey FOREIGN KEY (accounting_period_id) REFERENCES public.accounting_periods(id);


--
-- Name: cash_balancing_days cash_balancing_days_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cash_balancing_days
    ADD CONSTRAINT cash_balancing_days_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: cash_balancing_import_runs cash_balancing_import_runs_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cash_balancing_import_runs
    ADD CONSTRAINT cash_balancing_import_runs_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: cash_balancing_import_runs cash_balancing_import_runs_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cash_balancing_import_runs
    ADD CONSTRAINT cash_balancing_import_runs_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.cash_balancing_sources(id);


--
-- Name: cash_balancing_lines cash_balancing_lines_cash_balancing_day_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cash_balancing_lines
    ADD CONSTRAINT cash_balancing_lines_cash_balancing_day_id_fkey FOREIGN KEY (cash_balancing_day_id) REFERENCES public.cash_balancing_days(id) ON DELETE CASCADE;


--
-- Name: cash_balancing_rows cash_balancing_rows_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cash_balancing_rows
    ADD CONSTRAINT cash_balancing_rows_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: cash_balancing_rows cash_balancing_rows_import_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cash_balancing_rows
    ADD CONSTRAINT cash_balancing_rows_import_run_id_fkey FOREIGN KEY (import_run_id) REFERENCES public.cash_balancing_import_runs(id);


--
-- Name: cash_balancing_rows cash_balancing_rows_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cash_balancing_rows
    ADD CONSTRAINT cash_balancing_rows_source_id_fkey FOREIGN KEY (source_id) REFERENCES public.cash_balancing_sources(id);


--
-- Name: cash_balancing_sources cash_balancing_sources_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cash_balancing_sources
    ADD CONSTRAINT cash_balancing_sources_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: close_checklist_items close_checklist_items_accounting_period_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.close_checklist_items
    ADD CONSTRAINT close_checklist_items_accounting_period_id_fkey FOREIGN KEY (accounting_period_id) REFERENCES public.accounting_periods(id);


--
-- Name: close_checklist_items close_checklist_items_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.close_checklist_items
    ADD CONSTRAINT close_checklist_items_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: direct_vendor_ap_invoice_events direct_vendor_ap_invoice_events_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.direct_vendor_ap_invoice_events
    ADD CONSTRAINT direct_vendor_ap_invoice_events_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: direct_vendor_ap_invoice_events direct_vendor_ap_invoice_events_invoice_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.direct_vendor_ap_invoice_events
    ADD CONSTRAINT direct_vendor_ap_invoice_events_invoice_id_fkey FOREIGN KEY (invoice_id) REFERENCES public.direct_vendor_ap_invoices(id) ON DELETE CASCADE;


--
-- Name: direct_vendor_ap_invoices direct_vendor_ap_invoices_accounting_period_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.direct_vendor_ap_invoices
    ADD CONSTRAINT direct_vendor_ap_invoices_accounting_period_id_fkey FOREIGN KEY (accounting_period_id) REFERENCES public.accounting_periods(id);


--
-- Name: direct_vendor_ap_invoices direct_vendor_ap_invoices_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.direct_vendor_ap_invoices
    ADD CONSTRAINT direct_vendor_ap_invoices_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: document_lines document_lines_normalized_document_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.document_lines
    ADD CONSTRAINT document_lines_normalized_document_id_fkey FOREIGN KEY (normalized_document_id) REFERENCES public.normalized_documents(id) ON DELETE CASCADE;


--
-- Name: ecommerce_payout_cycles ecommerce_payout_cycles_accounting_period_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ecommerce_payout_cycles
    ADD CONSTRAINT ecommerce_payout_cycles_accounting_period_id_fkey FOREIGN KEY (accounting_period_id) REFERENCES public.accounting_periods(id);


--
-- Name: ecommerce_payout_cycles ecommerce_payout_cycles_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ecommerce_payout_cycles
    ADD CONSTRAINT ecommerce_payout_cycles_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: ecommerce_payout_cycles ecommerce_payout_cycles_source_file_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ecommerce_payout_cycles
    ADD CONSTRAINT ecommerce_payout_cycles_source_file_id_fkey FOREIGN KEY (source_file_id) REFERENCES public.source_files(id);


--
-- Name: entities entities_organization_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entities
    ADD CONSTRAINT entities_organization_id_fkey FOREIGN KEY (organization_id) REFERENCES public.organizations(id);


--
-- Name: entity_integrations entity_integrations_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entity_integrations
    ADD CONSTRAINT entity_integrations_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id) ON DELETE CASCADE;


--
-- Name: entity_settings entity_settings_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.entity_settings
    ADD CONSTRAINT entity_settings_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id) ON DELETE CASCADE;


--
-- Name: exception_queue exception_queue_accounting_period_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.exception_queue
    ADD CONSTRAINT exception_queue_accounting_period_id_fkey FOREIGN KEY (accounting_period_id) REFERENCES public.accounting_periods(id);


--
-- Name: exception_queue exception_queue_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.exception_queue
    ADD CONSTRAINT exception_queue_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: hh_ap_documents hh_ap_documents_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_documents
    ADD CONSTRAINT hh_ap_documents_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: hh_ap_invoice_overrides hh_ap_invoice_overrides_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_invoice_overrides
    ADD CONSTRAINT hh_ap_invoice_overrides_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id) ON DELETE CASCADE;


--
-- Name: hh_ap_invoices hh_ap_invoices_document_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_invoices
    ADD CONSTRAINT hh_ap_invoices_document_id_fkey FOREIGN KEY (document_id) REFERENCES public.hh_ap_documents(id);


--
-- Name: hh_ap_invoices hh_ap_invoices_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_invoices
    ADD CONSTRAINT hh_ap_invoices_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: hh_ap_remittance_bank_match_events hh_ap_remittance_bank_match_even_bank_transaction_match_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_remittance_bank_match_events
    ADD CONSTRAINT hh_ap_remittance_bank_match_even_bank_transaction_match_id_fkey FOREIGN KEY (bank_transaction_match_id) REFERENCES public.bank_transaction_matches(id) ON DELETE SET NULL;


--
-- Name: hh_ap_remittance_bank_match_events hh_ap_remittance_bank_match_events_bank_transaction_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_remittance_bank_match_events
    ADD CONSTRAINT hh_ap_remittance_bank_match_events_bank_transaction_id_fkey FOREIGN KEY (bank_transaction_id) REFERENCES public.bank_transactions(id) ON DELETE SET NULL;


--
-- Name: hh_ap_remittance_bank_match_events hh_ap_remittance_bank_match_events_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_remittance_bank_match_events
    ADD CONSTRAINT hh_ap_remittance_bank_match_events_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: hh_ap_remittance_bank_match_events hh_ap_remittance_bank_match_events_remittance_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_remittance_bank_match_events
    ADD CONSTRAINT hh_ap_remittance_bank_match_events_remittance_id_fkey FOREIGN KEY (remittance_id) REFERENCES public.hh_ap_remittances(id) ON DELETE CASCADE;


--
-- Name: hh_ap_remittance_lines hh_ap_remittance_lines_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_remittance_lines
    ADD CONSTRAINT hh_ap_remittance_lines_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: hh_ap_remittance_lines hh_ap_remittance_lines_matched_invoice_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_remittance_lines
    ADD CONSTRAINT hh_ap_remittance_lines_matched_invoice_id_fkey FOREIGN KEY (matched_invoice_id) REFERENCES public.hh_ap_invoices(id);


--
-- Name: hh_ap_remittance_lines hh_ap_remittance_lines_remittance_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_remittance_lines
    ADD CONSTRAINT hh_ap_remittance_lines_remittance_id_fkey FOREIGN KEY (remittance_id) REFERENCES public.hh_ap_remittances(id) ON DELETE CASCADE;


--
-- Name: hh_ap_remittances hh_ap_remittances_document_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_remittances
    ADD CONSTRAINT hh_ap_remittances_document_id_fkey FOREIGN KEY (document_id) REFERENCES public.hh_ap_documents(id);


--
-- Name: hh_ap_remittances hh_ap_remittances_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_remittances
    ADD CONSTRAINT hh_ap_remittances_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: hh_ap_statement_lines hh_ap_statement_lines_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_statement_lines
    ADD CONSTRAINT hh_ap_statement_lines_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: hh_ap_statement_lines hh_ap_statement_lines_matched_invoice_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_statement_lines
    ADD CONSTRAINT hh_ap_statement_lines_matched_invoice_id_fkey FOREIGN KEY (matched_invoice_id) REFERENCES public.hh_ap_invoices(id);


--
-- Name: hh_ap_statement_lines hh_ap_statement_lines_statement_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_statement_lines
    ADD CONSTRAINT hh_ap_statement_lines_statement_id_fkey FOREIGN KEY (statement_id) REFERENCES public.hh_ap_statements(id) ON DELETE CASCADE;


--
-- Name: hh_ap_statements hh_ap_statements_document_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_statements
    ADD CONSTRAINT hh_ap_statements_document_id_fkey FOREIGN KEY (document_id) REFERENCES public.hh_ap_documents(id);


--
-- Name: hh_ap_statements hh_ap_statements_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_ap_statements
    ADD CONSTRAINT hh_ap_statements_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: hh_statement_lines hh_statement_lines_matched_document_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_statement_lines
    ADD CONSTRAINT hh_statement_lines_matched_document_id_fkey FOREIGN KEY (matched_document_id) REFERENCES public.normalized_documents(id);


--
-- Name: hh_statement_lines hh_statement_lines_normalized_document_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hh_statement_lines
    ADD CONSTRAINT hh_statement_lines_normalized_document_id_fkey FOREIGN KEY (normalized_document_id) REFERENCES public.normalized_documents(id) ON DELETE CASCADE;


--
-- Name: journal_batch_workflow_events journal_batch_workflow_events_accounting_period_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.journal_batch_workflow_events
    ADD CONSTRAINT journal_batch_workflow_events_accounting_period_id_fkey FOREIGN KEY (accounting_period_id) REFERENCES public.accounting_periods(id);


--
-- Name: journal_batch_workflow_events journal_batch_workflow_events_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.journal_batch_workflow_events
    ADD CONSTRAINT journal_batch_workflow_events_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: journal_batch_workflow_events journal_batch_workflow_events_journal_batch_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.journal_batch_workflow_events
    ADD CONSTRAINT journal_batch_workflow_events_journal_batch_id_fkey FOREIGN KEY (journal_batch_id) REFERENCES public.journal_batches(id) ON DELETE CASCADE;


--
-- Name: journal_batches journal_batches_accounting_period_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.journal_batches
    ADD CONSTRAINT journal_batches_accounting_period_id_fkey FOREIGN KEY (accounting_period_id) REFERENCES public.accounting_periods(id) ON DELETE CASCADE;


--
-- Name: journal_batches journal_batches_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.journal_batches
    ADD CONSTRAINT journal_batches_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id) ON DELETE CASCADE;


--
-- Name: journal_lines journal_lines_journal_batch_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.journal_lines
    ADD CONSTRAINT journal_lines_journal_batch_id_fkey FOREIGN KEY (journal_batch_id) REFERENCES public.journal_batches(id) ON DELETE CASCADE;


--
-- Name: normalized_documents normalized_documents_source_file_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.normalized_documents
    ADD CONSTRAINT normalized_documents_source_file_id_fkey FOREIGN KEY (source_file_id) REFERENCES public.source_files(id) ON DELETE CASCADE;


--
-- Name: payroll_batches payroll_batches_accounting_period_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.payroll_batches
    ADD CONSTRAINT payroll_batches_accounting_period_id_fkey FOREIGN KEY (accounting_period_id) REFERENCES public.accounting_periods(id);


--
-- Name: payroll_batches payroll_batches_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.payroll_batches
    ADD CONSTRAINT payroll_batches_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: payroll_batches payroll_batches_source_file_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.payroll_batches
    ADD CONSTRAINT payroll_batches_source_file_id_fkey FOREIGN KEY (source_file_id) REFERENCES public.source_files(id);


--
-- Name: posting_rules posting_rules_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.posting_rules
    ADD CONSTRAINT posting_rules_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: quickbooks_connections quickbooks_connections_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.quickbooks_connections
    ADD CONSTRAINT quickbooks_connections_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: quickbooks_sync_runs quickbooks_sync_runs_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.quickbooks_sync_runs
    ADD CONSTRAINT quickbooks_sync_runs_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: quickbooks_sync_runs quickbooks_sync_runs_quickbooks_connection_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.quickbooks_sync_runs
    ADD CONSTRAINT quickbooks_sync_runs_quickbooks_connection_id_fkey FOREIGN KEY (quickbooks_connection_id) REFERENCES public.quickbooks_connections(id);


--
-- Name: quickbooks_transactions quickbooks_transactions_accounting_period_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.quickbooks_transactions
    ADD CONSTRAINT quickbooks_transactions_accounting_period_id_fkey FOREIGN KEY (accounting_period_id) REFERENCES public.accounting_periods(id);


--
-- Name: quickbooks_transactions quickbooks_transactions_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.quickbooks_transactions
    ADD CONSTRAINT quickbooks_transactions_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: recurring_month_end_rules recurring_month_end_rules_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.recurring_month_end_rules
    ADD CONSTRAINT recurring_month_end_rules_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: rule_runs rule_runs_accounting_period_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.rule_runs
    ADD CONSTRAINT rule_runs_accounting_period_id_fkey FOREIGN KEY (accounting_period_id) REFERENCES public.accounting_periods(id);


--
-- Name: rule_runs rule_runs_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.rule_runs
    ADD CONSTRAINT rule_runs_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: source_files source_files_accounting_period_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_files
    ADD CONSTRAINT source_files_accounting_period_id_fkey FOREIGN KEY (accounting_period_id) REFERENCES public.accounting_periods(id);


--
-- Name: source_files source_files_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.source_files
    ADD CONSTRAINT source_files_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: suggested_entries suggested_entries_accounting_period_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.suggested_entries
    ADD CONSTRAINT suggested_entries_accounting_period_id_fkey FOREIGN KEY (accounting_period_id) REFERENCES public.accounting_periods(id);


--
-- Name: suggested_entries suggested_entries_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.suggested_entries
    ADD CONSTRAINT suggested_entries_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- Name: suggested_entry_lines suggested_entry_lines_suggested_entry_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.suggested_entry_lines
    ADD CONSTRAINT suggested_entry_lines_suggested_entry_id_fkey FOREIGN KEY (suggested_entry_id) REFERENCES public.suggested_entries(id) ON DELETE CASCADE;


--
-- Name: vendors vendors_entity_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.vendors
    ADD CONSTRAINT vendors_entity_id_fkey FOREIGN KEY (entity_id) REFERENCES public.entities(id);


--
-- PostgreSQL database dump complete
--

\unrestrict Fah63hMU2y5DPGKgef79vadBPZfGIhcdC0VkqhcwgWKoHoK47KR6bkD87k8Jycq


