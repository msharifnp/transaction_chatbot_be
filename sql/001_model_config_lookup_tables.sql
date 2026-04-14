CREATE TABLE IF NOT EXISTS "data"."ModelDefinition" (
    "Id" SERIAL PRIMARY KEY,
    "ProviderName" VARCHAR(100) NOT NULL,
    "ModelName" VARCHAR(100) NOT NULL,
    "IsActive" BOOLEAN NOT NULL DEFAULT TRUE,
    "SortOrder" INTEGER NOT NULL DEFAULT 0,
    "CreatedAt" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "UpdatedAt" TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT "UQ_ModelDefinition_Provider_ModelName" UNIQUE ("ProviderName", "ModelName")
);

CREATE TABLE data."AccountMasters"
(
  "Id" text PRIMARY KEY,
  "TenantId" text NOT NULL,
  "AccountNumber" text NOT NULL,
  "RentalCharge" numeric(18,2),
  "BandWidth" text,
  "ChargePerMinute" numeric(18,2),
  "ServiceName" text,
  "SiteName" text,
  "SiteLocationCode" text,
  "SiteAddress" text,
  "CostName" text,
  "CostCode" text,
  "LineName" text,
  "ConnectionName" text,
  "ProviderName" text,
  "DepartmentName" text,
  "LastUpdated" timestamp DEFAULT now()
);

CREATE INDEX Idx_Account_Masters ON data."AccountMasters" ("TenantId", "AccountNumber");



CREATE TABLE data."InvoiceLines"
(
    "Id" TEXT PRIMARY KEY,
	"InvoiceDate" TIMESTAMP,
    "BillReceiveDate" TIMESTAMP,
    "TenantId" TEXT NOT NULL,
    "AccountNumber" TEXT NOT NULL,
    "InvoiceNumber" TEXT,
    "InvoiceStatusType" TEXT,
    "InvoiceApprovalStatus" TEXT,
    "PaymentStatus" TEXT,
    "NetTotal" NUMERIC(18,2),
    "TotalTax" NUMERIC(18,2),
    "GrandTotal" NUMERIC(18,2),
    "UsageCharge" NUMERIC(18,2),
    "ExpectedAmount" NUMERIC(18,2),
    "VerificationResult" TEXT,
    "LastUpdated" TIMESTAMP DEFAULT now()
);



CREATE INDEX Idx_Invoice_Lines ON data."InvoiceLines" ("TenantId", "AccountNumber","InvoiceDate");


CREATE TABLE data."ChatInvoices"
(
    "Id" TEXT PRIMARY KEY,
    "InvoiceDate" TIMESTAMP,
    "BillReceiveDate" TIMESTAMP,
    "TenantId" TEXT NOT NULL,
    "AccountNumber" TEXT NOT NULL,
    "InvoiceNumber" TEXT,
    "InvoiceStatusType" TEXT,
    "InvoiceApprovalStatus" TEXT,
    "PaymentStatus" TEXT,
    "NetTotal" NUMERIC(18,2),
    "TotalTax" NUMERIC(18,2),
    "GrandTotal" NUMERIC(18,2),
    "UsageCharge" NUMERIC(18,2),
    "ExpectedAmount" NUMERIC(18,2),
    "VerificationResult" TEXT,
    "RentalCharge" NUMERIC(18,2),
    "BandWidth" TEXT,
    "ChargePerMinute" NUMERIC(18,2),
    "ServiceName" TEXT,
    "SiteName" TEXT,
    "SiteLocationCode" TEXT,
    "SiteAddress" TEXT,
    "CostName" TEXT,
    "CostCode" TEXT,
    "LineName" TEXT,
    "ConnectionName" TEXT,
    "ProviderName" TEXT,
    "DepartmentName" TEXT,
    "LastUpdated" TIMESTAMP DEFAULT now()
);

CREATE INDEX Idx_Chat_Invoices ON data."ChatInvoices" ("TenantId","InvoiceDate");

INSERT INTO "data"."ModelDefinition" ("ProviderName", "ModelName", "SortOrder")
VALUES
    ('OpenAI',  'gpt-4', 1),
    ('OpenAI',  'gpt-4-turbo', 2),
    ('OpenAI',  'gpt-4o', 3),
    ('OpenAI',  'gpt-4o-mini', 4),
    ('OpenAI',  'gpt-4.1', 5),
    ('OpenAI',  'gpt-4.1-mini', 6),
    ('OpenAI',  'gpt-4.1-nano', 7),
    ('OpenAI',  'gpt-5', 8),
    ('OpenAI',  'gpt-5.1', 9),
    ('OpenAI',  'gpt-5.2', 10),
    ('OpenAI',  'gpt-5.3', 11),
    ('OpenAI',  'gpt-5.4', 12),

    ('Gemini',  'gemini-1.5-pro', 1),
    ('Gemini',  'gemini-1.5-flash', 2),
    ('Gemini',  'gemini-2.0-pro', 3),
    ('Gemini',  'gemini-2.0-flash', 4),
    ('Gemini',  'gemini-2.0-flash-lite', 5),
    ('Gemini',  'gemini-2.5-pro', 6),

    ('Anthropic', 'Claude-3-Opus', 1),
    ('Anthropic', 'Claude-3-Sonnet', 2),
    ('Anthropic', 'Claude-3-Haiku', 3),
    ('Anthropic', 'Claude-3.5-Sonnet', 4),
    ('Anthropic', 'Claude-3.5-Haiku', 5),
    ('Anthropic', 'Claude-4.5-Sonnet', 6),
    ('Anthropic', 'Claude-4.5-Haiku', 7),
    ('Google Cloud', 'chirp-3', 1)

ON CONFLICT ("ProviderName", "ModelName") DO NOTHING;


CREATE TABLE "data"."ModelConfig" (
	"Id" bigserial NOT NULL,
	"TenantId" varchar(100) NOT NULL,
	"Purpose" varchar(50) NOT NULL,
	"Provider" varchar(50) NOT NULL,
	"ModelName" varchar(100) NOT NULL,
	"CredentialsRef" text NOT NULL,
	"Config" jsonb NULL,
	"CreatedAt" timestamp DEFAULT CURRENT_TIMESTAMP NULL,
	"UpdatedAt" timestamp NULL,
	CONSTRAINT "ModelConfig_pkey" PRIMARY KEY ("Id"),
	CONSTRAINT uq_tenant_purpose UNIQUE ("TenantId", "Purpose")
);

select *from "data"."ModelConfig" mc 

UPDATE "data"."ModelConfig"
SET 
    "Provider"  = 'Gemini',
    "ModelName" = 'gemini-2.5-pro'
WHERE "Id" = 1;



CREATE TABLE "data"."GeneratedFiles" (
    "FileId" SERIAL PRIMARY KEY,
    "TenantId" VARCHAR(255) NOT NULL,
    "AccountNumber" VARCHAR(100) NOT NULL,
    "FileName" VARCHAR(255) NOT NULL,
    "FilePath" TEXT NOT NULL,  
    "FileSize" BIGINT,
    "FileType" VARCHAR(50) DEFAULT 'comparison_report',
    "CreatedAt" TIMESTAMP NOT NULL DEFAULT NOW(),
    "ExpiresAt" TIMESTAMP,  -- Optional: auto-delete after X days
    "DownloadCount" INTEGER DEFAULT 0,
    "LastDownloadedAt" TIMESTAMP,
    "IsDeleted" BOOLEAN DEFAULT FALSE
    );


CREATE INDEX Idx_GeneratedFiles ON data."GeneratedFiles" ("TenantId","AccountNumber");

select * from "data"."GeneratedFiles" 


drop table "data". "AiTokenUsage"

CREATE table "data". "AiTokenUsage" (
    "Id" SERIAL PRIMARY KEY,
    "TenantId" VARCHAR(255) NOT NULL,
    "UserId" VARCHAR(255) NOT NULL,
    "SessionId" VARCHAR(255) NOT NULL,
    "Purpose" VARCHAR(255) NOT NULL,
    "PromptTokens" INT,
    "CompletionTokens" INT,
    "ThoughtsTokens" INT,
    "CacheTokens" INT,
    "TotalTokens" INT,
    "ModelName" VARCHAR(255) NOT NULL,
    "Provider" VARCHAR(255) NOT NULL,
    "LatencyMs" INT NULL,
    "CreatedAt" TIMESTAMP DEFAULT NOW()
);



CREATE INDEX idx_ai_token_usage_main 
ON "data"."AiTokenUsage" ("TenantId", "UserId", "CreatedAt");





select *from "data"."AiTokenUsage"


CREATE TABLE "data"."AiTokenUsageSummary" (
    "Id"           SERIAL PRIMARY KEY,
    "TenantId"     VARCHAR(255) NOT NULL,
    "UserId"       VARCHAR(255) NOT NULL,
    "Provider"     VARCHAR(255) NOT NULL,
    "Date"         DATE NOT NULL,
    "InputTokens"  BIGINT NOT NULL DEFAULT 0,
    "OutputTokens" BIGINT NOT NULL DEFAULT 0,
    "TotalTokens"  BIGINT NOT NULL DEFAULT 0,
    "RecordCount"  INT NOT NULL DEFAULT 0,
    "UpdatedAt"    TIMESTAMP DEFAULT NOW(),

    CONSTRAINT "uq_token_summary" 
        UNIQUE ("TenantId", "UserId", "Provider", "Date")
);


CREATE INDEX idx_ai_token_usage_summary 
ON "data"."AiTokenUsageSummary" ("TenantId", "UserId","Date");

INSERT INTO "data"."AiTokenUsageSummary"
    ("TenantId", "UserId", "Provider", "Date",
     "InputTokens", "OutputTokens", "TotalTokens", "RecordCount")

SELECT
    "TenantId",
    "UserId",
    "Provider",
    DATE("CreatedAt")                                        AS "Date",
    SUM(COALESCE("PromptTokens", 0))                         AS "InputTokens",
    SUM(
        COALESCE("CompletionTokens", 0) +
        COALESCE("ThoughtsTokens", 0)   +
        COALESCE("CacheTokens", 0)
    )                                                        AS "OutputTokens",
    SUM(COALESCE("TotalTokens", 0))                          AS "TotalTokens",
    COUNT(*)                                                 AS "RecordCount"

FROM "data"."AiTokenUsage"
WHERE DATE("CreatedAt") = CURRENT_DATE   -- today's data only, recalculates each run

GROUP BY "TenantId", "UserId", "Provider", DATE("CreatedAt")

ON CONFLICT ("TenantId", "UserId", "Provider", "Date")
DO UPDATE SET
    "InputTokens"  = EXCLUDED."InputTokens",
    "OutputTokens" = EXCLUDED."OutputTokens",
    "TotalTokens"  = EXCLUDED."TotalTokens",
    "RecordCount"  = EXCLUDED."RecordCount",
    "UpdatedAt"    = NOW();

select *from "data"."AiTokenUsageSummary"



CREATE OR REPLACE PROCEDURE sync_account_masters()
LANGUAGE plpgsql
AS $$
BEGIN
	MERGE INTO data."AccountMasters" t
	USING (
	    SELECT
	        am."Id"::text AS "Id",
	        am."TenantId"::text AS "TenantId",
	        am."AccountNumber"::text AS "AccountNumber",
	        am."RentalCharge"::numeric(18,2) AS "RentalCharge",
	        am."BandWidth"::text AS "BandWidth",
	        am."ChargePerMinute"::numeric(18,2) AS "ChargePerMinute",
	        sm."Name" AS "ServiceName",
	        st."Name" AS "SiteName",
	        st."LocationCode" AS "SiteLocationCode",
	        st."Address" AS "SiteAddress",
	        cc."Name" AS "CostName",
	        cc."Code" AS "CostCode",
	        lt."Name" AS "LineName",
	        ct."Name" AS "ConnectionName",
	        p."Name" AS "ProviderName",
	        d."Name" AS "DepartmentName"
	    FROM public."AccountMasters" am
	    LEFT JOIN public."Providers" p ON am."ProviderId" = p."Id"
	    LEFT JOIN public."ConnectionTypes" ct ON am."ConnectionTypeId" = ct."Id"
	    LEFT JOIN public."LineTypes" lt ON am."LineTypeId" = lt."Id"
	    LEFT JOIN public."CostCenters" cc ON am."CostCenterId" = cc."Id"
	    LEFT JOIN public."SiteMasters" st ON am."SiteMasterId" = st."Id"
	    LEFT JOIN public."ServiceMasters" sm ON am."ServiceMasterId" = sm."Id"
	    LEFT JOIN public."Departments" d ON am."DepartmentId" = d."Id"
	) s
	ON t."Id" = s."Id"
	
	
	WHEN MATCHED THEN
	UPDATE SET
	    "TenantId" = s."TenantId",
	    "AccountNumber" = s."AccountNumber",
	    "RentalCharge" = s."RentalCharge",
	    "BandWidth" = s."BandWidth",
	    "ChargePerMinute" = s."ChargePerMinute",
	    "ServiceName" = s."ServiceName",
	    "SiteName" = s."SiteName",
	    "SiteLocationCode" = s."SiteLocationCode",
	    "SiteAddress" = s."SiteAddress",
	    "CostName" = s."CostName",
	    "CostCode" = s."CostCode",
	    "LineName" = s."LineName",
	    "ConnectionName" = s."ConnectionName",
	    "ProviderName" = s."ProviderName",
	    "DepartmentName" = s."DepartmentName",
	    "LastUpdated" = now()
	
	
	WHEN NOT MATCHED THEN
	INSERT (
	    "Id", 
		"TenantId", 
		"AccountNumber",
	    "RentalCharge", 
		"BandWidth", 
		"ChargePerMinute",
	    "ServiceName", 
		"SiteName", 
		"SiteLocationCode",
	    "SiteAddress", 
		"CostName", 
		"CostCode",
	    "LineName", 
		"ConnectionName",
	    "ProviderName", 
		"DepartmentName", 
		"LastUpdated"
	)
	VALUES (
	    s."Id", 
		s."TenantId", 
		s."AccountNumber",
	    s."RentalCharge", 
		s."BandWidth", 
		s."ChargePerMinute",
	    s."ServiceName", 
		s."SiteName", 
		s."SiteLocationCode",
	    s."SiteAddress", 
		s."CostName", 
		s."CostCode",
	    s."LineName", 
		s."ConnectionName",
	    s."ProviderName", 
		s."DepartmentName", now()
	)
	
	
	WHEN NOT MATCHED BY SOURCE THEN
	DELETE;
END;
$$;

CALL sync_account_masters();






CREATE EXTENSION IF NOT EXISTS pg_cron;






CREATE OR REPLACE PROCEDURE sync_invoice_lines()
LANGUAGE plpgsql
AS $$
BEGIN
    MERGE INTO data."InvoiceLines" t
    USING (
        SELECT
            il."Id"::text AS "Id",
            il."InvoiceDate"::timestamp AS "InvoiceDate",
            ih."BillReceivedDate"::timestamp AS "BillReceiveDate",
            am."TenantId"::text AS "TenantId",
            am."AccountNumber"::text AS "AccountNumber",
            ih."InvoiceNumber"::text AS "InvoiceNumber",

            CASE il."InvoiceStatusType"
                WHEN 0 THEN 'System Accepted'
                WHEN 1 THEN 'Accepted'
                WHEN 2 THEN 'System Disputed'
                WHEN 3 THEN 'Disputed'
            END AS "InvoiceStatusType",

            CASE il."InvoiceApprovalStatus"
                WHEN 0 THEN 'Pending'
                WHEN 1 THEN 'Initiated'
                WHEN 2 THEN 'Approval InProgress'
                WHEN 3 THEN 'Approval Completed'
            END AS "InvoiceApprovalStatus",

            CASE il."PaymentStatus"
                WHEN 0 THEN 'Ready For Payment'
                WHEN 1 THEN 'Partially Settled'
                WHEN 2 THEN 'Settled'
            END AS "PaymentStatus",

            il."NetTotal"::numeric(18,2) AS "NetTotal",
            il."TotalTax"::numeric(18,2) AS "TotalTax",
            il."GrandTotal"::numeric(18,2) AS "GrandTotal",
            il."UsageCharge"::numeric(18,2) AS "UsageCharge",
            il."ExpectedAmount"::numeric(18,2) AS "ExpectedAmount",

            CASE
                WHEN il."VerificationResult" = TRUE THEN 'Verified'
                WHEN il."VerificationResult" = FALSE THEN 'Not Verified'
                ELSE 'Unknown'
            END AS "VerificationResult"

        FROM public."InvoiceLines" il
        LEFT JOIN public."InvoiceHeaders" ih
            ON il."InvoiceHeadersId" = ih."Id"
        LEFT JOIN public."AccountMasters" am
            ON il."AccountMasterId" = am."Id"
    ) s
    ON t."Id" = s."Id"

    -- UPDATE EXISTING ROWS
    WHEN MATCHED THEN
        UPDATE SET
            "InvoiceDate" = s."InvoiceDate",
            "BillReceiveDate" = s."BillReceiveDate",
            "TenantId" = s."TenantId",
            "AccountNumber" = s."AccountNumber",
            "InvoiceNumber" = s."InvoiceNumber",
            "InvoiceStatusType" = s."InvoiceStatusType",
            "InvoiceApprovalStatus" = s."InvoiceApprovalStatus",
            "PaymentStatus" = s."PaymentStatus",
            "NetTotal" = s."NetTotal",
            "TotalTax" = s."TotalTax",
            "GrandTotal" = s."GrandTotal",
            "UsageCharge" = s."UsageCharge",
            "ExpectedAmount" = s."ExpectedAmount",
            "VerificationResult" = s."VerificationResult",
            "LastUpdated" = now()

    -- INSERT NEW ROWS
    WHEN NOT MATCHED THEN
        INSERT (
            "Id",
            "InvoiceDate",
            "BillReceiveDate",
            "TenantId",
            "AccountNumber",
            "InvoiceNumber",
            "InvoiceStatusType",
            "InvoiceApprovalStatus",
            "PaymentStatus",
            "NetTotal",
            "TotalTax",
            "GrandTotal",
            "UsageCharge",
            "ExpectedAmount",
            "VerificationResult",
            "LastUpdated"
        )
        VALUES (
            s."Id",
            s."InvoiceDate",
            s."BillReceiveDate",
            s."TenantId",
            s."AccountNumber",
            s."InvoiceNumber",
            s."InvoiceStatusType",
            s."InvoiceApprovalStatus",
            s."PaymentStatus",
            s."NetTotal",
            s."TotalTax",
            s."GrandTotal",
            s."UsageCharge",
            s."ExpectedAmount",
            s."VerificationResult",
            now()
        )

    -- DELETE REMOVED ROWS
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
END;
$$;

CALL sync_invoice_lines();

select *from "InvoiceLines" il 

CREATE OR REPLACE PROCEDURE sync_chat_invoices()
LANGUAGE plpgsql
AS $$
BEGIN
    MERGE INTO data."ChatInvoices" t
    USING (
        SELECT
            il."Id"::text AS "Id",
            il."InvoiceDate"::timestamp AS "InvoiceDate",
            ih."BillReceivedDate"::timestamp AS "BillReceiveDate",
            am."TenantId"::text AS "TenantId",
            am."AccountNumber"::text AS "AccountNumber",
            ih."InvoiceNumber"::text AS "InvoiceNumber",

            CASE il."InvoiceStatusType"
                WHEN 0 THEN 'System Accepted'
                WHEN 1 THEN 'Accepted'
                WHEN 2 THEN 'System Disputed'
                WHEN 3 THEN 'Disputed'
            END AS invoice_status_type,

            CASE il."InvoiceApprovalStatus"
                WHEN 0 THEN 'Pending'
                WHEN 1 THEN 'Initiated'
                WHEN 2 THEN 'Approval InProgress'
                WHEN 3 THEN 'Approval Completed'
            END AS invoice_approval_status,

            CASE il."PaymentStatus"
                WHEN 0 THEN 'Ready For Payment'
                WHEN 1 THEN 'Partially Settled'
                WHEN 2 THEN 'Settled'
            END AS payment_status,

            il."NetTotal"::numeric(18,2) AS "NetTotal",
            il."TotalTax"::numeric(18,2) AS "TotalTax",
            il."GrandTotal"::numeric(18,2) AS "GrandTotal",
            il."UsageCharge"::numeric(18,2) AS "UsageCharge",
            il."ExpectedAmount"::numeric(18,2) AS "ExpectedAmount",

            CASE
                WHEN il."VerificationResult" = TRUE THEN 'Verified'
                WHEN il."VerificationResult" = FALSE THEN 'Not Verified'
                ELSE 'Unknown'
            END AS verification_result,

            am."RentalCharge"::numeric(18,2) AS "RentalCharge",
            am."BandWidth" AS "BandWidth",
            am."ChargePerMinute"::numeric(18,2) AS "ChargePerMinute",

            sm."Name" AS "ServiceName",
            st."Name" AS "SiteName",
            st."LocationCode" AS "SiteLocationCode",
            st."Address" AS "SiteAddress",
            cc."Name" AS "CostName",
            cc."Code" AS "CostCode",
            lt."Name" AS "LineName",
            ct."Name" AS "ConnectionName",
            p."Name" AS "ProviderName",
            d."Name" AS "DepartmentName"

        FROM public."InvoiceLines" il
        LEFT JOIN public."InvoiceHeaders" ih ON il."InvoiceHeadersId" = ih."Id"
        LEFT JOIN public."AccountMasters" am ON il."AccountMasterId" = am."Id"
        LEFT JOIN public."Providers" p ON am."ProviderId" = p."Id"
        LEFT JOIN public."ConnectionTypes" ct ON am."ConnectionTypeId" = ct."Id"
        LEFT JOIN public."LineTypes" lt ON am."LineTypeId" = lt."Id"
        LEFT JOIN public."CostCenters" cc ON am."CostCenterId" = cc."Id"
        LEFT JOIN public."SiteMasters" st ON am."SiteMasterId" = st."Id"
        LEFT JOIN public."ServiceMasters" sm ON am."ServiceMasterId" = sm."Id"
        LEFT JOIN public."Departments" d ON am."DepartmentId" = d."Id"
    ) s
    ON t."Id" = s."Id" and t."TenantId" = s."TenantId"

    -- UPDATE EXISTING
    WHEN MATCHED THEN
        UPDATE SET
            "InvoiceDate" = s."InvoiceDate",
            "BillReceiveDate" = s."BillReceiveDate",
            "TenantId" = s."TenantId",
            "AccountNumber" = s."AccountNumber",
            "InvoiceNumber" = s."InvoiceNumber",
            "InvoiceStatusType" = s."InvoiceStatusType",
            "InvoiceApprovalStatus" = s."InvoiceApprovalStatus",
            "PaymentStatus" = s."PaymentStatus",
            "NetTotal" = s."NetTotal",
            "TotalTax" = s."TotalTax",
            "GrandTotal" = s."GrandTotal",
            "UsageCharge" = s."UsageCharge",
            "ExpectedAmount" = s."ExpectedAmount",
            "VerificationResult" = s."VerificationResult",
            "RentalCharge" = s."RentalCharge",
            "BandWidth" = s."BandWidth",
            "ChargePerMinute" = s."ChargePerMinute",
            "ServiceName" = s."ServiceName",
            "SiteName" = s."SiteName",
            "SiteLocationCode" = s."SiteLocationCode",
            "SiteAddress" = s."SiteAddress",
            "CostName" = s."CostName",
            "CostCode" = s."CostCode",
            "LineName" = s."LineName",
            "ConnectionName" = s."ConnectionName",
            "ProviderName" = s."ProviderName",
            "DepartmentName" = s."DepartmentName",
            "LastUpdated" = now()

    -- INSERT NEW
    WHEN NOT MATCHED THEN
        INSERT (
            "Id",
            "InvoiceDate",
            "BillReceiveDate",
            "TenantId",
            "AccountNumber",
            "InvoiceNumber",
            "InvoiceStatusType",
            "InvoiceApprovalStatus",
            "PaymentStatus",
            "NetTotal",
            "TotalTax",
            "GrandTotal",
            "UsageCharge",
            "ExpectedAmount",
            "VerificationResult",
            "RentalCharge",
            "BandWidth",
            "ChargePerMinute",
            "ServiceName",
            "SiteName",
            "SiteLocationCode",
            "SiteAddress",
            "CostName",
            "CostCode",
            "LineName",
            "ConnectionName",
            "ProviderName",
            "DepartmentName",
            "LastUpdated"
        )
        VALUES (
            s."Id",
            s."InvoiceDate",
            s."BillReceiveDate",
            s."TenantId",
            s."AccountNumber",
            s."InvoiceNumber",
            s."InvoiceStatusType",
            s."InvoiceApprovalStatus",
            s."PaymentStatus",
            s."NetTotal",
            s."TotalTax",
            s."GrandTotal",
            s."UsageCharge",
            s."ExpectedAmount",
            s."VerificationResult",
            s."RentalCharge",
            s."BandWidth",
            s."ChargePerMinute",
            s."ServiceName",
            s."SiteName",
            s."SiteLocationCode",
            s."SiteAddress",
            s."CostName",
            s."CostCode",
            s."LineName",
            s."ConnectionName",
            s."ProviderName",
            s."DepartmentName",
            now()
        )

    -- DELETE REMOVED ROWS
    WHEN NOT MATCHED BY SOURCE THEN
        DELETE;
END;
$$;

CALL sync_chat_invoices();


