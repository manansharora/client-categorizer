Main Dataset Structure (Sheet: pricingmodel_Full_Data_data)

This sheet contains client-level RFQ / sales records.
Each row represents one transaction / quote / sales entry.

Columns (with inferred data types)

A. aspen_l2_client_name
	•	Type: string
	•	Description: Legal name of the client
	•	Examples:
	•	CITADEL ADVISORS LLC
	•	BREVAN HOWARD ASSET MANAGEMENT
	•	DANSKE BANK A/S

⸻

B. CcyPair
	•	Type: string
	•	Description: Currency pair or currency indicator
	•	Examples:
	•	EURGBP
	•	EURUSD
	•	USDINR
	•	GBPNZD

⸻

C. Client Region
	•	Type: enum (string)
	•	Observed Values:
	•	EUROPE
	•	APAC
	•	America
	•	CEEMA

⸻

D. Client Sector
	•	Type: enum (string)
	•	Observed Values:
	•	HF / Real Money
	•	Banks / PB
	•	Corporates
	•	Internal

⸻

E. clientregion
	•	Type: string (country)
	•	Examples:
	•	FRANCE
	•	UNITED KINGDOM
	•	CANADA
	•	SINGAPORE
	•	GERMANY
	•	TURKEY
	•	DENMARK

⸻

F. date
	•	Type: date
	•	Format Observed: MM/DD/YYYY
	•	Examples:
	•	1/28/2026
	•	1/21/2026
	•	02-04-2026

⸻

G. NotionalBucket
	•	Type: enum (ordered categorical string)

Allowed Values (explicitly defined):

1. <5M eur
2. 5-25M eur
3. 25-50M eur
4. 50-100M eur
5. >=100M eur

This is an ordinal category and should be treated as ordered for sorting/analytics.

⸻

H. productType
	•	Type: enum (string)
	•	Description: Financial product classification
	•	Full list of possible values (from third image):

EUR
KNO
NDO
EKIKO
EKI
DIG
FWDACC
NT
VKO
RKO
TPF
PTPF
DOT
OT
VOLSWAP
WRKI
RKI
MBAR
DKO
STRUCTSWP
CORRSWP
EKO
MDIG
DKI
DIGRKO
WRKO
WKNO

DCD
DNT
AVGSTRIKE
KNI
GENACCRUAL
WDIGKNO
COMMODITYFORWARD
KIKO
DIGKNO
RFADER
FWDSTRUCT
WDIGRKO
FVA
BOWO
COMMODITYSWAP
KOFVA
WDKO
VARSWAP
DIGDKO
AVGRATE
COMMODITYFUTURE
FXFUTOPT
WDIGDKO

WKNI
WDKI
AMER
BASKET
COMPOUND
RTIMER
AVGRATE_FWD
WDNT

You should treat this as a closed enum set unless extended later.

⸻

I. Sales
	•	Type: string (person name)
	•	Examples:
	•	Amanda A Harris
	•	Nicole Tay
	•	Max Formato
	•	Iris Wong
	•	Martin Hesse

⸻

J. Sales_Region
	•	Type: enum (string)
	•	Observed Values:
	•	Europe
	•	Asia
	•	Americas

⸻

K. salesdesk
	•	Type: string
	•	Examples:
	•	Investor - London
	•	Sales - Singapore
	•	EM - London
	•	Nordic - Stockholm
	•	PB - Singapore
	•	FX IG AND EM SALES

⸻

L. Tenor
	•	Type: integer (weeks)
	•	Examples:
	•	1
	•	7
	•	14
	•	21
	•	41
	•	66
	•	273

⸻

M. tenor_bucket
	•	Type: enum (string, ordered)
	•	Observed Values:
	•	<1W
	•	1W
	•	2W-1M
	•	1M-3M
	•	6M-1Y

This should be treated as an ordinal time bucket.

⸻

N. Hit Notional
	•	Type: decimal (millions)
	•	Format: string with suffix M
	•	Examples:
	•	15.00M
	•	103.50M
	•	0.12M
	•	84.10M

You may want to normalize to:

hit_notional_millions: float


⸻

2️⃣ Filter Behavior (From Dropdown Screenshot)

The NotionalBucket column:
	•	Supports sorting A → Z and Z → A
	•	Supports multi-select filtering
	•	Treated as categorical text
	•	Has search functionality

Your coding agent should:
	•	Store as ENUM
	•	Preserve ordering (1–5)
	•	Allow filtering by one or more buckets

⸻

3️⃣ Data Modeling Recommendation

If building a backend system:

Suggested SQL Schema (Example)

CREATE TABLE rfq_data (
    id SERIAL PRIMARY KEY,
    client_name TEXT,
    ccy_pair VARCHAR(10),
    client_region VARCHAR(20),
    client_sector VARCHAR(30),
    client_country VARCHAR(50),
    trade_date DATE,
    notional_bucket SMALLINT, -- 1-5
    product_type VARCHAR(50),
    sales_person VARCHAR(100),
    sales_region VARCHAR(20),
    sales_desk VARCHAR(100),
    tenor_weeks INTEGER,
    tenor_bucket VARCHAR(20),
    hit_notional_millions DECIMAL(12,2)
);


⸻

4️⃣ Business Meaning Summary

This dataset describes:
	•	Clients (by sector and geography)
	•	FX / structured product RFQs or trades
	•	Product classifications
	•	Sales coverage structure
	•	Trade size bucket (NotionalBucket)
	•	Trade tenor bucket
	•	Hit notional (executed size)
