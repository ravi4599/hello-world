CREATE VIEW IF NOT EXISTS MO_NIELSEN_TRADITIONAL_BV
COPY GRANTS
AS SELECT
AirDetectedEvent,
Product,
"Week Of",
"Air Date",
"Market Code",
"Market Rank",
Market,
Network,
Station,
"Media Type",
"Type of Demographic",
Demographic,
"Data Stream",
"Air ISCI",
"Cmml Title",
"Allocated Impression",
"Allocated Rating",
"DMA UE",
"Rtg Source",
Clearance,
BRAND,
LOADDATETIME,
LOADID,
LOADTYPE,
FILENAME
FROM RDS_DEV.IRB.MO_NIELSEN_TRADITIONAL;