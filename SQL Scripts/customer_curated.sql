CREATE EXTERNAL TABLE `customer_curated`(
  `customername` string, 
  `email` string, 
  `phone` string, 
  `birthday` string, 
  `serialnumber` string, 
  `registrationdate` bigint, 
  `lastupdatedate` bigint, 
  `sharewithresearchasofdate` bigint, 
  `sharewithpublicasofdate` bigint, 
  `sharewithfriendsasofdate` bigint)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://stedi-human-balance-sowmi/customer/curated/'
TBLPROPERTIES (
  'CreatedByJob'='customer trusted to curated', 
  'CreatedByJobRun'='jr_386d97e11d04e778a7d72f33a3ce09db54b4c257a83f2c6866619e07105fd5d5', 
  'classification'='parquet', 
  'useGlueParquetWriter'='true')