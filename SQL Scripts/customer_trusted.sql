CREATE EXTERNAL TABLE `customer_trusted`(
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
  's3://stedi-human-balance-sowmi/customer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='customer landing to trusted', 
  'CreatedByJobRun'='jr_8982285c06d8f25f9452e5d9ae3b15e82246d6612f6c38eb05e4248877c01716', 
  'classification'='parquet', 
  'useGlueParquetWriter'='true')