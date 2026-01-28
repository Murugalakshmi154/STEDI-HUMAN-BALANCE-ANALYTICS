CREATE EXTERNAL TABLE `accelerometer_trusted`(
  `user` string, 
  `timestamp` bigint, 
  `x` double, 
  `y` double, 
  `z` double)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://stedi-human-balance-sowmi/accelerometer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='accelerometer landing to trusted', 
  'CreatedByJobRun'='jr_bec220960042746ccff3e9971c678539498ca79f1826c6bfcccfdde775d79e75', 
  'classification'='parquet', 
  'useGlueParquetWriter'='true')