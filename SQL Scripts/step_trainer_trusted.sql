CREATE EXTERNAL TABLE `step_trainer_trusted`(
  `serialnumber` string, 
  `sensorreadingtime` bigint, 
  `distancefromobject` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://stedi-human-balance-sowmi/step_trainer/trusted/'
TBLPROPERTIES (
  'CreatedByJob'='step trainer landing to trusted', 
  'CreatedByJobRun'='jr_7ceb32f2f1520d21ed09af8904d0027f21a3c3f8624b9467f9e1ecd72b4b5838', 
  'classification'='parquet', 
  'useGlueParquetWriter'='true')