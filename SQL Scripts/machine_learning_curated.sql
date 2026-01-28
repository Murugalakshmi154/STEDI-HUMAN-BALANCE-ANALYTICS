CREATE EXTERNAL TABLE `machine_learning_curated`(
  `user` string, 
  `timestamp` bigint, 
  `x` double, 
  `y` double, 
  `z` double, 
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
  's3://stedi-human-balance-sowmi/machine_learning/curated/'
TBLPROPERTIES (
  'CreatedByJob'='machine learning curated', 
  'CreatedByJobRun'='jr_78a9e7e46a74be43d4eaab0848084723b6e76637a651b7e0e3f7dfc695489343', 
  'classification'='parquet', 
  'useGlueParquetWriter'='true')