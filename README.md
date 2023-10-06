# Databricks streaming data ETL

![image](https://github.com/Maulik-A/Databricks_stream_ETL/assets/58858333/7ad38e32-ea62-41c5-8614-600c55de5467)

1) Azure datafactory triggers every hour/day to pull the data and save in the Azure blob storage (Data Lake).
2) Databricks ingest the data in the Lakehouse. Autoloader moves the cleans the data and load from Bronze to Silver Delta live table (DLT).
3) Further aggregation is done on the data and loaded in the Gold DLT.
4) Eventually data is visualised on the PowerBI.
