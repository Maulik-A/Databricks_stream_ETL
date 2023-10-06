-- Databricks notebook source
create streaming live table green_bronze

comment 'Loding data from blob to table'

AS select * from cloud_files(
  "/mnt/green-data/Bronze/","json" )

-- COMMAND ----------

CREATE OR REFRESH STREAMING live TABLE green_silver
AS 
select
  `from` ts,
  generationmix:[0].perc  biomass,
  generationmix:[1].perc  coal,
  generationmix:[2].perc  imports,
  generationmix:[3].perc  gas,
  generationmix:[4].perc  nuclear,
  generationmix:[5].perc  other,
  generationmix:[6].perc  hydro,
  generationmix:[7].perc  solar,
  generationmix:[8].perc  wind
from stream(live.green_bronze)

-- COMMAND ----------

CREATE OR REFRESH STREAMING live TABLE green_gold
AS 
select
  cast(ts as TIMESTAMP) ,
  cast(biomass as DOUBLE),
  generationmix:[1].perc  coal,
  generationmix:[2].perc  imports,
  generationmix:[3].perc  gas,
  generationmix:[4].perc  nuclear,
  generationmix:[5].perc  other,
  generationmix:[6].perc  hydro,
  generationmix:[7].perc  solar,
  generationmix:[8].perc  wind
from stream(live.green_silver)
