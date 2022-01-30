import sys, getopt
from pyspark.sql import SparkSession
from pyspark.sql.functions import when,col
from pyspark.sql.functions import sum as SUM
from pyspark.sql.functions import *

if __name__ == "__main__":	
	spark = SparkSession.builder.appName("Dados preposto de Vendas Walmart").getOrCreate()
	
		
	# *****************************************************************
	# Extração do dataset via diretório com o schema definido 
	# *****************************************************************
	
	dataset_schema = "Store INT, Date STRING, Weekly_Sales FLOAT, Holiday_Flag STRING, Temperature FLOAT, Fuel_Price FLOAT, CPI FLOAT, Unemployment FLOAT"	
	
	df_data_vendas = spark.read.csv("/home/gabriel/Downloads/dataset/data/Walmart.csv", header=True, schema=dataset_schema)
	
	# *****************************************************************
	# Trabalhando com o dataframe -> Visualização completa do df
	# *****************************************************************
	
	df_vendas = df_data_vendas.withColumn("Holiday_Flag", when(col("Holiday_Flag") == "0", "Feriado").when(col("Holiday_Flag") == "1", "Nao Feriado")) \
		.withColumnRenamed("Store","walmartLoja") \
		.withColumnRenamed("Date","dataVenda") \
		.withColumnRenamed("Weekly_Sales","vendasSemanais") \
		.withColumnRenamed("Holiday_Flag","feriado") \
		.withColumnRenamed("Temperature","temperatura") \
		.withColumnRenamed("Fuel_Price","precoCombustivel") \
		.withColumnRenamed("CPI","cpi") \
		.withColumnRenamed("Unemployment","taxaDesemprego") \
		.orderBy("Date", "Store")
	
	
	# *****************************************************************
	# Gerando relatórios a partir do dataframe
	# *****************************************************************
	
	
	df_consolidado = df_vendas.createOrReplaceTempView("vw_consolidado_Walmart_vendas")
	
	spark.sql("INSERT overwrite db_raw_data.tb_temp_walmart_vendas \
		  SELECT \
			walmartLoja, \
			substr(dataVenda, 4, 7) as mesAno, \
			CAST(sum(vendasSemanais) as INT) as vendasMensais, \
			CAST(avg(precoCombustivel) as INT) as avg_precoCombustivel, \
			'$' as  moeda_avg_precoCombustivel, \
			'Dólar' as moeda_desc_avg_precoCombustivel, \
			CAST(avg(temperatura) as INT) as avg_temperatura, \
			CAST(avg(taxaDesemprego)  as INT) as avg_taxaDesemprego \
		   FROM vw_consolidado_Walmart_vendas \
		   group by walmartLoja, dataVenda \
		   order by walmartLoja, dataVenda \
		  ")
		  
	spark.stop()
	
