#! /bin/python
from pyspark import SparkContext
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions
from pyspark.sql.session import SparkSession
sc =SparkContext.getOrCreate()
spark = SparkSession(sc)

df_generation = spark.table("work_dataeng.generation_leandro")

df_pokemon = spark.table("work_dataeng.pokemon_bruno")
df_pokemon.show()


df_generation = df_generation.filter( "date_introduced < '2002-11-21'")


df_pokemon = df_pokemon.withColumnRenamed('generation', 'pgeneration')
df_join = df_pokemon.join(df_generation, on=[df_pokemon.pgeneration == df_generation.generation], how = 'inner')


df_join.show()


df_join.createOrReplaceTempView("df_joint") 


spark.sql("CREATE TABLE IF NOT EXISTS work_dataeng.pokemons_oldschool_leandro AS SELECT * FROM df_joint")


