###
# coding: utf-8

# In[33]:


from pyspark.sql import SparkSession, dataframe
from pyspark.sql.functions import when, col, sum, count, isnan, round
from pyspark.sql.functions import regexp_replace, concat_ws, sha2, rtrim, substring
from pyspark.sql.functions import unix_timestamp, from_unixtime, to_date
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext

import os
import re

from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import when

spark = SparkSession.builder.master("local[*]")    .enableHiveSupport()    .getOrCreate()


# In[34]:


def salvar_df(df, file):
    output = "/input/projeto_hive/gold/" + file
    erase = "hdfs dfs -rm " + output + "/*"
    rename = "hdfs dfs -get /datalake/gold/"+file+"/part-* /input/projeto_hive/gold/"+file+".csv"
    print(rename)
    
    
    df.coalesce(1).write        .format("csv")        .option("header", True)        .option("delimiter", ";")        .mode("overwrite")        .save("/datalake/gold/"+file+"/")

    os.system(erase)
    os.system(rename)


# In[35]:


df_divisao = spark.sql("select * from desafio_curso.tbl_divisao")
df_regiao = spark.sql("select * from desafio_curso.tbl_regiao")
df_clientes = spark.sql("select * from desafio_curso.tbl_clientes")
df_endereco = spark.sql("select * from desafio_curso.tbl_endereco")
df_vendas = spark.sql("select * from desafio_curso.tbl_vendas")


# In[36]:


df_vendas = df_vendas.select('*', substring('invoice_date', 1,4).alias('Ano'),substring('invoice_date', 6,2).alias('Mes'), substring('invoice_date', 9,2).alias('Dia')).where(df_vendas.invoice_date != 'invoice_date')


# In[37]:


df_divisao_alias = df_divisao.alias("divisao")
df_regiao_alias = df_regiao.alias("regiao")
df_clientes_alias = df_clientes.alias("clientes")
df_endereco_alias = df_endereco.alias("endereco")
df_vendas_alias = df_vendas.alias("vendas")


# In[38]:


df_stage_FINAL = df_divisao_alias.join(df_regiao_alias, col("divisao.Division") == col("regiao.Region_Code"), "inner")                                  .join(df_clientes_alias, "Division")                                  .join(df_endereco_alias, "Address_Number")                                  .join(df_vendas_alias, "CustomerKey")                                  .select("divisao.*", "regiao.Region_Name", "clientes.*", "endereco.*", "vendas.*")


# In[39]:


from pyspark.sql.functions import col, when, trim
from pyspark.sql.types import StringType

# Função para preencher colunas string com 'Não informado'
def fill_string_cols_with_non_informado(df):
    for coluna in df.columns:
        tipo_dado = df.schema[coluna].dataType
        if isinstance(tipo_dado, StringType):
            df = df.withColumn(coluna, when(((trim(col(coluna)) == "") | (trim(col(coluna)) == chr(8))) | (col(coluna).isNull()), "Não informado").otherwise(col(coluna)))
    return df

# Função para preencher colunas numéricas com valor zero
def fill_numeric_cols_with_zero(df, columns):
    for coluna in columns:
        df = df.withColumn(coluna, when(col(coluna).isNull(), 0).otherwise(col(coluna)))
    return df

# Aplicar as funções para preenchimento de dados no DataFrame
df_stage_FINAL = fill_string_cols_with_non_informado(df_stage_FINAL)
df_stage_FINAL = fill_numeric_cols_with_zero(df_stage_FINAL, ['discount_amount', 'item_number', 'sales_price'])


# In[40]:


### Criando as PK


# In[41]:



df_stage_FINAL = df_stage_FINAL.withColumn('PK_TEMPO', sha2(concat_ws("",df_stage_FINAL.invoice_date, df_stage_FINAL.Ano,df_stage_FINAL.Mes,df_stage_FINAL.Dia), 256))

df_stage_FINAL = df_stage_FINAL.withColumn('PK_CLIENTES', sha2(concat_ws("",df_stage_FINAL.customerkey,df_stage_FINAL.customer,df_stage_FINAL.customer_type,df_stage_FINAL.business_unit,df_stage_FINAL.business_family,df_stage_FINAL.division,df_stage_FINAL.line_of_business,df_stage_FINAL.phone, df_stage_FINAL.regional_sales_mgr,df_stage_FINAL.search_type), 256))

df_stage_FINAL = df_stage_FINAL.withColumn('PK_LOCALIDADE', sha2(concat_ws("",df_stage_FINAL.division,df_stage_FINAL.division_name,df_stage_FINAL.region_code,df_stage_FINAL.Region_Name), 256))


# In[42]:


# Testando 


# In[43]:


df_stage_FINAL.show(10, truncate=False)


# In[44]:


df_stage_FINAL.createOrReplaceTempView("stage")


# In[45]:


spark.sql("select * from stage").show(2, truncate=False)


# In[46]:



FT_VENDAS = spark.sql("SELECT PK_CLIENTES, PK_TEMPO, PK_LOCALIDADE, COUNT(order_number) AS QUANTIDADE, SUM(sales_amount) as VALOR_TOTAL from stage group by PK_CLIENTES, PK_TEMPO, PK_LOCALIDADE ORDER BY VALOR_TOTAL DESC")


# In[47]:


df_DIM_CLIENTES = spark.sql("SELECT DISTINCT PK_CLIENTES, customer, customerkey, address_number FROM STAGE")
df_DIM_TEMPO = spark.sql("SELECT DISTINCT PK_TEMPO, invoice_date FROM STAGE")
df_DIM_LOCALIDADE = spark.sql("SELECT DISTINCT PK_LOCALIDADE, address_number, state, city, country FROM STAGE")


# In[48]:


FT_VENDAS.show(10, truncate=False)


# In[49]:


# mostrar natela a estrutura dos dataframes
salvar_df(FT_VENDAS, 'FT_VENDAS')


# In[50]:


salvar_df(df_DIM_CLIENTES, 'df_DIM_CLIENTES')


# In[51]:


salvar_df(df_DIM_TEMPO, 'df_DIM_TEMPO')


# In[52]:


salvar_df(df_DIM_LOCALIDADE, 'df_DIM_LOCALIDADE')

