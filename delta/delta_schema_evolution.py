# 4.1. Configuração do PySpark + Delta
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = SparkSession.builder.appName("Delta") \
    .config("spark.driver.host", "localhost") \
    .config("spark.submit.deployMode", "client") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# 4.2. Criando DataFrame de Celulares
df = spark.createDataFrame(
    [
        ("iPhone 6", "Apple"),
        ("Galaxy S22", "Samsung"),
        ("Redmi Note 11", "Xiaomi"),
    ],
    ["nome", "marca"],
)

df.write.format("delta").saveAsTable("celulares")


# 4.3. Tentando Adicionar DataFrame com Schema Diferente
df = spark.createDataFrame(
    [
        ("Galaxy S9", "Samsung", 2016),
    ],
    ["nome", "marca", "ano"],
)

df.write.mode("append").format("delta").saveAsTable("celulares")


# 4.4. Habilitando o Schema Evolution

df = spark.createDataFrame(
    [
        ("Galaxy S9", "Samsung", 2016),
    ],
    ["nome", "marca", "ano"],
)

df.write.option("mergeSchema", True).mode("append").format("delta").saveAsTable("celulares")


# 4.5 Visualizando Tabela Resultante
df = spark.read.table("celulares")
df.show()
# +-------------+-------+----+
#|         nome|  marca| ano|
# +-------------+-------+----+
# |    Galaxy S9|Samsung|2016|
# |Redmi Note 11| Xiaomi|null|
# |   Galaxy S22|Samsung|null|
# |     iPhone 6|  Apple|null|
# +-------------+-------+----+


