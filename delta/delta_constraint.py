# 2. Configuração do PySpark + Delta
from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

builder = SparkSession.builder.appName("DeltaLakeConstraints") \
    .config("spark.driver.host", "localhost") \
    .config("spark.submit.deployMode", "client") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# 3.1 Criando DataFrame de Usuários
df_usuarios = spark.createDataFrame(
    [
        ("Thiago", 12, "00011122233"),
        ("Guilherme", 14, "00011122233"),
        ("Joana", 35, "00011122233"),
        ("Maristela", 40, "00011122233"),
    ],
    ["nome", "idade", "cpf"],
)

df_usuarios.write.format("delta").saveAsTable("usuarios")

# 3.2 Adicionando Constraint de Número de Caracteres
spark.sql("""
    ALTER TABLE default.usuarios
    ADD CONSTRAINT cpfLength11 CHECK (LENGTH(cpf) == 11)

""")


# 3.3 Adicionando Constraint de Valores Positivos
spark.sql("""
    ALTER TABLE default.usuarios
    ADD CONSTRAINT idadePositiva CHECK (idade >= 0)
""")


# 3.4 Listando as Constraints da Tabela
spark.sql("SHOW TBLPROPERTIES usuarios").show()


# 3.5 Verificando o Funcionamento das Constraints
df = spark.createDataFrame(
    [
        ("João", 10, "1"),
    ],
    ["nome", "idade", "cpf"],
)

df.write.format("delta").mode("append").saveAsTable("usuarios")

df = spark.createDataFrame(
    [
        ("João", -1, "12345678901"),
    ],
    ["nome", "idade", "cpf"],
)

df.write.format("delta").mode("append").saveAsTable("usuarios")


# 3.5 Valores Not Nullable
spark.sql("""
    CREATE TABLE default.usuarios (
        nome STRING,
        cpf STRING NOT NULL
    ) USING DELTA;
""")

df = spark.createDataFrame(
    [
        ("Thiago", "00011122233"),
        ("Guilherme", "00011122233"),
        ("Joana", "00011122233"),
        ("Maristela", "00011122233"),
    ],
    ["nome", "cpf"],
)

df.write.format("delta").mode("append").saveAsTable("usuarios")


df = spark.createDataFrame(
    [
        ("João", None),
    ],
    schema=StructType([
        StructField("nome", StringType()),
        StructField("cpf", StringType()),
    ])
)

df.write.format("delta").mode("append").saveAsTable("usuarios")

