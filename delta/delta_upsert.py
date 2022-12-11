# 3.2. Configuração do PySpark + Delta
from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = SparkSession.builder.appName("Delta") \
    .config("spark.driver.host", "localhost") \
    .config("spark.submit.deployMode", "client") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# 3.3. Criando Tabela de Usuários
df_table_usuarios = spark.createDataFrame(
    [
        ("Thiago", 12, "11111111111"),
        ("Guilherme", 14, "22222222222"),
        ("Joana", 35, "33333333333"),
        ("Maristela", 40, "44444444444"),
    ],
    ["nome", "idade", "cpf"],
)

df_table_usuarios.write.format("delta").saveAsTable("usuarios")


# 3.4. Criando DataFrame de Ingestão de Usuários
df_novos_usuarios = spark.createDataFrame(
    [
        ("Thiago Heron", 99, "11111111111"),       # Usuário Inserido
        ("Cristiano Ronaldo", 37, "55555555555"),  # Novo Usuário
        ("Messi", 35, "66666666666"),              # Novo Usuário
        ("Neymar", 30, "77777777777"),             # Novo Usuário
    ],
    ["nome", "idade", "cpf"],
)

# 3.5. Upsert: Atualizando e/ou Adicionando Usuários
df_table_usuarios = DeltaTable.forPath(spark, "spark-warehouse/usuarios")

df_table_usuarios.alias("table_usuarios") \
  .merge(
    df_novos_usuarios.alias("novos_usuarios"),
    "table_usuarios.cpf = novos_usuarios.cpf"
  ) \
  .whenMatchedUpdate(set =
    {
      "cpf": "novos_usuarios.cpf",
      "nome": "novos_usuarios.nome",
      "idade": "novos_usuarios.idade"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "cpf": "novos_usuarios.cpf",
      "nome": "novos_usuarios.nome",
      "idade": "novos_usuarios.idade"
    }
  ) \
  .execute()


# 3.6 Visualização do Upsert
df_table_usuarios = DeltaTable.forPath(spark, "spark-warehouse/usuarios")
df_table_usuarios.toDF().show()
# +-----------------+-----+-----------+
# |             nome|idade|        cpf|
# +-----------------+-----+-----------+
# |     Thiago Heron|   99|11111111111|
# |Cristiano Ronaldo|   37|55555555555|
# |            Messi|   35|66666666666|
# |           Neymar|   30|77777777777|
# |        Guilherme|   14|22222222222|
# |        Maristela|   40|44444444444|
# |            Joana|   35|33333333333|
# +-----------------+-----+-----------+


# 3.7 Otimização de Data Deduplication
df_table_usuarios = DeltaTable.forPath(spark, "spark-warehouse/usuarios")


df_table_usuarios.alias("table_usuarios").merge(
  df_novos_usuarios.alias("novos_usuarios"),
    "table_usuarios.cpf = novos_usuarios.cpf "
    "AND table_usuarios.date > current_date() - INTERVAL 7 DAYS"
  ) \
  .whenNotMatchedInsertAll("novos_usuarios.date > current_date() - INTERVAL 7 DAYS") \
  .execute()
