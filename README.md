# SPARK_BACKUP
### SPARK PARQUET SCD 2

```python
from pyspark.sql import SparkSession, functions as F

# --------------------------
# Spark session setup
# --------------------------
spark = SparkSession.builder \
    .appName("SCD2_Customer_External_Hive") \
    .config("spark.sql.warehouse.dir", "/warehouse/external/") \
    .enableHiveSupport() \
    .getOrCreate()

# --------------------------
# Oracle source connection
# --------------------------
oracle_url = "jdbc:oracle:thin:@//oracle_host:1521/orclpdb"
oracle_props = {
    "user": "oracle_user",
    "password": "oracle_pass",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

# --------------------------
# Hive destination connection
# --------------------------
# (This URL is for HiveServer2 JDBC; Spark connects through it when reading/writing)
hive_url = "jdbc:hive2://hive_host:10000/default"
hive_props = {
    "user": "hive_user",
    "password": "hive_pass",
    "driver": "org.apache.hive.jdbc.HiveDriver"
}

# Hive external table details
hive_table = "dim_customer"
hive_path = "/warehouse/external/dim_customer/"

# --------------------------
# Step 1: Read source (Oracle)
# --------------------------
src_df = spark.read.jdbc(url=oracle_url, table="CUSTOMER", properties=oracle_props)

# --------------------------
# Step 2: Read target (Hive external)
# --------------------------
# If table doesn’t exist, create it through Hive JDBC once:
spark.read.jdbc(url=hive_url, table=hive_table, properties=hive_props)

spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {hive_table} (
    id INT,
    name STRING,
    effective_date DATE,
    expiration_date DATE,
    current_flag BOOLEAN
)
STORED AS PARQUET
LOCATION '{hive_path}'
""")

tgt_df = spark.read.jdbc(url=hive_url, table=hive_table, properties=hive_props)

# --------------------------
# Step 3: Detect changes
# --------------------------
joined = src_df.alias("src").join(
    tgt_df.filter("current_flag = true").alias("tgt"),
    on="id",
    how="fullouter"
)

change_df = joined.withColumn(
    "action",
    F.when(F.col("tgt.id").isNull(), "INSERT")
     .when(F.col("src.id").isNull(), "EXPIRE")
     .when(F.col("src.name") != F.col("tgt.name"), "UPDATE")
     .otherwise("NOCHANGE")
)

# --------------------------
# Step 4: Apply SCD logic
# --------------------------
today = F.current_date()

expired_df = change_df.filter("action IN ('UPDATE','EXPIRE')") \
    .select("tgt.*") \
    .withColumn("expiration_date", today) \
    .withColumn("current_flag", F.lit(False))

new_df = change_df.filter("action IN ('INSERT','UPDATE')") \
    .select("src.*") \
    .withColumn("effective_date", today) \
    .withColumn("expiration_date", F.lit("9999-12-31")) \
    .withColumn("current_flag", F.lit(True))

unchanged_df = change_df.filter("action = 'NOCHANGE'").select("tgt.*")

final_df = unchanged_df.unionByName(expired_df).unionByName(new_df)

# --------------------------
# Step 5: Write back to Hive via Hive JDBC
# --------------------------
final_df.write \
    .mode("overwrite") \
    .option("path", hive_path) \
    .format("parquet") \
    .saveAsTable(hive_table)


```

### SPARK SQL SCD2 ORC

```python
from pyspark.sql import SparkSession

# --------------------------
# 1️⃣ Spark session with external Hive metastore
# --------------------------
spark = SparkSession.builder \
    .appName("SCD2_Hive_ORC_SQL") \
    .config("spark.sql.warehouse.dir", "/warehouse/external/") \
    .config("hive.metastore.uris", "thrift://hive-metastore-host:9083") \  # External Hive metastore URL
    .enableHiveSupport() \
    .getOrCreate()

# --------------------------
# 2️⃣ Oracle source connection
# --------------------------
oracle_url = "jdbc:oracle:thin:@//oracle_host:1521/orclpdb"
oracle_props = {
    "user": "oracle_user",
    "password": "oracle_pass",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

# Load Oracle data into a staging table
src_df = spark.read.jdbc(url=oracle_url, table="CUSTOMER", properties=oracle_props)
src_df.createOrReplaceTempView("staging_customer")

# --------------------------
# 3️⃣ Create external Hive ORC table (if not exists)
# --------------------------
hive_table = "dim_customer_orc"
hive_path = "/warehouse/external/dim_customer_orc/"

spark.sql(f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {hive_table} (
    id INT,
    name STRING,
    effective_date DATE,
    expiration_date DATE,
    current_flag BOOLEAN
)
STORED AS ORC
LOCATION '{hive_path}'
TBLPROPERTIES ('transactional'='true')
""")

# --------------------------
# 4️⃣ Expire old records (UPDATE)
# --------------------------
spark.sql(f"""
UPDATE {hive_table}
SET expiration_date = CURRENT_DATE,
    current_flag = FALSE
WHERE id IN (
    SELECT tgt.id
    FROM {hive_table} tgt
    JOIN staging_customer src
      ON tgt.id = src.id
    WHERE tgt.current_flag = TRUE
      AND tgt.name <> src.name
)
""")

# --------------------------
# 5️⃣ Insert new / updated records (INSERT)
# --------------------------
spark.sql(f"""
INSERT INTO {hive_table}
SELECT src.id,
       src.name,
       CURRENT_DATE AS effective_date,
       NULL AS expiration_date,
       TRUE AS current_flag
FROM staging_customer src
LEFT JOIN {hive_table} tgt
  ON src.id = tgt.id AND tgt.current_flag = TRUE
WHERE tgt.id IS NULL OR src.name <> tgt.name
""")

# --------------------------
# 6️⃣ Done
# --------------------------
print("SCD Type 2 load to External Hive ORC using Spark SQL completed ✅")

```
