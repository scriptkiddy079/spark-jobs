from geo_pyspark.register import GeoSparkRegistrator, upload_jars
from geo_pyspark.utils import GeoSparkKryoRegistrator, KryoSerializer
from pyspark.sql import SparkSession
import os
import psql_read
import psql_write
from geospark.sql.types import GeometryType

# from geospark.register import upload_jars
# from geospark.register import GeoSparkRegistrator

os.environ["spark.pyspark.python"] = "/usr/bin/python3"
os.environ["spark.pyspark.driver.python"] = "/usr/bin/python3"
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["SPARK_HOME"] = "/home/prabhat/.local/lib/python3.6/site-packages/pyspark"
os.environ["PYSPARK_SUBMIT_ARGS"] = "pyspark-shell"
os.environ["JAVA_HOME"] = "/usr"
os.environ["geospark.global.charset"] = "utf8"
flag = upload_jars()

print(flag)
# ,org.datasyslab:geospark:1.3.2,org.datasyslab:geospark-sql_2.3:1.3.2
spark = SparkSession.builder.config("spark.jars.packages",
                                    "org.postgresql:postgresql:42.2.15").config(
    "spark.pyspark.python", "/usr/bin/python3").config("spark.pyspark.driver.python", "/usr/bin/python3").config(
    "spark.dynamicAllocation.schedulerBacklogTimeout", "0.5s") \
    .config("spark.serializer", KryoSerializer.getName) \
    .config("spark.kryo.registrator", GeoSparkKryoRegistrator.getName) \
    .config("spark.speculation", "true").config("spark.speculation.interval", "1ms").config(
    "spark.speculation.multiplier", "1.5") \
    .config('spark.executor.memory', '12g') \
    .master("local[8]").appName("create_tables").getOrCreate()

GeoSparkRegistrator.registerAll(spark)
query_geo = "(select id,type,name,st_astext(geom) as geom from movoto.geographic_boundary)as t"
spark.read.jdbc(url=psql_read.url,
                table=query_geo, properties=psql_read.db_properties, numPartitions=8).createOrReplaceTempView(
    "geographic_boundary")
print("Geographic boundary loaded!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
# df = df["geom"].cast(GeometryType.typeName())
df_geo = spark.sql(
    "select id,type,name,case when geom is null then null else ST_GeomFromWKT(geom) end as geom  from "
    "geographic_boundary").createOrReplaceTempView("geographic_boundary")

query_places = "(select name,state_place_fips as fips,st_astext(geom) as geom from geo.geo.places) as p"
spark.read.jdbc(url=psql_read.url, table=query_places, properties=psql_read.db_properties,
                numPartitions=8).createOrReplaceTempView("places")
print("Places loaded!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

spark.sql(
    "select name,fips,case when geom is null then null else ST_GeomFromWKT(geom) end as geom from places").createOrReplaceTempView(
    "places")

df2 = spark.sql(
    "select gb.id as geo_id, gb.type as geo_type,gp.fips as fips from geographic_boundary gb inner join places gp on "
    "gb.geom is not null and gp.geom is not null and ST_Contains(gb.geom,gp.geom)")

df2.show()
