from geo_pyspark.register import GeoSparkRegistrator, upload_jars
from geo_pyspark.utils import GeoSparkKryoRegistrator, KryoSerializer
from pyspark import SparkContext
from pyspark.sql import SparkSession
import os
import psql_read
import psql_write
from geospark.sql.types import GeometryType

# from geospark.register import upload_jars
# from geospark.register import GeoSparkRegistrator
# --------------------------------------------------------------------------------------------------------------------------------------------------------------
url = "jdbc:postgresql://production-16.svcolo.movoto.net:5432/geo"
db_properties = {}
db_properties["user"] = "analytics"
db_properties["password"] = "igen"
db_properties["driver"] = "org.postgresql.Driver"
db_properties["queryTimeout"] = "99999999"
# db_properties["isolationLevel"]="READ_COMMITTED"
db_properties["fetchsize"] = "99999999"
db_properties["numPartitions"] = "32"
db_properties["partitionColumn"] = "id"
db_properties["lowerBound"] = "0"
db_properties["upperBound"] = "250000"
# --------------------------------------------------------------------------------------------------------------------------------------------------------------

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
    .config('spark.executor.memory', '15g') \
    .master("local[8]").appName("create_tables").getOrCreate()

GeoSparkRegistrator.registerAll(spark)
query_geo = "(select id,type,name,st_astext(geom) as geom from movoto.geographic_boundary)as t"
spark.read.jdbc(url=url,
                table=query_geo, properties=db_properties).repartition(32, "geom").createOrReplaceTempView(
    "geographic_boundary")
print("Geographic boundary loaded!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
# df = df["geom"].cast(GeometryType.typeName())
df_geo = spark.sql(
    "select id,type,name,case when geom is null then null else ST_GeomFromWKT(geom) end as geom  from "
    "geographic_boundary").createOrReplaceTempView("geographic_boundary")

query_places = "(select name,state_place_fips as fips,st_astext(geom) as geom from geo.geo.places) as p"
db_properties = {}
db_properties["user"] = "analytics"
db_properties["password"] = "igen"
db_properties["driver"] = "org.postgresql.Driver"
db_properties["queryTimeout"] = "99999999"
# db_properties["isolationLevel"]="READ_COMMITTED"
db_properties["fetchsize"] = "99999999"
db_properties["numPartitions"] = "8"

spark.read.jdbc(url=url, table=query_places, properties=db_properties).repartition(32, "geom").createOrReplaceTempView(
    "places")
print("Places loaded!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

spark.sql(
    "select name,fips,case when geom is null then null else ST_GeomFromWKT(geom) end as geom from places").createOrReplaceTempView(
    "places")
df1 = spark.sql(
    "select gb.id as geo_id, gb.type as geo_type,gp.fips as fips from geographic_boundary gb inner join places gp on "
    "gb.geom is not null and gp.geom is not null and ST_Intersects(gb.geom,gp.geom)")

df1.show()
# python3  setup.py bdist_egg egg_info
