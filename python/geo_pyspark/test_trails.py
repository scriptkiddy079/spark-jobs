import os
from geo_pyspark.utils import KryoSerializer, GeoSparkKryoRegistrator
from geo_pyspark.register import GeoSparkRegistrator, upload_jars
from pyspark import SparkConf
from pyspark.sql import SparkSession

os.environ["spark.pyspark.python"] = "/usr/bin/python3"
os.environ["spark.pyspark.driver.python"] = "/usr/bin/python3"
os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["SPARK_HOME"] = "/home/prabhat/.local/lib/python3.6/site-packages/pyspark"
os.environ["PYSPARK_SUBMIT_ARGS"] = "pyspark-shell"
os.environ["JAVA_HOME"] = "/usr"
os.environ["geospark.global.charset"] = "utf8"

pairs = [("spark.jars.packages", "org.postgresql:postgresql:42.2.15,org.datasyslab:geospark:1.2.0,org.datasyslab:geospark-sql_2.3:1.2.0"),("spark.serializer", KryoSerializer.getName) \
    , ("spark.kryo.registrator", GeoSparkKryoRegistrator.getName) \
    , ("spark.speculation", "true"), ("spark.speculation.interval", "1s"), ("spark.speculation.multiplier", "2")]

num_partitions = "8"


def process(spark):
    url = "jdbc:postgresql://production-16.svcolo.movoto.net:5432/geo"
    db_properties = {}
    db_properties["user"] = "analytics"
    db_properties["password"] = "igen"
    db_properties["driver"] = "org.postgresql.Driver"
    db_properties["queryTimeout"] = "99999999"
    # db_properties["isolationLevel"]="READ_COMMITTED"
    db_properties["fetchsize"] = "99999999"
    db_properties["numPartitions"] = num_partitions
    db_properties["partitionColumn"] = "id"
    db_properties["lowerBound"] = "0"
    db_properties["upperBound"] = "253762"

    query_geo = "(select id,type,ST_AsText(geom) as geom,disable from movoto.geographic_boundary limit 100)as t "
    spark.read.jdbc(url=url,
                    table=query_geo, properties=db_properties).createOrReplaceTempView("geographic_boundary")

    print("Geographic boundary loaded!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")

    spark.sql(
        "select id,type,case when geom is null then null else ST_GeomFromWKT(geom) end as geom,disable  from "
        "geographic_boundary").createOrReplaceTempView("geographic_boundary")

    query_trails = "(select id,ST_AsText(geom) as geom from poi.trails limit 100) as p"
    db_properties = {}
    db_properties["user"] = "analytics"
    db_properties["password"] = "igen"
    db_properties["driver"] = "org.postgresql.Driver"
    db_properties["queryTimeout"] = "99999999"
    # db_properties["isolationLevel"]="READ_COMMITTED"
    db_properties["fetchsize"] = "99999999"
    db_properties["numPartitions"] = num_partitions
    db_properties["partitionColumn"] = "id"
    db_properties["lowerBound"] = "0"
    db_properties["upperBound"] = "234003"

    spark.read.jdbc(url=url, table=query_trails, properties=db_properties).createOrReplaceTempView("trails")

    print("Trails loaded!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    spark.sql(
        "select id,case when geom is null then null else ST_GeomFromWKT(geom) end as geom from trails").createOrReplaceTempView(
        "trails")

    # Database = <database_name>
    # schema = <schema>
    url = "jdbc:postgresql://production-16.svcolo.movoto.net:5432/geo"
    db_properties = {}
    db_properties["user"] = "analytics"
    db_properties["password"] = "igen"
    db_properties["driver"] = "org.postgresql.Driver"
    db_properties["numPartitions"] = num_partitions
    # db_properties["partitionColumn"] = "geometric_boundary_id"
    db_properties["queryTimeout"] = "10000"
    db_properties["batchsize"] = "99999999"
    db_properties["isolationLevel"] = "READ_COMMITTED"

    df = spark.sql(
        "select b.id as geo_id, b.type as geo_type, count(distinct t.id) as trails,SUM(case when t.geom is not null then ST_Length(ST_GeomFromWKT(ST_AsText(ST_Intersection(b.geom, t.geom)))) else 0.0 end) * 0.000621371  as trail_length_mi FROM geographic_boundary b left JOIN trails t ON b.geom is not null and t.geom is not null and b.disable = False and ST_Intersects("
        "b.geom, t.geom) group by b.id, b.type")
    # df.show(20)
    df.write.jdbc(url=url, table="tmp.trails", properties=db_properties,
                  mode="overwrite")


if __name__ == '__main__':
    conf = SparkConf().setAll(pairs).setMaster("local[*]")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    flag = upload_jars()
    print(flag)
    loaded = GeoSparkRegistrator.registerAll(spark)
    print(
        "########################################################################################################################################################################",
        flush=True)
    print("GEOSPARK LOADED : ", loaded, flush=True)
    process(spark)
