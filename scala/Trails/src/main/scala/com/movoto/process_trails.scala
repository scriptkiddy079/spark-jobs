package com.movoto

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator
import org.datasyslab.geosparksql.utils

object process_trails {
  val num_partitions = "48"

  def main(args: Array[String]): Unit = {
    //    classOf[org.postgresql.Driver]
    //    val conn_str = "jdbc:postgresql://production-16.svcolo.movoto.net:5432/geo"
    //    val conn = DriverManager.getConnection(conn_str)
    val spark: SparkSession = SparkSession.builder().config("spark.serializer", classOf[KryoSerializer].getName).config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).getOrCreate()
    spark.sparkContext.setCheckpointDir("hdfs:////tmp/")
    GeoSparkSQLRegistrator.registerAll(spark)

    val df_geo: DataFrame = spark.read.parquet("hdfs:////tmp/geo.parquet")
    //    val df_geo: DataFrame = df_par.repartition(num_partitions.toInt, col("id"), col("type"))
    df_geo.createOrReplaceTempView("geographic_boundary")
    spark.sql("select id,type,case when geom is null then null else ST_GeomFromWKT(geom) end as geom,disable  from geographic_boundary").createOrReplaceTempView("geographic_boundary")
    print("Geographic boundary loaded!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    val df_trails: DataFrame = spark.read.parquet("hdfs:////tmp/trails.parquet")
    //    df_trails.repartitionByRange(num_partitions.toInt, col("id"))
    df_trails.createOrReplaceTempView("trails")
    spark.sql("select id,ST_GeomFromWKT(geom) as geom from trails where geom is not null and ST_NumGeometries(ST_GeomFromWKT(geom))=1").createOrReplaceTempView("trails")
    print("Trails loaded!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    val url = "jdbc:postgresql://production-16.svcolo.movoto.net:5432/geo"
    val db_properties = new Properties()
    db_properties.put("user", "analytics")
    db_properties.put("password", "igen")
    db_properties.put("driver", "org.postgresql.Driver")
    db_properties.put("numPartitions", num_partitions)
    db_properties.put("batchsize", "5000")
    //    db_properties.load(new FileInputStream("./write_result"))
    val df: DataFrame = spark.sql("select b.id as geo_id, b.type as geo_type, count(distinct t.id) as trails,SUM(case when t.geom is not null then ST_Length(ST_GeomFromWKT(ST_AsText(ST_Intersection(b.geom, t.geom)))) else 0.0 end) * 0.000621371  as trail_length_mi FROM geographic_boundary b left JOIN trails t ON b.geom is not null  and ST_NumGeometries(b.geom)=1 and b.disable = False and ST_Intersects(b.geom, t.geom) group by b.id, b.type")
    //    df.write.mode(SaveMode.Overwrite).parquet("hdfs:////tmp/tmp_trails.parquet")
    val df_check = df.checkpoint(eager = false)
    df_check.write.mode(SaveMode.Overwrite).jdbc(url = url, table = "tmp.trails", db_properties)

  }
}
