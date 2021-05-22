package conf

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait SparkApplication{

  def sparkAppName:String

  def funcSparkContext(f:(SparkSession=>Unit)): Unit =
  {
    val sc = SparkSession.builder()
      .appName(sparkAppName)
      .master("local[1]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
      .getOrCreate()
       f(sc)



  }

  def funcStreamingContext(f:(SparkContext=>Unit)): Unit =
  {
    val sc = SparkSession.builder()
      .appName(sparkAppName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
      .getOrCreate()
        f(sc.sparkContext)



  }




}
