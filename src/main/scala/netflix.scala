import conf.{SparkApplication, SparkStreamingApplication}

import scala.concurrent.duration.{Duration, SECONDS}




class Netflix extends SparkApplication with SparkStreamingApplication{
  override def sparkAppName: String = "mypayments"

  override def batchDuration: streamingDurations = streamingDurations(Duration(5,SECONDS),"hdfs://127.0.0.1:9000/user/spark/checkpoints")

   def startprocessing()={

     funcSparkContext{
       sc=> val data = sc.read
         .format("json")
         .option("inferSchema","true")
         .option("multiLine","true")
         .load("hdfs://127.0.0.1:9000/user/spark/jsondataset/Onepiece.json")
         data.show

     }
   }

}


object  netflix extends  App
{
  val data = new Netflix();
  data.startprocessing();
}