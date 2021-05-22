package conf

import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.concurrent.duration.FiniteDuration

trait SparkStreamingApplication extends SparkApplication {

  def batchDuration: streamingDurations

  def funcSparkStreamingContext(f:(StreamingContext=>Unit)): Unit =
  {
    funcStreamingContext{
      sc => val stc= new StreamingContext(sc,Seconds(batchDuration.batch_duration.toSeconds))
              stc.checkpoint(batchDuration.checkpoint)
            f(stc)
            stc.start()
            stc.awaitTermination()

    }
  }

  case class streamingDurations(batch_duration: FiniteDuration, checkpoint:String)

}
