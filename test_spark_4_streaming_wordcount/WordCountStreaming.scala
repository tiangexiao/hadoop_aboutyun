import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

import org.apache.spark.streaming.{Seconds, StreamingContext}
/*使用sparkstreaming 进行流处理  需要在命令行里面输入 nc -lk 9999 其中9999位下面的端口号，然后输入字符，会进行wordcount*/
object WordCountStreaming{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count steaming").setMaster("local[*]")

    val ssc = new StreamingContext(conf,Seconds(10))

    val hostname = "192.168.1.100"
    val port = 9999

    val lines = ssc.socketTextStream(hostname, port)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
