import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
/*最简单的WordCout程序，用scala语言来实现
了解了一个spark程序运行的基本过程 
1：打开一个SparkContext（使用conf），作用相当于一个driver
2：编写RDD的操作
3：关闭sc
这个程序里面都有
*/
object WorldCount{
  def main(args: Array[String]): Unit = {
    val path = "file:///home/hadoop/spark/in.txt"
    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile(path)

    lines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).collect().foreach(println)

    sc.stop()
  }
}
