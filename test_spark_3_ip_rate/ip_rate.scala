import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex
/*
从日志文件当中统计出来ip的占比

*/
object ip_rate{
  //将ip的点分十进制转化为数字的函数
  def ip2num(ip : String) : Long ={
    val fragements = ip.split("\\.")
    var ip_num = 0L
    for(i <- 0 until fragements.length){
      ip_num =  fragements(i).toLong | ip_num << 8L
    }
    ip_num
  }
  /*
  * binary search
  * 从ip库当中找出来某个特定的ip属于哪个位置，返回的是ip在库中的下标
  * */
  def binanySearch(lines : Array[(Long,Long,String)],ip:Long):Int = {
    var low = 0
    var high = lines.length -1
    while(low <= high){
      val middle = (low + high) / 2
      if((ip >= lines(middle)._1) && (ip <= lines(middle)._2)){
        return middle
      }
      if (ip < lines(middle)._1){
        high = middle-1
      } else {
        low = middle + 1
      }
    }
      -1
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ip rata").setMaster("local")
    val sc = new SparkContext(conf)
    val ipTablePath = "file:///home/sdb1/spark_data/ip.txt"
    val logFilePath = "file:///home/sdb1/spark_data/log.txt"
    /*第一步： 把ip库的数据和日志库的数据加载进来，并进行简单的处理*/
    val ipTableRDD = sc.textFile(ipTablePath).map(line =>
    {
      val fields = line.trim().split("\t")
      val s_ip = ip2num(fields(0).trim())
      val d_ip = ip2num(fields(1).trim())
      val address = fields(2).trim()
      (s_ip,d_ip,address)
    })

    val ipTableArray = ipTableRDD.collect()

    val ipTableBroadCast = sc.broadcast(ipTableArray)

    val ipRegx = new Regex("((\\d{1,3}\\.){3}\\d{1,3})")
    // get ip data
    val ipDataRdd = sc.textFile(logFilePath).map(line =>{
      ipRegx.findFirstIn(line.toString()).mkString("")
    })
    /*第二步，将ip库和日志库进行拼接，返回的是（（s_ip,d_ip,address），数量）*/
    val result = ipDataRdd.map(ip =>{
      var info :Any = None
      if(!ip.isEmpty){
        val ipNum = ip2num(ip)
        val index = binanySearch(ipTableBroadCast.value,ipNum)
        info = ipTableBroadCast.value(index)
      }
      (info,1L)

    }).reduceByKey(_+_)
    /*第三步：计算ip的总数*/
    val total = result.reduce((x, y) =>{
      val v = x._2 + y._2
      ("Total",v)
    })
    /*第四步，计算比例*/
    val rate = result.map( x => {
      val r = x._2.toFloat /total._2
      (x._1,r)
    })


    for(i <- rate.collect()){
      println(i)
    }

    sc.stop()


  }

}
