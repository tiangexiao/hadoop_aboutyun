import org.apache.spark.{SparkConf, SparkContext}
/*
ip地址数据预处理
使用scala来编写ip地址的转化过程，将文本中的点分十进制的ip地址转化为数字类型
A.B.C.D  -> (((A*256)+B)*256+C)*256+D
是项目中的数据预处理阶段
*/
object ip_trans{

  //ip string to ip num
  def ip2num(ip : String) : Long ={
    val fragements = ip.split("\\.")
    var ip_num = 0L
    for(i <- 0 until fragements.length){
      ip_num =  fragements(i).toLong | ip_num << 8L
    }
    ip_num
  }

  def main(args: Array[String]): Unit = {


    /* test ip2num successful ip_context is 1.0.0.255 ip number is 16777471
    val ip_context = "1.0.0.255"
    val ip_num = ip2num(ip_context)
    println("ip_context is "+ip_context+" ip number is "+ip_num)
    */
    val conf = new SparkConf().setAppName("ipRule").setMaster("local")
    val sc = new SparkContext(conf)
    val ip_path = "file:///home/sdb1/spark_data/ip.txt"

    val ipRulRdd = sc.textFile(ip_path).map( line =>
      {
        val fields = line.trim().split("\t")
        val s_ip = ip2num(fields(0).trim())
        val d_ip = ip2num(fields(1).trim())
        val address = fields(2).trim()
        (s_ip,d_ip,address)
      }
    )

    ipRulRdd.collect().foreach(println)
    sc.stop()



  }
}
