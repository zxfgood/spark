package sparkInit

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zxf
  * @todo 对spark参数进行初始化
  */
object SparkConfig {
  var conf:SparkConf=null
  var spark:SparkSession=null
  var sparkContext:SparkContext=null
  def getsc(AppName:String)={
    System.setProperty("spark.executor.memory", "512m")
    var name=AppName
    if(name==null){
      name="FP-Growth"
    }
    //提交的jar包在你本机上的位置
    //设置driver端的ip,这里是你本机的ip
    conf = new SparkConf().setAppName(name).setMaster("spark://192.168.1.51:7077").setJars(List("C:\\Users\\dell\\Desktop\\fpgrowth.jar")).setIfMissing("spark.driver.host", "192.168.17.1")
    spark = SparkSession.builder().config(conf).getOrCreate()//有就拿过来，没有就创建，类似于单例模式：
    sparkContext =spark.sparkContext//有就拿过来，没有就创建，类似于单例模式：
  }
}
