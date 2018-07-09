package regression

import model.BookForecast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import sparkInit.SparkConfig

import scala.collection.mutable.ArrayBuffer

class RegressionForecast {
  /**
    * @todo 获得图书预测数据
    * @param path 图书预测数据存放路径
    * @return
    */
  def getBookFoecast(path:String): RDD[Array[String]] ={
    val bookInfo = SparkConfig.sparkContext.textFile(path).map(_.split(",")).map(x=>{
      val uuid=x(0).trim
      val duplicateNum=x(1).trim
      val forecastNum=x(2)
      val bookType=x(3).trim
      val typeName=x(4).trim
      val adviseBuyNum=""
      val beyondForecastNum=""
      Array(uuid,duplicateNum,forecastNum,bookType,typeName,adviseBuyNum,beyondForecastNum)
    }).cache()
    bookInfo
  }

  /**
    * @todo 获得线性回归方程
    * @param path 线性方程相关系数存放的位置
    * @return
    */
  def getSequation(path:String): RDD[Array[String]]={
    val sequationInfo = SparkConfig.sparkContext.textFile(path).map(_.split(",")).map(x=>{
      //val uuid=x(0).trim
      val newVal=x(1).trim
      val sequation=x(2).trim
      val bookType=x(3).trim
      Array(newVal,sequation,bookType)
    }).cache()
    sequationInfo
  }
  def bookForecast(bookInfo:RDD[Array[String]],sequation:RDD[Array[String]]): DataFrame ={
    val spark=SparkConfig.spark
    //原类型和伴生对象都找不到的隐式值，会找手动导入的implicit,为了支持RDD到DataFrame的隐式转换
    import spark.implicits._
    val yValue=new ArrayBuffer[String]
    sequation.collect().foreach(x=>{
      var y=x(2)+":"
      val min=x(0).split("&")
      val seq=x(1).split("=")(1).split("[+]")
      var value:Double=0
      for(l<- 0 to min.length-1){
        value=value+min(l).toDouble * seq(l).substring(0,seq(l).length-2).toDouble
      }
      value=value+seq.last.toDouble
      yValue+=y+value.round.toString
    })
    val book=bookInfo.map(x=>{
      val uuid=x(0).trim
      val duplicateNum=x(1).trim
      val bookType=x(3).trim
      val typeName=x(4).trim
      val adviseBuyNum=x(5)
      val beyondForecastNum=x(6)
      var forecastNum=x(2)
      yValue.foreach(x=>{
        if(x.contains(bookType)){
        forecastNum=x.split(":")(1)
        }
      })
      new BookForecast(uuid,duplicateNum,forecastNum,bookType,typeName,"","")
    }).toDF("uuid","duplicateNum","forecastNum","type","typeName","adviseBuyNum","beyondForecastNum")
    book
  }
}
