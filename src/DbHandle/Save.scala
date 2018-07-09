package DbHandle

import model.{BookSequation, Poorapriorirule}
import org.apache.spark.sql.DataFrame
import sparkInit.SparkConfig
import util.UUIDSuite

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * @todo 将df数据存入数据库
  */
object Save {

  /**
    * @todo 将关联规则数组转化成df数据
    * @param data 关联规则数组
    * @return df数据
    */
  def dataToDf(data:ArrayBuffer[String]): DataFrame ={
    val sparkContext=SparkConfig.sparkContext
    val spark=SparkConfig.spark
    import spark.implicits._
    //声明一个可变list
      val listData=mutable.MutableList[Poorapriorirule]()
    //对关联数组进行分离得到各项数据
      for (x<-0 to data.length-1){
        //对规则和支持度置信度进行分离
        val ruledata = data(x).split(":")
        //获得置信度
        val confidence = ruledata(1)
        //获得支持度
        val support = ruledata(2)
        //获得关联规则
        val associate = ruledata(0)
        //将贫困等级和规则属性进行分离
        val item = associate.trim.split(",")
        //得到关联规则前项
        val consequent = item(item.length - 1)
        val c = item.mkString(",")
        //得到贫困等级
        val antecedent = c.substring(0, c.length() - 3)
        //生成uuid
        val uuid2=UUIDSuite.createUUID()
        val poor2=new Poorapriorirule(uuid2,confidence, antecedent, consequent, support)
        listData+= poor2
      }
    //将list转换成df数据
    val df=listData.toDS().toDF("uuid","confidence","rule1","rule2","support")
    df

  }
  /**
    * @todo 将关联规则数组转化成df数据
    * @param data 关联规则数组
    * @return df数据
    */
  def regressionDataToDf(data:ArrayBuffer[String]): DataFrame ={
    val spark=SparkConfig.spark
    import spark.implicits._
    //声明一个可变list
    val listData=mutable.MutableList[BookSequation]()
    //对关联数组进行分离得到各项数据
    for (x<-0 to data.length-1){
      //获得图书类型
      val bookType = data(x).split(":")(0).split(",")(0)
      //获得方程相关系数
      val seqVal=data(x).split(":")(1).split(",")
      var sequation="y="
      val length=seqVal.length
      for(x<- 0 to length-2){
        if(x==0){
          sequation+=seqVal(0)+"x1"
        }else{
          sequation+="+"+seqVal(x)+"x"+(x+1).toString
        }
      }
      sequation+="+"+seqVal(length-1)
      //获得最小值
      val newVal=data(x).split(":")(2)
      //生成uuid
      val uuid=UUIDSuite.createUUID()
      val bookSequation=new BookSequation(uuid,newVal,sequation, bookType,"")
      listData+= bookSequation
    }
    //将list转换成df数据
    val df=listData.toDS().toDF("uuid","newVal","sequation","type","name")
    df

  }
  /**
    * @todo 将df数据存入数据库
    * @param data df数据
    * @param driver 数据库驱动名
    * @param url 数据库地址
    * @param dbtable 插入的数据库表
    * @param user 数据库用户名
    * @param password 数据库用户名密码
    * @param batchsize 仅适用于write数据。JDBC批量大小，用于确定每次insert的行数
    * @param truncate 仅适用于write数据。当SaveMode.Overwrite启用时，此选项会truncate在MySQL中的表，而不是删除，再重建其现有的表
    */
  def saveIntoDb(data:DataFrame,driver:String,url:String,dbtable:String,user:String,password:String,batchsize:String,truncate:String){
    data.write.mode("append").format("jdbc").options(
      Map(
        "driver" -> driver,
        "url" -> url,
        "dbtable" -> dbtable,
        "user" -> user,
        "password" -> password,
        "batchsize" -> batchsize,
        "truncate" -> truncate)).save()
  }
}
