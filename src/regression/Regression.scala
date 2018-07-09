package regression

import model.bookinfo
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import sparkInit.SparkConfig

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class Regression extends Serializable {

  /**
    * @todo 获得图书信息
    * @param path 图书信息存放的路径
    * @return
    */
  def getBookinfo(path:String): RDD[bookinfo] ={
    val bookInfo = SparkConfig.sparkContext.textFile(path).map(_.split(",")).map(attributes=>bookinfo(attributes(0).trim,
      attributes(1).trim,
      attributes(2).trim,
      attributes(3).trim,
      attributes(4).trim,
      attributes(5).trim,
      attributes(6).trim,
      attributes(7).trim,
      attributes(8).trim,
      attributes(9).trim
    )).cache()
    bookInfo
  }
  /**
    * @todo 获得符合相关系数的属性值
    * @param bookinfo 图书信息
    * @return 符合相关系数的属性值 格式(图书类型，“列名：属性值”，....... )
    */
  def  getCorrelation(bookinfo:RDD[bookinfo]):RDD[ArrayBuffer[String]]={
    val booktype=new ArrayBuffer[String]

    var correlationData:RDD[ArrayBuffer[String]]=null
    val Correlation=new ArrayBuffer[ArrayBuffer[Double]]
    //图书类型
    for(x<- 'A' to 'Z')
      booktype+=x.toString
    booktype.foreach(t=>{
      //对图书类型进行匹配，统计数据中含有图书类型数量，数量大于0，表示含有数据
      val count=bookinfo.filter(_.bookType==t).count()
      if(count>0){
        //获得含有此图书类型是数据
        val booninfoType=bookinfo.filter(_.bookType==t)
        //符合相关系数的属性名放入数组
        val property=new ArrayBuffer[String]
      //新书复本率数据
      val duplicateNum=booninfoType.map(x=>x.duplicateNum.toDouble)
      //文献利用率数据
      val bookUseRate=booninfoType.map(x=>x.bookUseRate.toDouble)
      //图书借阅量数据
      val borrowTimes=booninfoType.map(x=>x.borrowTimes.toDouble)
      //新书总种数数据
      val newBookTotalType=booninfoType.map(x=>x.newBookTotalType.toDouble)
      //新书总册数数据
      val newBookTotalNum=booninfoType.map(x=>x.newBookTotalNum.toDouble)
      //总藏书册数数据
      val totalNum=booninfoType.map(x=>x.totalNum.toDouble)
      val g1=Statistics.corr(borrowTimes,duplicateNum)
      //g+=g1
      //相关系数绝对值大于0.5，将大于0.5的属性名放入数组
      if(g1< -0.5||g1>0.5){
        property+="borrowTimes"
      }
      val g2=Statistics.corr(newBookTotalNum,duplicateNum)
      if(g2< -0.5||g2>0.5){
        property+="newBookTotalNum"
      }
      val g3=Statistics.corr(newBookTotalType,duplicateNum)
      if(g3< -0.5||g3>0.5){
        property+="newBookTotalType"
      }
      val g4=Statistics.corr(totalNum,duplicateNum)
      if(g4< -0.5||g4>0.5){
        property+="totalNum"
      }
      val g5=Statistics.corr(bookUseRate,duplicateNum)
      if(g5< -0.5||g5>0.5){
        property+="bookUseRate"
      }
        //获得符合图书类型符合相关系数属性的数据
        val correlationlist=booninfoType.map(b=>{
          val propertyData=new ArrayBuffer[String]
            propertyData+=t
            propertyData += ("duplicateNum:" + b.duplicateNum)
            property.foreach(p => {
              if (p.equals("borrowTimes"))
                propertyData += ("borrowTimes:" + b.borrowTimes)
              if (p.equals("newBookTotalNum"))
                propertyData += ("newBookTotalNum:" + b.newBookTotalNum)
              if (p.equals("newBookTotalType"))
                propertyData += ("newBookTotalType:" + b.newBookTotalType)
              if (p.equals("totalNum"))
                propertyData += ("totalNum:" + b.totalNum)
              if (p.equals("bookUseRate"))
                propertyData += ("bookUseRate:" + b.bookUseRate)
            })
            propertyData
        })
        if (correlationData==null){
          correlationData=correlationlist
        }else{
          //将符合条件的rdd数据加入集合中
          correlationData=correlationData.++(correlationlist)
        }
        }
    })
    correlationData
  }

  /**
    * @todo 获得线性回归的模型
    * @param data 训练数据
    * @return 线性回归模型和相关属性的最小值（（“图书类型，相关属性名，....：相关系数，....：最小值&....”）
    */
  def getModel(data:RDD[ArrayBuffer[String]]): ArrayBuffer[String]={
   val spark=SparkConfig.spark
    //原类型和伴生对象都找不到的隐式值，会找手动导入的implicit,为了支持RDD到DataFrame的隐式转换
    //123
    import spark.implicits._
    //图书类型set
    var typeSet=new mutable.HashSet[String]
    //存放相关系数和特征向量的数组
    var modelArray=new ArrayBuffer[String]
    //相关系数数组
    val coefficientArray=new ArrayBuffer[Array[Double]]
    //获得不同的图书类型
    data.collect.foreach(x=>typeSet+=x(0))
    for(x<- typeSet) {
      var modelProerty = ""
      var i = 0
      var modelP=""
      while (i < 6) {
        var typeData:RDD[ArrayBuffer[String]]=data.filter(_ (0).equals(x))
        modelP=""
        if(!modelProerty.equals("")){
        //获得含有图书类型的数据
          typeData = data.filter(_ (0).equals(x)).map(t=>{
            val pData=new ArrayBuffer[String]
            pData+=t(0)
            pData+=t(1)
            for(y<- 2 to t.size-1){
              val p=t(y).split(":")(0)
              if(modelProerty.contains(p)||modelProerty.equals(p)){
                pData+=t(y)
              }
            }
            pData
          })
        }
        //保留符合相关系数的属性数组
        val propertyArray = new ArrayBuffer[String]
        //获得符合相关系数的属性数组
        val d = typeData.collect()(0)
        for (y <- 2 to d.length - 1) {
          propertyArray += d(y).split(":")(0)
        }
        val colArray = propertyArray.toArray
        val assembler = new VectorAssembler().setInputCols(colArray).setOutputCol("features")
        //df的列名
        val schemaString = "duplicateNum," + propertyArray.mkString(",")
        val fields = schemaString.split(",").map(fieldName => StructField(fieldName, DoubleType, nullable = true))
        val schema = StructType(fields)
        val testDataRdd = typeData.map(d => {
          var propertyData = new ArrayBuffer[Double]
          for (y <- 1 to d.size - 1) {
            propertyData += d(y).split(":")(1).toDouble
          }
          Row.fromSeq(propertyData)
        })
        val testDataDf = spark.createDataFrame(testDataRdd, schema)
        val lrRegression = new LinearRegression().setSolver("normal").setFeaturesCol("features").setLabelCol("duplicateNum").setFitIntercept(true)
        val vecDF: DataFrame = assembler.transform(testDataDf)
        // 将训练集合代入模型进行训练
        val lrModel = lrRegression.fit(vecDF)
        //相关系数值
        val coefficient = lrModel.coefficients.toArray
        val coefficientBuff = new ArrayBuffer[Double]
        coefficient.foreach(x => coefficientBuff += x.toDouble)
        //r2检验
        val trainingSummary = lrModel.summary
        //r2检验值
        trainingSummary.r2
        //t检验的t值
        val t = trainingSummary.tValues
        var length = coefficient.size - 1
        while (length >= 0) {
          //t值的绝对值大于2.015进行保留
          if (!(-2.015 > t(length) || t(length) > 2.015)) {
            //去除t值不符合的相关系数
            coefficientBuff.remove(length)
            //去除t值不符合的属性
            propertyArray.remove(length)
          }
          length = length - 1
        }
        modelProerty=""
        modelProerty = propertyArray.mkString(",")
        //存放属性最小值的数组
        val minVal= new ArrayBuffer[Double]
        val td=typeData.collect()
        val tsize=td(0).size-1
        val dLength=td.length-1
        //获取符合条件的最小值
        for(c<- 2 to tsize){
          val vBuffer = new ArrayBuffer[Double]
          td.foreach(x=>{
            val v=x(c).split(":")(1).toDouble
            vBuffer+=v
          })
          val min=vBuffer.toList.min
          minVal+=min
        }
        val modelString = x + "," + propertyArray.mkString(",") + ":" + coefficientBuff.mkString(",") + "," + lrModel.intercept.toString+":"+minVal.mkString("&")
        modelP= modelString
        i=i+1
      }
      modelArray+=modelP
    }
    modelArray
      }
}
