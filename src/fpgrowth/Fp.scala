package fpgrowth

import org.apache.spark.mllib.fpm
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import sparkInit.SparkConfig

import scala.collection.mutable.ArrayBuffer

/**
  * @author zxf
  * @todo (获得fp关联算法的频繁项结果集)
  */
class Fp {


  /**
    * @todo 将输入数据进行处理得到符合算法条件的数据
    * @param path 关联数据路径
    * @return
    */
  def getTransactions(path:String):RDD[Array[String]]={
    val poorStu = SparkConfig.sparkContext.textFile(path).map(_.split(",")).map(attributes =>
      {
          var uuid:String=""
          //独生子女
          var childForMartyrs:String=""
          //家庭年收入
          var familyAnnualIncome:String=""
          //家庭人口数
          var familyPopNum:String=""
          //健康状态
          var heathy:String=""
          //孤残
          var isDisabled:String=""
          //是否有危重病人
          var isHasPatient:String=""
          var isMakeupExam:String=""
          //是否单亲
          var isSingleParent:String=""
          //贫困等级
          var poorRank:String=""
          //户口性质
          var preSchoolResidence:String=""
          //助学贷款
          var studentLoan:String=""
        //按照不同等级进行不同分类
          if(attributes(1).trim.equals("是")){
            childForMartyrs="a1"
          }else{
            childForMartyrs="a2"
          }
          if(attributes(2).trim.toInt<20000){
            familyAnnualIncome="b1"
          }
          if(attributes(2).trim.toInt<80000&&attributes(2).trim.toInt>=20000){
            familyAnnualIncome="b2"
          }
          if(attributes(2).trim.toInt>=80000&&attributes(2).trim.toInt<=180000){
            familyAnnualIncome="b3"
          }
          if(attributes(3).trim.toInt<=3&&attributes(3).trim.toInt>=1){
            familyPopNum="c1"
          }
          if(attributes(3).trim.toInt<=6&&attributes(3).trim.toInt>=4){
            familyPopNum="c2"
          }
          if(attributes(3).trim.toInt>6){
            familyPopNum="c3"
          }
          if(attributes(4).trim.equals("良好")){
            heathy="d1"
          }else if(attributes(4).trim.equals("一般")){
            heathy="d2"
          }else{
            heathy="d3"
          }
          if(attributes(5).trim.equals("是")){
            isDisabled="e1"
          }else{
            isDisabled="e2"
          }
          if(attributes(6).trim.equals("有")){
            isHasPatient="f1"
          }else{
            isHasPatient="f2"
          }
          if(attributes(7).trim.equals("是")){
            isMakeupExam="k1"
          }else{
            isMakeupExam="k2"
          }
          if(attributes(8).trim.equals("是")){
            isSingleParent="g1"
          }else{
            isSingleParent="g2"
          }
          if(attributes(9).trim.equals("特困")){
            poorRank="h1"
          }else if(attributes(9).trim.equals("困难")){
            poorRank="h2"
          }else{
            poorRank="h3"
          }
          if(attributes(10).trim.equals("农村")){
            preSchoolResidence="i1"
          }else{
            preSchoolResidence="i2"
          }
          if(attributes(11).trim.toDouble==0){
          studentLoan="j1"
          }else if(attributes(11).trim.toDouble<=3000&&attributes(11).trim.toDouble>0){
            studentLoan="j2"
          }else if(attributes(11).trim.toDouble>3000&&attributes(11).trim.toDouble<=5000){
            studentLoan="j3"
          }else if(attributes(11).trim.toDouble>5000&&attributes(11).trim.toDouble<=10000){
            studentLoan="j4"
          }
          Array(attributes(0).trim,childForMartyrs,familyAnnualIncome,familyPopNum,heathy,isDisabled,
            isHasPatient,isMakeupExam,isSingleParent,poorRank,preSchoolResidence,studentLoan)
        }
                    )
    poorStu
  }

  /**
    * @todo(获得输入贫困等级的频繁项集)
    * @param transactions 训练数据集
    * @param minSupport 最小支持度
    * @param poor 贫困等级
    * @return 贫困等级模型和数据集条数
    */
  def getResult(transactions:RDD[Array[String]],minSupport:Double,poor:String): (fpm.FPGrowthModel[String],Long) ={
    //val data =SparkConfig.sparkContext.textFile(path)
    //将每一行数据分隔开，在把每一行数据分开，得到rdd数据
    //val transactions: RDD[Array[String]] = getTransactions(path).cache()
    //根据输入的贫困等级获得模型数据
    val transactionsmodel=transactions.filter(_.mkString.contains(poor)).cache()
    //获得数据源个数
    val count=transactionsmodel.count()
    //添加最小支持度规则，设置数据分区
    val fpg = new FPGrowth().setMinSupport(minSupport).setNumPartitions(3)
    //计算一个FP-Growth模型,包含频繁项集
    val model = fpg.run(transactionsmodel)
    //val model = fpg.run(h1)
    (model,count)
}

  /**
    * @todo 将得到贫困等级关联规则按照前项长度进行排序
    * @param ruleArray 关联规则数组
    * @return 排序好的关联规则数组
    */
  def arraySequence(ruleArray:ArrayBuffer[String]):ArrayBuffer[String]={
    val numbersize=ruleArray.size-1
    for(i<-1 to numbersize){
      var insernode=ruleArray(i)
      //关联规则前项的长度
      var insernodelength=ruleArray(i).split(":")(0).length()
      var m=i-1
      while(m>=0&&insernodelength>ruleArray(m).split(":")(0).length()){
        ruleArray(m+1)=ruleArray(m)
        m=m-1
      }
      ruleArray(m+1)=insernode
    }
    ruleArray
  }

  /**
    * @todo 获得符合前项最多条件的强关联规则
    * @param model 关联算法的FP-Growth模型
    * @param minConfidence 最小置信度
    * @param count 数据源数据个数
    * @return 符合条件的关联规则的数组
    */
  def getRules(model:fpm.FPGrowthModel[String],minConfidence:Double,count:Long):ArrayBuffer[String]={
    var Arrayrech1 = new ArrayBuffer[String]
    var Arrayrech = new ArrayBuffer[String]
    //antecedent表示前项，consequent表示后项,往贫困等级为h1的数组添加符合最小置信度的数据
    model.generateAssociationRules(minConfidence).collect().foreach(rule => {
      if (rule.consequent.mkString.contains("h")) {
        //前项后项每个之间用“，”隔开，和置信度之间用：隔开
        val rec = rule.antecedent.mkString(",") + "," + rule.consequent.mkString+":"+rule.confidence
        Arrayrech1 += rec
      }
    }
    )
    //对数组安装前项最多进行排序
    Arrayrech1=arraySequence(Arrayrech1)
    //将字符串按照“：”分隔，得到关联规则，得到前项最多的长度
    val ch1length=Arrayrech1(0).split(":")(0).length()
    Arrayrech1.foreach(x=>
      if(x.split(":")(0).length()==ch1length)
        Arrayrech+=x
    )
    //获得前项长度最多的支持度
    model.freqItemsets.collect().foreach (itemset =>
      {
        var flag=true
        val item=itemset.items.mkString(",").toString()
        if(item.contains("h")){
          //用频繁项集和关联规则的前后项进行配对，如果频繁项集包含关联规则，则关联规则的支持度为频繁项集的支持度
        for(x<-0 to Arrayrech.size-1){
          flag=true
          val rules=Arrayrech(x).split(":")(0).split(",")
          var rulelength=Arrayrech(x).split(":")(0).length
          rules.foreach(f=> {
            if (flag) {
              if (!item.contains(f))
                flag = false
            }

          }
          )
          if(flag&&rulelength==item.length){
            //计算支持度
            Arrayrech(x)=Arrayrech(x)+":"+(itemset.freq/count.toDouble).toString()
          }
        }
        }
      }
    )
      Arrayrech
      }

  /**
    * @todo 查看所有符合最小支持度和最小置信度的关联规则
    * @param model 关联算法的FP-Growth模型
    * @param minConfidence 最小置信度
    */
  def showRlues(model:fpm.FPGrowthModel[String],minConfidence:Double): Unit ={
    //获得频繁项集的项数
    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[",",","]") + ", " + itemset.freq)
          }
    //获得所有符合最小支持度的关联规则
    model.generateAssociationRules(minConfidence).collect().foreach(rule => {
      if (rule.antecedent.length>0&&rule.consequent.mkString.contains("h")){
        println(rule.antecedent.mkString(",") + "-->" +
          rule.consequent.mkString(",") + "-->" + rule.confidence)
      }
  })
  }
}
