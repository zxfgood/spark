package fpgrowth

import model.{PoorStu, Poorapriorirule}
import org.apache.spark.sql.DataFrame
import sparkInit.SparkConfig

/**
  * @author zxf
  * @todo 对数据进行预测
  */
object PoorForecast extends Serializable {
  val spark=SparkConfig.spark
  //原类型和伴生对象都找不到的隐式值，会找手动导入的implicit,为了支持RDD到DataFrame的隐式转换
  import spark.implicits._

  /**
    * @todo 获得贫困生的预测数据
    * @param path 贫困生预测数据路径
    * @return 贫困生的df数据
    */
  def getPoorStu(path:String):DataFrame={
    //rdd和df区别介绍：https://www.csdn.net/article/2015-04-03/2824407
    //RDD不支持sparksql操作,所以要转成df,DataFrame是一种以RDD为基础的分布式数据集，类似于传统数据库中的二维表格
    val poorStuInfoDF = SparkConfig.sparkContext.textFile(path).map(_.split(","))
      .map(attributes =>
        PoorStu(attributes(0).trim,
          attributes(1).trim,
          attributes(2).trim,
          attributes(3).trim,
          attributes(4).trim,
          attributes(5).trim,
          attributes(6).trim,
          attributes(7).trim,
          attributes(8).trim,
          attributes(9).trim,
          attributes(10).trim,
          attributes(11).trim,
          attributes(12).trim,
          attributes(13).trim,
          attributes(14).trim,
          attributes(15).trim,
          attributes(16).trim)).cache().toDF()
          poorStuInfoDF
  }

  /**
    * @todo 获得关联规则数据
    * @param path 关联规则数据路径
    * @return 关联规则的df数据
    */
  def getPoorRules(path:String):DataFrame={
    //将关联规则，支持度，置信度，UUID分隔开
    val poorRulesInfoDF = SparkConfig.sparkContext.textFile(path).map(_.split(":"))
      .map(attributes => Poorapriorirule(attributes(0).trim,
          attributes(1).trim,
          attributes(2).trim,
          attributes(3).trim,
          attributes(4).trim)).cache().toDF()
          poorRulesInfoDF
  }

  /**
    * @todo 获得预测结果
    * @param poorStuDf 预测贫困学生数据
    * @param poorRulesDf 关联规则数据
    * @return
    */
  def getForecastResult(poorStuDf:DataFrame,poorRulesDf:DataFrame): DataFrame ={
    poorStuDf.registerTempTable("poorstu")
    poorRulesDf.registerTempTable("poorrule")
    var uuid:String=""
    var childForMartyrs:String=""
    var collegeName:String=""
    var familyAnnualIncome:String=""
    var familyPopNum=""
    var heathy=""
    var isDisabled=""
    var isHasPatient=""
    var isMakeupExam=""
    var isSingleParent=""
    var major=""
    var poorRank=""
    var preSchoolResidence=""
    var stuName=""
    var studentID=""
    var studentLoan=""
    var schoolYear=""
    //获得贫困规则的属性和对应的等级
    val poorPoorRulesdata = spark.sql("select rule1,rule2 from poorrule").rdd.map(r =>
      (Array(r.getAs[String]("rule1"), r.getAs[String]("rule2")))
    )
    //将rdd数据转化成array以便后面处理
    val listData=poorPoorRulesdata.collect()
    val pooStudata=spark.sql("select * from poorstu").rdd.map(s => {
      //属性集
      var antecedent=""
      uuid = s.getAs[String]("uuid")
      childForMartyrs = s.getAs[String]("childForMartyrs")
      collegeName = s.getAs[String]("collegeName")
      familyAnnualIncome = s.getAs[String]("familyAnnualIncome")
      familyPopNum = s.getAs[String]("familyPopNum")
      heathy = s.getAs[String]("heathy")
      isDisabled = s.getAs[String]("isDisabled")
      isHasPatient = s.getAs[String]("isHasPatient")
      isMakeupExam = s.getAs[String]("isMakeupExam")
      isSingleParent = s.getAs[String]("isSingleParent")
      major = s.getAs[String]("major")
      preSchoolResidence = s.getAs[String]("preSchoolResidence")
      stuName = s.getAs[String]("stuName")
      studentID = s.getAs[String]("studentID")
      studentLoan = s.getAs[String]("studentLoan")
      schoolYear = s.getAs[String]("schoolYear")
      if (childForMartyrs.trim.equals("是")) {
        antecedent += "a1,"
      } else {
        antecedent += "a2,"
      }
      if (familyAnnualIncome.trim.toInt < 20000) {
        antecedent += "b1,"
      }
      if (familyAnnualIncome.trim.toInt < 80000 && familyAnnualIncome.trim.toInt >= 20000) {
        antecedent += "b2,"
      }
      if (familyAnnualIncome.trim.toInt >= 80000 && familyAnnualIncome.trim.toInt <= 180000) {
        antecedent += "b3,"
      }
      if (familyPopNum.trim.toInt <= 3 && familyPopNum.trim.toInt >= 1) {
        antecedent += "c1,"
      }
      if (familyPopNum.trim.toInt <= 6 && familyPopNum.trim.toInt >= 4) {
        antecedent += "c2,"
      }
      if (familyPopNum.trim.toInt > 6) {
        antecedent += "c3,"
      }
      if (heathy.trim.equals("良好")) {
        antecedent += "d1,"
      } else if (heathy.trim.equals("一般")) {
        antecedent += "d2,"
      } else {
        antecedent += "d3,"
      }
      if (isDisabled.trim.equals("是")) {
        antecedent += "e1,"
      } else {
        antecedent += "e2,"
      }
      if (isHasPatient.trim.equals("是")) {
        antecedent += "f1,"
      } else {
        antecedent += "f2,"
      }
      if (isMakeupExam.trim.equals("是")) {
        antecedent += "k1,"
      } else {
        antecedent += "k2,"
      }
      if (isSingleParent.trim.equals("有")) {
        antecedent += "g1,"
      } else {
        antecedent += "g2,"
      }
      if (preSchoolResidence.trim.equals("农村")) {
        antecedent += "i1,"
      } else {
        antecedent += "i2,"
      }
      if (studentLoan.trim.toDouble == 0) {
        antecedent += "j1"
      } else if (studentLoan.trim.toDouble <= 3000 && studentLoan.trim.toDouble > 0) {
        antecedent += "j2"
      } else if (studentLoan.trim.toDouble > 3000 && studentLoan.trim.toDouble <= 5000) {
        antecedent += "j3"
      } else if (studentLoan.trim.toDouble > 5000 && studentLoan.trim.toDouble <= 10000) {
        antecedent += "j4"
      }
      //贫困等级
      var rule2 = "123"
      for(x<- 0 to listData.length-1){

        if((rule2=="")|| (rule2 == "123"))
        {
          rule2=getPoor(antecedent,listData(x))
        }
      }
      var poorRank2 =""
      if(rule2.equals("h1")){
        poorRank2="特困"
      }
      if(rule2.equals("h2")){
        poorRank2="困难"
      }
      if(rule2.equals("h3")){
        poorRank2="一般"
      }
      if(rule2.equals("")){
        poorRank2="非贫困"
      }
      PoorStu(uuid, childForMartyrs, collegeName, familyAnnualIncome, familyPopNum, heathy,
        isDisabled, isHasPatient, isMakeupExam, isSingleParent, major, poorRank2, preSchoolResidence,
        stuName, studentID, studentLoan, schoolYear)
    }).toDF("uuid","childForMartyrs","collegeName","familyAnnualIncome","familyPopNum","heathy","isDisabled","isHasPatient","isMakeupExam","isSingleParent","major","poorRank","preSchoolResidence","stuName","studentID","studentLoan","schoolYear")
    pooStudata
  }

  /**
    *@todo 获得贫困等级
    * @param antecedent 预测是属性集
    * @param arrayRlue 关联规则
    * @return
    */
  def getPoor(antecedent:String,arrayRlue: Array[String]):String={
    val rules=arrayRlue(0)
    val rulesArray=rules.split(",")
    var poor=""
    var flag=true
    //判断属性集是否包含关联规则的全部前项，如果包含则贫困等级为当前规则的后项
    for(x<- 0 to rulesArray.length-1){
      if(flag){
      if(!antecedent.contains(rulesArray(x))){
        flag=false
      }
    }
    }
    if(flag){
      poor=arrayRlue(1)
    }
    poor
  }
}


