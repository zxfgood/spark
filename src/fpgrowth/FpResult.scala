package fpgrowth

import DbHandle.Save
import sparkInit.SparkConfig

object FpResult {
  def main(args: Array[String]): Unit = {
    SparkConfig.getsc("FP-Growth")
   /* val fp=new Fp()
    //val transactions=fp.getTransactions("hdfs://192.168.1.51:8020/user/hive/warehouse/hive.db/poorstu/part-m-00000")
    val transactions=fp.getTransactions("hdfs://192.168.1.51:8020/user/hive/warehouse/hive.db/poorstu/part-m-00000")
    val modeldatah1=fp.getResult(transactions,0.9,"h1")
    val modelh1=modeldatah1._1
    val counth1=modeldatah1._2
    val h1rule=fp.getRules(modelh1,1.0,counth1)
    //val model=fp.getResult("hdfs://192.168.1.51:8020/user/hive/warehouse/hive.db/poorstu/part-m-00000",0.9,1.0)
    val modeldatah2=fp.getResult(transactions,0.9,"h2")
    val modelh2=modeldatah2._1
    val counth2=modeldatah2._2
    val h2rule=fp.getRules(modelh2,1.0,counth2)
    val modeldatah3=fp.getResult(transactions,0.9,"h3")
    val modelh3=modeldatah3._1
    val counth3=modeldatah3._2
    val h3rule=fp.getRules(modelh3,1.0,counth3)*/
    //fp.showRlues(model,1.0)
    //val save=new Save()
    /*val h1df=Save.dataToDf(h1rule)
    Save.saveIntoDb(h1df,"com.mysql.jdbc.Driver","jdbc:mysql://192.168.17.1:3306","cbd.tb_poorapriorirule","root","1234","1000","true")
    val h2df=Save.dataToDf(h2rule)
    Save.saveIntoDb(h2df,"com.mysql.jdbc.Driver","jdbc:mysql://192.168.17.1:3306","cbd.tb_poorapriorirule","root","1234","1000","true")
    val h3df=Save.dataToDf(h3rule)
    Save.saveIntoDb(h3df,"com.mysql.jdbc.Driver","jdbc:mysql://192.168.17.1:3306","cbd.tb_poorapriorirule","root","1234","1000","true")*/
    //val pf=new PoorForecast
    val rules=PoorForecast/*pf*/.getPoorRules("hdfs://192.168.1.51:8020/user/hive/warehouse/hive.db/tb_poorapriorirule/part-m-00000")
    val stufc=PoorForecast/*pf*/.getPoorStu("hdfs://192.168.1.51:8020/user/hive/warehouse/hive.db/tb_poorstuforecast/part-m-00000")
    val df=PoorForecast/*pf*/.getForecastResult(stufc,rules)
    Save.saveIntoDb(df,"com.mysql.jdbc.Driver","jdbc:mysql://192.168.17.1:3306","cbd.tb_poorstuforecast","root","1234","1000","true")
  }
}
