package regression

import DbHandle.Save
import sparkInit.SparkConfig

object RegressionMain {
  def main(args: Array[String]): Unit = {
    SparkConfig.getsc("regression")
    /*val regression=new Regression()
    val a=regression.getBookinfo("hdfs://192.168.1.51:8020/user/hive/warehouse/hive.db/tb_bookcirculationinfo/part-m-00000")
    val c=regression.getCorrelation(a)
    val model=regression.getModel(c)
    val df=Save.regressionDataToDf(model)
    Save.saveIntoDb(df,"com.mysql.jdbc.Driver","jdbc:mysql://192.168.17.1:3306","cbd.tb_booksequation","root","1234","1000","true")*/
    val bookForecast=new RegressionForecast()
    val book=bookForecast.getBookFoecast("hdfs://192.168.1.51:8020/user/hive/warehouse/hive.db/show_bookforecast/part-m-00000")
    val sequation=bookForecast.getSequation("hdfs://192.168.1.51:8020/user/hive/warehouse/hive.db/tb_booksequation/part-m-00000")
    val bookdf=bookForecast.bookForecast(book,sequation)
    Save.saveIntoDb(bookdf,"com.mysql.jdbc.Driver","jdbc:mysql://192.168.17.1:3306","cbd.show_bookforecast","root","1234","1000","true")
  }
}
