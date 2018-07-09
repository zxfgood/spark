package regression

import model.Subbookinfo
import org.apache.spark.rdd.RDD

class Pearson {
  def  getCorrelation(bookinfo:RDD[Subbookinfo]): Unit ={
    val duplicateNumrdd=bookinfo.map(book=>{book.duplicateNum})
    bookinfo.collect()
  }
}
