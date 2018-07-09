package test

import scala.collection.mutable.ArrayBuffer

object Test2 {
  def main(args: Array[String]): Unit = {
    val a=Array("1","2","3")
    val b=new ArrayBuffer[String]
    a.foreach(x=>b+=x)
    b.foreach(x=>println(x))
  }
}
