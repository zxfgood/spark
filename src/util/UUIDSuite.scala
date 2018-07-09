package util

import java.util.UUID

/**
  * @author zxf
  *@todo 随机生成UUID
  */
object UUIDSuite {
  def createUUID():String={
      UUID.randomUUID().toString
  }
}
