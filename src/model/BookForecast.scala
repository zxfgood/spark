package model

/**
  * @todo 图书预测类
  * @param uuid
  * @param duplicateNum 图书复本率
  * @param forecastNum 预测花费书量
  * @param bookType 图书类型
  * @param typeName 类型名
  * @param adviseBuyNum 建议购买数量
  * @param beyondForecastNum
  */
case class BookForecast(
                         uuid:String,
                         duplicateNum:String,
                         forecastNum:String,
                         bookType:String,
                         typeName:String,
                         adviseBuyNum:String,
                         beyondForecastNum:String
                       )
