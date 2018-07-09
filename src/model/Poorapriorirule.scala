package model

/**
  * @todo 学生关联规则模型类
  * @param uuid UUID
  * @param confidence 置信度
  * @param rule1 贫困属性
  * @param rule2 贫困等级
  * @param support 支持度
  */
case class Poorapriorirule(uuid:String,
                           confidence:String,
                           rule1:String,
                           rule2:String,
                           support:String
                          )