package model

/**
  * @todo 贫困学生预测模型类
  * @param uuid uuid
  * @param childForMartyrs 烈士子女
  * @param collegeName 学院名称
  * @param familyAnnualIncome 家庭年收入
  * @param familyPopNum 家庭人口数
  * @param heathy 健康状态
  * @param isDisabled 是否残疾
  * @param isHasPatient 是否有危重病人
  * @param isMakeupExam 是否补考
  * @param isSingleParent 是否单亲
  * @param major 专业
  * @param poorRank 贫困等级
  * @param preSchoolResidence 户口性质
  * @param stuName 学生姓名
  * @param studentID 学生学号
  * @param studentLoan 学生贷款
  * @param schoolYear 入学年份
  */
case class PoorStu(
                    uuid:String,
                    childForMartyrs:String,
                    collegeName:String,
                    familyAnnualIncome:String,
                    familyPopNum:String,
                    heathy:String,
                    isDisabled:String,
                    isHasPatient:String,
                    isMakeupExam:String,
                    isSingleParent:String,
                    major:String,
                    poorRank:String,
                    preSchoolResidence:String,
                    stuName:String,
                    studentID:String,
                    studentLoan:String,
                    schoolYear:String)