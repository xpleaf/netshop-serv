package cn.xpleaf.netshop.serv.scala.analysis.module.olap.session

import cn.xpleaf.netshop.serv.java.analysis.utils.StringUtils

/**
  * @author xpleaf 
  */
object UserSessionAggStatsAnalysisAppTest {

    def main(args: Array[String]): Unit = {
        // test01()
        test02()
    }

    // 测试字符串split
    def test01(): Unit = {
        val str:String =
            """
              |hello|
              |xpleaf|
            """.stripMargin
        println(str)
        val arr = str.split("\\|\n")
        println(arr(0))
        println(arr(1))
    }

    // 测试通过自定义工具类根据分隔符获取字符串中的内容
    def test02(): Unit = {
        val info:String =
            """|name=xpleaf|
               |hobby=spark|
            """.stripMargin
        val name = StringUtils.getFieldFromConcatString(info, "\\|\n", "name")
        val hobby = StringUtils.getFieldFromConcatString(info, "\\|\n", "hobby")
        println(s"name=$name, hobby=$hobby")
    }

}
