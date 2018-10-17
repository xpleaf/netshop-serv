package cn.xpleaf.netshop.serv.scala.analysis.module.olap.session

/**
  * @author xpleaf 
  */
object UserSessionAggStatsAnalysisAppTest {

    def main(args: Array[String]): Unit = {
        test01()
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

}
