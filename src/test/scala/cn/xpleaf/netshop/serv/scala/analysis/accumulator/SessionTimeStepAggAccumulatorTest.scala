package cn.xpleaf.netshop.serv.scala.analysis.accumulator

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author xpleaf 
  */
object SessionTimeStepAggAccumulatorTest {

    def main(args: Array[String]): Unit = {
        test01()
    }

    // 测试自定义累加器
    def test01(): Unit = {
        val conf = new SparkConf()
            .setMaster("local[2]")
            .setAppName(s"${SessionTimeStepAggAccumulatorTest.getClass.getSimpleName}")
        val sc = new SparkContext(conf)
        val initialValue =
            s"td_1s_3s=0|" +
            s"td_4s_6s=0|" +
            s"td_7s_9s=0|" +
            s"td_10s_30s=0|" +
            s"td_30s_60s=0|" +
            s"td_1m_3m=0|" +
            s"td_3m_10m=0|" +
            s"td_10m_30m=0|" +
            s"td_30m=0|" +
            s"sl_1_3=0|" +
            s"sl_4_6=0|" +
            s"sl_7_9=0|" +
            s"sl_10_30=0|" +
            s"sl_30_60=0|" +
            s"sl_60=0|" +
            s"session_count=0|"
        val sessionTimeStepAggAccumulator = sc.accumulator[String](initialValue)(new SessionTimeStepAggAccumulator())

        sessionTimeStepAggAccumulator.add("td_1s_3s")
        sessionTimeStepAggAccumulator.add("td_1s_3s")
        sessionTimeStepAggAccumulator.add("session_count")
        println(sessionTimeStepAggAccumulator.value)
    }

}
