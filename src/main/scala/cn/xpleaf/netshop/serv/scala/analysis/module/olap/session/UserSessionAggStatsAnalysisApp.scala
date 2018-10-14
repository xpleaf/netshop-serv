package cn.xpleaf.netshop.serv.scala.analysis.module.olap.session

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

/**
  * @author xpleaf
  *
  * 完成用户session聚合统计分析操作
  *
  * 根据产品运营人员的个性化定制，针对不同特征人群做数据删选，基于此做session的统计分析
  *     1、年龄维度：可以针对不同的年龄段的人群做年龄分析，80后 90后 00后 70后
  *     2、时间维度：可以针对不同用户操作时间段做分析，T+1，T+3、T+7
  *     3、地域维度：针对不同省市的用户做考察
  *     4、职业维度：针对不同职业的人群做考察
  *     5、性别维度：针对不同性别的喜好做统计分析
  *
  *     生成对应的task任务(在mysql表task中产生一条task记录)，同时通知spark启动对应的spark作业，从task任务中
  *   获取对应的task任务，针对该个性化定制，做session数据的删选，然后做对应的数据统计。
  *   最后将统计结果落地，运营人员，要基于此，绘制各种报表图，以作决策支撑。
  * 1、按条件筛选session
  * 2、统计出符合条件的session中，访问时长在1s~3s、4s~6s、7s~9s、10s~30s、30s~60s、1m~3m、3m~10m、
  * 10m~30m、30m以上各个范围内的session占比；访问步长在1~3、4~6、7~9、10~30、30~60、60以上各个
  * 范围内的session占比
  * 3、在符合条件的session中，按照时间比例随机抽取100个session
  * 4、在符合条件的session中，获取点击、下单和支付数量排名前10的品类
  * 5、对于排名前10的品类，分别获取其点击次数排名前10的session
  */
object UserSessionAggStatsAnalysisApp {

    def main(args: Array[String]): Unit = {
        // 获取logger日志记录
        val logger:Logger = LoggerFactory.getLogger(UserSessionAggStatsAnalysisApp.getClass)
        // 读取传入的taskID参数
        var taskID:Int = 0
        if(args.length > 0) {
            try {
                taskID = Integer.valueOf(args(0))
            } catch {
                case numEx:NumberFormatException =>
                    numEx.printStackTrace()
                    logger.error("数字转换出现异常，确认传入的taskID是否为数字类型！")
                    logger.error(numEx.getMessage)
                    System.exit(-1)
                case ex:Exception =>
                    ex.printStackTrace()
                    logger.error("数字转换出现异常，确认传入的taskID是否为数字类型！")
                    logger.error(ex.getMessage)
                    System.exit(-1)
            }
        } else {
            logger.error("没有传入taskID，执行UserSessionAggStatsAnalysisApp操作失败！")
            System.exit(-1)
        }

        // 初始化SparkConf、SparkContext和HiveContext
        val conf = new SparkConf()
            .setMaster("local[2]")      // 本地测试时打开
            .setAppName(s"${UserSessionAggStatsAnalysisApp.getClass.getSimpleName}_taskID_$taskID")
        val sc = new SparkContext(conf)
        val hiveContext = new HiveContext(sc)

        // 读取taskID对应的配置信息




        // 关闭SparkContext
        sc.stop()

    }

}
