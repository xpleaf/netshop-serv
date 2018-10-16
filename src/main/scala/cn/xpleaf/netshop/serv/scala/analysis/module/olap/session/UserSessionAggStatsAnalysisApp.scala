package cn.xpleaf.netshop.serv.scala.analysis.module.olap.session

import cn.xpleaf.netshop.serv.java.analysis.dao.{ITaskDao, TaskDaoImpl}
import cn.xpleaf.netshop.serv.java.analysis.domain.Task
import cn.xpleaf.netshop.serv.java.analysis.utils.ParamUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
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
        val taskDao:ITaskDao = new TaskDaoImpl()
        val task:Task = taskDao.getTaskById(taskID)
        val paramJson:String = task.getTask_param

        // 1.初步做时间范围内数据的筛选
        val sessionRowRDD:RDD[Row] = getRangeSessionRDD(hiveContext, paramJson)
        println("-------------------------->sessionRowRDD's size: " + sessionRowRDD.count())
        //--------------------------------------------------------------------------------------------//

        /**
          * 2.将上述生成的sessionRowRDD转换为sessionId2ActionRDD
          * k,v
          * k：sessionId
          * v：row表示的action操作
          */
        val sessionId2ActionRDD:RDD[(String, Row)] = sessionRowRDD.map{
            case row => (row.getAs("session_id"), row)
        }
        println("-------------------------->sessionId2ActionRDD's size: " + sessionId2ActionRDD.count())
        //--------------------------------------------------------------------------------------------//

        /**
          * 3.将相同sessionId的row拉取到一起，形成sessionId2ActionsRDD
          * k,v
          * k：sessionId
          * v：Iterable[Row]
          * 表示将一次用户session的所有操作都聚合到一起
          * 问题：这里是否需要进行数据倾斜调优，因为是使用groupByKey进行操作？
          * 不需要，因为一次用户session，即便每秒点击一次，一天24小时点击，一天就86400次
          * 当然，不可能该用户连续一个月或者几个月都这样不停地点击，所以这里暂时不考虑调优
          * 如果需要的话，也说得过去吧，只是目前没有这个必要，调优的直接参考之前的文档即可
          */
        val sessionId2ActionsRDD:RDD[(String, Iterable[Row])] = sessionId2ActionRDD.groupByKey()
        println("-------------------------->sessionIdsActionsRDD's size: " + sessionId2ActionsRDD.count())
        //--------------------------------------------------------------------------------------------//


        // 关闭SparkContext
        sc.stop()

    }

    /**
      * 初步做时间范围内数据的筛选
      */
    def getRangeSessionRDD(hiveContext: HiveContext, paramJson:String):RDD[Row] = {
        // 拿到计算的时间范围，注意查询时间范围，hive中的user_visit_session表，the_date的格式为yyyy-MM-dd
        // 不支持后面查询有具体时间的，因为其在hive中是以partition的格式进行存储的
        val startDate:String = ParamUtil.getValueByKey(paramJson, "startDate").toString
        val endDate:String = ParamUtil.getValueByKey(paramJson, "endDate").toString
        // 生成sql语句
        val sql:String =
            s"""
              |select *
              |from user_visit_session
              |where the_date>='$startDate' and the_date<='$endDate'
            """.stripMargin
        // 使用netshop这个库，否则下面的查询没有作用
        hiveContext.sql("use netshop")
        // 向hive中查询
        val df:DataFrame = hiveContext.sql(sql)
        df.rdd
    }

}
