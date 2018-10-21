package cn.xpleaf.netshop.serv.scala.analysis.module.olap.session

import cn.xpleaf.netshop.serv.java.analysis.dao.{ITaskDao, TaskDaoImpl}
import cn.xpleaf.netshop.serv.java.analysis.domain.Task
import cn.xpleaf.netshop.serv.java.analysis.utils.{DateUtils, ParamUtil, StringUtils, ValidationUtils}
import cn.xpleaf.netshop.serv.scala.analysis.accumulator.SessionTimeStepAggAccumulator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
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
            .setMaster("local[4]")      // 本地测试时打开
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
        sessionId2ActionRDD.cache()
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
        sessionId2ActionsRDD.cache()
        println("-------------------------->sessionId2ActionsRDD's size: " + sessionId2ActionsRDD.count())
        sessionId2ActionsRDD.take(10).foreach(t => {
            println(s"sessionId: ${t._1}, rows: ${t._2}")
        })
        //--------------------------------------------------------------------------------------------//

        /**
          * 4.将sessionId2ActionsRDD转换为userId2PartAggInfoRDD
          * k,v
          * k：userId
          * v：row的聚合字符串表示partAggInfo
          * 其实就是将用户一次session操作（因为sessionId2ActionsRDD就是根据sessionId进行聚合）的信息聚合起来
          * a9ae622b4b8b43379b79184d2e36927e,row
          * a9ae622b4b8b43379b79184d2e36927e,row
          * a9ae622b4b8b43379b79184d2e36927e,row
          * row中有userId，那么userId肯定是一样的，用来标识某一用户，所以k就为userId，
          * 那么v，就是上面所有row的聚合信息，处理为字符串，如：searchKeys='杀猪菜','芳华','前任3'|order_category_ids=1,2,3,
          *
          * 从上面的思路可以知道，最后出来的userId2PartAggInfoRDD，k相同即userId相同的RDD可能会有多个，
          * 但这也是正常的，一份userId的RDD，就代表一次用户session会话，多份userId相同的RDD，就代表多次用户session会话
          * 那为什么是partAggInfo呢？因为这时我们的RDD还没有用户信息，这个后面会再做处理
          *
          * 根据这样处理的思路也可以知道，sessionId2ActionsRDD和userId2PartAggInfoRDD的大小应该是相等的
          * 为什么？
          * 因为getUserId2PartAggInfoRDD方法中使用的操作是map，是一对一的输入输出关系，所以肯定是一样的，
          * 现在只是将其sessionId2ActionsRDD以另外一种方式来进行表示：k为userId，v为聚合后的用户action
          */
        val userId2PartAggInfoRDD:RDD[(String, String)] = getUserId2PartAggInfoRDD(sessionId2ActionsRDD)
        userId2PartAggInfoRDD.cache()
        println("-------------------------->userId2PartAggInfoRDD's size: " + userId2PartAggInfoRDD.count())
        userId2PartAggInfoRDD.take(10).foreach(tuple => println(s"userId: ${tuple._1}, partAggInfo: ${tuple._2}\n"))
        //--------------------------------------------------------------------------------------------//

        /**
          * 5.将用户信息user_info表中的信息和partAggInfo聚合在一起
          * k,v
          * k：sessionId
          * v：partAggInfo与user_info表中数据聚合之后的string数据
          * 其实上面生成userId2PartAggInfoRDD的目的就是为了能够与user_info表中的数据进行join，因为在user_info表中，user_id是唯一的
          * 这样之后得到的RDD其实跟sessionId2ActionsRDD类似，只是此时v变成了包含用户信息的session聚合信息
          */
        val sessionId2FullAggInfoRDD:RDD[(String, String)] = getSessionId2FullAggInfoRDD(hiveContext, userId2PartAggInfoRDD)
        sessionId2FullAggInfoRDD.cache()
        println("-------------------------->sessionId2FullAggInfoRDD's size: " + sessionId2FullAggInfoRDD.count())
        sessionId2FullAggInfoRDD.take(10).foreach(tuple => println(s"sessionId: ${tuple._1}, fullAggInfo: ${tuple._2}\n"))
        //--------------------------------------------------------------------------------------------//

        /**
          * 6.根据用户在params提供的针对用户特征筛选数据参数，对以上聚合结果sessionId2FullAggInfoRDD进行过滤
          * k,v
          * k：v中符合条件的sessionId
          * v：fullAggInfo
          * 使用的是filter算子，因此有一个优化点，即filter之后重新打算数据，以使数据更加均匀，这一步后面可以再做
          * 参考：《Spark笔记整理（九）：性能优化概述与开发调优》http://blog.51cto.com/xpleaf/2113139
          * 其实就是使用filter之后进行coalesce操作
          *
          * filter的时候需要注意，在获取年龄范围时，ValidationUtils获取json字符串中的年龄之后，是使用
          * Integer.valueOf(dataFieldStr)
          * 来对数字字符串进行格式化的，但由于其使用的ParamUtil中使用gson来解析json字符串，而在gson中，
          * 对于数字，统一处理为double类型的数字object，这样的话，上面的转换就会有问题，那么在不修改两个
          * 工具类的前提下，如何使功能正常？那就是传入的json参数中，startAge和endAge都为数字字符串，而不是数字，即：
          * {"startAge":"20","endAge":"50","startDate":"2018-10-14","endDate":"2018-10-15"}
          */
        val filteredSessionId2FullAggInfoRDD:RDD[(String, String)] = getFilteredSessionId2FullAggInfoRDD(paramJson, sessionId2FullAggInfoRDD)
        filteredSessionId2FullAggInfoRDD.cache()
        println("-------------------------->filteredSessionId2FullAggInfoRDD's size: " + filteredSessionId2FullAggInfoRDD.count())
        filteredSessionId2FullAggInfoRDD.take(10).foreach(tuple => println(s"sessionId: ${tuple._1}, fullAggInfo: ${tuple._2}\n"))
        //--------------------------------------------------------------------------------------------//

        /**
          * 7.对过滤后的RDD，即filteredSessionId2FullAggInfoRDD
          * 统计出访问时长在1s~3s、4s~6s、7s~9s、10s~30s、30s~60s、1m~3m、3m~10m、
          * 10m~30m、30m以上各个范围内的session占比；访问步长在1~3、4~6、7~9、10~30、30~60、60以上各个
          * 范围内的session占比
          */
        // 先初始化一个累加器，因为统计后的数据都保存在累加器中
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
        val sessionTimeStepAggAccumulator = sc.accumulator[String](initialValue)(new SessionTimeStepAggAccumulator)
        // 调用方法进行统计
        calculateTimeStepOperation(filteredSessionId2FullAggInfoRDD, sessionTimeStepAggAccumulator)
        println("-------------------------->sessionTimeStepAggAccumulator's value: " + sessionTimeStepAggAccumulator.value)

        // 关闭SparkContext
        sc.stop()

    }

    /*================================================上面是main方法，下面是内部使用方法================================================*/

    /**
      * 统计出访问时长在1s~3s、4s~6s、7s~9s、10s~30s、30s~60s、1m~3m、3m~10m、
      * 10m~30m、30m以上各个范围内的session占比；访问步长在1~3、4~6、7~9、10~30、30~60、60以上各个
      * 范围内的session占比
      */
    def calculateTimeStepOperation(filteredSessionId2FullAggInfoRDD: RDD[(String, String)], sessionTimeStepAggAccumulator: Accumulator[String]):Unit = {
        filteredSessionId2FullAggInfoRDD.foreach{ case (sessionId, fullAggInfo) =>
            // 1.先计算session的数量
            sessionTimeStepAggAccumulator.add("session_count")
            // 拿到访问时长和访问步长
            val visitLength: Int = StringUtils.getFieldFromConcatString(fullAggInfo, "\\|", "visit_length").toInt
            val stepLength: Int = StringUtils.getFieldFromConcatString(fullAggInfo, "\\|", "step_length").toInt
            // 2.找出访问时长的范围并赋值
            if (visitLength >= 1 && visitLength <= 3) {
                sessionTimeStepAggAccumulator.add("td_1s_3s")
            } else if (visitLength >= 4 && visitLength <= 6) {
                sessionTimeStepAggAccumulator.add("td_4s_6s")
            } else if (visitLength >= 7 && visitLength <= 9) {
                sessionTimeStepAggAccumulator.add("td_7s_9s")
            } else if (visitLength >= 10 && visitLength <= 30) {
                sessionTimeStepAggAccumulator.add("td_10s_30s")
            } else if (visitLength > 30 && visitLength <= 60) {
                sessionTimeStepAggAccumulator.add("td_30s_60s")
            } else if (visitLength > 60 && visitLength <= 180) {
                sessionTimeStepAggAccumulator.add("td_1m_3m")
            } else if (visitLength > 180 && visitLength <= 600) {
                sessionTimeStepAggAccumulator.add("td_3m_10m")
            } else if (visitLength > 600 && visitLength <= 1800) {
                sessionTimeStepAggAccumulator.add("td_10m_30m")
            } else if (visitLength > 1800) {
                sessionTimeStepAggAccumulator.add("td_30m")
            }
            // 3.找出访问步长的范围并赋值
            if(stepLength >= 1 && stepLength <= 3) {
                sessionTimeStepAggAccumulator.add("sl_1_3")
            } else if (stepLength >= 4 && stepLength <= 6) {
                sessionTimeStepAggAccumulator.add("sl_4_6")
            } else if (stepLength >= 7 && stepLength <= 9) {
                sessionTimeStepAggAccumulator.add("sl_7_9")
            } else if (stepLength >= 10 && stepLength <= 30) {
                sessionTimeStepAggAccumulator.add("sl_10_30")
            } else if (stepLength > 30 && stepLength <= 60) {
                sessionTimeStepAggAccumulator.add("sl_30_60")
            } else if (stepLength > 60) {
                sessionTimeStepAggAccumulator.add("sl_60")
            }

        }
    }

    /**
      * 根据用户在params提供的针对用户特征筛选数据参数，对sessionId2FullAggInfoRDD进行过滤
      */
    def getFilteredSessionId2FullAggInfoRDD(paramJson: String, sessionId2FullAggInfoRDD: RDD[(String, String)]):RDD[(String, String)] = {
        val startAge = ParamUtil.getValueByKey(paramJson, "startAge")
        val endAge = ParamUtil.getValueByKey(paramJson, "endAge")
        val sex = ParamUtil.getValueByKey(paramJson, "sex")
        val cities = ParamUtil.getValueByKey(paramJson, "cities")
        val professionals = ParamUtil.getValueByKey(paramJson, "professionals")

        // 进行参数的汇总
        var parameters = ""
        if(startAge != null) {
            parameters = s"${parameters}startAge=$startAge|"
        }
        if(endAge != null) {
            parameters = s"${parameters}endAge=$endAge|"
        }
        if(sex != null) {
            parameters = s"${parameters}sex=$sex|"
        }
        if(cities != null) {
            parameters = s"${parameters}cities=$cities|"
        }
        if(professionals != null) {
            parameters = s"${parameters}professionals=$professionals|"
        }
        // 使用filter算子执行过滤，flag为false的会被过滤掉
        sessionId2FullAggInfoRDD.filter{case (sessionId, fullAggInfo) =>
            var flag = true     // 下面只要有一个条件不满足，flag就为false，数据就会被过滤掉
            // 判断当前年龄是否在[startAge, endAge]之间
            if(flag && !ValidationUtils
                .between(fullAggInfo, "age", parameters, "startAge", "endAge")) {
                flag = false
            }
            // 判断当前sex是否为指定的sex
            if(flag && !ValidationUtils.equal(fullAggInfo, "sex", parameters, "sex")) {
                flag = false
            }
            // 判断当前地域是否在指定的cities
            if(flag && !ValidationUtils.in(fullAggInfo, "city", parameters, "cities")) {
                flag = false
            }
            // 判断当前职业是否在professionals中
            if(flag && !ValidationUtils.in(fullAggInfo, "professional", parameters, "professionals")) {
                flag = false
            }

            flag
        }
    }

    /**
      * 将用户信息user_info表中的信息和partAggInfo聚合在一起
      */
    def getSessionId2FullAggInfoRDD(hiveContext: HiveContext, userId2PartAggInfoRDD: RDD[(String, String)]):RDD[(String, String)] = {
        // 从hive中加载user_info表数据
        val sql = "SELECT * FROM user_info"
        hiveContext.sql("use netshop")
        val userInfoDf:DataFrame = hiveContext.sql(sql)
        // 将userInfoDf转换为userId2UserInfoRDD
        val userId2UserInfoRDD:RDD[(String, Row)] = userInfoDf.rdd.map(row => {
            val userId:String = row.getAs[Long]("id").toString
            (userId, row)
        })
        // 将userId2PartAggInfoRDD与userId2UserInfoRDD进行join操作，连接在一起
        val userId2FullAggInfoRDD:RDD[(String, (String, Row))] = userId2PartAggInfoRDD.join(userId2UserInfoRDD)
        // 将userId2FullAggInfoRDD转换为sessionId2FullAggInfoRDD
        val sessionId2FullAggInfoRDD:RDD[(String, String)] = userId2FullAggInfoRDD.map{ case (userId:String, (partAggInfo:String, row:Row)) =>
            // 获得row中user的关键信息
            val age:Int = row.getAs[Int]("age")
            val professional:String = row.getAs[String]("professional")
            val city:String = row.getAs[String]("city")
            val sex:String = row.getAs[String]("sex")
            /**
              * 将user信息与partAggInfo信息连接在一起
              */
            val fullAggInfo =
                s"$partAggInfo|" +
                s"age=$age|" +
                s"professional=$professional|" +
                s"city=$city|" +
                s"sex=$sex|"
            // 获得sessionId\
            val sessionId = StringUtils.getFieldFromConcatString(partAggInfo, "\\|", "session_id")
            (sessionId, fullAggInfo)
        }
        sessionId2FullAggInfoRDD
    }

    /**
      * 将sessionId2ActionsRDD转换为userId2PartAggInfoRDD
      */
    def getUserId2PartAggInfoRDD(sessionId2ActionsRDD: RDD[(String, Iterable[Row])]):RDD[(String, String)] = {
        // 开始执行操作，每次map操作就是对用户的一次访问session的处理
        val userId2PartAggInfoRDD:RDD[(String, String)] = sessionId2ActionsRDD.map{ case (sessionId:String, rows:Iterable[Row]) =>

            var userId:String = null                    // 用户id，一次session中的userId都是一样的
            var startTime:String = null             // session开始时间
            var endTime:String = null               // session结束时间
            var searchKeywords:String = null        // session时间内聚合的搜索关键词集合
            var clickCategoryIds:String = null      // session时间内聚合的点击品类id集合
            var orderCategoryIds:String = null      // session时间内聚合的下单品类id集合
            var payCategoryIds:String = null        // session时间内聚合的支付品类id集合

            // 处理每一行，就是该session中用户的每一次action
            // 显然一次action只可能执行一次操作
            for(row <- rows) {
                if(userId == null) {
                    userId = row.getAs[Long]("user_id").toString
                }
                // 初始化startTime和endTime
                val actionTime = row.getAs[String]("action_time")
                if(startTime == null) {
                    startTime = actionTime
                }
                if(endTime == null) {
                    endTime = actionTime
                }
                // 判断并对startTime和endTime进行赋值
                if(DateUtils.before(actionTime, startTime)) {
                    startTime = actionTime
                }
                if(DateUtils.after(actionTime, endTime)) {
                    endTime = actionTime
                }
                // 处理搜索关键字
                val searchKeyword = row.getAs[String]("search_keyword")
                if(searchKeyword != null && !"null".equals(searchKeyword)) {    // 必须加!"null".equals，详见开发文档
                    if(searchKeywords != null) {
                        if (!searchKeywords.split(",").contains(searchKeyword)) {
                            searchKeywords = String.format("%s,%s", searchKeywords, searchKeyword)
                        }
                    } else {    // 初始化searchKeywords，下面的操作类似
                        searchKeywords = searchKeyword
                    }
                }
                // 处理点击品类id
                val clickCategoryId = row.getAs[Long]("click_category_id")
                if(clickCategoryId != null) {
                    if(clickCategoryIds != null) {
                        if (!clickCategoryIds.split(",").contains(clickCategoryId)) {
                            clickCategoryIds = String.format("%s,%s", clickCategoryIds, clickCategoryId.toString)
                        }
                    } else {
                        clickCategoryIds = clickCategoryId.toString
                    }
                }
                // 处理订单品类id
                val orderCategoryId = row.getAs[String]("order_category_ids")
                if(orderCategoryId != null && !"null".equals(orderCategoryId)) {
                    if(orderCategoryIds != null) {
                        if (!orderCategoryIds.split(",").contains(orderCategoryId)) {
                            orderCategoryIds = String.format("%s,%s", orderCategoryIds, orderCategoryId)
                        }
                    } else {
                        orderCategoryIds = orderCategoryId
                    }
                }
                // 处理支付品类id
                val payCategoryId = row.getAs[String]("pay_category_ids")
                if(payCategoryId != null && !"null".equals(payCategoryId)) {
                    if(payCategoryIds != null) {
                        if (!payCategoryIds.split(",").contains(payCategoryId)) {
                            payCategoryIds = String.format("%s,%s", payCategoryIds, payCategoryId)
                        }
                    } else {
                        payCategoryIds = payCategoryId
                    }
                }

            }
            // 计算访问session时长
            val visitLength:Int = DateUtils.minus(endTime, startTime)
            // 计算访问步长，就是执行了多少次操作
            val stepLength:Long = rows.size
            /**
              * 生成partAggInfo
              * 这里是用startTime作为sessionTime
              */
            val partAggInfo:String =
                s"session_id=$sessionId|" +
                s"search_keywords=$searchKeywords|" +
                s"click_category_ids=$clickCategoryIds|" +
                s"order_category_ids=$orderCategoryIds|" +
                s"pay_category_ids=$payCategoryIds|" +
                s"visit_length=$visitLength|" +
                s"step_length=$stepLength|" +
                s"start_time=$startTime|"

            (userId, partAggInfo)
        }
        userId2PartAggInfoRDD
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
