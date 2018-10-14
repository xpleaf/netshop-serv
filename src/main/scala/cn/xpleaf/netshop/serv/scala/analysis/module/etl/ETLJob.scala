package cn.xpleaf.netshop.serv.scala.analysis.module.etl

import java.util.{Calendar, TimeZone}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  * @author xpleaf
  */
object ETLJob {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .setMaster("local[2]")  // 本地测试时打开
            .setAppName(s"${ETLJob.getClass.getSimpleName}")
        val sc = new SparkContext(conf)

        val hiveContext = new HiveContext(sc)

        // 0.先获取年月日
        val now:Calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT+8"))
        val year:String = now.get(Calendar.YEAR).toString
        val month:String = (now.get(Calendar.MONTH) + 1).toString
        val day:String = if(now.get(Calendar.DAY_OF_MONTH) > 9) now.get(Calendar.DAY_OF_MONTH).toString else "0" + now.get(Calendar.DAY_OF_MONTH)

        // 0.1 使用netshop库
        val sql01 = "use netshop"
        hiveContext.sql(sql01).show()

        // 0.2 为避免重复加载数据，先删除原来的数据
        val sql02 = s"alter table user_visit_session drop partition(the_date='${year}-${month}-${day}')"
        hiveContext.sql(sql02).show()

        // 1.先删除外部表external_user_visit_session
        val sql1 = "drop table if exists `external_user_visit_session`"
        hiveContext.sql(sql1).show()

        // 2.创建外部表external_user_visit_session
        val sql2 = """create external table external_user_visit_session(
        log_date string,
        log_thread string,
        log_class string,
        log_info string,
        the_date date,
        user_id bigint,
        session_id string,
        page_id bigint,
        action_time string,
        search_keyword string,
        click_category_id bigint,
        click_product_id bigint,
        order_category_ids string,
        order_product_ids string,
        pay_category_ids string,
        pay_product_ids string,
        city_id bigint
        )row format delimited
        fields terminated by '\t'"""
        hiveContext.sql(sql2).show()


        // 3.设置external_user_visit_session的数据位置
        // 需要指定绝对路径，否则会报错： {0}  is not absolute or has no scheme information.
        // Please specify a complete absolute uri with scheme information
/*        val sql3 = s"""alter table external_user_visit_session
            set location 'hdfs://ns1/logs/netshop/${year}/${month}/${day}'"""*/
        // 本地测试用下面的，连接本地的集群，注意不加配置文件hdfs-site.xml和core-site.xml的话，会报错：
        // java.io.IOException Incomplete HDFS URI, no host: hdfs:///logs/netshop/2018/10/14
        val sql3 = s"""alter table external_user_visit_session
            set location 'hdfs:///logs/netshop/${year}/${month}/${day}'"""
        hiveContext.sql(sql3).show()


        // 4.插入数据到user_visit_session表中
        val sql4 = s"""insert into table user_visit_session partition(the_date='${year}-${month}-${day}')
        select
        user_id,
        session_id,
        page_id,
        action_time,
        search_keyword,
        click_category_id,
        click_product_id,
        order_category_ids,
        order_product_ids,
        pay_category_ids,
        pay_product_ids,city_id
        from external_user_visit_session"""
        hiveContext.sql(sql4).show()


        // 5.删除外部表external_user_visit_session
        val sql5 = "drop table if exists `external_user_visit_session`"
        hiveContext.sql(sql5).show()


        // 6.查询当天的数据量
        val sql6 = s"""select count(*)
        from user_visit_session
        where the_date='${year}-${month}-${day}'"""
        hiveContext.sql(sql6).show()

        // 7.查询总的数据量
        val sql7 = "select count(*) from user_visit_session"
        hiveContext.sql(sql7).show()

        sc.stop()
    }

}
