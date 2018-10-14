package cn.xpleaf.netshop.serv.java.analysis.constants;

/**
 * 项目常量类
 */
public interface Constants {
    //spark作业的运行模式|local|test|production
    String SPARK_JOB_RUN_MODE = "spark.job.run.mode";

    /**
     * 执行spark session分析的测试任务id
     */
    String SPARK_JOB_SESSION_TASK_ID = "spark.job.session.task.id";
    //执行page页面流的操作
    String SPARK_JOB_PAGE_TASK_ID = "spark.job.page.task.id";
    //执行区域热门商品的操作
    String SPARK_JOB_PRODUCT_TASK_ID = "spark.job.product.task.id";

    /**
     * spark任务参数之startDate
     */
    String SPARK_TASK_PARAM_START_DATE = "startDate";
    /**
     * spark任务参数之endDate
     */
    String SPARK_TASK_PARAM_END_DATE = "endDate";

    /**
     * 聚合参数信息
     */
    String FIELD_SESSION_ID = "session_id";
    String FIELD_USER_ID = "user_id";
    String FIELD_VISIT_LENGTH = "visit_length";
    String FIELD_STEP_LENGTH = "step_length";
    String FIELD_SEARCH_KEYWORDS = "search_keywords";
    String FIELD_CLICK_CATEGORY_IDS = "click_category_ids";
    String FIELD_ORDER_CATEGORY_IDS = "order_category_ids";
    String FIELD_PAY_CATEGORY_IDS = "pay_category_ids";
    String FIELD_START_TIME = "start_time";
    String FIELD_END_TIME = "end_time";
    String FIELD_AGE = "age";
    String FIELD_CITY = "city";
    String FIELD_PROFESSIONAL = "professional";
    String FIELD_SEX = "sex";




    String PARAM_START_AGE = "startAge";
    String PARAM_END_AGE = "endAge";
    String PARAM_SEX = "sex";
    String PARAM_PROFESSIONALS = "professionals";
    String PARAM_CITIES = "cities";

    /**
     * 关于访问时长和访问步长的定义
     * 访问时长在1s~3s、4s~6s、7s~9s、10s~30s、30s~60s、1m~3m、3m~10m、
     * 10m~30m、30m以上各个范围内的session占比；访问步长在1~3、4~6、7~9、10~30、30~60、60以上各个
     */
    String TIME_DURATION_1s_3s = "td_1s_3s";
    String TIME_DURATION_4s_6s = "td_4s_6s";
    String TIME_DURATION_7s_9s = "td_7s_9s";
    String TIME_DURATION_10s_30s = "td_10s_30s";
    String TIME_DURATION_30s_60s = "td_30s_60s";
    String TIME_DURATION_1m_3m = "td_1m_3m";
    String TIME_DURATION_3m_10m = "td_3m_10m";
    String TIME_DURATION_10m_30m = "td_10m_30m";
    String TIME_DURATION_30m = "td_30m";

    String STEP_LENGTH_1_3 = "sl_1_3";
    String STEP_LENGTH_4_6 = "sl_4_6";
    String STEP_LENGTH_7_9 = "sl_7_9";
    String STEP_LENGTH_10_30 = "sl_10_30";
    String STEP_LENGTH_30_60 = "sl_30_60";
    String STEP_LENGTH_60 = "sl_60";

    String SESSION_COUNT = "session_count";


    /**
     * db
     */
    String JDBC_DRIVER_CLASS_NAME = "driverClassName";
    String JDBC_URL = "url";
    String JDBC_USERNAME = "username";
    String JDBC_PASSWORD = "password";

    /**
     * 页面流切片
     */
    String PARAM_PAGE_FLOW_SPLIT = "pageFlow";
    /**
     * kafka topic
     */
    String AD_REAL_TIME_LOG_TOPIC = "ad.real.time.log.topic";
    String BOOTSTRAP_SERVERS = "bootstrap.servers";
    String BLACKLIST_USER_MAX_CLICK = "blacklist.user.max.click";
}
