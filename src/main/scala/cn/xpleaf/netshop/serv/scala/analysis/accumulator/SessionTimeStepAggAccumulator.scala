package cn.xpleaf.netshop.serv.scala.analysis.accumulator

import cn.xpleaf.netshop.serv.java.analysis.utils.StringUtils
import org.apache.spark.AccumulatorParam

/**
  * @author xpleaf
  *
  * 自定义累加器，实现累加用户session访问时长和步长的业务逻辑操作
  *     我们已经知道的信息是：
  *         当前session的时长visit_length和步长step_length
  *         可以通过visit_length--->计算1s_3s=count|4s_7s=count...
  *         可以通过step_length--->计算1_3=count|4_7=count...
  *     我们最终得到的一个结果就应该是
  *         1s_3s=5
  *         4s_7s=10
  *         1_3=10
  *         4_7=15
  *     1s_3s=0|\n4s_7s=10|\n1_3=10|\n4_7=15
  *
  * 需要注意的是，这里extends AccumulatorParam[String]，泛型使用的是String，
  * 所以后面在实现抽象方法时，其参数都为String类型的，默认的累加器在新建时，是可以指定类型的：
  * val counterAcc = sc.accumulator[Int](0)
  * 但是显然如果不自己自定义一个，不管是Int类型还是String类型的，都只能实现有限的功能
  * 问题：那我泛型参数不是可以填写一个自定义的对象？
  * 自定义对象必然需要经过序列化和反序列化的处理，其性能实际上可能不如这里自定义的使用
  * 字符串来作为泛型参数的累加器，因为字符串的拼接操作速度很快，而且这里的字符串量很少，
  * 因此，性能相比使用自定义的对象，性能应该会更加优越，这点在分布式计算中也是需要考虑的
  */
class SessionTimeStepAggAccumulator extends AccumulatorParam[String] {

    /**
      * 对自定义累加器做初始化
      * --->应该是，初始化值之后会调用该方法
      * @param initialValue 初始化值，在新建该对象时需要使用到，到时使用时直接使用空串""来初始化即可
      */
    override def zero(initialValue: String): String = {
        if(initialValue == null || "".equals(initialValue)) {
            throw new Exception("初始化值不能为null或空串，请参照相关文档或咨询叶子传入指定初始化值.")
        }
        /**
          * 坑：
          * 原来的想法是这样的，构造自定义的累加器时，想传入空串""，然后在这里进行初始化值，并返回
          * 但是实际测试中发现，这样做是没有效果的，即初始值的设置只能在构造该累加器时进行设置
          * 因为，当初的理解是，该方法为：对自定义累加器做初始化，实际上是：初始化值之后会调用该方法
          */
        initialValue
    }

    /**
      * 将新的字段添加进老的字段中
      * 当执行acc.add(field)的时候，就会执行该操作
      * @param allFields    累加器中已经存在的值，这里是使用初始化的值
      * @param accField     要在累加器中增加的值，如：td_30s_60s
      * @return             修改后的字符串值
      */
    override def addInPlace(allFields: String, accField: String): String = {
        if(allFields.contains(accField)) {
            /**
              * 如果存在该字段名，则在字段名对应的值中加1
              * 此时的操作是在每个task中进行的累加器操作
              */
            val oldValue:String = StringUtils.getFieldFromConcatString(allFields, "\\|", accField)
            val newValue:Int = oldValue.toInt + 1
            // 直接返回修改后的字符串
            StringUtils.setFieldInConcatString(allFields, "\\|", accField, newValue.toString)
        } else {
            /**
              * 如果不包含，说明这时是不同task之间的累加器进行合并，第一次合并时，allFields为initialValue
              * 之后就为合并之后的值，如果有两个task，那么就会合并两次，这点尤其需要注意，
              * 那么accField是什么呢？就为前面的task计算的结果，就是那个task中的allFields
              * 也就是说，spark累加器的机制是这样子的：
              * 1.每个task中计算，各自的累加器值先是独立进行计算的
              * 2.task中计算完成之后，再将各个task计算后的累加器进行合并
              * 3.在合并时，spark会构建一个全新的累加器去进行合并（这也符合上面的解析）
              * 更正：这种合并应该是不同线程之间的合并，并不是不同task，因为即便只有一个task，也会有这种情况
              * 目前的理解是这样，可能会有偏差，但至少也解决了问题
              */
            // 将accField中的每个值合并到allFields中
            println("执行合并操作------->accField: " + accField)
            println("执行合并操作------->allFields: " + allFields)
            StringUtils.combineAccumulatorsValue(allFields, accField)
        }
    }
}
/*
处理跟实际想的也许有些偏差：
执行合并操作------->accField: td_1s_3s=7|td_4s_6s=6|td_7s_9s=7|td_10s_30s=50|td_30s_60s=48|td_1m_3m=102|td_3m_10m=412|td_10m_30m=1181|td_30m=3183|sl_1_3=5198|sl_4_6=5385|sl_7_9=2233|sl_10_30=481|sl_30_60=0|sl_60=0|session_count=13297
执行合并操作------->allFields: td_1s_3s=0|td_4s_6s=0|td_7s_9s=0|td_10s_30s=0|td_30s_60s=0|td_1m_3m=0|td_3m_10m=0|td_10m_30m=0|td_30m=0|sl_1_3=0|sl_4_6=0|sl_7_9=0|sl_10_30=0|sl_30_60=0|sl_60=0|session_count=0|

执行合并操作------->accField: td_1s_3s=7|td_4s_6s=4|td_7s_9s=6|td_10s_30s=37|td_30s_60s=41|td_1m_3m=105|td_3m_10m=400|td_10m_30m=1226|td_30m=3231|sl_1_3=5094|sl_4_6=5486|sl_7_9=2368|sl_10_30=431|sl_30_60=0|sl_60=0|session_count=13379
执行合并操作------->allFields: td_1s_3s=14|td_4s_6s=12|td_7s_9s=14|td_10s_30s=100|td_30s_60s=96|td_1m_3m=204|td_3m_10m=824|td_10m_30m=2362|td_30m=6366|sl_1_3=10396|sl_4_6=10770|sl_7_9=4466|sl_10_30=962|sl_30_60=0|sl_60=0|session_count=26594

-------------------------->sessionTimeStepAggAccumulator's value: td_1s_3s=14|td_4s_6s=8|td_7s_9s=12|td_10s_30s=74|td_30s_60s=82|td_1m_3m=210|td_3m_10m=800|td_10m_30m=2452|td_30m=6462|sl_1_3=10188|sl_4_6=10972|sl_7_9=4736|sl_10_30=862|sl_30_60=0|sl_60=0|session_count=26758
但不可否认的是，值确实是会合并的。
 */