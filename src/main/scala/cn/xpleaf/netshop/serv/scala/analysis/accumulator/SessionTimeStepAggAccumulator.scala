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
        if(allFields.contains(accField)) {  // 如果存在该字段名，则在字段名对应的值中加1
            val oldValue:String = StringUtils.getFieldFromConcatString(allFields, "\\|", accField)
            val newValue:Int = oldValue.toInt + 1
            // 直接返回修改后的字符串
            StringUtils.setFieldInConcatString(allFields, "\\|", accField, newValue.toString)
        } else {                            // 否则不进行操作
            println("累加器中不存在该字段名，添加值失败！")
            allFields
        }
    }
}
