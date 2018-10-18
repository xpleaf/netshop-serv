package cn.xpleaf.netshop.serv.java.analysis.utils;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author xpleaf
 * @date 2018/10/18 下午11:54
 */
public class ParamUtilTest {

    // 测试获取参数
    @Test
    public void test01() throws Exception {
        String paramJson = "{\"startAge\":\"20\",\"endAge\":\"50\",\"startDate\":\"2018-10-14\",\"endDate\":\"2018-10-15\"}";
        Object startAge = ParamUtil.getValueByKey(paramJson, "startAge");
        System.out.println(startAge);
    }

}