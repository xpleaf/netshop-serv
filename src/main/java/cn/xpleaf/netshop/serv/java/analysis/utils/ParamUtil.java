package cn.xpleaf.netshop.serv.java.analysis.utils;

import com.google.gson.Gson;

import java.util.Map;

/**
 * @author xpleaf
 * @date 2018/10/15 下午2:26
 */
public class ParamUtil {

    // 通过key拿到value
    public static Object getValueByKey(String paramJson, String key) {
        Gson gson = new Gson();
        Map map = gson.fromJson(paramJson, Map.class);
        return map.get(key);
    }

}
