package cn.xpleaf.netshop.serv.java.analysis.utils;

import cn.xpleaf.netshop.serv.java.analysis.conf.ConfigurationManager;
import cn.xpleaf.netshop.serv.java.analysis.constants.Constants;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * 参数工具类
 * @author thinkpad
 *
 */
public class ParamUtils {

	/**
	 * 从命令行参数中提取任务id
	 * @param args 命令行参数
	 * @return 任务id
	 */
	public static Long getTaskIdFromArgs(String[] args, String taskType) {

		if("local".equalsIgnoreCase(ConfigurationManager.getStringProperty(Constants.SPARK_JOB_RUN_MODE))) {
			return ConfigurationManager.getLongProperty(taskType);
		} else {//在生产环境中，根据参数的第一个参数来判断spark作业用的是哪一个模块
			try {
				if(args != null && args.length > 0) {
					return Long.valueOf(args[0]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return null;
	}
	
	/**
	 * 从JSON对象中提取参数
	 * @param jsonObject JSON对象
	 * @return 参数
	 */
	public static String getParam(JSONObject jsonObject, String field) {
		JSONArray jsonArray = jsonObject.getJSONArray(field);
		if(jsonArray != null && jsonArray.size() > 0) {
			return jsonArray.getString(0);
		}
		return null;
	}
	
}
