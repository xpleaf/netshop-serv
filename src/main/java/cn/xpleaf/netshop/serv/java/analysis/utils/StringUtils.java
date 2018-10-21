package cn.xpleaf.netshop.serv.java.analysis.utils;

/**
 * 字符串工具类
 * @author thinkpad
 *
 */
public class StringUtils {

	/**
	 * 判断字符串是否为空
	 * @param str 字符串
	 * @return 是否为空
	 */
	public static boolean isEmpty(String str) {
		return str == null || "".equals(str);
	}
	
	/**
	 * 判断字符串是否不为空
	 * @param str 字符串
	 * @return 是否不为空
	 */
	public static boolean isNotEmpty(String str) {
		return str != null && !"".equals(str);
	}
	
	/**
	 * 截断字符串两侧的逗号
	 * @param str 字符串
	 * @return 字符串
	 */
	public static String trimComma(String str) {
		int start = 0, end = str.length() - 1;
		while((start <= end) && str.charAt(start) == ',') {
			start++;
		}
		while((start <= end) && str.charAt(end) == ',') {
			end--;
		}
		return str.substring(start, end + 1);
	}
	
	/**
	 * 补全两位数字
	 * @param str
	 * @return
	 */
	public static String fulfuill(String str) {
		if(str.length() == 2) {
			return str;
		} else {
			return "0" + str;
		}
	}
	
	/**
	 * 从拼接的字符串中提取字段
	 * @param str 字符串
	 * @param delimiter 分隔符 
	 * @param field 字段
	 * @return 字段值
	 */
	public static String getFieldFromConcatString(String str, 
			String delimiter, String field) {
		try {
			String[] fields = str.split(delimiter);
			for(String concatField : fields) {
				// searchKeywords=|clickCategoryIds=1,2,3
				if(concatField.split("=").length == 2) {
					String fieldName = concatField.split("=")[0];
					String fieldValue = concatField.split("=")[1];
					if(fieldName.equals(field)) {
						return fieldValue;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 从拼接的字符串中给字段设置值
	 * @param str 字符串
	 * @param delimiter 分隔符 
	 * @param field 字段名
	 * @param newFieldValue 新的field值
	 * @return 字段值
	 */
	public static String setFieldInConcatString(String str, 
			String delimiter, String field, String newFieldValue) {
		String[] fields = str.split(delimiter);
		
		for(int i = 0; i < fields.length; i++) {
			String fieldName = fields[i].split("=")[0];
			if(fieldName.equals(field)) {
				String concatField = fieldName + "=" + newFieldValue;
				fields[i] = concatField;
				break;
			}
		}
		
		StringBuffer buffer = new StringBuffer("");
		for(int i = 0; i < fields.length; i++) {
			buffer.append(fields[i]);
			if(i < fields.length - 1) {
				buffer.append("|");  
			}
		}
		return buffer.toString();
	}

    /**
     * 将不同task中的SessionTimeStepAggAccumulator计算之后的allFields
     * （在新的累加器中，其成为accField）
     * 合并到新的累加器的allFields中
     * @param allFields
     * @param accField
     * @return
     */
	public static String combineAccumulatorsValue(String allFields, String accField) {
	    // 先从accField中获取各个值
	    String td_1s_3s_accField = StringUtils.getFieldFromConcatString(accField, "\\|", "td_1s_3s");
        String td_4s_6s_accField = StringUtils.getFieldFromConcatString(accField, "\\|", "td_4s_6s");
        String td_7s_9s_accField = StringUtils.getFieldFromConcatString(accField, "\\|", "td_7s_9s");
        String td_10s_30s_accField = StringUtils.getFieldFromConcatString(accField, "\\|", "td_10s_30s");
        String td_30s_60s_accField = StringUtils.getFieldFromConcatString(accField, "\\|", "td_30s_60s");
        String td_1m_3m_accField = StringUtils.getFieldFromConcatString(accField, "\\|", "td_1m_3m");
        String td_3m_10m_accField = StringUtils.getFieldFromConcatString(accField, "\\|", "td_3m_10m");
        String td_10m_30m_accField = StringUtils.getFieldFromConcatString(accField, "\\|", "td_10m_30m");
        String td_30m_accField = StringUtils.getFieldFromConcatString(accField, "\\|", "td_30m");
        String sl_1_3_accField = StringUtils.getFieldFromConcatString(accField, "\\|", "sl_1_3");
        String sl_4_6_accField = StringUtils.getFieldFromConcatString(accField, "\\|", "sl_4_6");
        String sl_7_9_accField = StringUtils.getFieldFromConcatString(accField, "\\|", "sl_7_9");
        String sl_10_30_accField = StringUtils.getFieldFromConcatString(accField, "\\|", "sl_10_30");
        String sl_30_60_accField = StringUtils.getFieldFromConcatString(accField, "\\|", "sl_30_60");
        String sl_60_accField = StringUtils.getFieldFromConcatString(accField, "\\|", "sl_60");
        String session_count_accField = StringUtils.getFieldFromConcatString(accField, "\\|", "session_count");
        // 再从allFields中拿到各个值
        String td_1s_3s_allFields = StringUtils.getFieldFromConcatString(accField, "\\|", "td_1s_3s");
        String td_4s_6s_allFields = StringUtils.getFieldFromConcatString(accField, "\\|", "td_4s_6s");
        String td_7s_9s_allFields = StringUtils.getFieldFromConcatString(accField, "\\|", "td_7s_9s");
        String td_10s_30s_allFields = StringUtils.getFieldFromConcatString(accField, "\\|", "td_10s_30s");
        String td_30s_60s_allFields = StringUtils.getFieldFromConcatString(accField, "\\|", "td_30s_60s");
        String td_1m_3m_allFields = StringUtils.getFieldFromConcatString(accField, "\\|", "td_1m_3m");
        String td_3m_10m_allFields = StringUtils.getFieldFromConcatString(accField, "\\|", "td_3m_10m");
        String td_10m_30m_allFields = StringUtils.getFieldFromConcatString(accField, "\\|", "td_10m_30m");
        String td_30m_allFields = StringUtils.getFieldFromConcatString(accField, "\\|", "td_30m");
        String sl_1_3_allFields = StringUtils.getFieldFromConcatString(accField, "\\|", "sl_1_3");
        String sl_4_6_allFields = StringUtils.getFieldFromConcatString(accField, "\\|", "sl_4_6");
        String sl_7_9_allFields = StringUtils.getFieldFromConcatString(accField, "\\|", "sl_7_9");
        String sl_10_30_allFields = StringUtils.getFieldFromConcatString(accField, "\\|", "sl_10_30");
        String sl_30_60_allFields = StringUtils.getFieldFromConcatString(accField, "\\|", "sl_30_60");
        String sl_60_allFields = StringUtils.getFieldFromConcatString(accField, "\\|", "sl_60");
        String session_count_allFields = StringUtils.getFieldFromConcatString(accField, "\\|", "session_count");
        // 再设置值到allFields中，字符串是不可变的，所以一定要重新赋值
        allFields = StringUtils.setFieldInConcatString(allFields, "\\|", "td_1s_3s",
                String.valueOf(Long.valueOf(td_1s_3s_accField) + Long.valueOf(td_1s_3s_allFields)));
        allFields = StringUtils.setFieldInConcatString(allFields, "\\|", "td_4s_6s",
                String.valueOf(Long.valueOf(td_4s_6s_accField) + Long.valueOf(td_4s_6s_allFields)));
        allFields = StringUtils.setFieldInConcatString(allFields, "\\|", "td_7s_9s",
                String.valueOf(Long.valueOf(td_7s_9s_accField) + Long.valueOf(td_7s_9s_allFields)));
        allFields = StringUtils.setFieldInConcatString(allFields, "\\|", "td_10s_30s",
                String.valueOf(Long.valueOf(td_10s_30s_accField) + Long.valueOf(td_10s_30s_allFields)));
        allFields = StringUtils.setFieldInConcatString(allFields, "\\|", "td_30s_60s",
                String.valueOf(Long.valueOf(td_30s_60s_accField) + Long.valueOf(td_30s_60s_allFields)));
        allFields = StringUtils.setFieldInConcatString(allFields, "\\|", "td_1m_3m",
                String.valueOf(Long.valueOf(td_1m_3m_accField) + Long.valueOf(td_1m_3m_allFields)));
        allFields = StringUtils.setFieldInConcatString(allFields, "\\|", "td_3m_10m",
                String.valueOf(Long.valueOf(td_3m_10m_accField) + Long.valueOf(td_3m_10m_allFields)));
        allFields = StringUtils.setFieldInConcatString(allFields, "\\|", "td_10m_30m",
                String.valueOf(Long.valueOf(td_10m_30m_accField) + Long.valueOf(td_10m_30m_allFields)));
        allFields = StringUtils.setFieldInConcatString(allFields, "\\|", "td_30m",
                String.valueOf(Long.valueOf(td_30m_accField) + Long.valueOf(td_30m_allFields)));
        allFields = StringUtils.setFieldInConcatString(allFields, "\\|", "sl_1_3",
                String.valueOf(Long.valueOf(sl_1_3_accField) + Long.valueOf(sl_1_3_allFields)));
        allFields = StringUtils.setFieldInConcatString(allFields, "\\|", "sl_4_6",
                String.valueOf(Long.valueOf(sl_4_6_accField) + Long.valueOf(sl_4_6_allFields)));
        allFields = StringUtils.setFieldInConcatString(allFields, "\\|", "sl_7_9",
                String.valueOf(Long.valueOf(sl_7_9_accField) + Long.valueOf(sl_7_9_allFields)));
        allFields = StringUtils.setFieldInConcatString(allFields, "\\|", "sl_10_30",
                String.valueOf(Long.valueOf(sl_10_30_accField) + Long.valueOf(sl_10_30_allFields)));
        allFields = StringUtils.setFieldInConcatString(allFields, "\\|", "sl_30_60",
                String.valueOf(Long.valueOf(sl_30_60_accField) + Long.valueOf(sl_30_60_allFields)));
        allFields = StringUtils.setFieldInConcatString(allFields, "\\|", "sl_60",
                String.valueOf(Long.valueOf(sl_60_accField) + Long.valueOf(sl_60_allFields)));
        allFields = StringUtils.setFieldInConcatString(allFields, "\\|", "session_count",
                String.valueOf(Long.valueOf(session_count_accField) + Long.valueOf(session_count_allFields)));
        // 返回allFields
        return allFields;
    }

	public static void main(String[] args) {
		String str = ",,aaa,,";
		System.out.println(trimComma(str));
	}
	
}
