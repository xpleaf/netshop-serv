package cn.xpleaf.netshop.serv.java.analysis.conf;

import cn.xpleaf.netshop.serv.java.analysis.constants.Constants;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理类，主要负责管理 读取全局配置信息
 */
public class ConfigurationManager {

    private static Properties properties;

    public static String mode;

    private ConfigurationManager() {
    }

    static {
        properties = new Properties();
        try {
            InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("conf.properties");
            properties.load(in);
            mode = properties.getProperty(Constants.SPARK_JOB_RUN_MODE);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getStringProperty(String key) {
        return properties.getProperty(key);
    }

    public static long getLongProperty(String key) {
        return Long.valueOf(properties.getProperty(key));
    }

    public static int getIntegerProperty(String key) {
        return Integer.valueOf(properties.getProperty(key));
    }

    public static double getDoubleProperty(String key) {
        return Long.valueOf(properties.getProperty(key));
    }
}
