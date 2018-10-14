package cn.xpleaf.netshop.serv.java.analysis.utils;

import cn.xpleaf.netshop.serv.java.analysis.conf.ConfigurationManager;
import cn.xpleaf.netshop.serv.java.analysis.constants.Constants;
import cn.xpleaf.netshop.serv.java.analysis.constants.DeployMode;
import org.apache.commons.dbcp.BasicDataSourceFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * DBCP数据库连接池
 *  我们这里使用的dbcp的数据库连接池，就是用dbcp提供的数据库datasource即可
 */
public class DBCPUtil {

    private DBCPUtil(){}
    private static Properties properties;
    private static DataSource ds;
    public static String jdbcUrl;
    public static String jdbcUsername;
    public static String jdbcPassword;
    static {
        try {
            properties = new Properties();
            String mode = ConfigurationManager.getStringProperty(Constants.SPARK_JOB_RUN_MODE);
            InputStream in = null;
            if(DeployMode.LOCAL.toString().equalsIgnoreCase(mode)) {//本地模式
                in = DBCPUtil.class.getClassLoader().getResourceAsStream("local/dbcp-config.properties");
            } else if (DeployMode.TEST.toString().equalsIgnoreCase(mode)) {
                in = DBCPUtil.class.getClassLoader().getResourceAsStream("test/dbcp-config.properties");
            } else if (DeployMode.PRODUCTION.toString().equalsIgnoreCase(mode)) {
                in = DBCPUtil.class.getClassLoader().getResourceAsStream("production/dbcp-config.properties");
            } else {
                throw new ExceptionInInitializerError();
            }
            properties.load(in);
            ds = BasicDataSourceFactory.createDataSource(properties);
            jdbcUrl = properties.getProperty(Constants.JDBC_URL);
            jdbcUsername = properties.getProperty(Constants.JDBC_USERNAME);
            jdbcPassword = properties.getProperty(Constants.JDBC_PASSWORD);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static DataSource getDataSource() {
        return ds;
    }

    public static Connection getConnection() {
        try {
            return ds.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

}
