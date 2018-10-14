package cn.xpleaf.netshop.serv.java.analysis.mock;

import cn.xpleaf.netshop.serv.java.analysis.utils.DateUtils;
import cn.xpleaf.netshop.serv.java.analysis.utils.StringUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;


/**
 * 模拟数据程序
 * @author thinkpad
 *
 */
public class MockData {

	/**
	 * 模拟数据
	 * @param sc
	 * @param sqlContext
	 */
	public static void mock(JavaSparkContext sc,
			SQLContext sqlContext) {
		List<Row> rows = new ArrayList<Row>();
		
		String[] searchKeywords = new String[] {"东北杀猪菜", "战狼2", "兰州拉面", "东来顺刷锅",
				"呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "南锣鼓巷", "山西刀削面", "温泉"};
		String date = DateUtils.getTodayDate();
		String[] actions = new String[]{"search", "click", "order", "pay"};
		Random random = new Random();
		
		for(int i = 0; i < 100; i++) {
			long userId = random.nextInt(100);
			
			for(int j = 0; j < 10; j++) {
				String sessionId = UUID.randomUUID().toString().replace("-", "");
				String baseActionTime = date + " " + StringUtils.fulfuill(random.nextInt(24) + "");
				
				Long clickCategoryId = null;
				  
				for(int k = 0; k < random.nextInt(100); k++) {
//				for(int k = 0; k < 100; k++) {
					long pageId = random.nextInt(10);
					String actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(60))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(60)));
					String searchKeyword = null;
					Long clickProductId = null;
					String orderCategoryIds = null;
					String orderProductIds = null;
					String payCategoryIds = null;
					String payProductIds = null;
					
					String action = actions[random.nextInt(4)];
					if("search".equals(action)) {
						searchKeyword = searchKeywords[random.nextInt(10)];   
					} else if("click".equals(action)) {
						if(clickCategoryId == null) {
							clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));    
						}
						clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));  
					} else if("order".equals(action)) {
						orderCategoryIds = String.valueOf(random.nextInt(100));  
						orderProductIds = String.valueOf(random.nextInt(100));
					} else if("pay".equals(action)) {
						payCategoryIds = String.valueOf(random.nextInt(100));  
						payProductIds = String.valueOf(random.nextInt(100));
					}
					
					Row row = RowFactory.create(date, userId, sessionId,
							pageId, actionTime, searchKeyword,
							clickCategoryId, clickProductId,
							orderCategoryIds, orderProductIds,
							payCategoryIds, payProductIds, 
							Long.valueOf(String.valueOf(random.nextInt(10))));    
					rows.add(row);
				}
			}
		}
		
		JavaRDD<Row> rowsRDD = sc.parallelize(rows);
		
		StructType schema = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("date", DataTypes.StringType, true),
				DataTypes.createStructField("user_id", DataTypes.LongType, true),
				DataTypes.createStructField("session_id", DataTypes.StringType, true),
				DataTypes.createStructField("page_id", DataTypes.LongType, true),
				DataTypes.createStructField("action_time", DataTypes.StringType, true),
				DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
				DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
				DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
				DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
				DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
				DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
				DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true),
				DataTypes.createStructField("city_id", DataTypes.LongType, true)));
		
		DataFrame df = sqlContext.createDataFrame(rowsRDD, schema);
		
		df.registerTempTable("user_visit_action");  
		for(Row _row : df.take(1)) {
			System.out.println("user_visit_action===>" + _row);
		}
		
		/**
		 * ==================================================================
		 */
		
		rows.clear();
		String[] sexes = new String[]{"male", "female"};
		for(int i = 0; i < 100; i ++) {
			long userid = i;
			String username = "user_" + i;
			String name = "name_" + i;
			int age = random.nextInt(50) + 10;
			String professional = "professional_" + random.nextInt(100);
			String city = "city_" + random.nextInt(100);
			String sex = sexes[random.nextInt(2)];
			
			Row row = RowFactory.create(userid, username, name, age,
					professional, city, sex);
			rows.add(row);
		}
		
		rowsRDD = sc.parallelize(rows);
		
		StructType schema2 = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("user_id", DataTypes.LongType, true),
				DataTypes.createStructField("username", DataTypes.StringType, true),
				DataTypes.createStructField("name", DataTypes.StringType, true),
				DataTypes.createStructField("age", DataTypes.IntegerType, true),
				DataTypes.createStructField("professional", DataTypes.StringType, true),
				DataTypes.createStructField("city", DataTypes.StringType, true),
				DataTypes.createStructField("sex", DataTypes.StringType, true)));
		
		DataFrame df2 = sqlContext.createDataFrame(rowsRDD, schema2);
		for(Row _row : df2.take(1)) {
			System.out.println("user_info===>" + _row);
		}
		
		df2.registerTempTable("user_info");  
		
		/**
		 * ==================================================================
		 */
		rows.clear();
		
		int[] productStatus = new int[]{0, 1};
		
		for(int i = 0; i < 100; i ++) {
			long productId = i;
			String productName = "product" + i;
			String extendInfo = "{\"product_status\": " + productStatus[random.nextInt(2)] + "}";    
			
			Row row = RowFactory.create(productId, productName, extendInfo);
			rows.add(row);
		}
		
		rowsRDD = sc.parallelize(rows);
		
		StructType schema3 = DataTypes.createStructType(Arrays.asList(
				DataTypes.createStructField("product_id", DataTypes.LongType, true),
				DataTypes.createStructField("product_name", DataTypes.StringType, true),
				DataTypes.createStructField("extend_info", DataTypes.StringType, true)));
		
		DataFrame df3 = sqlContext.createDataFrame(rowsRDD, schema3);
		for(Row _row : df3.take(1)) {
			System.out.println("product_info===>" + _row);
		}
		
		df3.registerTempTable("product_info"); 
	}

	@Test
    public void test01() throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/netshop", "root", "root");
        // 创建QueryRunner类对象
        QueryRunner qr = new QueryRunner();
        String sql = "INSERT INTO user (username,name,age,professional,city,sex)VALUES(?,?,?,?,?,?)";

        String[] professionals = {"职业-1", "职业-2", "职业-3", "职业-4", "职业-5", "职业-6", "职业-7", "职业-8", "职业-9", "职业-10",
                           "职业-11", "职业-12", "职业-13", "职业-14", "职业-15", "职业-16", "职业-17", "职业-18", "职业-19", "职业-20",
                           "职业-21", "职业-22", "职业-23", "职业-24", "职业-25", "职业-26", "职业-27", "职业-28", "职业-29", "职业-30"};
        String[] cities = {"广州", "杭州", "北京", "上海", "东京", "首尔", "旧金山"};
        String[] sexes = {"male", "female"};

        Random ran = new Random();
        for(int i = 10001; i < 20000; i++) {
            String username = "user-" + i;
            String name = "name-" + i;
            int age = 10 + ran.nextInt(60);
            String professional = professionals[ran.nextInt(professionals.length)];
            String city = cities[ran.nextInt(cities.length)];
            String sex = sexes[ran.nextInt(sexes.length)];
            //将三个?占位符的实际参数,写在数组中
            Object[] params = {username, name, age, professional, city, sex};
            //调用QueryRunner类的方法update执行SQL语句
            int row = qr.update(conn, sql, params);
            System.out.println(row);
        }

    }

    @Test
    public void test02() throws Exception {
	    String session = "2018-09-20 23:15:34,011\t[main]\t[cn.xpleaf.bigdata.analysis.mock.MockLogData]\t[INFO]\t2018-09-20\t3507\t30a8462d155d451990c4a3d18f93fd89\t0\t2018-09-20 09:40:40\tnull\tnull\tnull\t8\t72\tnull\tnull\t0";
        String[] split = session.split("\t");
        System.out.println();
    }

}
