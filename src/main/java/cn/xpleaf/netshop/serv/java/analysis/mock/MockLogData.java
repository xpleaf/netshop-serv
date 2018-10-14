package cn.xpleaf.netshop.serv.java.analysis.mock;

import cn.xpleaf.netshop.serv.java.analysis.utils.DateUtils;
import cn.xpleaf.netshop.serv.java.analysis.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

/**
 * @author Leaf
 * @date 2018/9/20 下午9:06
 */
public class MockLogData {

    private static Logger logger = LoggerFactory.getLogger(MockLogData.class);

    public static void main(String[] args) {
        mock();
        System.out.println();
        for (String session : sessionList) {
            // System.out.println(session);
            logger.info(session);
        }
        System.out.println(sessionList.size());
    }

    private static int userNum = 9000;
    private static int sessionTimes = 3;
    private static int actionTimes = 20;

    private static List<String> sessionList = new ArrayList<>();

    // 所以session数据量最多是：userNum * sessionTimes * actionTimes
    // 9000 * 3 * 20，平均一天的session量是12w左右，一个月也就360w

    public static void mock() {
        String[] searchKeywords = {"家用电器", "手机", "数码", "电脑", "办公", "家居",
                                   "家具", "厨具", "美妆", "箱包", "钟表", "珠宝",
                                   "运动", "鞋子", "母婴", "玩具乐器", "酒类"};
        // 拿到今天日期，格式为：yyyy-MM-dd
        String date = DateUtils.getTodayDate();
//        String date = "2018-09-20";
        String[] actions = {"search", "click", "order", "pay"};
        Random random = new Random();

        // 开始生成用户session数据
        for(int i = 0; i < userNum; i++) {
            // 这里的每一次循环，代表一个用户
            // 随机拿到一个用户的id
            long userId = random.nextInt(userNum);
            long cityId = userId % 7;

            for(int j = 0; j < sessionTimes; j++) {
                // ---->这个用户，一天，至少打开了这么多次浏览器
                // 一次循环，就代表用户开始一次session，即重新打开一次浏览器
                // 再看后面加上小时，其实就是用户一个小时内的一次session访问
                String sessionId = UUID.randomUUID().toString().replace("-", "");
                // 在原来的时间基础上加入小时，即格式为：yyyy-MM-dd HH
                String baseActionTime = date + " " + StringUtils.fulfuill(random.nextInt(24) + "");

                Long clickCategoryId = null;

                for(int k = 0; k < random.nextInt(actionTimes); k++) {
                    // ----->每一次执行了这么多的操作
                    // 这里的每一次循环，就代表用户的每一次action
                    long pageId = random.nextInt(10);   // 用于做页面转化率的统计
                    // 精确到分秒的时间
                    String actionTime = baseActionTime +
                            ":" + StringUtils.fulfuill(random.nextInt(60) + "") +   // 随机生成分
                            ":" + StringUtils.fulfuill(random.nextInt(60) + "");    // 随机生成秒

                    String searchKeyword = null;
                    Long clickProductId = null;
                    String orderCategoryIds = null;
                    String orderProductIds = null;
                    String payCategoryIds = null;
                    String payProductIds = null;

                    String action = actions[random.nextInt(4)];
                    if("search".equals(action)) {
                        searchKeyword = searchKeywords[random.nextInt(searchKeywords.length)];
                    } else if("click".equals(action)) {
                        if(clickCategoryId == null) {
                            clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(17))); // 目前有17个类目
                        }
                        clickProductId = Long.valueOf(String.valueOf(random.nextInt(108)));     // 目前有108个商品
                    } else if("order".equals(action)) {
                        orderCategoryIds = String.valueOf(random.nextInt(17));
                        orderProductIds = String.valueOf(random.nextInt(108));
                    } else if("pay".equals(action)) {
                        payCategoryIds = String.valueOf(random.nextInt(17));
                        payProductIds = String.valueOf(random.nextInt(108));
                    }

                    sessionList.add(date + "\t" +
                                    userId + "\t" +
                                    sessionId + "\t" +
                                    pageId + "\t" +
                                    actionTime + "\t" +
                                    searchKeyword + "\t" +
                                    clickCategoryId + "\t" +
                                    clickProductId + "\t" +
                                    orderCategoryIds + "\t" +
                                    orderProductIds + "\t" +
                                    payCategoryIds + "\t" +
                                    payProductIds + "\t" +
                                    cityId);
                }
            }
        }
    }

}
