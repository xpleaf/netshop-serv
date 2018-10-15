package cn.xpleaf.netshop.serv.java.analysis.dao;

import cn.xpleaf.netshop.serv.java.analysis.domain.Task;
import com.google.gson.Gson;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author xpleaf
 * @date 2018/10/15 下午2:04
 */
public class TaskDaoImplTest {

    private ITaskDao taskDao;
    private Gson gson;

    @Before
    public void init() throws Exception {
        taskDao = new TaskDaoImpl();
        gson = new Gson();
    }

    // 测试getTaskById
    @Test
    public void test01() {
        Task task = taskDao.getTaskById(1);
        System.out.println(task);
        String taskParam = task.getTask_param();
        Map param = gson.fromJson(taskParam, Map.class);
        System.out.println(param);
    }
}