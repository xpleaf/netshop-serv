package cn.xpleaf.netshop.serv.java.analysis.dao;

import cn.xpleaf.netshop.serv.java.analysis.domain.Task;
import cn.xpleaf.netshop.serv.java.analysis.utils.DatasourceUtil;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

/**
 * @author xpleaf
 * @date 2018/10/15 下午1:59
 */
public class TaskDaoImpl implements ITaskDao {

    private QueryRunner qr = new QueryRunner(DatasourceUtil.getDataSource());

    private Logger logger = LoggerFactory.getLogger(TaskDaoImpl.class);

    @Override
    public Task getTaskById(int task_id) {
        String sql = "SELECT * FROM task WHERE task_id=?";
        try {
            return qr.query(sql, new BeanHandler<Task>(Task.class), task_id);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }

        return null;
    }
}
