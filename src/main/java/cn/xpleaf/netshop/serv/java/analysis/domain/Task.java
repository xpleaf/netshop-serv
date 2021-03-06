package cn.xpleaf.netshop.serv.java.analysis.domain;

import java.util.Date;

/**
 * @author xpleaf
 * @date 2018/10/15 下午1:54
 */
public class Task {

    private int task_id;
    private String task_name;
    private Date create_time;
    private Date start_time;
    private Date finish_time;
    private String task_type;
    private String task_status;
    private String task_param;

    public Task() {
    }

    public int getTask_id() {
        return task_id;
    }

    public void setTask_id(int task_id) {
        this.task_id = task_id;
    }

    public String getTask_name() {
        return task_name;
    }

    public void setTask_name(String task_name) {
        this.task_name = task_name;
    }

    public Date getCreate_time() {
        return create_time;
    }

    public void setCreate_time(Date create_time) {
        this.create_time = create_time;
    }

    public Date getStart_time() {
        return start_time;
    }

    public void setStart_time(Date start_time) {
        this.start_time = start_time;
    }

    public Date getFinish_time() {
        return finish_time;
    }

    public void setFinish_time(Date finish_time) {
        this.finish_time = finish_time;
    }

    public String getTask_type() {
        return task_type;
    }

    public void setTask_type(String task_type) {
        this.task_type = task_type;
    }

    public String getTask_status() {
        return task_status;
    }

    public void setTask_status(String task_status) {
        this.task_status = task_status;
    }

    public String getTask_param() {
        return task_param;
    }

    public void setTask_param(String task_param) {
        this.task_param = task_param;
    }

    @Override
    public String toString() {
        return "Task{" +
                "task_id=" + task_id +
                ", task_name='" + task_name + '\'' +
                ", create_time=" + create_time +
                ", start_time=" + start_time +
                ", finish_time=" + finish_time +
                ", task_type='" + task_type + '\'' +
                ", task_status='" + task_status + '\'' +
                ", task_param='" + task_param + '\'' +
                '}';
    }
}
