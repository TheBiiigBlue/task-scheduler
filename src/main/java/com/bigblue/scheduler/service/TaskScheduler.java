package com.bigblue.scheduler.service;

import com.bigblue.scheduler.base.enums.TaskStatus;
import com.bigblue.scheduler.domain.NodeTask;

import java.util.Map;

/**
 * @Author: TheBigBlue
 * @Description: 任务调度器
 * @Date: 2020/6/13
 */
public interface TaskScheduler {

    /**
     * @Author: TheBigBlue
     * @Description: 组装Tasks
     * @Date: 2020/6/13
     * @Param nodeTasks:
     * @Return:
     **/
    void startNodeTasks(Map<String, NodeTask> nodeTasks) throws RuntimeException;

    /**
     * @Author: TheBigBlue
     * @Description: 组装Tasks
     * @Date: 2020/6/13
     * @Param nodeTasks:
     * @Return:
     **/
    void startNodeTasks(Map<String, NodeTask> nodeTasks, TaskListener statusListener) throws RuntimeException;

    /**
     * @Author: TheBigBlue
     * @Description: 开始调度
     * @Date: 2020/6/13
     * @Param taskId:
     * @Return:
     **/
    void startTaskSchedule(String taskId);

    /**
     * @Author: TheBigBlue
     * @Description: 取消调度
     * @Date: 2020/6/13
     * @Param taskId:
     * @Param taskStatus:
     * @Return:
     **/
    void cancelTaskSchedule(String taskId, TaskStatus taskStatus);
}
