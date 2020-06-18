package com.bigblue.scheduler.service;


import com.bigblue.scheduler.domain.ParentTask;

import java.util.Map;

/**
 * @Author: TheBigBlue
 * @Description: 任务监听器
 * @Date: 2020/6/12
 */
public interface TaskListener {

    /**
     * 当nodeTask执行success后，会触发该方法
     */
    void process(String taskId, ParentTask parentTask, Map<String, Object> result);

    /**
     * 当nodeTask执行fail后，会触发该方法
     */
    void onFail(String taskId, ParentTask parentTask, Throwable t);
}
