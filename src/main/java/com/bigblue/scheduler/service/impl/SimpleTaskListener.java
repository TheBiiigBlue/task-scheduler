package com.bigblue.scheduler.service.impl;

import com.bigblue.scheduler.base.log.SchedulerLogger;
import com.bigblue.scheduler.base.utils.SpringUtil;
import com.bigblue.scheduler.domain.ParentTask;
import com.bigblue.scheduler.service.TaskListener;

import java.util.Map;

/**
 * @Author: TheBigBlue
 * @Description: 监听器实现，默认只是打印
 * @Date: 2020/6/12
 */
public class SimpleTaskListener implements TaskListener {

    private static SchedulerLogger logger = SpringUtil.getBean(SchedulerLogger.class);

    @Override
    public void process(String taskId, ParentTask parentTask, Map<String, Object> result) {
        logger.getLogger(parentTask.getId()).info("nodeTask: [{}] success , 当前进度：{}", taskId, parentTask.getProgress());
    }

    @Override
    public void onFail(String taskId, ParentTask parentTask, Throwable t) {
        logger.getLogger(parentTask.getId()).error("nodeTask: [{}] fail , currentProgress: {}, exception：{}", taskId, parentTask.getProgress(), t.getMessage());
    }
}
