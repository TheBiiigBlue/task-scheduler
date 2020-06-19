package com.bigblue.scheduler.service.impl;

import com.bigblue.scheduler.domain.ParentTask;
import com.bigblue.scheduler.manager.TaskExecCallback;
import com.bigblue.scheduler.service.TaskListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @Author: TheBigBlue
 * @Description: 监听器实现，默认只是打印
 * @Date: 2020/6/12
 */
public class SimpleTaskListener implements TaskListener {

    private static Logger logger = LoggerFactory.getLogger(SimpleTaskListener.class);

    @Override
    public void process(String taskId, ParentTask parentTask, Map<String, Object> result) {
        logger.info("nodeTask: [{}] success , 当前进度：{}", taskId, parentTask.getProgress());
    }

    @Override
    public void onFail(String taskId, ParentTask parentTask, Throwable t) {
        logger.error("nodeTask: [{}] fail , currentProgress: {}, exception：{}", taskId, parentTask.getProgress(), t.getMessage());
    }
}
