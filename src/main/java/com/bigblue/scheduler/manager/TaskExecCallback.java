package com.bigblue.scheduler.manager;

import com.alibaba.fastjson.JSONObject;
import com.bigblue.scheduler.base.enums.TaskStatus;
import com.bigblue.scheduler.base.log.SchedulerLogger;
import com.bigblue.scheduler.base.utils.SpringUtil;
import com.bigblue.scheduler.domain.ParentTask;
import com.google.common.util.concurrent.FutureCallback;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @Author: TheBigBlue
 * @Description:
 * @Date: 2020/6/11
 */
@Getter
@Setter
@NoArgsConstructor
public class TaskExecCallback implements FutureCallback<Map<String, Object>> {

    private static SchedulerLogger logger = SpringUtil.getBean(SchedulerLogger.class);

    private String jobId;
    private String nodeTaskId;

    private TaskManager taskManager;
    private DAGTaskScheduler taskScheduler;

    public TaskExecCallback(String jobId, String nodeTaskId, TaskManager taskManager, DAGTaskScheduler taskScheduler) {
        this.jobId = jobId;
        this.nodeTaskId = nodeTaskId;
        this.taskManager = taskManager;
        this.taskScheduler = taskScheduler;
    }

    @Override
    public void onSuccess(Map<String, Object> result) throws RuntimeException {
        try {
            //更新任务状态
            ParentTask parentTask = taskManager.getParentTask(jobId);
            if (parentTask == null) {
                logger.getLogger(jobId).warn("parentTask has finish [or] any nodeTask exception,jobId: {}", jobId);
                return;
            }
            if (!taskManager.updateTaskStatus(jobId, nodeTaskId, TaskStatus.success)) {
                //更新失败
                throw new RuntimeException("update nodeTask status fail, jobId: " + jobId + ", nodeTaskId: " + nodeTaskId);
            }
            //执行监听器
            parentTask.getTaskListener().process(nodeTaskId, parentTask, result);
            logger.getLogger(jobId).info("nodeTasks status, jobId: {}, status: {}", jobId, JSONObject.toJSONString(parentTask.getTasksStatus()));
        } catch (Exception e) {
            this.onFailure(e);
        }
    }

    @Override
    public void onFailure(Throwable t) throws RuntimeException {
        //触发任务状态监听器
        ParentTask parentTask = taskManager.getParentTask(jobId);
        parentTask.getTaskListener().onFail(nodeTaskId, parentTask, t);
        logger.getLogger(jobId).error("nodeTask exec fail, jobId: {}, nodeTaskId: {},  status: {}, exception: {}",
                jobId, nodeTaskId, JSONObject.toJSONString(parentTask.getTasksStatus()), t.getMessage());
        taskManager.updateTaskStatus(jobId, nodeTaskId, TaskStatus.fail);
    }
}

