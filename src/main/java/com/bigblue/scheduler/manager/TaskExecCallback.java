package com.bigblue.scheduler.manager;

import com.alibaba.fastjson.JSONObject;
import com.bigblue.scheduler.base.enums.TaskStatus;
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

    private static Logger logger = LoggerFactory.getLogger(TaskExecCallback.class);

    private String parentTaskId;
    private String nodeTaskId;

    private TaskManager taskManager;
    private DAGTaskScheduler taskScheduler;

    public TaskExecCallback(String parentTaskId, String nodeTaskId, TaskManager taskManager, DAGTaskScheduler taskScheduler) {
        this.parentTaskId = parentTaskId;
        this.nodeTaskId = nodeTaskId;
        this.taskManager = taskManager;
        this.taskScheduler = taskScheduler;
    }

    @Override
    public void onSuccess(Map<String, Object> result) throws RuntimeException {
        try {
            //更新任务状态
            ParentTask parentTask = taskManager.getParentTask(parentTaskId);
            if (parentTask == null) {
                logger.warn("parentTask has finish [or] any nodeTask exception,parentTaskId: {}", parentTaskId);
                return;
            }
            if (!taskManager.updateTaskStatus(parentTaskId, nodeTaskId, TaskStatus.success)) {
                //更新失败
                throw new RuntimeException("update nodeTask status fail, parentTaskId: " + parentTaskId + ", nodeTaskId: " + nodeTaskId);
            }
            //执行监听器
            parentTask.getTaskListener().process(nodeTaskId, parentTask, result);
            // 添加执行结果到 BlockingQueue
            taskScheduler.addNodeTaskResultToTail(parentTaskId, result);
            logger.info("nodeTasks status, parentTaskId: {}, status: {}", parentTaskId, JSONObject.toJSONString(parentTask.getTasksStatus()));
        } catch (Exception e) {
            this.onFailure(e);
        }
    }

    @Override
    public void onFailure(Throwable t) throws RuntimeException {
        //触发任务状态监听器
        ParentTask parentTask = taskManager.getParentTask(parentTaskId);
        parentTask.getTaskListener().onFail(nodeTaskId, parentTask, t);
        logger.error("nodeTask exec fail, parentTaskId: {}, nodeTaskId: {},  status: {}, exception: {}",
                parentTaskId, nodeTaskId, JSONObject.toJSONString(parentTask.getTasksStatus()), t.getMessage());
        taskManager.updateTaskStatus(parentTaskId, nodeTaskId, TaskStatus.fail);
    }
}

