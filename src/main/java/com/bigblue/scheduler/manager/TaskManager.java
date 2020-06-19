package com.bigblue.scheduler.manager;

import com.bigblue.scheduler.base.enums.TaskStatus;
import com.bigblue.scheduler.base.utils.GuavaUtils;
import com.bigblue.scheduler.domain.NodeTask;
import com.bigblue.scheduler.domain.ParentTask;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Author: TheBigBlue
 * @Description: 任务管理器
 * @Date: 2020/6/11
 */
@Component
@Order(1001)
public class TaskManager {

    private static Logger logger = LoggerFactory.getLogger(TaskManager.class);

    //维护整个任务的状态
    private Map<String, ParentTask> parentTasks = Maps.newConcurrentMap();

    /**
     * 当 ParentTask finish or fail，则不会更新NodeTask的状态
     *
     * @param jobId
     * @param taskStatus
     * @return false: 说明 ParentTask已经失败
     */
    public boolean updateParentTaskStatus(String jobId, TaskStatus taskStatus) {
        if (Strings.isNullOrEmpty(jobId) || taskStatus == null) {
            throw new RuntimeException("updateParentTaskStatus fail: params can not be null");
        }
        ParentTask parentTask = getParentTask(jobId);
        if (parentTask == null) {
            logger.warn("parentTask has finished [or] any nodeTask exception,jobId: {}", jobId);
            return false;
        }
        parentTask.setParentTaskStatus(taskStatus);
        return true;
    }

    /**
     * 当 ParentTask finish or fail，则不会更新NodeTask的状态
     *
     * @param jobId
     * @param nodeTaskId
     * @param taskStatus
     * @return false: 说明 ParentTask已经失败
     */
    public boolean updateTaskStatus(String jobId, String nodeTaskId, TaskStatus taskStatus) {
        if (Strings.isNullOrEmpty(jobId) || Strings.isNullOrEmpty(nodeTaskId) || taskStatus == null) {
            throw new RuntimeException("updateNodeTaskStatus fail: params can not be null");
        }
        ParentTask parentTask = getParentTask(jobId);
        if (parentTask == null) {
            logger.warn("parentTask has finished [or] any nodeTask exception,jobId: {}, nodeTaskId: {}", jobId, nodeTaskId);
            return false;
        }
        try {
            //更新Task状态
            NodeTask nodeTask = parentTask.getNodeTask(nodeTaskId);
            nodeTask.setTaskStatus(taskStatus);
            //更新ParentTask状态
            if (taskStatus == TaskStatus.success) {
                parentTask.nodeTaskSuccess();
            } else if (taskStatus == TaskStatus.fail) {
                parentTask.nodeTaskFail();
            }
            //缓存执行状态和进度  TODO 更新数据库
            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("message", "success");
            resultMap.put("progress", parentTask.getProgress());
            resultMap.put("status", parentTask.getTasksStatus());
            resultMap.put("jobStatus", parentTask.getParentTaskStatus());
            GuavaUtils.put(jobId, resultMap);
        } catch (Exception e) {
            logger.warn("update taskStatus failed， jobId: " + jobId + ", nodeTaskId: " + nodeTaskId, e);
            return false;
        }
        return true;
    }

    /**
     * 如果查询不到(返回NULL)，则说明ParentTask success finish 或 any nodeTask exception
     * 特别说明： 凡是调用此方法，都需要对null进行判断并处理
     *
     * @param jobId
     * @return 可能返回NUlL
     */
    public ParentTask getParentTask(String jobId) {
        if (Strings.isNullOrEmpty(jobId)) {
            throw new RuntimeException("jobId can not be null");
        }
        return parentTasks.get(jobId);
    }

    /**
     * 当ParentTask完成或NodeTask异常，可能返回null
     * 特别说明： 凡是调用此方法，都需要对null进行判断并处理
     *
     * @param jobId
     * @param nodeTaskId
     */
    private NodeTask getNodeTask(String jobId, String nodeTaskId) {
        if (Strings.isNullOrEmpty(jobId) || Strings.isNullOrEmpty(nodeTaskId)) {
            throw new RuntimeException("jobId or nodeTaskId can not be null");
        }
        ParentTask parentTask = getParentTask(jobId);
        if (parentTask == null) {
            throw new RuntimeException("parentTask has finish [or] any nodeTask exception,jobId: " + jobId);
        }
        NodeTask nodeTask = parentTask.getNodeTask(nodeTaskId);
        if (nodeTask == null) {
            throw new RuntimeException("No nodeTask(jobId: " + jobId + ", nodeTaskId: " + nodeTaskId + ")");
        }
        return nodeTask;
    }

    /**
     * 当前任务是否可以进行调度
     *
     * @param jobId
     * @param nodeTaskId
     * @return
     */
    public boolean canNodeTaskSchedule(String jobId, String nodeTaskId) {
        NodeTask nodeTask = getNodeTask(jobId, nodeTaskId);
        if (CollectionUtils.isEmpty(nodeTask.getDependences())) {
            return true;
        }
        // 判断依赖NodeTask是否执行完成
        for (Object dependTaskId : nodeTask.getDependences()) {
            NodeTask dependTask = getNodeTask(jobId, (String) dependTaskId);
            if (dependTask.getTaskStatus() != TaskStatus.success) {
                return false;
            }
        }
        return true;
    }

    /**
     * 所有 {@code nodeState==NodeTaskStatus.init}的任务
     *
     * @param parentTask
     * @return
     */
    public List<NodeTask> nodeTasksToBeScheduled(ParentTask parentTask) {
        List<NodeTask> nodeTasks = Lists.newArrayList();
        //获取可被调度的Task
        for (NodeTask nodeTask : parentTask.getNodeTasks().values()) {
            if (nodeTask.getTaskStatus() == TaskStatus.init) {
                nodeTasks.add(nodeTask);
            }
        }
        return nodeTasks;
    }

    /**
     * 获取没有依赖的NodeTasks
     *
     * @param jobId
     * @return
     */
    public List<NodeTask> getNoDependentNodeTasks(String jobId) {
        List<NodeTask> nodeTasks = Lists.newArrayList();
        ParentTask parentTask = getParentTask(jobId);
        if (parentTask == null) {
            logger.warn("parentTask has finish [or] any nodeTask exception,jobId: {}", jobId);
            return nodeTasks;
        }
        parentTask.getNodeTasks().values().forEach(nodeTask -> {
            if (CollectionUtils.isEmpty(nodeTask.getDependences())) {
                nodeTasks.add(nodeTask);
            }
        });
        return nodeTasks;
    }

    /**
     * 添加一个partentTask
     *
     * @param parentTask
     */
    public void addTask(ParentTask parentTask) {
        // 判断是否重复
        if (parentTasks.get(parentTask.getId()) != null) {
            throw new RuntimeException("parentTask( id: " + parentTask.getId() + ") has exist, please change the parentTask id");
        }
        parentTasks.put(parentTask.getId(), parentTask);
    }

    /**
     * 添加一个partentTask
     */
    public void removeTask(String jobId) {
        parentTasks.remove(jobId);
    }
}
