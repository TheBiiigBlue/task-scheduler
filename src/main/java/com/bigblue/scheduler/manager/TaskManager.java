package com.bigblue.scheduler.manager;

import com.bigblue.scheduler.base.enums.TaskStatus;
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
     * @param parentTaskId
     * @param taskStatus
     * @return false: 说明 ParentTask已经失败
     */
    public boolean updateParentTaskStatus(String parentTaskId, TaskStatus taskStatus) {
        if (Strings.isNullOrEmpty(parentTaskId) || taskStatus == null) {
            throw new RuntimeException("updateParentTaskStatus fail: params can not be null");
        }
        ParentTask parentTask = getParentTask(parentTaskId);
        if (parentTask == null) {
            logger.warn("parentTask has finished [or] any nodeTask exception,parentTaskId: {}", parentTaskId);
            return false;
        }
        parentTask.setParentTaskStatus(taskStatus);
        return true;
    }

    /**
     * 当 ParentTask finish or fail，则不会更新NodeTask的状态
     *
     * @param parentTaskId
     * @param nodeTaskId
     * @param taskStatus
     * @return false: 说明 ParentTask已经失败
     */
    public boolean updateTaskStatus(String parentTaskId, String nodeTaskId, TaskStatus taskStatus) {
        if (Strings.isNullOrEmpty(parentTaskId) || Strings.isNullOrEmpty(nodeTaskId) || taskStatus == null) {
            throw new RuntimeException("updateNodeTaskStatus fail: params can not be null");
        }
        ParentTask parentTask = getParentTask(parentTaskId);
        if (parentTask == null) {
            logger.warn("parentTask has finished [or] any nodeTask exception,parentTaskId: {}, nodeTaskId: {}", parentTaskId, nodeTaskId);
            return false;
        }
        try {
            NodeTask nodeTask = parentTask.getNodeTask(nodeTaskId);
            nodeTask.setTaskStatus(taskStatus);
            if (taskStatus == TaskStatus.success) {
                parentTask.nodeTaskSuccess();
            } else if (taskStatus == TaskStatus.fail) {
                parentTask.nodeTaskFail();
            }
        } catch (Exception e) {
            logger.warn("update taskStatus failed， parentTaskId: " + parentTaskId + ", nodeTaskId: " + nodeTaskId, e);
            return false;
        }
        return true;
    }

    /**
     * 如果查询不到(返回NULL)，则说明ParentTask success finish 或 any nodeTask exception
     * 特别说明： 凡是调用此方法，都需要对null进行判断并处理
     *
     * @param parentTaskId
     * @return 可能返回NUlL
     */
    public ParentTask getParentTask(String parentTaskId) {
        if (Strings.isNullOrEmpty(parentTaskId)) {
            throw new RuntimeException("parentTaskId can not be null");
        }
        return parentTasks.get(parentTaskId);
    }

    /**
     * 当ParentTask完成或NodeTask异常，可能返回null
     * 特别说明： 凡是调用此方法，都需要对null进行判断并处理
     *
     * @param parentTaskId
     * @param nodeTaskId
     */
    private NodeTask getNodeTask(String parentTaskId, String nodeTaskId) {
        if (Strings.isNullOrEmpty(parentTaskId) || Strings.isNullOrEmpty(nodeTaskId)) {
            throw new RuntimeException("parentTaskId or nodeTaskId can not be null");
        }
        ParentTask parentTask = getParentTask(parentTaskId);
        if (parentTask == null) {
            throw new RuntimeException("parentTask has finish [or] any nodeTask exception,parentTaskId: " + parentTaskId);
        }
        NodeTask nodeTask = parentTask.getNodeTask(nodeTaskId);
        if (nodeTask == null) {
            throw new RuntimeException("No nodeTask(parentTaskId: " + parentTaskId + ", nodeTaskId: " + nodeTaskId + ")");
        }
        return nodeTask;
    }

    /**
     * 当前任务是否可以进行调度
     *
     * @param parentTaskId
     * @param nodeTaskId
     * @return
     */
    public boolean canNodeTaskSchedule(String parentTaskId, String nodeTaskId) {
        NodeTask nodeTask = getNodeTask(parentTaskId, nodeTaskId);
        if (CollectionUtils.isEmpty(nodeTask.getDependences())) {
            return true;
        }
        // 判断依赖NodeTask是否执行完成
        for (Object dependTaskId : nodeTask.getDependences()) {
            NodeTask dependTask = getNodeTask(parentTaskId, (String) dependTaskId);
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
     * @param parentTaskId
     * @return
     */
    public List<NodeTask> getNoDependentNodeTasks(String parentTaskId) {
        List<NodeTask> nodeTasks = Lists.newArrayList();
        ParentTask parentTask = getParentTask(parentTaskId);
        if (parentTask == null) {
            logger.warn("parentTask has finish [or] any nodeTask exception,parentTaskId: {}", parentTaskId);
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
            throw new RuntimeException("ParentTask( id: " + parentTask.getId() + ") has exist, please change the parentTask id");
        }
        parentTasks.put(parentTask.getId(), parentTask);
    }

    /**
     * 添加一个partentTask
     */
    public void removeTask(String parentTaskId) {
        parentTasks.remove(parentTaskId);
    }
}
