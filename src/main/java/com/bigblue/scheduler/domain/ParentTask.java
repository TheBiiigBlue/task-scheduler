package com.bigblue.scheduler.domain;

import com.bigblue.scheduler.base.enums.TaskStatus;
import com.bigblue.scheduler.service.TaskListener;
import lombok.Builder;
import lombok.Data;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @Author: TheBigBlue
 * @Description:
 * @Date: 2020/6/11
 */
@Data
@Builder
public class ParentTask {
    private String id;
    private Map<String, NodeTask> nodeTasks;
    //成功结束的NodeTask个数
    private AtomicInteger nodeTaskSuccCnt;
    private volatile TaskStatus parentTaskStatus;
    private TaskListener taskListener;

    /**
     * @Author: TheBigBlue
     * @Description: 计算运行度
     * @Date: 2020/6/12
     * @Param:
     * @return:
     **/
    public double getProgress() {
        double fraction = (double) nodeTaskSuccCnt.get() / nodeTasks.size();
        return (double) Math.round(fraction * 100) / 100;
    }

    /**
     * @Author: TheBigBlue
     * @Description: 获取各Task状态
     * @Date: 2020/6/13
     * @Return:
     **/
    public List<TaskResult> getTasksStatus() {
        Collection<NodeTask> values = nodeTasks.values();
        return values.stream().map(nodeTask ->
                new TaskResult(nodeTask.getTaskId(), nodeTask.getTaskStatus(), nodeTask.getTakeTime()))
                .collect(Collectors.toList());
    }

    public NodeTask getNodeTask(String nodeTaskId) {
        return nodeTasks.get(nodeTaskId);
    }

    public int nodeTaskSuccess() {
        return nodeTaskSuccCnt.addAndGet(1);
    }

    public void nodeTaskFail() {
        this.parentTaskStatus = TaskStatus.fail;
    }

    public boolean isFail() {
        return parentTaskStatus == TaskStatus.fail;
    }

    /**
     * @Author: TheBigBlue
     * @Description: Task是否失败或完成
     * @Date: 2020/6/12
     * @Param:
     * @return:
     **/
    public boolean isFailOrFinish() {
        return nodeTaskSuccCnt.get() == nodeTasks.size() || parentTaskStatus == TaskStatus.fail;
    }

}
