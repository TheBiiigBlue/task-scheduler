package com.bigblue.scheduler.domain;

import com.bigblue.scheduler.base.enums.TaskStatus;
import com.bigblue.scheduler.base.log.SchedulerLogger;
import com.bigblue.scheduler.base.utils.SpringUtil;
import com.bigblue.scheduler.service.INodeTask;
import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * @Author: TheBigBlue
 * @Description: 任务信息，不同类型的任务需各自实现
 * @Date: 2020/6/11
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public abstract class NodeTask implements Callable<Map<String, Object>>, INodeTask {

    private SchedulerLogger logger = SpringUtil.getBean(SchedulerLogger.class);
    private String jobId;
    private String taskId;
    //依赖的nodeTask id
    private Set<String> dependences = Sets.newConcurrentHashSet();
    //当前Task运行状态
    private TaskStatus taskStatus = TaskStatus.init;
    //执行结果
    private Map<String, Object> result;
    //开始时间
    private long startTime;
    //结束时间
    private long endTime;
    //耗时
    private long takeTime;
    //任务类型 TODO
    private String type;
    //task元数据：可以是Json或其他  TODO
    private Map<String, Object> metadata;
    //元数据解析器 TODO
    private String metadataParserKey;
    //最长运行时间, -1:表示无时间限制 TODO
    private long maxRuntimeInSec = -1;

    public NodeTask(String taskId) {
        this.taskId = taskId;
    }

    /**
     * @param taskId      nodeTaskId
     * @param dependences 依赖
     */
    public NodeTask(String jobId, String taskId, Set<String> dependences) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.dependences = dependences;
    }

    public NodeTask(String taskId, Set<String> dependences, String type, Map<String, Object> metadata) {
        this.taskId = taskId;
        this.dependences = dependences;
        this.type = type;
        this.metadata = metadata;
    }

    /**
     * 多线程调度，子类需覆盖此方法
     *
     * @throws Exception
     */
    @Override
    public Map<String, Object> call() throws Exception {
        this.startTime = System.currentTimeMillis();
        logger.getLogger(jobId).info("begin to run task: {}, timestamp: {}", this.taskId, startTime);
        this.result = doTask();
        this.endTime = System.currentTimeMillis();
        this.takeTime = endTime - startTime;
        logger.getLogger(jobId).info("finish task: {}, timestamp: {}, cost time: {}ms", this.taskId, endTime, takeTime);
        return result;
    }
}
