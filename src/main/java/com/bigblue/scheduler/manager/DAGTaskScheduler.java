package com.bigblue.scheduler.manager;

import com.bigblue.scheduler.base.enums.TaskStatus;
import com.bigblue.scheduler.base.log.SchedulerLogger;
import com.bigblue.scheduler.base.utils.GuavaUtils;
import com.bigblue.scheduler.domain.NodeTask;
import com.bigblue.scheduler.domain.ParentTask;
import com.bigblue.scheduler.domain.json.JsonContent;
import com.bigblue.scheduler.service.TaskListener;
import com.bigblue.scheduler.service.TaskScheduler;
import com.bigblue.scheduler.service.impl.SimpleTaskListener;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DAG任务调度器
 * <p>
 * 任务调度思路概述：
 * 1.将所有的NodeTask封装为一个ParentTask，形成DAG有向无环图 ，每个ParentTask起一个线程调度；
 * 2.首先调度没有依赖的NodeTask，通过线程池提交任务，异步获取运行结果
 * 3.NodeTask运行完成，通过异步回调，并修改NodeTask状态
 * 4.ParentTask 继续调度下面的组件
 * 5.当该NodeTask所依赖的其他NodeTask都执行完成后，就可以被调度，并发调度
 * 6.当任一NodeTask执行失败，经过异步回调返回失败，并通知ParentTask调度线程终止调度，并修改任务状态
 * <p>
 * TODO 待完善功能：
 * 1.将父节点结果数据给子节点
 * 2.植入数据库
 *
 * @Author: TheBigBlue
 * @Description:
 * @Date: 2020/6/11
 */
@Component("dagTaskScheduler")
@Order(1002)
public class DAGTaskScheduler implements TaskScheduler {

    @Autowired
    private TaskManager taskManager;
    @Autowired
    private TaskParser taskParser;
    @Autowired
    private SchedulerLogger logger;

    /**
     * 任务调度器线程Map （每个ParentTask 对应一个Thread）
     */
    private Map<String, Thread> taskScheduleThreadMap = Maps.newConcurrentMap();

    /**
     * 可回调线程池
     */
    private ListeningExecutorService pool = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors()));

    @Override
    public String parseTasksAndSchedule(JsonContent jsonContent) {
        Map<String, NodeTask> nodeTasks = taskParser.parseNodeTasks(jsonContent);
        return startNodeTasks(jsonContent.getJobId(), nodeTasks, new SimpleTaskListener());
    }

    /**
     * 当前所有 NodeTasks 会被当成一个整体进行调度（形成一个 有向无环图 tasks）
     *
     * @param nodeTasks      所有的tasks
     * @param statusListener 用于监听任务的状态
     */
    @Override
    public String startNodeTasks(String jobId, Map<String, NodeTask> nodeTasks, TaskListener statusListener) {
        //分配partentTask
        //创建partentTask
        ParentTask parentTask = ParentTask.builder()
                .id(jobId)
                .nodeTasks(nodeTasks)
                .nodeTaskSuccCnt(new AtomicInteger(0))
                .taskListener(statusListener).build();
        //启动partentTask
        this.startParentTask(parentTask);
        return jobId;
    }

    /**
     * 开启线程，调度ParentTask
     *
     * @param parentTask
     */
    private void startParentTask(ParentTask parentTask) {
        String jobId = parentTask.getId();
        if (taskScheduleThreadMap.get(jobId) == null) {
            synchronized (taskScheduleThreadMap) {
                //taskManager添加task
                taskManager.addTask(parentTask);
                //起一个线程调度parentTask
                Thread scheduleThread = new Thread(() -> {
                    this.startTaskSchedule(jobId);
                });
                //维护threadmap
                taskScheduleThreadMap.put(jobId, scheduleThread);
                scheduleThread.start();
                logger.getLogger(jobId).info("partentTask started! jobId: {}", jobId);
            }
        } else {
            throw new RuntimeException("duplicate start parentTask:" + jobId);
        }
    }

    /**
     * 取消 ParentTask 调度
     *
     * @param jobId
     */
    @Override
    public void cancelTaskSchedule(String jobId, TaskStatus taskStatus) {
        try {
            // 可能两个NodeTask同时失败，同时取消
            synchronized (taskScheduleThreadMap) {
                if (taskScheduleThreadMap.get(jobId) != null) {
                    //更新taskManager任务状态并移除
                    taskManager.updateParentTaskStatus(jobId, taskStatus);
                    taskManager.removeTask(jobId);
                    //中断调度线程
                    taskScheduleThreadMap.get(jobId).interrupt();
                    taskScheduleThreadMap.remove(jobId);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**************************下面开始调度逻辑*****************************/

    /**
     * 开启任务调度
     * 对于每个 parentTask，都需要新启一个独立的Thread去调度；
     *
     * @param jobId
     */
    @Override
    public void startTaskSchedule(String jobId) {
        //运行没有依赖的task
        runNoDependentNodeTasks(jobId);
        while (true) {
            try {
                ParentTask parentTask = taskManager.getParentTask(jobId);
                //校验是否可调度：判断依赖节点是否已执行
                if (parentTask.isFailOrFinish()) {
                    logger.getLogger(jobId).info("nodeTask schedule finish or fail, jobId: {}", jobId);
                    break;
                }
                //获取准备调度的Task
                List<NodeTask> nodeTasksToBeScheduled = taskManager.nodeTasksToBeScheduled(parentTask);
                if (CollectionUtils.isEmpty(nodeTasksToBeScheduled)) {
                    //等待最后一个Task完成
                    while (!parentTask.isFailOrFinish()) {
                    }
                    //最后一个Task完成，退出
                    break;
                }
                //调度Task
                for (NodeTask nodeTask : nodeTasksToBeScheduled) {
                    //可调度，提交任务
                    if (taskManager.canNodeTaskSchedule(jobId, nodeTask.getTaskId())) {
                        submitTask(jobId, nodeTask);
                    }
                }
            } catch (Exception e) {
                logger.getLogger(jobId).error("nodeTask schedule fail, jobId: {}", jobId, e);
                break;
            }
        }
        //更新状态
        ParentTask parentTask = taskManager.getParentTask(jobId);
        logger.getLogger(jobId).info("jobId: {}, scheduled progress: {}", jobId, parentTask.getProgress());
        Map<String, Object> resultMap = (Map<String, Object>) GuavaUtils.get(jobId);
        TaskStatus parentTaskStatus;
        if (parentTask.isFail()) {
            parentTaskStatus = TaskStatus.fail;
            logger.getLogger(jobId).error("jobId: {}, scheduled fail, thread exit", jobId);
        } else {
            parentTaskStatus = TaskStatus.success;
            logger.getLogger(jobId).info("jobId: {}, scheduled success, thread exit", jobId);
        }
        //更新缓存状态
        if (!CollectionUtils.isEmpty(resultMap)) {
            resultMap.put("jobStatus", parentTaskStatus);
        }
        //终止调度线程
        cancelTaskSchedule(jobId, parentTaskStatus);
        //清除logger
        logger.removeLogger(jobId);
    }

    /**
     * 运行没有依赖的NodeTasks
     *
     * @param jobId
     */
    private void runNoDependentNodeTasks(String jobId) {
        List<NodeTask> noDependentNodeTasks = taskManager.getNoDependentNodeTasks(jobId);
        if (CollectionUtils.isEmpty(noDependentNodeTasks)) {
            throw new RuntimeException("there is no start tasks, nodeTasks may not be DAG");
        }
        noDependentNodeTasks.forEach(nodeTask -> submitTask(jobId, nodeTask));
        taskManager.updateParentTaskStatus(jobId, TaskStatus.running);
    }

    /**
     * 向线程池提交任务
     *
     * @param jobId
     * @param nodeTask
     * @return
     */
    private void submitTask(String jobId, NodeTask nodeTask) {
        String nodeTaskId = nodeTask.getTaskId();
        try {
            //向线程池提交任务
            ListenableFuture future = pool.submit(nodeTask);
            //更新状态
            if (!taskManager.updateTaskStatus(jobId, nodeTaskId, TaskStatus.running)) {
                //更新失败
                throw new RuntimeException("update nodeTask status fail, jobId: " + jobId + ", nodeTaskId: " + nodeTaskId);
            }
            logger.getLogger(jobId).info("nodeTask has bean submitted successfully, jobId: {}, nodeTaskId: {}", jobId, nodeTask.getTaskId());
            //设置异步回调
            Futures.addCallback(future, new TaskExecCallback(jobId, nodeTaskId, taskManager, this), pool);
        } catch (Exception e) {
            logger.getLogger(jobId).error("nodeTask submit fail, jobId: {}, nodeTaskId: {}", jobId, nodeTaskId, e);
            this.cancelTaskSchedule(jobId, TaskStatus.fail);
        }
    }

}
