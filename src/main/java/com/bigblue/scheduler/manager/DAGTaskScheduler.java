package com.bigblue.scheduler.manager;

import com.bigblue.scheduler.base.enums.TaskStatus;
import com.bigblue.scheduler.domain.NodeTask;
import com.bigblue.scheduler.domain.ParentTask;
import com.bigblue.scheduler.domain.json.JsonContent;
import com.bigblue.scheduler.service.TaskListener;
import com.bigblue.scheduler.service.TaskScheduler;
import com.bigblue.scheduler.service.impl.SimpleTaskListener;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.ObjectUtils;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DAG任务调度器
 * <p>
 * 任务调度思路概述：
 * 1.将所有的NodeTask封装为一个ParentTask，形成DAG有向无环图 ，每个ParentTask起一个线程调度；
 * 2.首先调度没有依赖的NodeTask，通过线程池提交任务，异步获取运行结果
 * 3.NodeTask运行完成，通过异步回调，向BlockingQueue中put一个Element，并修改NodeTask状态
 * 4.ParentTask调度线程循环监控该BlockingQueue，一旦获取到Element就执行下面NodeTask调度
 * 5.当该NodeTask所依赖的其他NodeTask都执行完成后，就可以被调度，并发调度
 * 6.当任一NodeTask执行失败，经过异步回调返回失败，并通知ParentTask调度线程终止调度，并修改任务状态
 * <p>
 * TODO 待完善功能：
 * 1.将父节点结果数据给子节点
 * 2.解析器，生成NodeTask，并生成跨库之类的全局标志
 * 3.植入数据库
 *
 * @Author: TheBigBlue
 * @Description:
 * @Date: 2020/6/11
 */
@Component("dagTaskScheduler")
@Order(1002)
public class DAGTaskScheduler implements TaskScheduler {

    private static Logger logger = LoggerFactory.getLogger(DAGTaskScheduler.class);

    @Autowired
    private TaskManager taskManager;
    @Autowired
    private TaskParser taskParser;

    /**
     * 任务调度器线程Map （每个ParentTask 对应一个Thread）
     */
    private Map<String, Thread> taskScheduleThreadMap = Maps.newConcurrentMap();

    /**
     * 可回调线程池
     */
    private ListeningExecutorService pool = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors()));

    /**
     * 已完成Task队列，每个ParentTask一个队列
     */
    private Map<String, BlockingQueue<Map<String, Object>>> tasksScheduleQueueMap = Maps.newConcurrentMap();

    @Override
    public String parseTasksAndSchedule(JsonContent jsonContent) {
        Map<String, NodeTask> nodeTasks = taskParser.parseNodeTasks(jsonContent);
        return startNodeTasks(nodeTasks);
    }

    /**
     * 解析项目内容，转化为执行逻辑NodeTasks，并执行
     *
     * @param jobContent
     * @return
     */
    @Override
    public String parseTasksAndSchedule(String jobContent) {
        Map<String, NodeTask> nodeTasks = taskParser.parseNodeTasks(jobContent);
        return startNodeTasks(nodeTasks);
    }

    /**
     * 当前所有 NodeTasks 会被当成一个整体进行调度（形成一个 有向无环图 tasks）
     *
     * @param nodeTasks 所有的tasks
     */
    @Override
    public String startNodeTasks(Map<String, NodeTask> nodeTasks) {
        return startNodeTasks(nodeTasks, new SimpleTaskListener());
    }

    /**
     * 当前所有 NodeTasks 会被当成一个整体进行调度（形成一个 有向无环图 tasks）
     *
     * @param nodeTasks      所有的tasks
     * @param statusListener 用于监听任务的状态
     */
    @Override
    public String startNodeTasks(Map<String, NodeTask> nodeTasks, TaskListener statusListener) {
        //分配partentTask
        String parentTaskId = UUID.randomUUID().toString().replace("-", "").substring(0, 7);
        //创建partentTask
        ParentTask parentTask = ParentTask.builder()
                .id(parentTaskId)
                .nodeTasks(nodeTasks)
                .nodeTaskSuccCnt(new AtomicInteger(0))
                .taskListener(statusListener).build();
        //启动partentTask
        this.startParentTask(parentTask);
        return parentTaskId;
    }

    /**
     * 开启线程，调度ParentTask
     *
     * @param parentTask
     */
    private void startParentTask(ParentTask parentTask) {
        String parentTaskId = parentTask.getId();
        if (taskScheduleThreadMap.get(parentTaskId) == null) {
            synchronized (taskScheduleThreadMap) {
                //taskManager添加task
                taskManager.addTask(parentTask);
                //起一个线程调度parentTask
                Thread scheduleThread = new Thread(() -> {
                    this.startTaskSchedule(parentTaskId);
                });
                //维护threadmap
                taskScheduleThreadMap.put(parentTaskId, scheduleThread);
                scheduleThread.start();
                logger.info("partentTask started! parentTaskId: {}", parentTaskId);
            }
        } else {
            throw new RuntimeException("duplicate start parentTask:" + parentTaskId);
        }
    }

    /**
     * 取消 ParentTask 调度
     *
     * @param parentTaskId
     */
    @Override
    public void cancelTaskSchedule(String parentTaskId, TaskStatus taskStatus) {
        // 可能两个NodeTask同时失败，同时取消
        synchronized (taskScheduleThreadMap) {
            if (taskScheduleThreadMap.get(parentTaskId) != null) {
                //更新taskManager任务状态并移除
                taskManager.updateParentTaskStatus(parentTaskId, taskStatus);
                taskManager.removeTask(parentTaskId);
                //清空调度队列
                tasksScheduleQueueMap.remove(parentTaskId);
                //中断调度线程
                taskScheduleThreadMap.get(parentTaskId).interrupt();
                taskScheduleThreadMap.remove(parentTaskId);
            }
        }
    }

    /**************************下面开始调度逻辑*****************************/

    /**
     * 开启任务调度
     * 对于每个 parentTask，都需要新启一个独立的Thread去调度；
     *
     * @param parentTaskId
     */
    @Override
    public void startTaskSchedule(String parentTaskId) {
        //初始化调度队列
        initTaskScheduleQueue(parentTaskId);
        //获取初始化后的调度队列
        BlockingQueue<Map<String, Object>> taskScheduleQueue = tasksScheduleQueueMap.get(parentTaskId);
        //运行没有依赖的task
        runNoDependentNodeTasks(parentTaskId);
        while (true) {
            try {
                //若获取到，说明有NodeTask已经执行完成
                taskScheduleQueue.take();
                ParentTask parentTask = taskManager.getParentTask(parentTaskId);
                //校验是否已经运行完毕
                if (parentTask.isFailOrFinish()) {
                    throw new RuntimeException("parentTask has finish [or] any nodeTask exception,parentTaskId: " + parentTaskId);
                }
                //获取准备调度的Task
                List<NodeTask> nodeTasksToBeScheduled = taskManager.nodeTasksToBeScheduled(parentTask);
                if (CollectionUtils.isEmpty(nodeTasksToBeScheduled)) {
                    //等待最后一个Task完成
                    while (!parentTask.isSuccess()) {
                    }
                    //最后一个Task完成，退出
                    break;
                }
                //调度Task
                for (NodeTask nodeTask : nodeTasksToBeScheduled) {
                    //校验是否可调度：判断依赖节点是否已执行
                    if (taskManager.canNodeTaskSchedule(parentTaskId, nodeTask.getId())) {
                        //可调度，提交任务，获取依赖任务的结果，TODO
                        if (parentTask.isFailOrFinish()) {
                            throw new RuntimeException("parentTask has finish [or] any nodeTask exception,parentTaskId: " + parentTaskId);
                        }
                        submitTask(parentTaskId, nodeTask);
                    }
                }
            } catch (Exception e) {
                logger.error("nodeTask schedule fail, parentTaskId: {}", parentTaskId);
                break;
            }
        }
        ParentTask parentTask = taskManager.getParentTask(parentTaskId);
        logger.info("parentTask: {}, scheduled progress: {}", parentTaskId, parentTask.getProgress());
        if (parentTask.isFail()) {
            cancelTaskSchedule(parentTaskId, TaskStatus.fail);
            logger.error("parentTask: {}, scheduled fail, thread exit", parentTaskId);
        } else {
            cancelTaskSchedule(parentTaskId, TaskStatus.success);
            logger.info("parentTask: {}, scheduled success, thread exit", parentTaskId);
        }
    }

    /**
     * 为当前ParentTask初始化BlockingQueue
     *
     * @param parentTaskId
     */
    private void initTaskScheduleQueue(String parentTaskId) {
        if (tasksScheduleQueueMap.get(parentTaskId) == null) {
            synchronized (tasksScheduleQueueMap) {
                if (tasksScheduleQueueMap.get(parentTaskId) == null) {
                    BlockingQueue<Map<String, Object>> queue = Queues.newLinkedBlockingQueue();
                    tasksScheduleQueueMap.put(parentTaskId, queue);
                }
            }
        }
    }

    /**
     * 运行没有依赖的NodeTasks
     *
     * @param parentTaskId
     */
    private void runNoDependentNodeTasks(String parentTaskId) {
        List<NodeTask> noDependentNodeTasks = taskManager.getNoDependentNodeTasks(parentTaskId);
        if (CollectionUtils.isEmpty(noDependentNodeTasks)) {
            throw new RuntimeException("there is no start tasks, nodeTasks may not be DAG");
        }
        noDependentNodeTasks.forEach(nodeTask -> submitTask(parentTaskId, nodeTask));
        taskManager.updateParentTaskStatus(parentTaskId, TaskStatus.running);
    }

    /**
     * 向线程池提交任务
     *
     * @param parentTaskId
     * @param nodeTask
     * @return
     */
    private void submitTask(String parentTaskId, NodeTask nodeTask) {
        String nodeTaskId = nodeTask.getId();
        try {
            //向线程池提交任务
            ListenableFuture future = pool.submit(nodeTask);
            //更新状态
            if (!taskManager.updateTaskStatus(parentTaskId, nodeTaskId, TaskStatus.running)) {
                //更新失败
                throw new RuntimeException("update nodeTask status fail, parentTaskId: " + parentTaskId + ", nodeTaskId: " + nodeTaskId);
            }
            logger.info("nodeTask has bean submitted successfully, parentTaskId: {}, nodeTaskId: {}", parentTaskId, nodeTask.getId());
            //设置异步回调
            Futures.addCallback(future, new TaskExecCallback(parentTaskId, nodeTaskId, taskManager, this));
        } catch (Exception e) {
            logger.error("nodeTask submit fail, parentTaskId: {}, nodeTaskId: {}", parentTaskId, nodeTaskId);
            this.cancelTaskSchedule(parentTaskId, TaskStatus.fail);
        }
    }

    /**
     * 运行成功后推入队列
     *
     * @param parentTaskId
     * @param result
     */
    public void addNodeTaskResultToTail(String parentTaskId, Map<String, Object> result) throws InterruptedException {
        if (ObjectUtils.isEmpty(result) || Strings.isNullOrEmpty(parentTaskId)) {
            return;
        }
        BlockingQueue<Map<String, Object>> blockingQueue = tasksScheduleQueueMap.get(parentTaskId);
        if (blockingQueue != null) {
            //插入到队尾
            blockingQueue.put(result);
        }
    }

}
