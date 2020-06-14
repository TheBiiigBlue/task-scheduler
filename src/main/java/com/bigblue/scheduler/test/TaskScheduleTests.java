package com.bigblue.scheduler.test;

import com.bigblue.scheduler.domain.NodeTask;
import com.bigblue.scheduler.service.TaskScheduler;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * @Author: TheBigBlue
 * @Description:
 * @Date: 2020/6/11
 */
@RestController
public class TaskScheduleTests {

    @Autowired
    private TaskScheduler taskScheduler;

    @GetMapping("/test1")
    public void test1() {
        //解析报文，生成Task
        Map<String, NodeTask> nodeTaskMap = dataParse1();
        //调度
        taskScheduler.startNodeTasks(nodeTaskMap);
    }

    @GetMapping("/test2")
    public void test2() {
        //解析报文，生成Task
        Map<String, NodeTask> nodeTaskMap = dataParse2();
        //调度
        taskScheduler.startNodeTasks(nodeTaskMap);
    }

    /**
     * A   B
     * C   D
     *   E
     * F   G
     *
     * @return
     */
    private Map<String, NodeTask> dataParse1() {
        NodeTask nodeTaskA = new MyNodeTask(3_000, "nodeA", null);
        NodeTask nodeTaskB = new MyNodeTask(4_000, "nodeB", null);
        NodeTask nodeTaskC = new MyNodeTask(2_000, "nodeC", Sets.newHashSet(nodeTaskA.getId()));
        NodeTask nodeTaskD = new MyNodeTask(3_000, "nodeD", Sets.newHashSet(nodeTaskB.getId()));
        NodeTask nodeTaskE = new MyNodeTask(5_000, "nodeE", Sets.newHashSet(nodeTaskC.getId(), nodeTaskD.getId()));
        NodeTask nodeTaskF = new MyNodeTask(3_000, "nodeF", Sets.newHashSet(nodeTaskE.getId()));
//        NodeTask nodeTaskF = new MyNodeTask(3_000, "nodeF", Sets.newHashSet());
        NodeTask nodeTaskG = new MyNodeTask(2_000, "nodeG", Sets.newHashSet(nodeTaskE.getId()));
//        NodeTask nodeTaskG = new MyNodeTask(2_000, "nodeG", Sets.newHashSet());

        Map<String, NodeTask> nodeTaskMap = Maps.newConcurrentMap();
        nodeTaskMap.put(nodeTaskA.getId(), nodeTaskA);
        nodeTaskMap.put(nodeTaskB.getId(), nodeTaskB);
        nodeTaskMap.put(nodeTaskC.getId(), nodeTaskC);
        nodeTaskMap.put(nodeTaskD.getId(), nodeTaskD);
        nodeTaskMap.put(nodeTaskE.getId(), nodeTaskE);
        nodeTaskMap.put(nodeTaskF.getId(), nodeTaskF);
        nodeTaskMap.put(nodeTaskG.getId(), nodeTaskG);
        return nodeTaskMap;
    }

    /**
     * A   B   F   G
     * C   D
     *   E
     *
     * @return
     */
    private Map<String, NodeTask> dataParse2() {
        NodeTask nodeTaskA = new MyNodeTask(3_000, "nodeA", null);
        NodeTask nodeTaskB = new MyNodeTask(4_000, "nodeB", null);
        NodeTask nodeTaskC = new MyNodeTask(2_000, "nodeC", Sets.newHashSet(nodeTaskA.getId()));
        NodeTask nodeTaskD = new MyNodeTask(3_000, "nodeD", Sets.newHashSet(nodeTaskB.getId()));
        NodeTask nodeTaskE = new MyNodeTask(5_000, "nodeE", Sets.newHashSet(nodeTaskC.getId(), nodeTaskD.getId()));
        NodeTask nodeTaskF = new MyNodeTask(3_000, "nodeF", Sets.newHashSet());
        NodeTask nodeTaskG = new MyNodeTask(2_000, "nodeG", Sets.newHashSet());

        Map<String, NodeTask> nodeTaskMap = Maps.newConcurrentMap();
        nodeTaskMap.put(nodeTaskA.getId(), nodeTaskA);
        nodeTaskMap.put(nodeTaskB.getId(), nodeTaskB);
        nodeTaskMap.put(nodeTaskC.getId(), nodeTaskC);
        nodeTaskMap.put(nodeTaskD.getId(), nodeTaskD);
        nodeTaskMap.put(nodeTaskE.getId(), nodeTaskE);
        nodeTaskMap.put(nodeTaskF.getId(), nodeTaskF);
        nodeTaskMap.put(nodeTaskG.getId(), nodeTaskG);
        return nodeTaskMap;
    }
}
