package com.bigblue.scheduler.test;

import com.bigblue.scheduler.base.utils.GuavaUtils;
import com.bigblue.scheduler.domain.NodeTask;
import com.bigblue.scheduler.domain.json.JsonContent;
import com.bigblue.scheduler.manager.TaskManager;
import com.bigblue.scheduler.service.TaskScheduler;
import com.bigblue.scheduler.service.impl.SimpleTaskListener;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @Author: TheBigBlue
 * @Description:
 * @Date: 2020/6/11
 */
@RestController
public class TaskScheduleTests {

    @Autowired
    private TaskScheduler taskScheduler;
    @Autowired
    private TaskManager taskManager;

    @GetMapping("/test1")
    public void test1() {
        //解析报文，生成Task
        Map<String, NodeTask> nodeTaskMap = dataParse1();
        //调度
        String jobId = UUID.randomUUID().toString().replace("-", "").substring(0, 7);
        taskScheduler.startNodeTasks(jobId, nodeTaskMap, new SimpleTaskListener());
    }

    @GetMapping("/test2")
    public void test2() {
        //解析报文，生成Task
        Map<String, NodeTask> nodeTaskMap = dataParse2();
        //调度
        String jobId = UUID.randomUUID().toString().replace("-", "").substring(0, 7);
        taskScheduler.startNodeTasks(jobId, nodeTaskMap, new SimpleTaskListener());
    }

    @PostMapping("/invoke")
    public Object invoke(@RequestBody JsonContent jsonContent) {
        Map<String, Object> map = new HashMap<>();
        try {
            String jobId = taskScheduler.parseTasksAndSchedule(jsonContent);
            map.put("message", "success");
            map.put("jobId", jobId);
        }catch (Exception e) {
            map.put("message", "fail");
            map.put("messageInfo", e.getMessage());
        }
        return map;
    }

    @GetMapping("/progress")
    public Object getProgress(String jobId) {
        Map<String, Object> resultMap = (Map<String, Object>) GuavaUtils.get(jobId);
        return resultMap == null ? new HashMap<>() : resultMap;
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
        Map<String, NodeTask> nodeTaskMap = dataParse();
        NodeTask nodeTaskF = new MyNodeTask(3_000, "123", "nodeF", null);
        NodeTask nodeTaskG = new MyNodeTask(2_000, "123", "nodeG", null);
        nodeTaskMap.put(nodeTaskF.getTaskId(), nodeTaskF);
        nodeTaskMap.put(nodeTaskG.getTaskId(), nodeTaskG);
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
        Map<String, NodeTask> nodeTaskMap = dataParse();
        NodeTask nodeTaskF = new MyNodeTask(3_000, "123", "nodeF", Sets.newHashSet(nodeTaskMap.get("nodeE").getTaskId()));
        NodeTask nodeTaskG = new MyNodeTask(2_000, "123", "nodeG", Sets.newHashSet(nodeTaskMap.get("nodeE").getTaskId()));
        nodeTaskMap.put(nodeTaskF.getTaskId(), nodeTaskF);
        nodeTaskMap.put(nodeTaskG.getTaskId(), nodeTaskG);
        return nodeTaskMap;
    }

    private Map<String, NodeTask> dataParse() {
        NodeTask nodeTaskA = new MyNodeTask(3_000, "123", "nodeA", null);
        NodeTask nodeTaskB = new MyNodeTask(4_000, "123", "nodeB", null);
        NodeTask nodeTaskC = new MyNodeTask(2_000, "123", "nodeC", Sets.newHashSet(nodeTaskA.getTaskId()));
        NodeTask nodeTaskD = new MyNodeTask(3_000, "123", "nodeD", Sets.newHashSet(nodeTaskB.getTaskId()));
        NodeTask nodeTaskE = new MyNodeTask(5_000, "123", "nodeE", Sets.newHashSet(nodeTaskC.getTaskId(), nodeTaskD.getTaskId()));

        Map<String, NodeTask> nodeTaskMap = Maps.newConcurrentMap();
        nodeTaskMap.put(nodeTaskA.getTaskId(), nodeTaskA);
        nodeTaskMap.put(nodeTaskB.getTaskId(), nodeTaskB);
        nodeTaskMap.put(nodeTaskC.getTaskId(), nodeTaskC);
        nodeTaskMap.put(nodeTaskD.getTaskId(), nodeTaskD);
        nodeTaskMap.put(nodeTaskE.getTaskId(), nodeTaskE);
        return nodeTaskMap;
    }
}
