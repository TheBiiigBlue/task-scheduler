package com.bigblue.scheduler.test;

import com.bigblue.scheduler.base.utils.GuavaUtils;
import com.bigblue.scheduler.domain.NodeTask;
import com.bigblue.scheduler.domain.ParentTask;
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
        taskScheduler.startNodeTasks(nodeTaskMap, new SimpleTaskListener());
    }

    @GetMapping("/test2")
    public void test2() {
        //解析报文，生成Task
        Map<String, NodeTask> nodeTaskMap = dataParse2();
        //调度
        taskScheduler.startNodeTasks(nodeTaskMap, new SimpleTaskListener());
    }

    @GetMapping("/test3")
    public void test3() {
        String jsonStr = "{\"edges\":[{\"id\":\"A_C\",\"index\":0,\"source\":\"nodeA\",\"target\":\"nodeC\",\"sourceAnchor\":0,\"targetAnchor\":0,\"state_icon_url\":\"/svg/warning.svg\"},{\"id\":\"B_D\",\"index\":1,\"source\":\"nodeB\",\"target\":\"nodeD\",\"sourceAnchor\":0,\"targetAnchor\":0,\"state_icon_url\":\"/svg/warning.svg\"},{\"id\":\"C_E\",\"index\":2,\"source\":\"nodeC\",\"target\":\"nodeE\",\"sourceAnchor\":1,\"targetAnchor\":0,\"state_icon_url\":\"/svg/warning.svg\"},{\"id\":\"D_E\",\"index\":3,\"source\":\"nodeD\",\"target\":\"nodeE\",\"sourceAnchor\":1,\"targetAnchor\":1,\"state_icon_url\":\"/svg/warning.svg\"}],\"nodes\":[{\"x\":569,\"y\":117,\"id\":\"nodeA\",\"size\":\"170*34\",\"type\":\"node\",\"index\":1,\"props\":{\"tblName\":\"aa\",\"tblAlias\":\"aa\"},\"shape\":\"TABLE_READ\",\"inputs\":[],\"outputs\":[{\"ioTyp\":\"dataFrame\",\"loadOrder\":\"0\",\"state_icon_url\":\"/svg/warning.svg\"}],\"state_icon_url\":\"/svg/success.svg\"},{\"x\":569,\"y\":117,\"id\":\"nodeB\",\"size\":\"170*34\",\"type\":\"node\",\"index\":2,\"props\":{\"tblName\":\"bb\",\"tblAlias\":\"bb\"},\"shape\":\"TABLE_READ\",\"inputs\":[],\"outputs\":[{\"ioTyp\":\"dataFrame\",\"loadOrder\":\"0\",\"state_icon_url\":\"/svg/warning.svg\"}],\"state_icon_url\":\"/svg/success.svg\"},{\"x\":393,\"y\":309,\"id\":\"nodeC\",\"size\":\"170*34\",\"type\":\"node\",\"index\":3,\"shape\":\"DATA_SPLIT\",\"inputs\":[{\"ioTyp\":\"dataFrame\",\"loadOrder\":\"0\",\"state_icon_url\":\"/svg/warning.svg\"}],\"outputs\":[{\"ioTyp\":\"dataFrame\",\"loadOrder\":\"1\",\"state_icon_url\":\"/svg/warning.svg\"}],\"state_icon_url\":\"/svg/success.svg\"},{\"x\":393,\"y\":309,\"id\":\"nodeD\",\"size\":\"170*34\",\"type\":\"node\",\"index\":4,\"shape\":\"DATA_SPLIT\",\"inputs\":[{\"ioTyp\":\"dataFrame\",\"loadOrder\":\"0\",\"state_icon_url\":\"/svg/warning.svg\"}],\"outputs\":[{\"ioTyp\":\"dataFrame\",\"loadOrder\":\"1\",\"state_icon_url\":\"/svg/warning.svg\"}],\"state_icon_url\":\"/svg/success.svg\"},{\"x\":393,\"y\":309,\"id\":\"nodeE\",\"size\":\"170*34\",\"type\":\"node\",\"index\":5,\"shape\":\"DATA_SPLIT\",\"inputs\":[{\"ioTyp\":\"dataFrame\",\"loadOrder\":\"0\",\"state_icon_url\":\"/svg/warning.svg\"},{\"ioTyp\":\"dataFrame\",\"loadOrder\":\"1\",\"state_icon_url\":\"/svg/warning.svg\"}],\"outputs\":[],\"state_icon_url\":\"/svg/success.svg\"},{\"x\":393,\"y\":309,\"id\":\"nodeF\",\"size\":\"170*34\",\"type\":\"node\",\"index\":6,\"shape\":\"DATA_SPLIT\",\"inputs\":[],\"outputs\":[],\"state_icon_url\":\"/svg/success.svg\"},{\"x\":393,\"y\":309,\"id\":\"nodeG\",\"size\":\"170*34\",\"type\":\"node\",\"index\":7,\"shape\":\"DATA_SPLIT\",\"inputs\":[],\"outputs\":[],\"state_icon_url\":\"/svg/success.svg\"}],\"prjId\":\"1E9DB9G6R0021F11A8C000009EF302CF\"}";
        taskScheduler.parseTasksAndSchedule(jsonStr);
    }

    @GetMapping("/test4")
    public void test4() {
        String jsonStr = "{\"edges\":[{\"id\":\"A_C\",\"index\":0,\"source\":\"nodeA\",\"target\":\"nodeC\",\"sourceAnchor\":0,\"targetAnchor\":0,\"state_icon_url\":\"/svg/warning.svg\"},{\"id\":\"B_D\",\"index\":1,\"source\":\"nodeB\",\"target\":\"nodeD\",\"sourceAnchor\":0,\"targetAnchor\":0,\"state_icon_url\":\"/svg/warning.svg\"},{\"id\":\"C_E\",\"index\":2,\"source\":\"nodeC\",\"target\":\"nodeE\",\"sourceAnchor\":1,\"targetAnchor\":0,\"state_icon_url\":\"/svg/warning.svg\"},{\"id\":\"D_E\",\"index\":3,\"source\":\"nodeD\",\"target\":\"nodeE\",\"sourceAnchor\":1,\"targetAnchor\":1,\"state_icon_url\":\"/svg/warning.svg\"},{\"id\":\"E_F\",\"index\":4,\"source\":\"nodeE\",\"target\":\"nodeF\",\"sourceAnchor\":2,\"targetAnchor\":0,\"state_icon_url\":\"/svg/warning.svg\"},{\"id\":\"E_G\",\"index\":5,\"source\":\"nodeE\",\"target\":\"nodeG\",\"sourceAnchor\":3,\"targetAnchor\":0,\"state_icon_url\":\"/svg/warning.svg\"}],\"nodes\":[{\"x\":569,\"y\":117,\"id\":\"nodeA\",\"size\":\"170*34\",\"type\":\"node\",\"index\":1,\"props\":{\"tblName\":\"aa\",\"tblAlias\":\"aa\"},\"shape\":\"TABLE_READ\",\"inputs\":[],\"outputs\":[{\"ioTyp\":\"dataFrame\",\"loadOrder\":\"0\",\"state_icon_url\":\"/svg/warning.svg\"}],\"state_icon_url\":\"/svg/success.svg\"},{\"x\":569,\"y\":117,\"id\":\"nodeB\",\"size\":\"170*34\",\"type\":\"node\",\"index\":2,\"props\":{\"tblName\":\"bb\",\"tblAlias\":\"bb\"},\"shape\":\"TABLE_READ\",\"inputs\":[],\"outputs\":[{\"ioTyp\":\"dataFrame\",\"loadOrder\":\"0\",\"state_icon_url\":\"/svg/warning.svg\"}],\"state_icon_url\":\"/svg/success.svg\"},{\"x\":393,\"y\":309,\"id\":\"nodeC\",\"size\":\"170*34\",\"type\":\"node\",\"index\":3,\"shape\":\"DATA_SPLIT\",\"inputs\":[{\"ioTyp\":\"dataFrame\",\"loadOrder\":\"0\",\"state_icon_url\":\"/svg/warning.svg\"}],\"outputs\":[{\"ioTyp\":\"dataFrame\",\"loadOrder\":\"1\",\"state_icon_url\":\"/svg/warning.svg\"}],\"state_icon_url\":\"/svg/success.svg\"},{\"x\":393,\"y\":309,\"id\":\"nodeD\",\"size\":\"170*34\",\"type\":\"node\",\"index\":4,\"shape\":\"DATA_SPLIT\",\"inputs\":[{\"ioTyp\":\"dataFrame\",\"loadOrder\":\"0\",\"state_icon_url\":\"/svg/warning.svg\"}],\"outputs\":[{\"ioTyp\":\"dataFrame\",\"loadOrder\":\"1\",\"state_icon_url\":\"/svg/warning.svg\"}],\"state_icon_url\":\"/svg/success.svg\"},{\"x\":393,\"y\":309,\"id\":\"nodeE\",\"size\":\"170*34\",\"type\":\"node\",\"index\":5,\"shape\":\"DATA_SPLIT\",\"inputs\":[{\"ioTyp\":\"dataFrame\",\"loadOrder\":\"0\",\"state_icon_url\":\"/svg/warning.svg\"},{\"ioTyp\":\"dataFrame\",\"loadOrder\":\"1\",\"state_icon_url\":\"/svg/warning.svg\"}],\"outputs\":[{\"ioTyp\":\"dataFrame\",\"loadOrder\":\"2\",\"state_icon_url\":\"/svg/warning.svg\"},{\"ioTyp\":\"dataFrame\",\"loadOrder\":\"3\",\"state_icon_url\":\"/svg/warning.svg\"}],\"state_icon_url\":\"/svg/success.svg\"},{\"x\":393,\"y\":309,\"id\":\"nodeF\",\"size\":\"170*34\",\"type\":\"node\",\"index\":6,\"shape\":\"DATA_SPLIT\",\"inputs\":[{\"ioTyp\":\"dataFrame\",\"loadOrder\":\"0\",\"state_icon_url\":\"/svg/warning.svg\"}],\"outputs\":[],\"state_icon_url\":\"/svg/success.svg\"},{\"x\":393,\"y\":309,\"id\":\"nodeG\",\"size\":\"170*34\",\"type\":\"node\",\"index\":7,\"shape\":\"DATA_SPLIT\",\"inputs\":[{\"ioTyp\":\"dataFrame\",\"loadOrder\":\"0\",\"state_icon_url\":\"/svg/warning.svg\"}],\"outputs\":[],\"state_icon_url\":\"/svg/success.svg\"}],\"prjId\":\"1E9DB9G6R0021F11A8C000009EF302CF\"}";
        taskScheduler.parseTasksAndSchedule(jsonStr);
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
        NodeTask nodeTaskF = new MyNodeTask(3_000, "nodeF", null);
        NodeTask nodeTaskG = new MyNodeTask(2_000, "nodeG", null);
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
        Map<String, NodeTask> nodeTaskMap = dataParse();
        NodeTask nodeTaskF = new MyNodeTask(3_000, "nodeF", Sets.newHashSet(nodeTaskMap.get("nodeE").getId()));
        NodeTask nodeTaskG = new MyNodeTask(2_000, "nodeG", Sets.newHashSet(nodeTaskMap.get("nodeE").getId()));
        nodeTaskMap.put(nodeTaskF.getId(), nodeTaskF);
        nodeTaskMap.put(nodeTaskG.getId(), nodeTaskG);
        return nodeTaskMap;
    }

    private Map<String, NodeTask> dataParse() {
        NodeTask nodeTaskA = new MyNodeTask(3_000, "nodeA", null);
        NodeTask nodeTaskB = new MyNodeTask(4_000, "nodeB", null);
        NodeTask nodeTaskC = new MyNodeTask(2_000, "nodeC", Sets.newHashSet(nodeTaskA.getId()));
        NodeTask nodeTaskD = new MyNodeTask(3_000, "nodeD", Sets.newHashSet(nodeTaskB.getId()));
        NodeTask nodeTaskE = new MyNodeTask(5_000, "nodeE", Sets.newHashSet(nodeTaskC.getId(), nodeTaskD.getId()));

        Map<String, NodeTask> nodeTaskMap = Maps.newConcurrentMap();
        nodeTaskMap.put(nodeTaskA.getId(), nodeTaskA);
        nodeTaskMap.put(nodeTaskB.getId(), nodeTaskB);
        nodeTaskMap.put(nodeTaskC.getId(), nodeTaskC);
        nodeTaskMap.put(nodeTaskD.getId(), nodeTaskD);
        nodeTaskMap.put(nodeTaskE.getId(), nodeTaskE);
        return nodeTaskMap;
    }
}
