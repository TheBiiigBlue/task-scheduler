package com.bigblue.scheduler.manager;

import com.alibaba.fastjson.JSONObject;
import com.bigblue.scheduler.domain.NodeTask;
import com.bigblue.scheduler.domain.json.JsonContent;
import com.bigblue.scheduler.domain.json.JsonEdge;
import com.bigblue.scheduler.domain.json.JsonNode;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.lang.reflect.Constructor;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @Author: TheBigBlue
 * @Description:
 * @Date: 2020/6/15
 */
@Component
@Order(1002)
public class TaskParser {

    /**
     * @Author: TheBigBlue
     * @Description: 解析项目内容，转化为执行逻辑NodeTasks
     * @Date: 2020/6/15
     * @Param jobContent:
     * @Return: java.util.Map<java.lang.String, com.bigblue.scheduler.domain.NodeTask>
     **/
    public Map<String, NodeTask> parseNodeTasks(String jobContent) {
        Random random = new Random(5);
        JsonContent jsonContent = JSONObject.parseObject(jobContent, JsonContent.class);
        List<JsonNode> nodes = jsonContent.getNodes();
        List<JsonEdge> edges = jsonContent.getEdges();
        Map<String, NodeTask> nodeTasks = nodes.stream().map(node -> {
            NodeTask nodeTask = null;
            Set<String> dependencies = getDependencies(edges, node);
            try {
                //TODO 根据type，反射不同的处理类
                Constructor<?> constructor = Class.forName("com.bigblue.scheduler.test.MyNodeTask")
                        .getConstructor(long.class, String.class, Set.class);
                random.nextInt();
                nodeTask = (NodeTask) constructor.newInstance((int) (1 + Math.random() * 5) * 1000, node.getId(), dependencies);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return nodeTask;
        }).collect(Collectors.toMap(NodeTask::getId, v -> v));
        return nodeTasks;
    }

    private Set<String> getDependencies(List<JsonEdge> edges, JsonNode node) {
        List<Map<String, Object>> inputs = node.getInputs();
        if (inputs != null && inputs.size() > 0) {
            Set<String> denpendencies = new HashSet<>();
            String id = node.getId();
            inputs.forEach(input -> {
                edges.forEach(edge -> {
                    if (Integer.valueOf((String) input.get("loadOrder")) == edge.getTargetAnchor()
                            && id.equals(edge.getTarget())) {
                        denpendencies.add(edge.getSource());
                    }
                });
            });
            return denpendencies;
        }
        return null;
    }
}
