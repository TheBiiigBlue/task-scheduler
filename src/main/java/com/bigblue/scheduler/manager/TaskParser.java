package com.bigblue.scheduler.manager;

import com.bigblue.scheduler.domain.NodeTask;
import com.bigblue.scheduler.domain.json.JsonContent;
import com.bigblue.scheduler.domain.json.JsonEdge;
import com.bigblue.scheduler.domain.json.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @Author: TheBigBlue
 * @Description:
 * @Date: 2020/6/15
 */
@Component
@Order(1002)
public class TaskParser {

    private static Logger logger = LoggerFactory.getLogger(TaskParser.class);

    /**
     * @Author: TheBigBlue
     * @Description: 解析项目内容，转化为执行逻辑NodeTasks
     * @Date: 2020/6/15
     * @Param jobContent:
     * @Return: java.util.Map<java.lang.String, com.bigblue.scheduler.domain.NodeTask>
     **/
    public Map<String, NodeTask> parseNodeTasks(JsonContent jsonContent) {
        List<JsonNode> nodes = jsonContent.getNodes();
        List<JsonEdge> edges = jsonContent.getEdges();
        String jobId = jsonContent.getJobId();
        Map<String, NodeTask> nodeTasks = nodes.stream().map(node -> {
            Set<String> dependencies = getDependencies(edges, node);
            String className = "com.bigblue.scheduler.test.MyNodeTask";
//            String className = node.getType();
            try {
                //TODO 根据type，反射不同的处理类
                Constructor<?> constructor = Class.forName(className)
                        .getConstructor(long.class, String.class, String.class, Set.class);
                return  (NodeTask) constructor.newInstance((int) (1 + Math.random() * 5) * 1000, jobId, node.getId(), dependencies);
            } catch (Exception e) {
                logger.error("no corresponding processing class: {}", className);
                throw new RuntimeException("no corresponding processing class");
            }
        }).collect(Collectors.toMap(NodeTask::getTaskId, v -> v));
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
