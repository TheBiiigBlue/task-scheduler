package com.bigblue.scheduler.domain.json;

import lombok.Data;

import java.util.List;

/**
 * @Author: TheBigBlue
 * @Description:
 * @Date: 2020/6/15
 */
@Data
public class JsonContent {

    private List<JsonEdge> edges;
    private List<JsonNode> nodes;
    private String jobId;
}
