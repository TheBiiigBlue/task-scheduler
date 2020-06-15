package com.bigblue.scheduler.domain.json;

import lombok.Data;

/**
 * @Author: TheBigBlue
 * @Description:
 * @Date: 2020/6/15
 */
@Data
public class JsonEdge {
    private String id;
    private int index;
    private String source;
    private String target;
    private int sourceAnchor;
    private int targetAnchor;
}
