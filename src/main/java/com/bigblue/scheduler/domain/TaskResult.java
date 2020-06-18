package com.bigblue.scheduler.domain;

import com.bigblue.scheduler.base.enums.TaskStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @Author: TheBigBlue
 * @Description: Task 执行结果
 * @Date: 2020/6/12
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TaskResult {
    //Task唯一标识
    private String id;
    //执行状态
    private TaskStatus status;
    //任务耗时
    private long takeTime;
}
