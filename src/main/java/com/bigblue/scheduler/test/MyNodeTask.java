package com.bigblue.scheduler.test;

import com.bigblue.scheduler.base.enums.TaskStatus;
import com.bigblue.scheduler.domain.NodeTask;
import com.bigblue.scheduler.domain.TaskResult;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @Author: TheBigBlue
 * @Description:
 * @Date: 2020/6/11
 */
@Data
public class MyNodeTask extends NodeTask {

    private static Logger logger = LoggerFactory.getLogger(MyNodeTask.class);

    private long runTime;

    public MyNodeTask(long runTime, String id, Set<String> dependences) {
        super(id, dependences);
        this.runTime = runTime;
    }

    @Override
    public TaskResult doTask() throws Exception {
//        System.out.println("begin to run MyNodeTask【" + this.getId() + "】, time escape(ms): [" + runTime + "]");
        TimeUnit.MILLISECONDS.sleep(runTime);
        if (this.getId().endsWith("F")) {
            throw new RuntimeException(this.getId() + " exception");
        }
        String msg = "finish MyNodeTask( " + this.getId() + " )";
//        System.out.println(msg);
        Map<String, Object> result = new HashMap<>();
        result.put("msg", msg);
        return new TaskResult(this.getId(), TaskStatus.success, result);
    }
}

