package com.alibaba.nacos.common.task.engine;

import com.alibaba.nacos.common.task.AbstractExecuteTask;

public class MyExecuteTask extends AbstractExecuteTask {

    @Override
    public void run() {
        System.out.println("MyExecuteTask...");
    }
}
