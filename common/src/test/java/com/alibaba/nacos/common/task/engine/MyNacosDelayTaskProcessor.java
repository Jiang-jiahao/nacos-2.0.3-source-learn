package com.alibaba.nacos.common.task.engine;

import com.alibaba.nacos.common.task.NacosTask;
import com.alibaba.nacos.common.task.NacosTaskProcessor;

public class MyNacosDelayTaskProcessor implements NacosTaskProcessor {

    @Override
    public boolean process(NacosTask task) {
        System.out.println("MyNacosDelayTaskProcessor start...");
        MyDelayTask delayTask = (MyDelayTask) task;
        System.out.println("name=" + delayTask.getName() + "；number=" + delayTask.getNumber());
        System.out.println("MyNacosDelayTaskProcessor end...");
        return true;
    }
}
