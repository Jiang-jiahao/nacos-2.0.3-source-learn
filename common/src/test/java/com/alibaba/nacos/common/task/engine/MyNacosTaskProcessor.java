package com.alibaba.nacos.common.task.engine;

import com.alibaba.nacos.common.task.NacosTask;
import com.alibaba.nacos.common.task.NacosTaskProcessor;

public class MyNacosTaskProcessor implements NacosTaskProcessor {

    @Override
    public boolean process(NacosTask task) {
        System.out.println("MyNacosTaskProcessor start...");
        Runnable runnable = (Runnable) task;
        runnable.run();
        System.out.println("MyNacosTaskProcessor end...");
        return false;
    }
}
