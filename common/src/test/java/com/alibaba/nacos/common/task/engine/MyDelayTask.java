package com.alibaba.nacos.common.task.engine;

import com.alibaba.nacos.common.task.AbstractDelayTask;

public class MyDelayTask extends AbstractDelayTask {

    private String name;

    private Integer number;

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Integer getNumber() {
        return number;
    }

    public void setNumber(Integer number) {
        this.number = number;
    }

    @Override
    public void merge(AbstractDelayTask task) {
        MyDelayTask oldMyDelayTask = (MyDelayTask) task;
        // 需要保留旧的number，这里可以进行合并
        this.number = oldMyDelayTask.getNumber();
    }
}
