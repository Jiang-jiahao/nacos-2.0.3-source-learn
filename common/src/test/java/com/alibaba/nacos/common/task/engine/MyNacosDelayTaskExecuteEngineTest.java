/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.common.task.engine;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MyNacosDelayTaskExecuteEngineTest {

    private NacosDelayTaskExecuteEngine nacosDelayTaskExecuteEngine;

    @Before
    public void setUp() throws Exception {
        nacosDelayTaskExecuteEngine = new NacosDelayTaskExecuteEngine(MyNacosDelayTaskExecuteEngineTest.class.getName());
    }

    @After
    public void tearDown() throws Exception {
        nacosDelayTaskExecuteEngine.shutdown();
    }


    @Test
    public void testAddTask() throws InterruptedException {
        nacosDelayTaskExecuteEngine.addProcessor(MyDelayTask.class, new MyNacosDelayTaskProcessor());
        MyDelayTask delayTask = new MyDelayTask();
        // 延迟1秒后执行
        delayTask.setTaskInterval(1000L);
        nacosDelayTaskExecuteEngine.addTask(MyDelayTask.class, delayTask);
        Thread.sleep(10000);
    }


    @Test
    public void testRetryTaskAfterFail() throws InterruptedException {
        // 执行任务失败，则会一直重试执行
        nacosDelayTaskExecuteEngine.addProcessor(MyDelayTask.class, new MyNacosDelayTaskFailProcessor());
        MyDelayTask delayTask = new MyDelayTask();
        // 延迟1秒后执行
        delayTask.setTaskInterval(1000L);
        nacosDelayTaskExecuteEngine.addTask(MyDelayTask.class, delayTask);
        Thread.sleep(10000);
    }


    @Test
    public void testTaskMerge() throws InterruptedException {
        nacosDelayTaskExecuteEngine.addProcessor(MyDelayTask.class, new MyNacosDelayTaskProcessor());
        MyDelayTask delayTaskA = new MyDelayTask();
        // 延迟1秒后执行
        delayTaskA.setTaskInterval(1000L);
        delayTaskA.setName("taskA");
        delayTaskA.setNumber(1);
        MyDelayTask delayTaskB = new MyDelayTask();
        // 延迟2秒后执行
        delayTaskB.setTaskInterval(2000L);
        delayTaskB.setName("taskB");
        delayTaskB.setNumber(2);
        nacosDelayTaskExecuteEngine.addTask(MyDelayTask.class, delayTaskA);
        Thread.sleep(10000); // 中间做一些处理会使merge方法失效
        nacosDelayTaskExecuteEngine.addTask(MyDelayTask.class, delayTaskB);
        Thread.sleep(3000);
    }

    @Test
    public void testTaskLoopMerge() throws InterruptedException {
        nacosDelayTaskExecuteEngine.addProcessor(MyDelayTask.class, new MyNacosDelayTaskFailProcessor());
        MyDelayTask delayTaskA = new MyDelayTask();
        // 延迟1秒后执行
        delayTaskA.setTaskInterval(1000L);
        delayTaskA.setName("taskA");
        delayTaskA.setNumber(1);
        MyDelayTask delayTaskB = new MyDelayTask();
        // 延迟1秒后执行
        delayTaskB.setTaskInterval(1000L);
        delayTaskB.setName("taskB");
        delayTaskB.setNumber(2);
        nacosDelayTaskExecuteEngine.addTask(MyDelayTask.class, delayTaskA);
        Thread.sleep(10000);
        nacosDelayTaskExecuteEngine.addTask(MyDelayTask.class, delayTaskB);
        Thread.sleep(3000);
    }
}
