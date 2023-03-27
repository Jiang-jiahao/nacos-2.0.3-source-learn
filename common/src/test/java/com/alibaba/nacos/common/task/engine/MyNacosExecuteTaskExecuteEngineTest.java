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

import com.alibaba.nacos.api.exception.NacosException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class MyNacosExecuteTaskExecuteEngineTest {

    private NacosExecuteTaskExecuteEngine executeTaskExecuteEngine;

    @Before
    public void setUp() {
        executeTaskExecuteEngine = new NacosExecuteTaskExecuteEngine("TaskExecuteEngine", null);
    }

    @After
    public void tearDown() throws NacosException {
        executeTaskExecuteEngine.shutdown();
    }


    @Test
    public void testAddTask() throws InterruptedException {
        // 直接添加
        executeTaskExecuteEngine.addTask("myTest", new MyExecuteTask());

        // 用对应的处理器执行
        executeTaskExecuteEngine.addProcessor(MyExecuteTask.class, new MyNacosTaskProcessor());
        executeTaskExecuteEngine.addTask(MyExecuteTask.class, new MyExecuteTask());
        Thread.sleep(1000);
    }
}
