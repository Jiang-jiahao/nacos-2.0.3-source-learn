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

package com.alibaba.nacos.common.notify;


import com.alibaba.nacos.common.notify.listener.Subscriber;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;


@RunWith(MockitoJUnitRunner.class)
public class MyNotifyCenterTest {

    private static class TestEvent extends Event {

    }


    private static class TestSubscribe extends Subscriber {


        @Override
        public void onEvent(Event event) {
            System.out.println(event);
        }

        @Override
        public Class<? extends Event> subscribeType() {
            return TestEvent.class;
        }
    }
    
    static {
        System.setProperty("nacos.core.notify.share-buffer-size", "8");
    }
    
    @Test
    public void test() throws Exception {
        NotifyCenter.registerSubscriber(new TestSubscribe());
        NotifyCenter.publishEvent(new TestEvent());
        NotifyCenter.publishEvent(new TestEvent());
    }
    

}
