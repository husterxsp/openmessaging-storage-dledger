/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger;

import com.alibaba.fastjson.JSON;
import com.beust.jcommander.JCommander;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class DLedger {

    private static Logger logger = LoggerFactory.getLogger(DLedger.class);

    public static void main(String args[]) {

        DLedgerConfig dLedgerConfig = new DLedgerConfig();

        // TODO 代码简化pr
        JCommander.newBuilder().addObject(dLedgerConfig).build().parse(args);

        DLedgerServer dLedgerServer = new DLedgerServer(dLedgerConfig);
        dLedgerServer.startup();

        logger.info("[{}] group {} start ok with config {}", dLedgerConfig.getSelfId(), dLedgerConfig.getGroup(), JSON.toJSONString(dLedgerConfig));

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            private volatile boolean hasShutdown = false;
            // TODO Shutdown 调用的次数，addShutdownHook 这里会调用多次吗？
            // rocketmq 也有。https://www.jianshu.com/p/3e025cf69a6a
            private AtomicInteger shutdownTimes = new AtomicInteger(0);

            @Override
            public void run() {
                synchronized (this) {
                    logger.info("Shutdown hook was invoked, {}", this.shutdownTimes.incrementAndGet());
                    if (!this.hasShutdown) {
                        this.hasShutdown = true;
                        long beginTime = System.currentTimeMillis();
                        dLedgerServer.shutdown();
                        long consumingTimeTotal = System.currentTimeMillis() - beginTime;
                        logger.info("Shutdown hook over, consuming total time(ms): {}", consumingTimeTotal);
                    }
                }
            }
        }, "ShutdownHook"));
    }
}
