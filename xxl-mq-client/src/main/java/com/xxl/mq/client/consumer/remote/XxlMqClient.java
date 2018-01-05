package com.xxl.mq.client.consumer.remote;

import com.xxl.mq.client.rpc.netcom.NetComClientProxy;
import com.xxl.mq.client.broker.remote.IXxlMqBroker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * Created by xuxueli on 16/8/30.
 */
public class XxlMqClient {
    private final static Logger logger = LoggerFactory.getLogger(XxlMqClient.class);

    private static IXxlMqBroker iXxlMqBroker;
    public static IXxlMqBroker getiXxlMqBroker() {
        if (iXxlMqBroker !=null) {
            return iXxlMqBroker;
        }
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            iXxlMqBroker = (IXxlMqBroker) new NetComClientProxy(IXxlMqBroker.class, 1000 * 5, null).getObject();
        } catch (Exception e) {
            logger.error("", e);
        } finally {
            countDownLatch.countDown();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("", e);
        }
        return iXxlMqBroker;
    }

}
