package com.xxl.mq.broker.rpc;

import com.xxl.mq.client.rpc.netcom.codec.model.RpcRequest;
import com.xxl.mq.client.rpc.netcom.codec.model.RpcResponse;
import com.xxl.mq.client.rpc.util.ZkServiceRegistry;
import net.sf.cglib.reflect.FastClass;
import net.sf.cglib.reflect.FastMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * netcom init
 *
 * @author xuxueli 2015-10-31 22:54:27
 */
public class NetComServerFactory {
    private static final Logger logger = LoggerFactory.getLogger(NetComServerFactory.class);

    // ---------------------- server start ----------------------
    private static int port;
    private static Map<String, Object> regitsryMap;
    private static Executor executor = Executors.newCachedThreadPool();

    public NetComServerFactory(int port, Map<String, Object> serviceMap) throws Exception {
        this.port = port;
        this.regitsryMap = serviceMap;

        // setver start
        new NettyServer().start(port);

        // 开启异步线程，时刻把本地broker地址写到zk
        NetComServerFactory.registry();
    }

    // registry (service) each 120s
    public static void registry() {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    // registry
                    try {
                        ZkServiceRegistry.registerServices(port, regitsryMap.keySet());
                        TimeUnit.SECONDS.sleep(60L);
                    } catch (Exception e) {
                        logger.error("", e);
                    }
                }
            }
        });
    }

    // ---------------------- server invoke ----------------------

    /**
     * init local rpc service map
     */

    public static RpcResponse invokeService(RpcRequest request) {
        Object serviceBean = regitsryMap.get(request.getRegistryKey());

        RpcResponse response = new RpcResponse();
        response.setRequestId(request.getRequestId());

        try {
            Class<?> serviceClass = serviceBean.getClass();
            String methodName = request.getMethodName();
            Class<?>[] parameterTypes = request.getParameterTypes();
            Object[] parameters = request.getParameters();

            /*Method method = serviceClass.getMethod(methodName, parameterTypes);
            method.setAccessible(true);
            return method.invoke(serviceBean, parameters);*/

            FastClass serviceFastClass = FastClass.create(serviceClass);
            FastMethod serviceFastMethod = serviceFastClass.getMethod(methodName, parameterTypes);

            Object result = serviceFastMethod.invoke(serviceBean, parameters);

            response.setResult(result);
        } catch (Throwable t) {
            t.printStackTrace();
            response.setError(t);
        }

        return response;
    }

}
