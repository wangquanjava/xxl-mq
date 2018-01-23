package com.xxl.mq.client.rpc.util;

import com.xxl.mq.client.consumer.annotation.MqConsumer;
import com.xxl.mq.client.consumer.annotation.MqConsumerType;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static com.xxl.mq.client.consumer.annotation.MqConsumerType.QUEUE;
import static com.xxl.mq.client.consumer.annotation.MqConsumerType.SERIAL_QUEUE;
import static com.xxl.mq.client.consumer.annotation.MqConsumerType.TOPIC;

/**
 * zookeeper service registry
 *
 * @author xuxueli 2015-10-29 14:43:46
 */
public class ZkQueueConsumerUtil {
    private static final Logger logger = LoggerFactory.getLogger(ZkQueueConsumerUtil.class);

    // ------------------------------ zookeeper client ------------------------------
    private static ZooKeeper zooKeeper;
    private static ReentrantLock INSTANCE_INIT_LOCK = new ReentrantLock(true);

    private static ZooKeeper getInstance() {
        if (zooKeeper == null) {
            try {
                if (INSTANCE_INIT_LOCK.tryLock(5, TimeUnit.SECONDS)) {
                    try {
                        // init zookeeper
                        /*final CountDownLatch countDownLatch = new CountDownLatch(1);
						countDownLatch.countDown();
						countDownLatch.await();*/
                        zooKeeper = new ZooKeeper(Environment.ZK_ADDRESS, 10000, new Watcher() {
                            @Override
                            public void process(WatchedEvent event) {

                                // session expire, close old and create new
                                if (event.getState() == Event.KeeperState.Expired) {
                                    try {
                                        zooKeeper.close();
                                    } catch (InterruptedException e) {
                                        logger.error("", e);
                                    }
                                    zooKeeper = null;
                                }

                                // refresh service address
                                logger.info("" + event);
                                if ((event.getType() == Event.EventType.NodeChildrenChanged
                                        && event.getPath() != null
                                        && event.getPath().startsWith(Environment.ZK_CONSUMER_PATH)
                                        )
                                        || event.getType() == Event.EventType.None) {
                                    try {
                                        discoverConsumers();
                                    } catch (Exception e) {
                                        logger.error("", e);
                                    }
                                }

                            }
                        });

                        // init base path
                        Stat baseStat = zooKeeper.exists(Environment.ZK_BASE_PATH, false);
                        if (baseStat == null) {
                            zooKeeper.create(Environment.ZK_BASE_PATH, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        }

                        // init consumer path
                        Stat stat = zooKeeper.exists(Environment.ZK_CONSUMER_PATH, false);
                        if (stat == null) {
                            zooKeeper.create(Environment.ZK_CONSUMER_PATH, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        }

                        logger.info(">>>>>>>>> xxl-rpc zookeeper connnect success.");
                    } finally {
                        INSTANCE_INIT_LOCK.unlock();
                    }
                }
            } catch (InterruptedException e) {
                logger.error("", e);
            } catch (IOException e) {
                logger.error("", e);
            } catch (KeeperException e) {
                logger.error("", e);
            }
        }
        if (zooKeeper == null) {
            throw new NullPointerException(">>>>>>>>>>> xxl-rpc, zookeeper connect fail.");
        }
        return zooKeeper;
    }

    // ------------------------------ register service ------------------------------
    public static final String localAddressRandom = IpUtil.getAddress(new Random().nextInt(50000));

    /**
     * register service
     */
    public static void registerConsumers(Set<String> registryKeyList) throws KeeperException, InterruptedException {

        // valid
        if (registryKeyList == null || registryKeyList.size() == 0) {
            return;
        }

        // address
        String address = localAddressRandom;

        // multi registry
        for (String registryKey : registryKeyList) {

            // "register key" path : /xxl-rpc/registry-key
            String registryKeyPath = Environment.ZK_CONSUMER_PATH.concat("/").concat(registryKey);
            Stat registryKeyPathStat = getInstance().exists(registryKeyPath, false);
            if (registryKeyPathStat == null) {
                getInstance().create(registryKeyPath, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }

            // "register key's address" path : /xxl-rpc/registry-key/address
            String registryKeyAddressPath = registryKeyPath.concat("/").concat(address);
            Stat addressStat = getInstance().exists(registryKeyAddressPath, false);
            if (addressStat == null) {
                String path = getInstance().create(registryKeyAddressPath, address.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);    // must be EPHEMERAL
            }
            logger.info(">>>>>>>>>>> xxl-rpc registe consumer item, registryKey:{}, address:{}, registryKeyAddressPath:{}", registryKey, address, registryKeyAddressPath);
        }

    }

    // ------------------------------ private discover service ------------------------------
    private static Executor executor = Executors.newCachedThreadPool();

    static {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        TimeUnit.SECONDS.sleep(60L);
                        discoverConsumers();
                    } catch (Exception e) {
                        logger.error("", e);
                    }

                }
            }
        });
    }

    // "resigtry key's address" path : /xxl-rpc/registry-key/address
    private static volatile ConcurrentMap<String, Set<String>> consumerAddress = new ConcurrentHashMap<String, Set<String>>();

    // only fresh node that used
    private static void discoverConsumers() {
        if (consumerAddress == null || consumerAddress.size() == 0) {
            return;
        }

        try {
            for (String registryKey : consumerAddress.keySet()) {
                Set<String> addressSet = new HashSet<>();

                // "resigtry key" path : /xxl-rpc/registry-key
                String registryKeyPath = Environment.ZK_CONSUMER_PATH.concat("/").concat(registryKey);
                Stat registryKeyPathStat = getInstance().exists(registryKeyPath, true);        // watch "create/delete/setData" of path

                if (registryKeyPathStat != null) {

                    // "resigtry key's address" path : /xxl-rpc/registry-key/address
                    List<String> addressList = getInstance().getChildren(registryKeyPath, true);    // watch "delete" of path and "create/delete" of child path
                    if (addressList != null && addressList.size() > 0) {
                        addressSet.addAll(addressList);
                    }

                }

                consumerAddress.put(registryKey, addressSet);
                logger.info(">>>>>>>>>>> xxl-rpc, discover consumer item, registryKey:{}, addressSet:{}", registryKey, addressSet);
            }
        } catch (Exception e) {
            logger.error("", e);
        }
    }

    // ------------------------------ public util ------------------------------
    public static ActiveInfo isActive(MqConsumer annotation) {
        // info
        String name = annotation.value();
        MqConsumerType type = annotation.type();

        // load address set
        Set<String> addressSet = consumerAddress.get(name);
        if (addressSet == null) {
            // 代表还未初始化过
            consumerAddress.put(name, new HashSet<String>());
            discoverConsumers();
            addressSet = consumerAddress.get(name);
        }
        if (addressSet.size() == 0) {
            // 代表初始化了，但是就是没有数据
            return null;
        }

        // parse rank
        TreeSet<String> sortSet = new TreeSet<String>(addressSet);
        int rank = 0; // 本机地址在所有消费者中的位置
        for (String item : sortSet) {
            if (item.equals(localAddressRandom)) {
                break;
            }
            rank++;
        }
        String registryTreeKeys = MessageFormat.format("LocalRegistryKey={0}, AllRegistryTree={1}", localAddressRandom, sortSet.toString());

        // for biz
        if (type == QUEUE) {
            return new ActiveInfo(rank, sortSet.size(), registryTreeKeys);
        } else if (type == SERIAL_QUEUE) {
            if (rank == 0) {
                return new ActiveInfo(0, 1, registryTreeKeys);
            }
            return null;
        } else if (type == TOPIC) {

        }
        return null;
    }

    public static class ActiveInfo {
        // consumer rank
        public int rank;
        // alive num
        public int total;
        // registry rank info
        public String registryRankInfo;

        public ActiveInfo(int rank, int total, String registryRankInfo) {
            this.rank = rank;
            this.total = total;
            this.registryRankInfo = registryRankInfo;
        }

        @Override
        public String toString() {
            return "ActiveInfo{" +
                    "rank=" + rank +
                    ", total=" + total +
                    ", registryRankInfo='" + registryRankInfo + '\'' +
                    '}';
        }
    }

}