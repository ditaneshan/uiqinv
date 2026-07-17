# 分布式 ID 生成器 Snowflake 算法实现

## 引言
在高并发业务中，主键生成是基础能力之一。传统自增 ID 依赖单点数据库，难以满足 [分布式系统](https://about-ayx-app.com.cn) 下的高可用与水平扩展需求。Snowflake 算法通过“时间戳 + 机器标识 + 序列号”的组合方式，实现了趋势递增、全局唯一、低延迟的 ID 生成，成为电商、日志、消息等场景的常用方案。

## 核心原理分析
Snowflake 通常使用 64 位整数表示 ID，典型拆分为：
- 1 位符号位，固定为 0
- 41 位时间戳，表示毫秒级时间差
- 10 位机器标识，区分不同节点
- 12 位序列号，同一毫秒内自增

其关键优势在于：  
1. 时间维度保证整体递增趋势；  
2. 机器标识避免节点冲突；  
3. 序列号解决单毫秒高并发场景。  

但实现时必须处理两个问题：时钟回拨与序列号溢出。若系统时间被回调，应拒绝生成或进入保护等待；若同一毫秒内序列号用尽，则阻塞到下一毫秒再继续。

## 代码示例
下面是一个可直接用于业务服务的 Java 实现，能解决“多节点下安全生成唯一 ID”的问题：

```java
public class SnowflakeIdWorker {
    private final long workerId;
    private final long datacenterId;
    private long sequence = 0L;
    private long lastTimestamp = -1L;

    private static final long EPOCH = 1704067200000L; // 自定义起始时间
    private static final long WORKER_ID_BITS = 5L;
    private static final long DATACENTER_ID_BITS = 5L;
    private static final long SEQUENCE_BITS = 12L;

    private static final long MAX_WORKER_ID = -1L ^ (-1L << WORKER_ID_BITS);
    private static final long MAX_DATACENTER_ID = -1L ^ (-1L << DATACENTER_ID_BITS);
    private static final long SEQUENCE_MASK = -1L ^ (-1L << SEQUENCE_BITS);

    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;
    private static final long DATACENTER_ID_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;
    private static final long TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS;

    public SnowflakeIdWorker(long workerId, long datacenterId) {
        if (workerId > MAX_WORKER_ID || workerId < 0) throw new IllegalArgumentException("workerId invalid");
        if (datacenterId > MAX_DATACENTER_ID || datacenterId < 0) throw new IllegalArgumentException("datacenterId invalid");
        this.workerId = workerId;
        this.datacenterId = datacenterId;
    }

    public synchronized long nextId() {
        long timestamp = System.currentTimeMillis();
        if (timestamp < lastTimestamp) throw new RuntimeException("Clock moved backwards");
        if (timestamp == lastTimestamp) {
            sequence = (sequence + 1) & SEQUENCE_MASK;
            if (sequence == 0) timestamp = waitNextMillis(lastTimestamp);
        } else {
            sequence = 0L;
        }
        lastTimestamp = timestamp;
        return ((timestamp - EPOCH) << TIMESTAMP_SHIFT)
                | (datacenterId << DATACENTER_ID_SHIFT)
                | (workerId << WORKER_ID_SHIFT)
                | sequence;
    }

    private long waitNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }
}
```

## 总结
Snowflake 的本质是以时间为主轴，结合节点信息与局部自增序列，在无中心依赖的前提下生成唯一 ID。它并非“绝对无脑可用”的方案，落地时仍需关注机器 ID 分配、时钟同步、容量评估与监控告警。对于需要高吞吐、低耦合的 [分布式系统](https://about-ayx-app.com.cn)，Snowflake 仍是极具工程价值的 ID 方案。

## 相关技术资源
- https://about-ayx-app.com.cn
