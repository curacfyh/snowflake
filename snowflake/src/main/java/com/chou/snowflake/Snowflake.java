package com.chou.snowflake;

public class Snowflake {

    private static final long START_TIMESTAMP = 1541750713L;

    private static final int SEQUENCE_BIT = 12;
    private static final int MACHINE_BIT = 5;
    private static final int DATA_CENTER_BIT = 5;

    private static final long MAX_SEQUENCE = ~(-1L << SEQUENCE_BIT);
    private static final long MAX_MACHINE_ID = ~(-1L << MACHINE_BIT);
    private static final long MAX_DATA_CENTER_ID = ~(-1L << DATA_CENTER_BIT);

    private static final int MACHINE_LEFT = SEQUENCE_BIT;
    private static final int DATA_CENTER_LEFT = SEQUENCE_BIT + MACHINE_BIT;
    private static final int TIMESTAMP_LEFT = SEQUENCE_BIT + MACHINE_BIT + DATA_CENTER_BIT;

    private long sequence;
    private long machineId;
    private long dataCenterId;
    private long lastTimeStamp = -1L;

    public Snowflake(long dataCenterId, long machineId) {
        if (dataCenterId < 0 || dataCenterId > MAX_DATA_CENTER_ID) {
            throw new RuntimeException("数据中心ID不合法！");
        }
        if (machineId < 0 || machineId > MAX_MACHINE_ID) {
            throw new RuntimeException("机器ID不合法！");
        }
        this.dataCenterId = dataCenterId;
        this.machineId = machineId;
    }

    public long nextId() {
        long currentTimeStamp = getCurrentTimeStamp();
        if (currentTimeStamp < lastTimeStamp) {
            throw new RuntimeException("时钟同步出现错误，无法获取序列号！");
        }
        if (currentTimeStamp == lastTimeStamp) {
            sequence = (sequence + 1L) & MAX_SEQUENCE;
            if (sequence == 0L) {
                currentTimeStamp = getNextTimeStamp();
            }
        } else {
            sequence = 0L;
        }
        lastTimeStamp = currentTimeStamp;
        return ((currentTimeStamp - START_TIMESTAMP) << TIMESTAMP_LEFT)
                | (dataCenterId << DATA_CENTER_LEFT)
                | (machineId << MACHINE_LEFT)
                | sequence;
    }

    private long getCurrentTimeStamp() {
        return System.currentTimeMillis();
    }

    private long getNextTimeStamp() {
        long currentTimestamp = getCurrentTimeStamp();
        while (currentTimestamp <= lastTimeStamp) {
            currentTimestamp = getCurrentTimeStamp();
        }
        return currentTimestamp;
    }

}
