package com.netflix.suro.nofify;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import com.netflix.suro.TagKey;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class QueueNotify implements Notify {
    public static final String TYPE = "queue";

    private static final int DEFAULT_LENGTH = 100;
    private static final long DEFAULT_TIMEOUT = 1000;

    private final BlockingQueue<String> queue;
    private final long timeout;

    @Monitor(name = TagKey.SENT_COUNT, type = DataSourceType.COUNTER)
    private AtomicLong sentMessageCount = new AtomicLong(0);
    @Monitor(name = TagKey.RECV_COUNT, type = DataSourceType.COUNTER)
    private AtomicLong recvMessageCount = new AtomicLong(0);
    @Monitor(name = TagKey.LOST_COUNT, type = DataSourceType.COUNTER)
    private AtomicLong lostMessageCount = new AtomicLong(0);

    public QueueNotify() {
        queue = new LinkedBlockingQueue<String>(DEFAULT_LENGTH);
        timeout = DEFAULT_TIMEOUT;

        Monitors.registerObject(this);
    }

    @JsonCreator
    public QueueNotify(
            @JsonProperty("length") int length,
            @JsonProperty("recvTimeout") long timeout) {
        this.queue = new LinkedBlockingQueue<String>(length > 0 ? length : DEFAULT_LENGTH);
        this.timeout = timeout > 0 ? timeout : DEFAULT_TIMEOUT;
    }

    @Override
    public void init() {
    }

    @Override
    public boolean send(String message) {
        if (queue.offer(message)) {
            sentMessageCount.incrementAndGet();
            return true;
        } else {
            lostMessageCount.incrementAndGet();
            return false;
        }
    }

    @Override
    public String recv() {
        try {
            return queue.poll(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return null;
        }
    }

    @Override
    public String getStat() {
        return String.format("QueueNotify with the size: %d, sent : %d, received: %d, dropped: %d",
                queue.size(), sentMessageCount.get(), recvMessageCount.get(), lostMessageCount.get());
    }
}