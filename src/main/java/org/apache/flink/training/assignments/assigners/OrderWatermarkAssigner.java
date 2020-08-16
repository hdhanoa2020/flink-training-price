package org.apache.flink.training.assignments.assigners;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.training.assignments.domain.Order;

import javax.annotation.Nullable;

public class OrderWatermarkAssigner implements AssignerWithPeriodicWatermarks<Order> {
    private long  currentMaxtimestamp = 0;
    private final long  allowedLatetime = 1000; // 1 sec latency

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxtimestamp-allowedLatetime);
    }

    @Override
    public long extractTimestamp(Order order, long l) {
        if(order.getOrderTime() == 0){
            order.setOrderTime(System.currentTimeMillis());
        }
        currentMaxtimestamp = Math.max(order.getOrderTime(),currentMaxtimestamp);
        return order.getOrderTime();
    }
}
