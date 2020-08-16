package org.apache.flink.training.assignments.assigners;


import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.training.assignments.domain.Order;
import org.apache.flink.training.assignments.domain.Price;

public class PriceAscendingWatermarker extends AscendingTimestampExtractor<Price> {
    @Override
    public long extractAscendingTimestamp(Price price) {

        return price.getTimestamp();
    }
}