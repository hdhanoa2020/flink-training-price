package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.assignments.domain.Allocation;
import org.apache.flink.training.assignments.domain.Order;

import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/*
add all the allocations for a cusip/symbol
 */
public class PostionsByCusip extends ProcessWindowFunction<Order, Tuple2<String, List<Allocation>>, String, TimeWindow> {

    private static final Logger LOG = LoggerFactory.getLogger(PostionsByCusip.class);

    @Override
    public void process(String  cusip, Context context, Iterable<Order> iterable, Collector<Tuple2<String, List<Allocation>>> collector) throws Exception {
        List<Allocation> allocations =  new ArrayList<Allocation>();
        for(Order o : iterable){
            //LOG.debug("ProcessPositions for cusip: {}",cusip);
            allocations.addAll(o.getAllocations());
        }
        Tuple2<String, List<Allocation>> aa = Tuple2.of(cusip, allocations);
        collector.collect(aa);
    }
}
