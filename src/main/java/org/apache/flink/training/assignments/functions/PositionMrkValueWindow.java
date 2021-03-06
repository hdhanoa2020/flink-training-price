package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.assignments.domain.Position;
        import org.apache.flink.util.Collector;
public class PositionMrkValueWindow implements AllWindowFunction<Position, Position,
        TimeWindow> {
    @Override
    public void apply(
            final TimeWindow timeWindow,
            final Iterable<Position> positions,
            final Collector<Position> collector
    ) throws Exception {
        //The main counting bit for position quantity
        for (Position position : positions
        ) {
            collector.collect(position);
        }
    }
}