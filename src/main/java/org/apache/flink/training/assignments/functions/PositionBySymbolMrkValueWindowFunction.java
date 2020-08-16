package org.apache.flink.training.assignments.functions;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.assignments.domain.PositionBySymbol;
import org.apache.flink.util.Collector;

public class PositionBySymbolMrkValueWindowFunction implements WindowFunction<PositionBySymbol, PositionBySymbol,
        String, TimeWindow> {

    @Override
    public void apply(final String key,
            final TimeWindow timeWindow,
            final Iterable<PositionBySymbol> positions,
            final Collector<PositionBySymbol> out
    ) throws Exception {

        for (PositionBySymbol position : positions
        ) {
            out.collect(position);
        }
    }
}
