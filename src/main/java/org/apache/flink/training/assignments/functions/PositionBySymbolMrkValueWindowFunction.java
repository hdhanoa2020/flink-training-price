package org.apache.flink.training.assignments.functions;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.PositionBySymbol;
import org.apache.flink.training.assignments.orders.KafkaMarketValueAssignment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class PositionBySymbolMrkValueWindowFunction implements WindowFunction<PositionBySymbol, PositionBySymbol,
        String, TimeWindow> {
    private static final Logger LOG = LoggerFactory.getLogger(PositionBySymbolMrkValueWindowFunction.class);

    @Override
    public void apply(final String key,
            final TimeWindow timeWindow,
            final Iterable<PositionBySymbol> positions,
            final Collector<PositionBySymbol> out
    ) throws Exception {
        PositionBySymbol lastPositioninWindow = null;
        for (PositionBySymbol position : positions
        ) {
            lastPositioninWindow = position;
        }
        lastPositioninWindow.setTimestamp(System.currentTimeMillis());
        out.collect(lastPositioninWindow);
        //LOG.info("MarketValue By Symbol 1 min account window  time : {}",convertToTimestamp());
    }

    private static String convertToTimestamp(){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("hh:mm:ss");

        Date date = new Date(System.currentTimeMillis());
        String time = simpleDateFormat.format(date);
        return time;

    }
}
