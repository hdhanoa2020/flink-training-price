package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

public class PositionMarketValueWindowFunction implements  WindowFunction<Position, Position,
        Tuple3<String, String,String>, TimeWindow> {
    private static final Logger LOG = LoggerFactory.getLogger(PositionMarketValueWindowFunction.class);

    @Override
    public void apply(final Tuple3<String, String,String> tuple3, //final String key,
            final TimeWindow timeWindow,
                      final Iterable<Position> positions,
                      final Collector<Position> out
    ) throws Exception {
        Position lastPositioninWindow = null;
       for (Position position : positions
        ) {
           lastPositioninWindow = position;
        }
        lastPositioninWindow.setTimestamp(System.currentTimeMillis());
        out.collect(lastPositioninWindow);
        //LOG.info("MarketValue By Account in 1 min  window time : {}",convertToTimestamp());
    }

    private static String convertToTimestamp(){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("hh:mm:ss");

        Date date = new Date(System.currentTimeMillis());
        String time = simpleDateFormat.format(date);
        return time;

    }


}
