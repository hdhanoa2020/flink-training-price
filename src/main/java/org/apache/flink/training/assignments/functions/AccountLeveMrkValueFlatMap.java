package org.apache.flink.training.assignments.functions;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.training.assignments.domain.Position;
import org.apache.flink.training.assignments.domain.Price;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;

public class AccountLeveMrkValueFlatMap extends RichCoFlatMapFunction<Position,Price, Position> {

    private static final Logger LOG = LoggerFactory.getLogger(AccountLeveMrkValueFlatMap.class);

    private ValueState<Price> priceState;
    private ValueState<Position> positionState;
   // ListState<Position> positionListState;



     @Override
    public void flatMap2(Price price, Collector<Position> out) throws Exception {
        Position position = positionState.value();
        priceState.update(price);

        if (position != null) {
            positionState.clear();
            position.setPrice(price.getPrice());
            BigDecimal mrk = price.getPrice().multiply(BigDecimal.valueOf(position.getQuantity()));
            position.setMarketValue(mrk);
            position.setMarketValue(mrk);
            out.collect(position);
        } /* else {
            priceState.update(price);
        }*/
    }

    @Override
    public void flatMap1(Position position, Collector<Position> out) throws Exception {
        Price price = priceState.value();
        if (price != null) {
            positionState.clear();
            position.setPrice(price.getPrice());
            BigDecimal mrk = price.getPrice().multiply(BigDecimal.valueOf(position.getQuantity()));
            position.setTimestamp(System.currentTimeMillis());
            position.setMarketValue(mrk);
            out.collect(position);
        } else {
            positionState.update(position);
        }
    }

    @Override
    public void open(Configuration config) {
        priceState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved priceState", Price.class));
        positionState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved positionState", Position.class));
    }
}

/*

    @Override
    public void open(Configuration config) {
        priceState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved priceState", Price.class));
        positionState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved positionState", Position.class));
    }

    @Override
    public void flatMap1(Position position, Collector<Position> out) throws Exception {
        //LOG.info("****flatMap1 {}", ++flatMap1);
        Price price = priceState.value();
        if (price != null) {
            // priceState.clear();
            out.collect(enrichPositionByActWithPrice(position,price.getPrice()));
        } else {
            positionState.update(position);
        }
    }

    @Override
    public void flatMap2(Price price, Collector<Position> out) throws Exception {
        //LOG.info("****flatMap2 {}", ++flatMap2);
        Position position = positionState.value();
        priceState.update(price);
        if (position != null) {
            positionState.clear();
            out.collect(enrichPositionByActWithPrice(position,price.getPrice()));
        }/**
         else {
         priceState.update(price);
         }**/
 /*   }

    private Position enrichPositionByActWithPrice(final Position position, final BigDecimal price) {

        position.setPrice(price);
        BigDecimal mrk = price.multiply(BigDecimal.valueOf(position.getQuantity()));
        position.setMarketValue(mrk);
        position.setTimestamp(System.currentTimeMillis());
        return position;
    }

    }*/