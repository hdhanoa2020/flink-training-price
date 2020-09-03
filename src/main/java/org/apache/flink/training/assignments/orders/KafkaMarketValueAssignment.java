package org.apache.flink.training.assignments.orders;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import org.apache.flink.training.assignments.domain.*;
import org.apache.flink.training.assignments.functions.*;
import org.apache.flink.training.assignments.serializers.*;

import org.apache.flink.training.assignments.sinks.LogSink;
import org.apache.flink.training.assignments.utils.ExerciseBase;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaMarketValueAssignment extends ExerciseBase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMarketValueAssignment.class);

    public static final String KAFKA_ADDRESS = "kafka.dest.harpreet1.wsn.riskfocus.com:9092";
    public static final String POSITIONS_BY_ACT_TOPIC = "positionsByAct";
    public static final String POSITIONS_BY_SYMBOL_TOPIC = "positionsBySymbol";
    public static final String PRICE_TOPIC = "price";
    public static final String OUT_TOPIC = "out";
    public static final String MV_BY_ACT_TOPIC = "mvByAct";
    public static final String MV_BY_SYMBOL_TOPIC = "mvBySymbol";
    public static final String KAFKA_GROUP = "";


    public static void main(String[] args) throws Exception {

        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.disableOperatorChaining();

        // Create tbe Kafka Consumer for price topic
        FlinkKafkaConsumer010<Price> flinkKafkaConsumer = createKafkaConsumer(PRICE_TOPIC,KAFKA_ADDRESS,KAFKA_GROUP);
        DataStream<Price> priceStream = env.addSource(flinkKafkaConsumer).name("KafkaPriceTopic").uid("KafkaPriceTopic")
                                        .keyBy(price -> price.getCusip());

        // Create the Kafka Consumer for positions by account
        FlinkKafkaConsumer010<Position> consumerPositionByAct = createConsumerPositionByAct(POSITIONS_BY_ACT_TOPIC,KAFKA_ADDRESS,KAFKA_GROUP);
        DataStream<Position> positionsByAcctStream = env
                .addSource(consumerPositionByAct)
                .name("KafkaPostionsByActReader").uid("KafkaPostionsByActReader")
                .keyBy(postion -> postion.getCusip());


        //join the positionByAct and Price
        DataStream<Position> mkvByAccount = positionsByAcctStream
                    .connect(priceStream)
                    .flatMap(new AccountLeveMrkValueFlatMap())
                    .uid("AcctPriceEnrichmentMap").name("AcctPriceEnrichmentMap")
                    //.keyBy(position -> position.getCusip())
                     .keyBy(
                         new KeySelector<Position, Tuple3<String, String,String>>() {
                     @Override
                     public Tuple3<String, String,String> getKey(Position value) throws Exception {
                        return Tuple3.of(value.getCusip(), value.getAccount(),value.getSubAccount());
                      }})
                    .timeWindow(Time.minutes(1))
                    .apply(new PositionMarketValueWindowFunction())
                    .name("AcctMrkValueInOneMinWindow")
                    .uid("AcctMrkValueInOneMinWindow");

        // sent to topic
        FlinkKafkaProducer010 flinkKafkaProducer = createMrkValueAccountProducer(MV_BY_ACT_TOPIC,KAFKA_ADDRESS);
        mkvByAccount.addSink(flinkKafkaProducer)
                .name("PublishPosMarketValueByActToKafka")
                .uid("PublishPosMarketValueByActToKafka");

       mkvByAccount.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.INFO, "MarketValueByAct: {}"));

        //  pass the order ID and timestamp for latency matrix
        //  DataStream<ComplianceResult>  results = mkvByAccount.map(new BuildTestResults());
        //  results.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.INFO, "TestResults: {}"));

        // Create tbe Kafka Consumer for positions by symbol
        FlinkKafkaConsumer010<PositionBySymbol> consumerPositionBySymbol = createConsumerPositionBySymbol(POSITIONS_BY_SYMBOL_TOPIC,KAFKA_ADDRESS,KAFKA_GROUP);
        DataStream<PositionBySymbol> positionsBySymbolStream = env
                .addSource(consumerPositionBySymbol).name("KafkaPostionsBySymReader")
                .uid("KafkaPostionsBySymReader")
                .keyBy(position -> position.getCusip());


        //join the positionBySymbol and Price
        DataStream<PositionBySymbol> mkvBySymbol = positionsBySymbolStream
                .connect(priceStream)
                .flatMap(new CusipLeveMrkValueFlatMap())
                .uid("CusipPriceEnrichmentMap").name("CusipPriceEnrichmentMap")
                .keyBy(positionByCusip -> positionByCusip.getCusip())
                .timeWindow(Time.minutes(1))
                .apply(new PositionBySymbolMrkValueWindowFunction())
                .name("CusipMrkValueInOneMinWindow")
                .uid("CusipMrkValueInOneMinWindow");


        // sent to topic
        FlinkKafkaProducer010 flinkKafkaProducerSym = createMrkValueSymbolProducer(MV_BY_SYMBOL_TOPIC,KAFKA_ADDRESS);
        mkvBySymbol.addSink(flinkKafkaProducerSym)
                .name("PublishPosMarketValueBySymbolToKafka")
                .uid("PublishPosMarketValueBySymbolToKafka");

        mkvBySymbol.addSink(new LogSink<>(LOG, LogSink.LoggerEnum.INFO, "MarketValueBySymbol: {}"));


        // sent to out topic for latency matrix
      //  FlinkKafkaProducer010 flinkKafkaProducerCompResults = createTestResultProducer(OUT_TOPIC,KAFKA_ADDRESS);
       // results.addSink(flinkKafkaProducerCompResults).name("complianceResult").uid("complianceResult");



        // execute the transformation pipeline
        env.execute("kafkaCalculateMarketValue");

    }




    public static FlinkKafkaConsumer010<Price> createKafkaConsumer(String topic, String kafkaAddress,String group)
    {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",kafkaAddress);
        prop.setProperty("group.id",group);

       var stringConsumer = new FlinkKafkaConsumer010<Price>(topic,new PriceKafkaDeserialization(),prop);
        return stringConsumer;
    }
    public static FlinkKafkaConsumer010<Position> createConsumerPositionByAct(String topic, String kafkaAddress, String group)
    {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",kafkaAddress);
        prop.setProperty("group.id",group);

        var stringConsumer = new FlinkKafkaConsumer010<Position>(topic,new PositionDeserializationSchema(),prop);
        return stringConsumer;

    }
    public static FlinkKafkaConsumer010<PositionBySymbol> createConsumerPositionBySymbol(String topic, String kafkaAddress, String group)
    {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",kafkaAddress);
        prop.setProperty("group.id",group);

        var stringConsumer = new FlinkKafkaConsumer010<PositionBySymbol>(topic,new PositionBySymbolDeserialization(),prop);
        return stringConsumer;

    }

    public static FlinkKafkaProducer010<ComplianceResult> createTestResultProducer(String topic, String kafkaAddress)
    {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",kafkaAddress);
        var producer010 = new FlinkKafkaProducer010<ComplianceResult>(topic,new ComplianceResultSerialization(), prop);
        return producer010;
    }

    public static FlinkKafkaProducer010<Position> createMrkValueAccountProducer(String topic, String kafkaAddress)
    {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",kafkaAddress);
        var producer010 = new FlinkKafkaProducer010<Position>(topic,new AccountAllocationSerializationSchema(), prop);
        return producer010;
    }
    public static FlinkKafkaProducer010<PositionBySymbol> createMrkValueSymbolProducer(String topic, String kafkaAddress)
    {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers",kafkaAddress);
        var producer010 = new FlinkKafkaProducer010<PositionBySymbol>(topic,new PositionBySymbolSerialization(), prop);
        return producer010;
    }


    public static StreamExecutionEnvironment setEnvAndCheckpoints(){
        Configuration conf = new Configuration();
        conf.setString("state.backend", "filesystem");
        conf.setString("state.savepoints.dir", "file:///tmp/savepoints");
        conf.setString("state.checkpoints.dir", "file:///tmp/checkpoints");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        env.enableCheckpointing(10000L);
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        return env;


    }

}