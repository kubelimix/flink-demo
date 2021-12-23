package com.limix.bigdata.demo;

import com.limix.bigdata.demo.domain.WordWithCount;
import com.limix.bigdata.demo.util.KafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink样例程序
 */
public class FlinkDemo {

    private static Logger LOGGER = LoggerFactory.getLogger(FlinkDemo.class);

    public static void main(String[] args) {
        boolean isOnline = true;
        if (args.length > 0 && "test".equals(args[0])) {
            isOnline = false;
        }

        // 1. 初始化flink
        LOGGER.info("begin to init flink");
        Configuration conf = new Configuration();
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        bsEnv.enableCheckpointing(10000L);
        bsEnv.setRestartStrategy(RestartStrategies.fallBackRestart());
        LOGGER.info("flink with (%s)", bsEnv.getConfig().toString());

        // 2. 数据来源
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop8:9092")
                .setTopics("flink_demo")
                .setGroupId("flink-demo")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> text = bsEnv.fromSource(source, WatermarkStrategy.noWatermarks(),
                "kafka source",
                TypeInformation.of(String.class));

        // 3. 计算
        DataStream<WordWithCount> windowCounts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {

            private static final long serialVersionUID = 6800597108091365154L;

            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                for(String word:value.split("//s")) {
                    out.collect(new WordWithCount(word, 1));
                }
            }
        }).keyBy((KeySelector<WordWithCount, String>) value -> value.word)
                .reduce((ReduceFunction<WordWithCount>) (value1, value2)
                        -> new WordWithCount(value1.word,value1.count+value2.count));

        // 4.输出-默认输出到控制台
        windowCounts.print();

        try {
            bsEnv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
