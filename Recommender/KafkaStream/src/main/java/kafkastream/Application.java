package kafkastream;

import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;


/**
 * @program: HaloRecSys
 * @description: KafKa streaming data preprocess
 * @author: HaloZhang
 * @create: 2021-05-03 14:57
 **/
public class Application {
    public static void main(String[] args) {
        String brokers = "localhost:9092";
        String zookeepers = "localhost:2181";

        // 输入和输出的Topics
        String from = "log";
        String to = "recommender";

        // 定义kafka streaming的配置
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);

        // 创建 kafka stream 配置对象
        StreamsConfig config = new StreamsConfig(settings);

        // 创建一个拓扑建构器
        TopologyBuilder builder = new TopologyBuilder();

        // 定义流处理的拓扑结构
        // 数据流向为 source ----> processor ----> sink
        //  topic   log   ------------------->   recommender
        // 这个程序会将 topic 为“log”的信息流获取来做处理，并以“recommender”为 新的 topic 转发出去

        builder.addSource("SOURCE", from) // 第二个参数是一个函数，这里写了一个lambda表达式
                .addProcessor("PROCESSOR", ()->new LogProcessor(), "SOURCE")
                .addSink("SINK", to, "PROCESSOR");

        KafkaStreams streams = new KafkaStreams( builder, config );

        streams.start();

        System.out.println("Kafka stream started!>>>>>>>>>>>");
    }
}
