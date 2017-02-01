package com.nVidia.examples.zkconsumer.simplemovingavg;

import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.commons.collections.buffer.CircularFifoBuffer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by vidya.priyadarshini on 01/02/17.
 */
public class SimpleMovingAvgZkConsumer {

    private Properties kafkaProperties = new Properties();
    private ConsumerConnector consumer;
    private ConsumerConfig config;
    private KafkaStream<String, String> stream;
    private String waitTime;

    public static void main(String[] args) {
        if(args.length==0) {
            System.out.println("SimpleMovingAvgZKConsumer {zookeeper} {group.id} {topic} {window-size} {wait-time}");
            return;
        }
        String next;
        int num;
        SimpleMovingAvgZkConsumer movingAvg = new SimpleMovingAvgZkConsumer();
        String zkUrl = args[0];
        String groupId = args[1];
        String topic = args[2];
        int window = Integer.parseInt(args[3]);
        movingAvg.waitTime = args[4];
        
        CircularFifoBuffer buffer = new CircularFifoBuffer(window);
        movingAvg.configure(zkUrl,groupId);
        movingAvg.start(topic);

        while((next = movingAvg.getNextMessage()) !=null) {
            int sum = 0;

            try {
                num = Integer.parseInt(next);
                buffer.add(num);
            } catch (NumberFormatException e) {
                //ignore strings
            }
            for(Object o : buffer) {
                sum += (Integer)o;
            }
            if(buffer.size() > 0) {
                System.out.println("Moving avg is: " + sum/buffer.size());
            }
            //uncomment if you wish to commit offsets on every message
            //movingAvg.consumer.commitOffsets();
        }

        movingAvg.consumer.shutdown();
        System.exit(0);
    }

    private void start(String topic) {
        consumer = Consumer.createJavaConsumerConnector(config);

        /* Tell kafka how many threads will read each topic. We have one topic per thread*/
        Map<String, Integer> topicCountMap = new HashMap();
        topicCountMap.put(topic,new Integer(1));

        /*default property for deserializer.encoding is UTF8*/
        StringDecoder decoder = new StringDecoder(new VerifiableProperties());

        /*Get streams of messages for each topic. Here it is one topic with a list of a single stream */
        stream = consumer.createMessageStreams(topicCountMap,decoder,decoder).get(topic).get(0);

    }

    private String getNextMessage() {
        ConsumerIterator<String, String> it = stream.iterator();

        try {
            return it.next().message();
        } catch (ConsumerTimeoutException e) {
            System.out.println("waited " + waitTime + " and no messages arrived");
            return null;
        }
    }

    private void configure(String zkUrl, String groupId) {
        kafkaProperties.put("zookeeper.connect",zkUrl);
        kafkaProperties.put("group.id",groupId);
        kafkaProperties.put("auto.commit.interval.ms","1000");
        kafkaProperties.put("auto.offset.reset","largest");
        kafkaProperties.put("consumer.timeout.ms", waitTime);

        //un-comment this if you want to acknowledge I have read messages till
        //a particular offset instead of auto commit in background
        //kafkaProperties.put("auto.commit.enable","false");
        config = new ConsumerConfig(kafkaProperties);

    }
}
