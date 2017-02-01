package com.nVidia.examples.oldproducer.simplecounter;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Created by vidya.priyadarshini on 01/02/17.
 */

public class SimpleCounterOldProducer {

    private Properties kafkaProps = new Properties();
    private Producer<String,String> producer;
    private ProducerConfig config;
    private static String topic;

    public static void main(String[] args) throws InterruptedException {

        if(args.length==0) {
            System.out.println("SimpleCounterOldProducer {broker-list} {topic} {sync} {delay (ms)} {count}");
            return;
        }

        SimpleCounterOldProducer counter = new SimpleCounterOldProducer();
        /*get arguments */
        String brokerlist = args[0];
        topic = args[1];
        String sync = args[2];
        int delay = Integer.parseInt(args[3]);
        int count = Integer.parseInt(args[4]);
        
        /*start a producer*/
        counter.configure(brokerlist,sync);
        counter.start();

        long startTime = System.currentTimeMillis();
        System.out.println("Starting...");
        counter.produce("Starting...");

        /*produce the numbers*/
        for(int i=0;i<count;i++){
            counter.produce(Integer.toString(i));
            Thread.sleep(delay);
        }

        long endTime = System.currentTimeMillis();
        System.out.println("... and we are done. This took "+ (endTime - startTime) + " ms.");
        counter.produce("... and we are done. This took "+ (endTime - startTime) + " ms.");

        /*close the producer and exit*/
        counter.producer.close();
        System.exit(0);

    }

    /*start the producer*/
    private void start() {
        producer = new Producer<String, String>(config);
    }

    private void configure(String brokerlist, String sync) {
        kafkaProps.put("metadata.broker.list", brokerlist);
        kafkaProps.put("serializer.class", "kafka.serializer.StringEncoder");
        kafkaProps.put("producer.type",sync);

        config = new ProducerConfig(kafkaProps);
    }

    private void produce(String s) {
        KeyedMessage<String,String> message = new KeyedMessage<String, String>(topic,null,s);
        producer.send(message);
    }


}
