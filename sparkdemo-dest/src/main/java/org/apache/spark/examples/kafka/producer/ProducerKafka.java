package org.apache.spark.examples.kafka.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * kafka生产者
 *
 */
public class ProducerKafka
{
    private final Producer<String, String> kafkaProducer;
    public final static String TOPIC = "spark-stream_demo";

    private ProducerKafka()
    {
	kafkaProducer = createKafkaProducer();
    }

    private Producer<String, String> createKafkaProducer()
    {
	Properties props = new Properties();
	props.put("bootstrap.servers", "127.0.0.1:9092");
	props.put("acks", "all");
	props.put("retries", 0);
	props.put("batch.size", 16384);
	props.put("linger.ms", 1);
	props.put("buffer.memory", 33554432);
	props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	Producer<String, String> kafkaProducer = new KafkaProducer<>(props);
	return kafkaProducer;
    }

    private String getJsonObject()
    {
	//随机发送几个数值组成list
	List<String> dataList = new ArrayList<>();
	int random = getRandomNum(200);
	if (random == 0) {
	    random = 100;
	}
	for(int i=0;i<random;i++) {
	    dataList.add(i+"");
	}
	
	return resultMapToString(dataList);
    }

    private void produce()
    {
	for (int i = 1; i < 1000; i++)
	{
	    try {
		Thread.sleep(1000);
	    }catch(InterruptedException e) {
		
	    }
	    String data = getJsonObject();
	    kafkaProducer.send(new ProducerRecord<>(TOPIC,data));
	    System.out.println(data);
	}
    }
    
    /**
     * resultMap to string
     * @param <T>
     * @param resultMap
     * @return
     */
    private String resultMapToString(List<String> resultList)
    {
	try
	{
	    ObjectMapper mapper = new ObjectMapper();
	    return  mapper.writeValueAsString(resultList);
	} catch (Exception e)
	{
	    System.out.println(e.toString());
	    return "Server exception";
	}
    }
    
    private int getRandomNum(int bound){
	Random random = new Random();
	return random.nextInt(bound);
}

    public static void main(String[] args)
    {
	new ProducerKafka().produce();
    }
}