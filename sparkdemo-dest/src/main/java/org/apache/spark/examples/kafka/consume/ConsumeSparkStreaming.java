package org.apache.spark.examples.kafka.consume;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.google.common.util.concurrent.AtomicDouble;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class ConsumeSparkStreaming
{
    public final static String TOPIC = "spark-stream_demo";

    private static AtomicLong totalCount = new AtomicLong(0);
    private static AtomicDouble totalAllSum = new AtomicDouble(0);

    public static void main(String[] args) throws InterruptedException
    {
	new ConsumeSparkStreaming().consume();
    }

    private void consume() throws InterruptedException
    {
	Map<String, Object> kafkaParams = new HashMap<>();
	kafkaParams.put("bootstrap.servers", "localhost:9092");
	kafkaParams.put("metadata.broker.list", "localhost:9092");
	kafkaParams.put("key.deserializer", StringDeserializer.class);
	kafkaParams.put("value.deserializer", StringDeserializer.class);
	kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
	kafkaParams.put("auto.offset.reset", "latest");
	kafkaParams.put("enable.auto.commit", false);

	Collection<String> topics = Arrays.asList(TOPIC);

	JavaStreamingContext jssc = getJavaStreamingContext("JavaDirectKafkaWordCount", "local[2]", null,
		Durations.seconds(3));

	JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(jssc,
		LocationStrategies.PreferConsistent(),
		ConsumerStrategies.<String, String> Subscribe(topics, kafkaParams));

	//对流中按时间间隔产生的分片进行遍历
	stream.foreachRDD(new VoidFunction2<JavaRDD<ConsumerRecord<String, String>>, Time>()
	{

	    private static final long serialVersionUID = 1L;

	    @Override
	    public void call(JavaRDD<ConsumerRecord<String, String>> v1, Time v2) throws Exception
	    {
		
		List<ConsumerRecord<String, String>> consumerRecords = v1.collect();

		System.out.println("获取消息:" + consumerRecords.size());

		Float totalSum = 0f;

		Long count = 0L;

		if (!consumerRecords.isEmpty())
		{

		    for (ConsumerRecord<String, String> consumerRecord : consumerRecords)
		    {
			String tempListStr = consumerRecord.value();
			System.out.println(tempListStr);

			Gson gson = new Gson();
			Type type = new TypeToken<List<String>>()
			{
			}.getType();
			List<String> tempList = gson.fromJson(tempListStr, type);
			if (null != tempList)
			{
			    totalSum += getSum(tempList);
			    count += tempList.size();
			}

		    }
		}

		totalCount.addAndGet(count);
		totalAllSum.addAndGet(totalSum);

		if (count != 0)
		{
		    float tempAverage = totalSum / count;

		    System.out.println("本组数据的平局值是" + tempAverage);
		}

		if (totalCount.get() != 0)
		{
		    float allAverage = (float) (totalAllSum.get() / totalCount.get());
		    System.out.println("历史接收所有数据的平局值是" + allAverage);
		}

	    }
	});
	
	
	jssc.start(); 
	jssc.awaitTermination();

    }

    private Long getSum(List<String> list)
    {
	if (list == null || list.size() == 0)
	{
	    return 0L;
	}
	Long sum = 0L;
	for (int i = 0; i < list.size(); i++)
	{
	    String str = list.get(i);
	    sum += Long.parseLong(str);
	}

	return sum;

    }

    private JavaStreamingContext getJavaStreamingContext(String appName, String master, String logLeverl,
	    Duration batchDuration)
    {
	SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster(master);
	sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
	sparkConf.set("spark.kryo.registrator", "org.apache.spark.examples.streaming.entity.MyRegistrator");

	JavaStreamingContext jsc = new JavaStreamingContext(sparkConf, batchDuration);
	return jsc;
    }

}
