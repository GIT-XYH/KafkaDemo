package com.rookiex01

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

import java.time.Duration
import java.util
import java.util.Properties

object ConsumerCommitOffsetDemo2 {

  def main(args: Array[String]): Unit = {

    // 1 配置参数
    val props = new Properties()
    //从哪些broker消费数据
    props.setProperty("bootstrap.servers", "node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092")
    // 反序列化的参数
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    // 指定group.id
    props.setProperty("group.id", "g08")

    // 指定消费的offset从哪里开始
    //earliest：从头开始 --from-beginning
    //latest:从消费者启动之后
    props.setProperty("auto.offset.reset", "earliest") //[latest, earliest, none]
    props.setProperty("enable.auto.commit", "false")
    // 2 消费者的实例对象
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

    // 订阅   参数类型  java的集合
    val topic: util.List[String] = java.util.Arrays.asList("wordcount")

    // 3 订阅主题
    consumer.subscribe(topic)

    while (true) {
      // 4  拉取数据
      val msgs: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(5000))

      //导入隐式转换
      import scala.collection.JavaConverters._
      //将Java的集合或迭代器转成Scala的集合或迭代器
      for (cr <- msgs.asScala) {
        //ConsumerRecord[String, String]
        println(cr)
      }
      //手动提交偏移量（需要自己写程序提交）
      if(!msgs.isEmpty){
        //是将偏移量写入到Kafka特殊的Topic中__consumer_offset
        consumer.commitAsync() //异步提交，该方法是非阻塞的（会开一个新的线程用来提交偏移量）
      }
    }

    //consumer.close()

  }
}
