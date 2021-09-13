package com.rookiex01

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

import java.time.Duration
import java.util
import java.util.Properties


//topic、分区、groupId -> 偏移量
object ConsumerDemo {

  def main(args: Array[String]): Unit = {

    // 1 配置参数
    val props = new Properties()
    //从哪些broker消费数据
    props.setProperty("bootstrap.servers", "node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092")
    // 反序列化的参数
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    // 指定group.id
    props.setProperty("group.id", "g03")

    // 指定消费的offset从哪里开始
    //earliest：从头开始 --from-beginning
    //latest:从消费者启动之后
    props.setProperty("auto.offset.reset", "earliest") //[latest, earliest, none]

    // 是否自动提交偏移量  offset
    // enable.auto.commit 默认值就是true【5秒钟更新一次】，消费者定期会更新偏移量 groupid,topic,parition -> offset
    //props.setProperty("enable.auto.commit", "true") // 不让kafka自动维护偏移量     手动维护偏移量
    //auto.commit.interval.ms=5000
    //设置消费者的事务隔离级别：只读已经提交事务的数据，脏数据不读
    // properties.setProperty("isolation.level", "read_committed");


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
      //println("---------------------------")
    }

    //consumer.close()

  }
}
