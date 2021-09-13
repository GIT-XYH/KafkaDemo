package com.rookiex01

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties

/**
 * @Author RookieX
 * @Date 2021/8/16 9:17 下午
 * @Description:
 */
object ProducerDemo {
  def main(args: Array[String]): Unit = {
    // 1 配置参数
    val props = new Properties()
    // 连接kafka节点
    props.setProperty("bootstrap.servers", "rookiex01:9092,rookiex02:9092,rookiex03:9092")
    //指定key序列化方式
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //指定value序列化方式
    props.setProperty("value.serializer", classOf[StringSerializer].getName) // 两种写法都行

    //ack通知：0， 1， -1
    //0 只要把数据发送过去，不管leader是否确认，就发送完成（发送速度最快，但是最不安全）
    //1 将数据发送个leader分区分区写入log文件，leader分确认，就是发送成功
    //-1/all 将数据发送给leader分区写入log文件，每一个follower分区也都要将log文件后然后进行要应达，才算发送成功（对数据安全性比较高的）
    //props.setProperty("acks", "1") //ack应答
    //props.setProperty("retries", "100") //重试次数，默认是无限重启
    //props.setProperty("compression.type", "gzip") //压缩方式，可以使用四种压缩方式'gzip', 'snappy', 'lz4', 'zstd'
    //props.setProperty("max.request.size",  1048576 * 2 + "") //发送数据的大小
    //props.setProperty("request.timeout.ms", "60000") //超时时间

    val topic = "WordCount" //三个分区{0, 1, 2}

    // 2 kafka的生产者
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)
    for (i <- 21 to 50) {
      // 3 封装的对象
      val record = new ProducerRecord[String, String](topic, "kafka," + i)
      producer.send(record) //将将数据发送到Kafka（是一条一条的发送吗？，还是先在客户端缓存，达到一定的大小在批量发送）
      if(i == 35) {
        println(66666)
        producer.flush()
        println(88888)
      }
    }

    println("message send success")

    //producer.flush()
    // 释放资源
    producer.close()
  }
}
