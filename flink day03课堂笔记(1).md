# flink day03课堂笔记

## datastream 开发

入门案例-wordcount实现

参考代码

```scala
package cn.itcast.stream

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/*
流式计算的wordcount
 */
object WordCountDemo {
  def main(args: Array[String]): Unit = {
    /*
     1 创建一个流处理的运行环境
     2 构建socket source数据源
     3 接收到的数据转为（单词，1）
     4 对元组使用keyby分组（类似于批处理中的groupby）
     5 使用窗口进行5s的计算
     6 sum出单词数量
     7 打印输出
     8 执行
     */
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 构建socket source数据源 ;socket参数：ip,port ,返回值类型是datastream
    val socketDs: DataStream[String] = env.socketTextStream("node1", 9999)
    // 3 接收到的数据转为（单词，1）
    val tupleDs: DataStream[(String, Int)] = socketDs.flatMap(_.split(" ")).map((_, 1))
    // 4 对元组使用keyby分组（类似于批处理中的groupby）
    val keyedStream: KeyedStream[(String, Int), Tuple] = tupleDs.keyBy(0)

    //    5 使用窗口进行5s的计算,没5s计算一次
    val windowStream: WindowedStream[(String, Int), Tuple, TimeWindow] = keyedStream.timeWindow(Time.seconds(5))
    //    6 sum出单词数量
    val resDs: DataStream[(String, Int)] = windowStream.sum(1)
    //    7 打印输出
    resDs.print()
    //      8 执行
    env.execute()

  }
}

```

![image-20200416122244378](assets/image-20200416122244378.png)

与批处理对比：

1 运行环境对象不同，streamexecutionenviroment

2 有些算子是不同

3 程序是一直运行，除非我们手动停止。



## flink stream source简介

flink的stream程序都是通过addSource(sourcefunction)来添加数据源，我们可以自定义数据源，通过继承ParallelSourceFunction RichParallelSourceFunction 来实现自己的数据源。

### 常见source

#### 基于集合

参考代码

```scala
package cn.itcast.stream.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

import scala.collection.immutable.{Queue, Stack}
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
/*
演示基于集合创建datastream
 */
object CollectionSourceDemo {
  def main(args: Array[String]): Unit = {
    // 1 获取流处理运行环境
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    //0.用element创建DataStream(fromElements)
    val ds0: DataStream[String] = senv.fromElements("spark", "flink")
    ds0.print()

    //1.用Tuple创建DataStream(fromElements)
    val ds1: DataStream[(Int, String)] = senv.fromElements((1, "spark"), (2, "flink"))
    ds1.print()

    //2.用Array创建DataStream
    val ds2: DataStream[String] = senv.fromCollection(Array("spark", "flink"))
    ds2.print()

    //3.用ArrayBuffer创建DataStream
    val ds3: DataStream[String] = senv.fromCollection(ArrayBuffer("spark", "flink"))
    ds3.print()

    //4.用List创建DataStream
    val ds4: DataStream[String] = senv.fromCollection(List("spark", "flink"))
    ds4.print()

    //5.用List创建DataStream
    val ds5: DataStream[String] = senv.fromCollection(ListBuffer("spark", "flink"))
    ds5.print()

    //6.用Vector创建DataStream
    val ds6: DataStream[String] = senv.fromCollection(Vector("spark", "flink"))
    ds6.print()

    //7.用Queue创建DataStream
    val ds7: DataStream[String] = senv.fromCollection(Queue("spark", "flink"))
    ds7.print()

    //8.用Stack创建DataStream
    val ds8: DataStream[String] = senv.fromCollection(Stack("spark", "flink"))
    ds8.print()

    //9.用Stream创建DataStream（Stream相当于lazy List，避免在中间过程中生成不必要的集合）
    val ds9: DataStream[String] = senv.fromCollection(Stream("spark", "flink"))
    ds9.print()

    //10.用Seq创建DataStream
    val ds10: DataStream[String] = senv.fromCollection(Seq("spark", "flink"))
    ds10.print()

    //11.用Set创建DataStream(不支持)
    //val ds11: DataStream[String] = senv.fromCollection(Set("spark", "flink"))
    //ds11.print()

    //12.用Iterable创建DataStream(不支持)
    //val ds12: DataStream[String] = senv.fromCollection(Iterable("spark", "flink"))
    //ds12.print()

    //13.用ArraySeq创建DataStream
    val ds13: DataStream[String] = senv.fromCollection(mutable.ArraySeq("spark", "flink"))
    ds13.print()

    //14.用ArrayStack创建DataStream
    val ds14: DataStream[String] = senv.fromCollection(mutable.ArrayStack("spark", "flink"))
    ds14.print()

    //15.用Map创建DataStream(不支持)
    //val ds15: DataStream[(Int, String)] = senv.fromCollection(Map(1 -> "spark", 2 -> "flink"))
    //ds15.print()

    //16.用Range创建DataStream
    val ds16: DataStream[Int] = senv.fromCollection(Range(1, 9))
    ds16.print()

    //17.用fromElements创建DataStream
    val ds17: DataStream[Long] = senv.generateSequence(1, 9)
    ds17.print()
    // 执行
    senv.execute()
  }
}

```

#### 基于文件

参考代码

```scala
//TODO 2.基于文件的source（File-based-source）
//0.创建运行环境
val env = StreamExecutionEnvironment.getExecutionEnvironment
//TODO 1.读取本地文件
val text1 = env.readTextFile("data2.csv")
text1.print()
//TODO 2.读取hdfs文件
val text2 = env.readTextFile("hdfs://hadoop01:9000/input/flink/README.txt")
text2.print()
env.execute()
```

#### socket stream

```scala
senv.socketTextStream(ip,port)
```

#### custom source

flink stream中我们可以通过实现sourcefunction或者实现parallesourcefunction来定义我们自己的数据源方法，然后通过senv.addSource(自定义sourcefunction),就可以读取数据进行转换处理

##### sourceFunction

参考源码中的实现逻辑：

![image-20200416124655677](assets/image-20200416124655677.png)



自定义非并行数据源代码实现：

```scala
package cn.itcast.stream.source.customsource

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/*
演示自定义非并行数据源实现
 */
object MySourceNoParalle {
  def main(args: Array[String]): Unit = {
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 添加自定义的数据源
    val myDs: DataStream[Long] = env.addSource(new MyNoParalleSourceFunction).setParallelism(1)
    //3 打印数据
    myDs.print()
    //启动
    env.execute()
  }
}

//SourceFunction泛型是我们自定义source的返回数据类型
class MyNoParalleSourceFunction extends SourceFunction[Long] {
  var ele: Long = 0
  var isRunning = true
  //发送数据，生产数据的方法
  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning) {
      ele += 1
      //通过上下文对象发送数据
      ctx.collect(ele)
      //降低发送速度
      Thread.sleep(1000)
    }
  }

  // 取消方法，取消是通过控制一个变量来影响run方法中的while循环
  override def cancel(): Unit = {
    isRunning = false //取消发送数据
  }
}
```

总结：

1 创建一个class实现sourcefunction接口

2 从写run方法，定义生产数据的业务逻辑，重写cancle方法

3 senv.addSource()添加自定义的source

##### ParallelSourceFunction

并行数据源

只需要 上面非并行自定义数据源实现的接口改为ParallelSourceFunction即可。

参考代码：

```scala
package cn.itcast.stream.source.customsource

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
演示自定义并行数据源实现
 */
object MyParallelSource {
  def main(args: Array[String]): Unit = {
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 添加自定义的数据源
    val myDs: DataStream[Long] = env.addSource(new MyNoParalleSourceFunction).setParallelism(3)
    //3 打印数据
    myDs.print()
    //启动
    env.execute()
  }
}

//ParallelSourceFunction泛型是我们自定义source的返回数据类型
class MyNoParalleSourceFunction extends ParallelSourceFunction[Long] {
  var ele: Long = 0
  var isRunning = true
  //发送数据，生产数据的方法
  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning) {
      ele += 1
      //通过上下文对象发送数据
      ctx.collect(ele)
      //降低发送速度
      Thread.sleep(1000)
    }
  }

  // 取消方法，取消是通过控制一个变量来影响run方法中的while循环
  override def cancel(): Unit = {
    isRunning = false //取消发送数据
  }
}
```

并行数据源的效果：

1 可以在source operator设置大于1的并行度

2 发送数据是重复。

![image-20200416133346469](assets/image-20200416133346469.png)

##### RichParallelSourceFunction

这是一个富有的并行数据源，可以提供open,close等方法（如果操作数据库可以实现在open或者close打开关闭连接）

参考代码：

```scala
package cn.itcast.stream.source.customsource

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
演示自定义非并行数据源实现
 */
object MyRichParallelSource {
  def main(args: Array[String]): Unit = {
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 添加自定义的数据源
    val myDs: DataStream[Long] = env.addSource(new MyRichParalleSourceFunction).setParallelism(3)
    //3 打印数据
    myDs.print()
    //启动
    env.execute()
  }
}

//SourceFunction泛型是我们自定义source的返回数据类型
class MyRichParalleSourceFunction extends RichParallelSourceFunction[Long] {
//todo 初始化方法比如打开数据库连接等昂贵操作
  override def open(parameters: Configuration): Unit = super.open(parameters)
//todo 关闭连接
  override def close(): Unit = super.close()

  var ele: Long = 0
  var isRunning = true
  //发送数据，生产数据的方法
  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning) {
      ele += 1
      //通过上下文对象发送数据
      ctx.collect(ele)
      //降低发送速度
      Thread.sleep(1000)
    }
  }

  // 取消方法，取消是通过控制一个变量来影响run方法中的while循环
  override def cancel(): Unit = {
    isRunning = false //取消发送数据
  }
}
```



非并行数据源：sourceFunction, source不能设置大于1的并行度，效率会比较低

并行数据源：ParallelSourceFunction，source可以设置大于1的并行度，效率会更高

富有的并行数据源：RichParallelSourceFunction，source可以设置大于1的并行度，此外还提供了open,close等高校的方法，效率会更高

##### 生成订单数据自定义source练习

参考代码

```sc
package cn.itcast.stream.source.customsource

import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import scala.util.Random

/*
自定义数据源，练习 生成订单数据
 */

//订单信息(订单ID、用户ID、订单金额、时间戳)
case class Order(id: String, userId: Int, money: Long, createTime: Long)

object OrderCustomSource {
  def main(args: Array[String]): Unit = {
    /*
    1. 创建订单样例类
    2. 获取流处理环境
    3. 创建自定义数据源
       - 循环1000次
       - 随机构建订单信息
       - 上下文收集数据
       - 每隔一秒执行一次循环
    4. 打印数据
    5. 执行任务
     */
    //1  获取流处理环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 加载自定义的order数据源,RichParallelSourceFunction泛型是生产的数据类型，order
    val orderDs: DataStream[Order] = env.addSource(new RichParallelSourceFunction[Order] {
      var isRunning = true

      //2.1生成订单数据方法
      override def run(ctx: SourceFunction.SourceContext[Order]): Unit = {
        //2.1.1 生成订单 业务逻辑
        while (isRunning) {
          //orderid
          val orderId = UUID.randomUUID().toString
          //userid
          val userId = Random.nextInt(3)
          //money
          val money = Random.nextInt(101)
          //createTime
          val createTime = System.currentTimeMillis()
          ctx.collect(Order(orderId, userId, money, createTime))
          //每隔一秒中执行一次
          TimeUnit.SECONDS.sleep(1)
        }
      }

      //2.2 取消数据的生成方法
      override def cancel(): Unit = {
        isRunning = false
      }
    }).setParallelism(1)
    //3 打印数据
    orderDs.print()
    // 4 启动
    env.execute()


  }
}

```

##### 自定义mysqlsource

选择使用RichParallelSourceFunction接口作为我们要实现的接口，可以利用其提供的open和close打开和关闭mysql的链接。

![image-20200416142922820](assets/image-20200416142922820.png)

参考代码

```scala
package cn.itcast.stream.source.customsource

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.concurrent.TimeUnit

import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
演示自定义并行数据源读取mysql
 */

//定义student 样例类
case class Student(id: Int, name: String, age: Int)

object MysqlRichParallelSource {
  def main(args: Array[String]): Unit = {
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 添加自定义的数据源
    val stuDs: DataStream[Student] = env.addSource(new MysqlRichParalleleSource).setParallelism(1)
    //3 打印数据
    stuDs.print()
    //4 启动
    env.execute()
  }
}

// 2自定义mysql并行数据源
class MysqlRichParalleleSource extends RichParallelSourceFunction[Student] {
  var ps: PreparedStatement = null
  var connection: Connection = null

  //2.1 开启mysql连接
  override def open(parameters: Configuration): Unit = {
    //驱动方式
    connection = DriverManager.getConnection("jdbc:mysql://node1:3306/test", "root", "123456")
    //准备sql语句查询表中全部数据
    var sql = "select id ,name,age from t_student";
    //准备执行语句对象
    ps = connection.prepareStatement(sql)
  }

  //2.3 释放资源，关闭连接
  override def close(): Unit = {
    if (connection != null) {
      connection.close()
    }
    if (ps != null) ps.close()
  }


  var isRunning = true

  // 2.2 读取mysql数据
  override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
    while (isRunning) {
      //读取mysql中的数据
      val result: ResultSet = ps.executeQuery()
      while (result.next()) {
        val userId = result.getInt("id")
        val name = result.getString("name")
        val age = result.getInt("age")
        //收集并发送
        ctx.collect(Student(userId, name, age))

      }

      //休眠5s,执行一次
      TimeUnit.SECONDS.sleep(5)
    }
  }

  //取消方法
  override def cancel(): Unit = {
    isRunning = false
  }
}
```

##### flink kafka source

flink框架提供了flinkkafkaconsumer011进行kafka数据的读取，

需要设置的一些参数：

```properties
1.主题名称/主题名称列表
2.DeserializationSchema / KeyedDeserializationSchema用于反序列化来自Kafka的数据
3.bootstrap.servers(以逗号分隔的Kafka机器位置)
4.group.id消费者群组的ID 
flink的ck机制必须要开启，才能保证一致性语义容错性有保证
env.enableCheckpointing(1000) // checkpoint every 1000 msecs
flink动态分区检测：
properties.setProperty("flink.partition-discovery.interval-millis", "5000")
flink程序source并行度与kafka中分区数量的关系
最好能保证一个source并行度负责一个kafka的分区数据，
一一对应。

```

如何使用flinkkafkaconsumer011读取数据？



```scala
package cn.itcast.stream.source.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig

/*
验证flinkkafkaconsumer如何消费kafka中的数据
 */
object TestFlinkKafkaConsumer {
  def main(args: Array[String]): Unit = {
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 添加自定义的数据源 ,泛型限定了从kafka读取数据的类型
    //2.1 构建properties对象
    val prop = new Properties()
    //kafka 集群地址
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092")
    //消费者组
    prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink")
    //动态分区检测
    prop.setProperty("flink.partition-discovery.interval-millis", "5000")
    //设置kv的反序列化使用的类
    prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    //设置默认消费的便宜量起始值
    prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest") //从最新处消费
    //定义topic
    val topic = "test"

    //获得了kafkaconsumer对象
    val flinkKafkaConsumer: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), prop)
    val kafkaDs: DataStream[String] = env.addSource(flinkKafkaConsumer)
    //3 打印数据
    kafkaDs.print()
    //4 启动
    env.execute()
  }
}

```

上面代码中配置的是最基本必要属性，如果有更加复杂要求需要再添加新的属性：

课件代码：

```sca
package cn.itcast.stream

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.CommonClientConfigs


object StreamKafkaSource {
  def main(args: Array[String]): Unit = {
    //1.准备环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(1000) // checkpoint every 1000 msecs

    //2.准备kafka连接参数
    val props = new Properties()
    props.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,"node1:9092,node02:9092,node03:9092")
    props.setProperty("group.id", "flink")
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("auto.offset.reset", "latest")
    props.setProperty("flink.partition-discovery.interval-millis", "5000")//动态感知kafka主题分区的变化
    val topic = "flink_kafka"

    //3.创建kafka数据源
    //FlinkKafkaConsumer从kafka获取的每条消息都会通过DeserialzationSchema的T deserialize(byte[] message)反序列化处理
    //SimpleStringSchema可以将Kafka的字节消息反序列化为Flink的字符串对象
    //JSONDeserializationSchema(只反序列化value)/JSONKeyValueDeserializationSchema可以把序列化后的Json反序列化成ObjectNode，ObjectNode可以通过objectNode.get(“field”).as(Int/String/…)() 来访问指定的字段
    //TypeInformationSerializationSchema/TypeInformationKeyValueSerializationSchema基于Flink的TypeInformation来创建schema，这种反序列化对于读写均是Flink的场景会比其他通用的序列化方式带来更高的性能。
    val kafkaConsumerSource = new FlinkKafkaConsumer011[String](topic,new SimpleStringSchema(),props)

    //4.指定消费者参数
    kafkaConsumerSource.setCommitOffsetsOnCheckpoints(true)//默认为true
    kafkaConsumerSource.setStartFromGroupOffsets()
    //默认值，从当前消费组记录的偏移量接着上次的开始消费,如果没有找到，
则使用consumer的properties的auto.offset.reset设置的策略
    //kafkaConsumerSource.setStartFromEarliest()//从最早的数据开始消费
    //kafkaConsumerSource.setStartFromLatest//从最新的数据开始消费
    //kafkaConsumerSource.setStartFromTimestamp(1568908800000L)//根据指定的时间戳消费数据
    /*val offsets = new util.HashMap[KafkaTopicPartition, java.lang.Long]()//key是KafkaTopicPartition(topic，分区id),value是偏移量
    offsets.put(new KafkaTopicPartition(topic, 0), 110L)
    offsets.put(new KafkaTopicPartition(topic, 1), 119L)
    offsets.put(new KafkaTopicPartition(topic, 2), 120L)*/
    //kafkaConsumerSource.setStartFromSpecificOffsets(offsets)//从指定的具体位置开始消费

    //5.从kafka数据源获取数据
    import org.apache.flink.api.scala._
    val kafkaData: DataStream[String] = env.addSource(kafkaConsumerSource)

    //6.处理输出数据
    kafkaData.print()

    //7.启动执行
    env.execute()
  }
}
```

## flink transformation

### keyby 

类似批处理中的group by算子，对数据流按照指定规则进行分区。

参考代码

```scala
package cn.itcast.stream.transformation

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
演示flink中keyby的用法
实现单词统计
 */
object KeyByDemo {
  def main(args: Array[String]): Unit = {
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 加载socketstream
    val socketDs: DataStream[String] = env.socketTextStream("node1", 9999)
    //3 对接收到的数据切分压平转成单词，1的元组
    val wordAndOneDs: DataStream[(String, Int)] = socketDs.flatMap(_.split(" ")).map(_ -> 1)
    // 4 按照单词分组
//    wordAndOneDs.keyBy(_._1).sum(1).print()
    wordAndOneDs.keyBy(0).sum(1).print()
    //5 启动
    env.execute()
  }
}

```

### connect 

两个可以合并为一个流，数据类型可以不同，union必须要求数据类型一致才能union.

```scala
package cn.itcast.stream.transformation

import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
/*
演示flink中connect的用法，把两个数据流连接到一起
需求：
创建两个流，一个产生数值，一个产生字符串数据
使用connect连接两个流，结果如何
 */
object ConnectDemo {
  def main(args: Array[String]): Unit = {
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 加载source
    val numDs: DataStream[Long] = env.addSource(new MyNumberSource)
    val strDs = env.addSource(new MyStrSource)
    // 3 使用connect进行两个连接操作
     val connectedDs: ConnectedStreams[Long, String] = numDs.connect(strDs)
    //传递两个函数，分别处理数据
    val resDs: DataStream[String] = connectedDs.map(l=>"long"+l, s=>"string"+s)
    //connect意义在哪里呢？只是把两个合并为一个，但是处理业务逻辑都是按照自己的方法处理？connect之后两条流可以共享状态数据
    resDs.print()
    //5 启动
    env.execute()
  }
}

//自定义产生递增的数字 第一个数据源
class MyNumberSource extends SourceFunction[Long]{
  var flag=true
  var num=1L
  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while(flag){
      num +=1
      ctx.collect(num)
      TimeUnit.SECONDS.sleep(1)
    }
  }

  override def cancel(): Unit = {
    flag=false
  }
}

// 自定义产生从1开始递增字符串
class MyStrSource extends SourceFunction[String]{
  var flag=true
  var num=1L
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while(flag){
      num +=1
      ctx.collect("str"+num)
      TimeUnit.SECONDS.sleep(1)
    }
  }

  override def cancel(): Unit = {
    flag=false
  }
}
```

### split+select

可以实现对数据流的切分，使用split切分流，通过select获取到切分之后的流：

参考代码：

```sca
package cn.itcast.stream.transformation

import org.apache.flink.streaming.api.scala.{DataStream, SplitStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
/*
示例
加载本地集合(1,2,3,4,5,6), 使用split进行数据分流,分为奇数和偶数. 并打印奇数结果
 */
object SplitSelectDemo {
  def main(args: Array[String]): Unit = {
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //2 加载source
    val numDs: DataStream[Int] = env.fromCollection(List(1,2,3,4,5,6))
    // 3 转换 使用split把数据流中的数据分为奇数和偶数
    val splitStream: SplitStream[Int] = numDs.split(
      item => {
        //模以2
        var res = item % 2
        //模式匹配的方式
        res match {
          case 0 => List("even") //偶数  even与odd只是名称，代表数据流的名称，但是必须放在list集合
          case 1 => List("odd") //奇数
        }
      }
    )
    // 4 从splitStream中获取奇数流和偶数流
    val evenDs: DataStream[Int] = splitStream.select("even")
    val oddDs = splitStream.select("odd")
    val allDs: DataStream[Int] = splitStream.select("even","odd")
    // 5打印结果
//    evenDs.print()

//    oddDs.print()
        allDs.print()

    // 5启动程序
    env.execute()
  }
}

```

## flink stream sink

自定义mysql sink

大致流程：

1 创建class实现RichSinkFunction,

2 重写invoke方法，执行我们真正写入逻辑的方法

3 利用open和close方法实现对数据库连接的管理

参考代码：

```scala
package cn.itcast.stream.sink

import java.sql.{Connection, DriverManager, PreparedStatement}


import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/*
flink程序计算结果保存到mysql中
 */
//定义student case class
case class Student(id: Int, name: String, age: Int)

object SinkToMysqlDemo {
  def main(args: Array[String]): Unit = {
    /*
    读取数据然后直接写入mysql,需要自己实现mysql sinkfunction
    自定义class实现RichSinkFunction重写open,invoke,close方法
     */
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 加载source
    val stuDs: DataStream[Student] = env.fromElements(Student(0, "tony", 18))
    // 3 直接写出到mysql
    stuDs.addSink(new MySqlSinkFunction)
    // 4 执行
    env.execute()
  }
}

//准备自定义mysql sinkfunciton
class MySqlSinkFunction extends RichSinkFunction[Student] {
  var ps: PreparedStatement = null
  var connection: Connection = null
  // 3.1 打开连接
  override def open(parameters: Configuration): Unit = {
    // 3.1.1驱动方式
    connection = DriverManager.getConnection("jdbc:mysql://node1:3306/test", "root", "123456")
    //3.1.2准备sql语句插入数据到mysql表中
    var sql = "insert into t_student(name,age) values(?,?)";
    //3.1.3准备执行语句对象
    ps = connection.prepareStatement(sql)
  }
  //关闭连接
  override def close(): Unit = {
    if (connection != null) {
      connection.close()
    }
    if (ps != null) ps.close()
  }

  // 3.2 这个方法负责写入数据到mysql中,value就是上游datastream传入需要写入mysql的数据
  override def invoke(value: Student, context: SinkFunction.Context[_]): Unit = {
    // 3.2.1设置参数
    ps.setString(1, value.name)
    ps.setInt(2, value.age)
    //3.2.2执行插入动作
    ps.executeUpdate()
  }

}
```

### flinkkafkaproducer

利用flink提供的flinkkafkaproducer实现写出数据到kafka中，

flinkkafkaproducer的构造参数需要指定序列化的schema，keyedSerializationWrapper使用这个约束！！

参考代码：

```sca
package cn.itcast.stream.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper
import org.apache.kafka.clients.producer.ProducerConfig

/*
flink程序计算结果保存到kafka
 */
//定义student case class

case class Student(id: Int, name: String, age: Int)

object SinkToKafkaDemo {
  def main(args: Array[String]): Unit = {
    /*
    flink读取数据然后把数据写入kafka中
     */
    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 加载source
    val stuDs: DataStream[Student] = env.fromElements(Student(0, "tony", 18))
    // 3 直接使用flinkkafkaproducer来生产数据到kafka
    //3.1 准备一个flinkkafkaproducer对象
    //写入kafka的数据类型
    //param1
    var topic="test"
    //param2
    val keyedSerializationWrapper: KeyedSerializationSchemaWrapper[String] =
      new KeyedSerializationSchemaWrapper(new SimpleStringSchema())
   //param3
    val prop = new Properties()
    prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node1:9092,node2:9092")
    val flinkKafkaProducer: FlinkKafkaProducer011[String] = new FlinkKafkaProducer011[String](
      topic,keyedSerializationWrapper,prop)
    // 4 sink 操作
    stuDs.map(_.toString).addSink(flinkKafkaProducer)
    // 5 执行
    env.execute()
  }
}


```



### redissink

借助于flink提供的redissink我们可以方便的把数据写入redis中，

使用redissink需要提供两个东西

1 连接redis的配置文件

2 提供一个redisMapper的实现类的对象，其中重写三个方法，分别定义了你的操作的数据结构，写入的key和value是什么

参考代码：

```sca
package cn.itcast.stream.sink
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/*
flink程序计算结果保存到redis，使用flink提供的redissink
 */

/*
从socket接收数据然后计算出单词的次数，最终使用redissink写数据到redis中
 */

object SinkToRedisDemo {
  def main(args: Array[String]): Unit = {

    //1 创建一个流处理的运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 加载socket数据，
    val socketDs: DataStream[String] = env.socketTextStream("node1",9999)
    /*
    单词计数的逻辑
     */
    // 3 转换
    val resDs: DataStream[(String, Int)] = socketDs.flatMap(_.split(" ")).map(_ ->1).keyBy(0).sum(1)
    // 4 sink 操作 使用redissink
    // 4.1 redissink的构造：1 需要redis配置文件（连接信息），2 redismapper对象
    //4.1.1 jedisconfig
    val config: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("node2").setPort(6379).build()

    resDs.addSink(new RedisSink[(String, Int)](config,new MyRedisMapper))

    // 5 执行
    env.execute()
  }
}
//4.1.2 redismapper的对象,泛型就是写入redis的数据类型
class MyRedisMapper extends RedisMapper[(String, Int)]{
  //获取命令描述器，确定数据结构,我们使用hash结构
  override def getCommandDescription: RedisCommandDescription = {
    //指定使用hset命令，并提供hash结构的第一个key
    new RedisCommandDescription(RedisCommand.HSET,"REDISSINK")
  }

  //指定你的key
  override def getKeyFromData(data: (String, Int)): String = {
    data._1
  }
//指定你的value
  override def getValueFromData(data: (String, Int)): String = {
    data._2.toString
  }
}

```



