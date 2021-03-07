# flink-day04 课堂笔记

## eventime与watermark

### 流式计算中时间的分类：

![image-20200417114302619](assets/image-20200417114302619.png)

1 eventTime:数据、事件产生的时间，

2 ingestionTime：进入flink/spark的时间

3 processingTime：进入到具体计算的operator的系统时间

分析：

spark streaming中的窗口计算使用的就是processingtime,与事件、数据真实发生的时间无关，就取决于什么到达处理节点；

flink中引入了eventtime机制，就是flink中可以指定窗口计算的时候按照事件时间（事件真实发生的时间）来进行计算。使用eventtime进行计算才是正确，符合数据发生的时间。

需要在env进行设置，同时你需要保证数据（事件）中要有eventtime时间字段：

```scal
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //设置使用事件时间
```



### watermark

按照eventtime进行是不是就高枕无忧了？

数据有可能会延迟产生 使用eventtime进行计算才是合理的；

![image-20200417114644444](assets/image-20200417114644444.png)

问题 二

数据乱序到达的问题，晚（eventime）的数据先到达，早（eventime）的数据后达到

![image-20200417114919977](assets/image-20200417114919977.png)



flink watermark原理参考画图



### flink watermark案例

api介绍

![image-20200417151520276](assets/image-20200417151520276.png)

我们一般选择使用周期性方式生成水印

案例

需求：添加水印统计信号灯通过的汽车数量。

参考代码：

```scala
package cn.itcast.flink.watermark


import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
演示使用周期性方式生成水印
 */
/*
需求：
编写代码, 计算5秒内（滚动时间窗口），每个信号灯汽车数量
信号灯数据(信号ID(String)、通过汽车数量、时间戳(事件时间))，要求添加水印来解决网络延迟问题。
 */
//3. 定义CarWc 样例类
case class CarWc(id: String, num: Int, ts: Long)

object WatermarkDemo {
  /*
  1. 创建流处理运行环境
2. 设置处理时间为EventTime ，设置水印的周期间隔,定期生成水印的时间
3. 定义CarWc 样例类
4. 使用socketstream发送数据
5. 添加水印
   - 允许延迟2秒
   - 在获取水印方法中，打印水印时间、事件时间和当前系统时间
6. 按照用户进行分流
7. 设置5秒的时间窗口
8. 进行聚合计算
9. 打印结果数据
10. 启动执行流处理
   */
  def main(args: Array[String]): Unit = {
    //1 创建流处理运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //2 设置处理时间为事件时间，
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //3 生成水印的周期 默认200ms
    env.getConfig.setAutoWatermarkInterval(200)

    // 默认程序并行度是机器的核数，8个并行度，注意在flink程序中如果是多并行度，水印时间是每个并行度比较最小的值作为当前流的watermark
    env.setParallelism(1)

    //4 添加socketsource
    val socketDs: DataStream[String] = env.socketTextStream("node1", 9999)
    // 5 数据处理之后添加水印
    val carWcDs: DataStream[CarWc] = socketDs.map(
      line => {
        //按照逗号切分数据组成carwc
        val arr = line.split(",")
        CarWc(arr(0), arr(1).trim.toInt, arr(2).trim.toLong)
      }
    )
    // 添加水印 周期性  AssignerWithPeriodicWatermarks 使用其子类 ,构造参数：水印允许的延迟时间,泛型是stream中的数据类型
    val watermarkDs: DataStream[CarWc] = carWcDs.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[CarWc](Time.seconds(2)) {
        // 水印机制是在eventtime基础之上减去一段时间，就是flink允许数据延迟的范围；eventtime是来自数据，flink是不知道eventtime是多少，以及是哪个字段
        //这个方法就是告诉flink你的数据哪个字段是eventime
        override def extractTimestamp(element: CarWc): Long = {
          element.ts
        }
      })
    // 6 设置窗口 5s的滚动窗口
    val windowStream: WindowedStream[CarWc, Tuple, TimeWindow] = watermarkDs.keyBy(0).
      window(TumblingEventTimeWindows.of(Time.seconds(5)))
    // 7 使用apply方法对窗口进行计算
    val windowDs: DataStream[CarWc] = windowStream.apply(
      //泛型：1 carwc,2 carwc,3 tuple,4 timewindow
      new WindowFunction[CarWc, CarWc, Tuple, TimeWindow] {
        //key:tuple,window:当前触发计算的window对象，input:当前窗口的数据，out:计算结果收集器
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[CarWc], out: Collector[CarWc]): Unit = {

          val wc: CarWc = input.reduce(
            (c1, c2) => {
              CarWc(c1.id, c1.num + c2.num, c2.ts) //累加出通过的汽车数量，关于时间在这里我们不关心
            }
          )
          //发送计算结果
          out.collect(wc)
          //获取到窗口开始和结束时间
          println("窗口开始时间》》" + window.getStart + "=====;窗口结束时间》》" + window.getEnd + ";窗口中的数据》》" +
            input.iterator.mkString(","))
        }
      }
    )
    // 打印结果
    windowDs.print()
    // 启动
    env.execute()


  }
}

```



总结：

![image-20200417155924737](assets/image-20200417155924737.png)

注意：

（1）基于事件时间进行计算的时候，判断数据是属于哪个窗口

判断标准：

eventtime >= window -starttime , enenttime <window-endtime

窗口是左闭右开的

[)

（2）窗口开始时间和结束时间的确定：源码解读

![image-20200417160310959](assets/image-20200417160310959.png)

![image-20200417160257500](assets/image-20200417160257500.png)



![image-20200417160642986](assets/image-20200417160642986.png)



窗口开始时间取决于第一条数据的eventtime的值！！

（3）添加水印方式选择的是周期性添加AssignerWithPeriodicWatermarks，传入的是的AssignerWithPeriodicWatermarks子类，BoundedOutOfOrdernessTimestampExtractor。



复杂版本--手动实现 

```scal
package cn.itcast.flink.watermark

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
演示使用周期性方式生成水印 -复杂版本--手动实现watermark机制
 */
/*
需求：
编写代码, 计算5秒内（滚动时间窗口），每个信号灯汽车数量
信号灯数据(信号ID(String)、通过汽车数量、时间戳(事件时间))，要求添加水印来解决网络延迟问题。
 */
//3. 定义CarWc 样例类
case class CarWc(id: String, num: Int, ts: Long)

object WatermarkDemo2 {
  /*
  1. 创建流处理运行环境
2. 设置处理时间为EventTime ，设置水印的周期间隔,定期生成水印的时间
3. 定义CarWc 样例类
4. 使用socketstream发送数据
5. 添加水印
   - 允许延迟2秒
   - 在获取水印方法中，打印水印时间、事件时间和当前系统时间
6. 按照用户进行分流
7. 设置5秒的时间窗口
8. 进行聚合计算
9. 打印结果数据
10. 启动执行流处理
   */
  def main(args: Array[String]): Unit = {
    //1 创建流处理运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //2 设置处理时间为事件时间，
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //3 生成水印的周期 默认200ms
    env.getConfig.setAutoWatermarkInterval(200)

    // 默认程序并行度是机器的核数，8个并行度，注意在flink程序中如果是多并行度，水印时间是每个并行度比较最小的值作为当前流的watermark
    env.setParallelism(1)

    //4 添加socketsource
    val socketDs: DataStream[String] = env.socketTextStream("node1", 9999)
    // 5 数据处理之后添加水印
    val carWcDs: DataStream[CarWc] = socketDs.map(
      line => {
        //按照逗号切分数据组成carwc
        val arr = line.split(",")
        CarWc(arr(0), arr(1).trim.toInt, arr(2).trim.toLong)
      }
    )
    // 5.2 添加水印 周期性  new AssignerWithPeriodicWatermarks
    val watermarkDs: DataStream[CarWc] = carWcDs.assignTimestampsAndWatermarks(
      new AssignerWithPeriodicWatermarks[CarWc] {
        // watermark=eventtime -延迟时间
        // 5.2.1 定义允许延迟的时间 2s
        val delayTime=2000

        //定义当前最大的时间戳
        var currentMaxTimestamp=0L
        /** The timestamp of the last emitted watermark. */
         var lastEmittedWatermark = Long.MinValue
        // todo 获取watermark时间  实现watermark不会倒退
        override def getCurrentWatermark: Watermark = {
          // 计算watermark
          val watermarkTime: Long = currentMaxTimestamp - delayTime
          if (watermarkTime >lastEmittedWatermark){
            lastEmittedWatermark =watermarkTime
          }
          new Watermark(lastEmittedWatermark)
        }
        //todo 抽取时间戳 element:新到达的元素，previousElementTimestamp：之前元素的时间戳
        // 5.2.2 抽取时间戳 计算watermark
        override def extractTimestamp(element: CarWc, previousElementTimestamp: Long): Long = {
          //获取到时间
          //注意的问题：时间倒退的问题：消息过来是乱序的，每次新来的消息时间戳不是一定变大的，所以会导致水印有可能倒退
          var eventTime = element.ts
          if (eventTime >currentMaxTimestamp){  //比较与之前最大的时间戳进行比较
            currentMaxTimestamp =eventTime
          }
          eventTime
        }
      }
      )
    // 6 设置窗口 5s的滚动窗口
    val windowStream: WindowedStream[CarWc, Tuple, TimeWindow] = watermarkDs.keyBy(0).
      window(TumblingEventTimeWindows.of(Time.seconds(5)))
    // 7 使用apply方法对窗口进行计算
    val windowDs: DataStream[CarWc] = windowStream.apply(
      //泛型：1 carwc,2 carwc,3 tuple,4 timewindow
      new WindowFunction[CarWc, CarWc, Tuple, TimeWindow] {
        //key:tuple,window:当前触发计算的window对象，input:当前窗口的数据，out:计算结果收集器
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[CarWc], out: Collector[CarWc]): Unit = {

          val wc: CarWc = input.reduce(
            (c1, c2) => {
              CarWc(c1.id, c1.num + c2.num, c2.ts) //累加出通过的汽车数量，关于时间在这里我们不关心
            }
          )
          //发送计算结果
          out.collect(wc)
          //获取到窗口开始和结束时间
          println("窗口开始时间》》" + window.getStart + "=====;窗口结束时间》》" + window.getEnd + ";窗口中的数据》》" +
            input.iterator.mkString(","))
        }
      }
    )
    // 打印结果
    windowDs.print()
    // 启动
    env.execute()


  }
}

```

注意：

![！！](assets/image-20200417163222305.png)

建议工作中就使用子类方式实现水印即可！

#### 再次理解watermark

基于watermark+eventime只能解决部分数据延迟问题，不能完全解决，对于watermark无法解决的延迟数据，flink默认是丢弃的，如果我们需要保证数据完全不丢失可以再使用allowedlateness+侧道输出来保证。

allowedlateness+侧道输出 API



（1）allowedLateness(lateness: Time) 这种方式设置的允许延迟时间与水印的延迟时间是一个累加的效果，

但是注意这个时间并不会影响窗口触发计算的标准，watermark >=window-endtime就会触发计算，

只是如果这设置了这个时间，窗口不会关闭和销毁而是继续等待我们这种方式设置的时间。

（2）侧道输出 保存极端延迟数据

sideOutputLateData(outputTag: OutputTag[T])：设置侧道输出保存延迟的数据

DataStream.getSideOutput(tag: OutputTag[X]) ：获取其中保存的延迟数据



#### 侧道输出+allowedlateness方案

设置侧道输出与allowedlateness

![image-20200417172907463](assets/image-20200417172907463.png)

获取侧道输出数据

![image-20200417172942245](assets/image-20200417172942245.png)

参考代码：

```scala
package cn.itcast.flink.watermark

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/*
演示使用周期性方式生成水印 -复杂版本--手动实现watermark机制,使用侧道输出+allowedlaten保证数据不丢失
 */
/*
需求：
编写代码, 计算5秒内（滚动时间窗口），每个信号灯汽车数量
信号灯数据(信号ID(String)、通过汽车数量、时间戳(事件时间))，要求添加水印来解决网络延迟问题。
 */
//3. 定义CarWc 样例类
case class CarWc(id: String, num: Int, ts: Long)

object SideOutputWKDemo {
  /*
  1. 创建流处理运行环境
2. 设置处理时间为EventTime ，设置水印的周期间隔,定期生成水印的时间
3. 定义CarWc 样例类
4. 使用socketstream发送数据
5. 添加水印
   - 允许延迟2秒
   - 在获取水印方法中，打印水印时间、事件时间和当前系统时间
6. 按照用户进行分流
7. 设置5秒的时间窗口
8. 进行聚合计算
9. 打印结果数据
10. 启动执行流处理
   */
  def main(args: Array[String]): Unit = {
    //1 创建流处理运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //2 设置处理时间为事件时间，
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //3 生成水印的周期 默认200ms
    env.getConfig.setAutoWatermarkInterval(200)

    // 默认程序并行度是机器的核数，8个并行度，注意在flink程序中如果是多并行度，水印时间是每个并行度比较最小的值作为当前流的watermark
    env.setParallelism(1)

    //4 添加socketsource
    val socketDs: DataStream[String] = env.socketTextStream("node1", 9999)
    // 5 数据处理之后添加水印
    val carWcDs: DataStream[CarWc] = socketDs.map(
      line => {
        //按照逗号切分数据组成carwc
        val arr = line.split(",")
        CarWc(arr(0), arr(1).trim.toInt, arr(2).trim.toLong)
      }
    )
    // 5.2 添加水印 周期性  new AssignerWithPeriodicWatermarks
    val watermarkDs: DataStream[CarWc] = carWcDs.assignTimestampsAndWatermarks(
      new AssignerWithPeriodicWatermarks[CarWc] {
        // watermark=eventtime -延迟时间
        // 5.2.1 定义允许延迟的时间 2s
        val delayTime=2000

        //定义当前最大的时间戳
        var currentMaxTimestamp=0L
        /** The timestamp of the last emitted watermark. */
         var lastEmittedWatermark = Long.MinValue
        // todo 获取watermark时间  实现watermark不会倒退
        override def getCurrentWatermark: Watermark = {
          // 计算watermark
          val watermarkTime: Long = currentMaxTimestamp - delayTime
          if (watermarkTime >lastEmittedWatermark){
            lastEmittedWatermark =watermarkTime
          }
          new Watermark(lastEmittedWatermark)
        }
        //todo 抽取时间戳 element:新到达的元素，previousElementTimestamp：之前元素的时间戳
        // 5.2.2 抽取时间戳 计算watermark
        override def extractTimestamp(element: CarWc, previousElementTimestamp: Long): Long = {
          //获取到时间
          //注意的问题：时间倒退的问题：消息过来是乱序的，每次新来的消息时间戳不是一定变大的，所以会导致水印有可能倒退
          var eventTime = element.ts
          if (eventTime >currentMaxTimestamp){  //比较与之前最大的时间戳进行比较
            currentMaxTimestamp =eventTime
          }
          eventTime
        }
      }
      )
    // 6 设置窗口 5s的滚动窗口
    //准备一个侧道输出对象
    val outputTag: OutputTag[CarWc] = new OutputTag[CarWc]("lateCarwc")
    val windowStream: WindowedStream[CarWc, Tuple, TimeWindow] = watermarkDs.keyBy(0).
      window(TumblingEventTimeWindows.of(Time.seconds(5)))
    //设置允许延迟时间  --》在水印基础上再次增加延迟允许延迟时间
      .allowedLateness(Time.seconds(5))
    //设置侧道输出
      .sideOutputLateData(outputTag)
    // 7 使用apply方法对窗口进行计算
    val windowDs: DataStream[CarWc] = windowStream.apply(
      //泛型：1 carwc,2 carwc,3 tuple,4 timewindow
      new WindowFunction[CarWc, CarWc, Tuple, TimeWindow] {
        //key:tuple,window:当前触发计算的window对象，input:当前窗口的数据，out:计算结果收集器
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[CarWc], out: Collector[CarWc]): Unit = {

          val wc: CarWc = input.reduce(
            (c1, c2) => {
              CarWc(c1.id, c1.num + c2.num, c2.ts) //累加出通过的汽车数量，关于时间在这里我们不关心
            }
          )
          //发送计算结果
          out.collect(wc)
          //获取到窗口开始和结束时间
          println("窗口开始时间》》" + window.getStart + "=====;窗口结束时间》》" + window.getEnd + ";窗口中的数据》》" +
            input.iterator.mkString(","))
        }
      }
    )
    // 打印结果
    windowDs.print()
    //获取侧道输出的数据
    val lateCarWc: DataStream[CarWc] = windowDs.getSideOutput(outputTag)
    lateCarWc.printToErr("侧道输出数据》》")
    // 启动
    env.execute()


  }
}

```

## flink state

### state是什么？

state就是task/operator计算的中间结果，

无状态计算：相同的输入得到相同的结果，只根据输入的数据无需借助其他数据就可以计算出我想要的结果，

有状态计算：相同的输入得到的可能是不同的结果，计算过程会需要中间结果或者历史结果进行联合处理。

![image-20200417180134187](assets/image-20200417180134187.png)

flink的wordcount 可以轻松实现累加效果，就是因为使用了state(keyed state)

![image-20200417180504322](assets/image-20200417180504322.png)



#### flink中state的分类

从是否被flink框架管理：

1 manage state:flink框架帮我们管理，自动内存，序列化等，支持数据结构：value,list,map等 ，manage state 推荐使用

2 raw state：需要自己管理，序列化；byte[], 除非自定义operator时可以考虑使用该种state

从是否与key相关分类：

manage state :

​	keyed state：都是基于keyedstream,只有keyedstream可以使用，与key绑定；

​	operator state：是非keystream时候使用，与operator绑定

raw state:operator state

![image-20200417181542445](assets/image-20200417181542445.png)

##### keyedstate

访问：getruntimecontext访问，要求operator是RichFunction;

数据结构：

valuestate:单一值， 是与key对应，我们无需关注key与state之间的映射关系，kv映射关系flink维护；

liststate:是一个列表结构， add：添加值；,遍历其中数据，get获取值

mapstate:状态值就是一个map结构，put,get，putall等

aggregationstate与reducingstate：存储的都是单值，但是需要提供一个reducefunciton aggregate策略，add添加数据之后其实得到的是之前定义function的计算结果。

##### operator state

访问：无需上下文

数据结构：

liststate,如果不能满足需求可以考虑自己定义一个operator state;

常见keyedstate 的api

![image-20200417182720831](assets/image-20200417182720831.png)



### value state案例

![image-20200417185945435](assets/image-20200417185945435.png)

总结：使用valuestate可以帮助我们存储keyedstream中key对象的value数据，而且无需关心kv之间的映射。

状态使用：

1 定义一个描述器

2 获取一个状态

代码：

```scala
package cn.itcast.flink.state.keyedstate

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/*
 使用ValueState保存中间结果对下面数据求出最大值
 */

object ValueStateDemo {
  def main(args: Array[String]): Unit = {
    /*
     1.获取流处理执行环境
      2.加载数据源 socke数据：k,v
      3.数据分组
      4.数据转换，定义ValueState,保存中间结果
      5.数据打印
      6.触发执行
     */
    //1 创建流处理运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 默认程序并行度是机器的核数
    env.setParallelism(1)
    // 2 使用socket source
    val socketDs: DataStream[String] = env.socketTextStream("node1", 9999)
    // 3 keyed stream 使用keyby进行分组
    val tupleDs: DataStream[(String, Int)] = socketDs.map(line => {
      val arr: Array[String] = line.split(",")
      //转为tuple类型
      (arr(0), arr(1).trim.toInt)
    })
    // 3.1 分组
    val keyStream: KeyedStream[(String, Int), Tuple] = tupleDs.keyBy(0)
    //    keyStream.maxBy(1)
    // 使用valuestate来存储两两比较之后的最大值，新数据到来之后如果比原来的最大值还大则把该值更新为状态值，保证状态中一直存储的是最大值
    //需要通过上下文件来获取keyedstate valuestate
    val maxDs: DataStream[(String, Int)] = keyStream.map(
      // 3.2 使用richfunction操作，需要通过上下文
      new RichMapFunction[(String, Int), (String, Int)] {
        // 3.2.1 声明一个valuestate （不是创建） ,value state无需关心key是谁以及kv之间的映射，flink维护
        var maxValueState: ValueState[Int] = _
        // 3.3 通过上下文才能获取真正的state,上下文件这种操作在执行一次的方法中使用并获取真正的状态对象
        override def open(parameters: Configuration): Unit = {
          // 3.3.1 定义一个state描述器  参数：state的名称，数据类型的字节码文件
          val maxValueDesc: ValueStateDescriptor[Int] = new ValueStateDescriptor[Int]("maxValue", classOf[Int])
          // 3.3.2 根据上下文基于描述器获取state
          maxValueState = getRuntimeContext.getState(maxValueDesc)
        }
        // 3.4 业务逻辑,可以获取到state数据
        override def map(value: (String, Int)): (String, Int) = {
          //value是一条新数据，需要与原来最大值（valuestate）进行比较判断
          // 3.4.1 获取valuestate中的数据
          val maxNumInState: Int = maxValueState.value()
          // 3.4.2 新数据进行比较
          if (value._2 > maxNumInState) { //新数据比之前存储的数据大
            //3.4.3 更新状态中的值
            maxValueState.update(value._2)
          }
          // 3.4.4 返回最大值
          (value._1, maxValueState.value())
        }
      }
    )
    // 4 打印数据
    maxDs.print()
    // 5 启动
    env.execute()
  }
}

```

### map state案例

与valuestate类似，只是更改了状态描述器。

代码

```scala
package cn.itcast.flink.state.keyedstate

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/*
 使用MapState保存中间结果计算分组的和
 */

object MapStateDemo {
  def main(args: Array[String]): Unit = {
    /*
     * 使用MapState保存中间结果对下面数据进行分组求和
      * 1.获取流处理执行环境
      * 2.加载数据源
      * 3.数据分组
      * 4.数据转换，定义MapState,保存中间结果
      * 5.数据打印
      * 6.触发执行
     */
    //1 创建流处理运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 加载数据
    val collectionDs: DataStream[(String, Int)] = env.fromCollection(List(
      ("java", 1),
      ("python", 3),
      ("java", 2),
      ("scala", 2),
      ("python", 1),
      ("java", 1),
      ("scala", 2)))
    // 3 对数据分组
    val keyStream: KeyedStream[(String, Int), Tuple] = collectionDs.keyBy(0)
    // 4 转换 使用mapstate
    // 4.1 mapstate获取与value一样需要上下文，所以我们的转换还的是一个richfunciton
    val mapStateDs: DataStream[(String, Int)] = keyStream.map(
      new RichMapFunction[(String, Int), (String, Int)] {
        // mapstate描述器
        var sumMapState: MapState[String, Int] = null

        // 4.1.1 获取mapstate
        override def open(parameters: Configuration): Unit = {
          // 1 定义一个mapstate描述器
          val mapStateDesc: MapStateDescriptor[String, Int] = new MapStateDescriptor[String, Int]("sumMap",
            //定义typeinformation类型来包装kv数据类型
            TypeInformation.of(classOf[String]), //key的infomation
            TypeInformation.of(classOf[Int])
          )
          // 2 根据描述器获取mapstate
          sumMapState = getRuntimeContext.getMapState(mapStateDesc)
        }

        // 4.1.2 使用mapstate获取历史结果 求和
        override def map(value: (String, Int)): (String, Int) = {
          // 1 获取到新数据的key
          val key: String = value._1
          //2 根据key去获取mapstate中的历史结果,stateValue就是之前数据的累加结果
          val stateValue: Int = sumMapState.get(key)
          sumMapState.put(key, stateValue + value._2)
          // 3 返回结果， 取出mapstate中的value值
          (key, sumMapState.get(key))
        }
      }
    )
    // 4 打印结果
    mapStateDs.print()
    // 5 启动
    env.execute()
  }
}

```

![image-20200417193250629](assets/image-20200417193250629.png)

总结：

上面mapstate的案例也可以通过value实现，

选择不同的数据结构主要是考虑存取数据方便以及结合你的业务。

### operator state

支持数据结构：ListState<T>

这种state是与key无关，只是与operator绑定。

在flink中官方提供的flinkkafkaconsumer中使用的就是operator state来存储消费数据的分区和偏移量数据；

如要使用operator state需要我们实现checkpointedfunction接口，重写其中的两个方法，如下：

![image-20200418171436340](assets/image-20200418171436340.png)



案例： 模仿kafkaconsumer定义一个operator state

需求：自定义一个source 实现checkpointedfunction ，定义一个liststate存储我们source中自定义的一个偏移量数据（每发送一条数据该值（offset)加1）；让程序出现异常看是否能从liststate中恢复该偏移量数据继续发送。

步骤：

   1.获取执行环境

   2.设置检查点机制：路径，重启策略

   3.自定义数据源

​     （1）需要继承SourceFunction和CheckpointedFunction

​     （2）设置listState,通过上下文对象context获取

​     (3)数据处理，保留offset

​     (4)制作快照

​    4.数据打印

​    5.触发执行



参考代码：

```scal
package cn.itcast.flink.state.operatorstate

import java.util
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
/*
 需求：自定义一个source 实现checkpointedfunction ，定义一个liststate存储我们source中自定义的一个偏移量数据
 （每发送一条数据该值（offset)加1）；让程序出现异常看是否能从liststate中恢复该偏移量数据继续发送。
 */

object OperatorStateDemo {
  def main(args: Array[String]): Unit = {
    /*
     * 1.获取执行环境

      2.设置检查点机制：路径，重启策略

      3.自定义数据源

        （1）需要继承SourceFunction和CheckpointedFunction

        （2）设置listState,通过上下文对象context获取

        (3)数据处理，保留offset

        (4)制作快照

       4.数据打印

       5.触发执行

     */
    //1 创建流处理运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //方便观察数据，设置并行度为1
    env.setParallelism(1)
    // 2 设置检查点相关属性，重启策略
    env.enableCheckpointing(1000)  //开启ck,每秒执行一次
    //设置检查点存储数据路径
    env.setStateBackend(new FsStateBackend("hdfs://node1:8020/flink/ck"))
    //任务取消时，保存检查点数据（后续可以从检查点中恢复数据）
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置可以同时进行几个ck任务
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    //固定延迟重启策略: 程序出现异常的时候，重启3次，每次延迟5秒钟重启，超过3次，程序退出
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000))
    // 2 自定义source
    val sourceDs: DataStream[Long] = env.addSource(new MySource())

    // 3 打印数据
    sourceDs.print()
    // 4 启动
    env.execute()
  }
}

// 2 自定义source 实现sourcefunction,以及checkpointedfunction
class MySource extends SourceFunction[Long] with CheckpointedFunction{
  var flag=true
  //定义发送数据的初始值
  var offset=0L
  //定义Liststate
  var offsetListState: ListState[Long]=null
  //2.1 生成数据的方法
  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while(flag){
      //发送数据，实现发送一个数值，每次增加1 ，来表是所谓消费者的偏移量数据,offset应该从offsetListstate中获取，获取不到再从0开始
      // 2.1.2 从liststate中获取offset值
      val listStateIter: util.Iterator[Long] = offsetListState.get().iterator()
      //2.1.3 获取迭代器中第一个即是我们要的offset数据
      if (listStateIter.hasNext){
        offset=listStateIter.next()
      }
      offset +=1
      ctx.collect(offset)
      println("发送数据 offset>>"+offset)
      TimeUnit.SECONDS.sleep(1)
      //设置故障，程序遇到异常
      if (offset %5==0){
        println("程序遇到异常。。。，将要重启。。")
        throw new RuntimeException("程序遇到异常。。。，将要重启。。")
      }
    }
  }

  override def cancel(): Unit = {
    flag=false
  }

  // 2.2.1 定义operatorstate中--liststate相关方法 ck时调用该方法执行状态数据持久化
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
//调用该方法时状态数据已经存入了外部持久化介质中，清空liststate之前的状态数据，把新的状态数据加入liststate
    offsetListState.clear()
    offsetListState.add(offset)
  }
// 2.2..2 定义operatorstate的初始化方法，定义我们liststate的数据结构
  override def initializeState(context: FunctionInitializationContext): Unit = {
    //定义state描述器 泛型：存入liststate中的数据类型，存入offset，所以是Long
    val offsetState: ListStateDescriptor[Long] = new ListStateDescriptor[Long]("offsetState",classOf[Long])
    //获取到想要的operatorstate -->liststate
    offsetListState = context.getOperatorStateStore.getListState(offsetState)

  }
}
```

总结：

1 实现operator state 需要实现ckponitedfunciton 重写 两个方法，一个是初始化状态方法，另一个是进行ck操作时我们把之前状态清空，加入新的状态数据

2 Liststate获取其中数据，使用get获取到iterator，遍历iterator可以获取到其中的数据

### BroadCast state

介绍

flink中我们可以把一个流广播到另一个流中，不是简单的双流join操作，广播流中的数据作为state存在，我们可以在事件流中获取广播流中存在state中的数据，方便我们的处理。

api介绍

![image-20200418180951427](assets/image-20200418180951427.png)

主要是基于流是否是keyedstream来区分不同的操作。

ProcessFunction:状态函数，在flink中状态函数非常有用，我们可以完全自定义如何来处理数据，

processelement,

对于双流connect之后我们可以使用flink提供的keyedBroadcastProcessfunction或者BroadcastStateProcessfunction对合并的进行处理。

适用场景：

需要基于动态更新的规则来处理事件流，我们可以考虑把规则数据作为流广播到事件流中。



#### broadcast state案例

需求分析：

![image-20200418192941146](assets/image-20200418192941146.png)



数据流与广播流数据准备：

事件流：使用kafka发送数据，事件流中的字段：userid,evnetime,type,productid

mysql数据（广播流数据）：userid,username,userage

获取到事件流之需要通过mysql中的数据进行用户数据补全操作，

有哪些实现方式：

1 flink消费kafka数据在datastream中使用mapfunction处理数据时直接查询mysql根据获取的数据来补全，

（性能太差），每次新到数据都要执行查询mysql动作， 考虑使用kv的内存数据库，性能只能说可以接受，

2 flink中 双流join ，datastream直接进行join操作，可以实现，但是如果我们要对mysql中的数据进行更新操作，

需要配置流及时更新到数据，另外双流join操作时key是否能对齐比较麻烦。

**3 双流的connect+broadcast state(广播流)，实现我们两个流之间数据的访问，事件流去获取到广播流中的数据。**



程序步骤：

 1.获取流处理执行环境

​    2.设置kafka配置

​    3.kafka数据转换：process

​    4.自定义source读取获取mysql数据源: (String, (String, Int))

​    5.定义mapState广播变量，获取mysql广播流

​    6.事件流和广播流连接

​    7.业务处理BroadcastProcessFunction，补全数据

编写代码实现案例

processfunction:flink中的状态函数，flink状态编程指就是这个函数，

![image-20200418195252297](assets/image-20200418195252297.png)

此方法还实现了AsyncRichFunction，可以获取到上下文件，我们可以定义keyedstate，

实现基于历史状态数据+定时器的处理逻辑，可以实现类似window的操作。



第一部分：事件流数据使用processfunction转换为tuple

![image-20200418200747029](assets/image-20200418200747029.png)



参考代码：

```sca
package cn.itcast.flink.state.broadcaststate

import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig


/*
假设有这样的一个需求，需要实时过滤出配置中的用户，并在事件流中补全这批用户的基础信息。
 */
object BroadCastStateDemo {
  def main(args: Array[String]): Unit = {
    /*
    步骤：
    1.获取流处理执行环境

    2.设置kafka配置

    3.kafka数据转换：processfunction

    4.自定义source读取获取mysql数据源: (String, (String, Int))

    5.定义mapState广播变量，获取mysql广播流

    6.事件流和广播流连接

    7.业务处理BroadcastProcessFunction，补全数据

     */
    //    1.获取流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 配置消费kafka中事件流的数据source
    val topic = "test"
    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092")
    val kafkaDs = env.addSource(new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), prop))
    // 3 使用processfunction来处理事件流中的数据 json格式数据转为tuple格式
    //3.1 processfunction
    val tupleDs: DataStream[(String, String, String, Int)] = kafkaDs.process(
      // 泛型：in：输入数据类型 String  out：输出数据类型 ,tuple(userid,eventime,type,productid)
      new ProcessFunction[String, (String, String, String, Int)] {
        // 3.2自定义业务处理逻辑 ，value:一条数据， out:数据收集器，数据处理之后可以使用收集器发送出去
        override def processElement(value: String, ctx: ProcessFunction[String, (String, String, String, Int)]#Context,
                                    out: Collector[(String, String, String, Int)]): Unit = {
          // 3.3 把kafka中的json数据转为tuple类型
          val jSONObject = JSON.parseObject(value)
          val userid: String = jSONObject.getString("userID")
          val eventTime: String = jSONObject.getString("eventTime")
          val eventType: String = jSONObject.getString("eventType")
          val productID: Int = jSONObject.getIntValue("productID")
          //3.4 发送数据
          out.collect((userid, eventTime, eventType, productID))
        }
        
       
      }
    )
    // 4 打印数据
    tupleDs.print()
    // 5 启动
    env.execute()

  }
}

```

第二部分

主要完成自定义读取mysql数据，然后使用broadcast广播该流数据。

```sc
package cn.itcast.flink.state.broadcaststate

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig


/*
假设有这样的一个需求，需要实时过滤出配置中的用户，并在事件流中补全这批用户的基础信息。
 */
object BroadCastStateDemo {
  def main(args: Array[String]): Unit = {
    /*
    步骤：
    1.获取流处理执行环境

    2.设置kafka配置

    3.kafka数据转换：processfunction

    4.自定义source读取获取mysql数据源: (String, (String, Int))

    5.定义mapState广播变量，获取mysql广播流

    6.事件流和广播流连接

    7.业务处理BroadcastProcessFunction，补全数据

     */
    //    1.获取流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 配置消费kafka中事件流的数据source
    //设置并行度为1，方便观察数据
    env.setParallelism(1)
    val topic = "test"
    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092")
    val kafkaDs = env.addSource(new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), prop))
    // 3 使用processfunction来处理事件流中的数据 json格式数据转为tuple格式
    //3.1 processfunction
    val tupleDs: DataStream[(String, String, String, Int)] = kafkaDs.process(
      // 泛型：in：输入数据类型 String  out：输出数据类型 ,tuple(userid,eventime,type,productid)
      new ProcessFunction[String, (String, String, String, Int)] {
        // 3.2自定义业务处理逻辑 ，value:一条数据， out:数据收集器，数据处理之后可以使用收集器发送出去
        override def processElement(value: String, ctx: ProcessFunction[String, (String, String, String, Int)]#Context,
                                    out: Collector[(String, String, String, Int)]): Unit = {
          // 3.3 把kafka中的json数据转为tuple类型
          val jSONObject = JSON.parseObject(value)
          val userid: String = jSONObject.getString("userID")
          val eventTime: String = jSONObject.getString("eventTime")
          val eventType: String = jSONObject.getString("eventType")
          val productID: Int = jSONObject.getIntValue("productID")
          //3.4 发送数据
          out.collect((userid, eventTime, eventType, productID))
        }


      }
    )
    // 4  加载用户配置的mysql数据流 自定义实现读取mysql的source
    val mysqlSource: DataStream[(String, (String, Int))] = env.addSource(new MysqlSource)
    // 5 需要把mysqlsource 广播出去，作为broadcaststate来使用 ,广播流直接使用broadcaststate广播，需要提供一个mapstatedescriptor
    val broadcastStateDesc: MapStateDescriptor[String, (String, Int)] = 
      new MapStateDescriptor[String, (String, Int)]("broadcastState",classOf[String],classOf[(String,Int)])
    val broadcastStream: BroadcastStream[(String, (String, Int))] = mysqlSource.broadcast(broadcastStateDesc)
    
    
    
    // 5 启动
    env.execute()

  }
}

// 4  加载用户配置的mysql数据流 自定义实现读取mysql的source  读取mysql返回数据类型：为了后续根据userid查询方便我们设置读取返回类型：
// (userId,(username,userage))
class MysqlSource extends RichSourceFunction[(String, (String, Int))] {
  var conn: Connection = null
  var ps: PreparedStatement = null
  var flag = true

  //打开mysql连接
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager
      .getConnection("jdbc:mysql://node1:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "123456")
    //准备preparestatement
    var sqlString = "select * from user_info"
    ps = conn.prepareStatement(sqlString)
  }

  override def close(): Unit = {
    if (conn != null) {
      conn.close()
    }
    if (ps != null) {
      ps.close()
    }
  }

  //读取数据并发送出去
  override def run(ctx: SourceFunction.SourceContext[(String, (String, Int))]): Unit = {
    while (flag) {
      val result: ResultSet = ps.executeQuery()
      while (result.next()) {
        val userId: String = result.getString("userID")
        val userName: String = result.getString("userName")
        val userAge: Int = result.getInt("userAge")
        // 收集器发送数据
        ctx.collect((userId, (userName, userAge)))
      }
      //休眠1秒
      TimeUnit.SECONDS.sleep(1)
    }
  }

  //取消方法
  override def cancel(): Unit = {
    flag = false
  }
}
```

总结：

如何广播一个流？

datastream.broacast(mapstatedescriptor),

broadcaststate中支持的数据结构都是mapstate.



第三部分：实现对connectstream使用processfunciton进行处理

![image-20200418211733901](assets/image-20200418211733901.png)

完整代码

```scala
package cn.itcast.flink.state.broadcaststate

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{MapStateDescriptor, ReadOnlyBroadcastState}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala.{BroadcastConnectedStream, DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig


/*
假设有这样的一个需求，需要实时过滤出配置中的用户，并在事件流中补全这批用户的基础信息。
 */
object BroadCastStateDemo {
  def main(args: Array[String]): Unit = {
    /*
    步骤：
    1.获取流处理执行环境

    2.设置kafka配置

    3.kafka数据转换：processfunction

    4.自定义source读取获取mysql数据源: (String, (String, Int))

    5.定义mapState广播变量，获取mysql广播流

    6.事件流和广播流连接

    7.业务处理BroadcastProcessFunction，补全数据

     */
    //    1.获取流处理执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2 配置消费kafka中事件流的数据source
    //设置并行度为1，方便观察数据
    env.setParallelism(1)
    val topic = "test"
    val prop = new Properties()
    prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092,node2:9092")
    val kafkaDs = env.addSource(new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), prop))
    // 3 使用processfunction来处理事件流中的数据 json格式数据转为tuple格式
    //3.1 processfunction
    val tupleDs: DataStream[(String, String, String, Int)] = kafkaDs.process(
      // 泛型：in：输入数据类型 String  out：输出数据类型 ,tuple(userid,eventime,type,productid)
      new ProcessFunction[String, (String, String, String, Int)] {
        // 3.2自定义业务处理逻辑 ，value:一条数据， out:数据收集器，数据处理之后可以使用收集器发送出去
        override def processElement(value: String, ctx: ProcessFunction[String, (String, String, String, Int)]#Context,
                                    out: Collector[(String, String, String, Int)]): Unit = {
          // 3.3 把kafka中的json数据转为tuple类型
          val jSONObject = JSON.parseObject(value)
          val userid: String = jSONObject.getString("userID")
          val eventTime: String = jSONObject.getString("eventTime")
          val eventType: String = jSONObject.getString("eventType")
          val productID: Int = jSONObject.getIntValue("productID")
          //3.4 发送数据
          out.collect((userid, eventTime, eventType, productID))
        }


      }
    )
    // 4  加载用户配置的mysql数据流 自定义实现读取mysql的source
    val mysqlSource: DataStream[(String, (String, Int))] = env.addSource(new MysqlSource)
    // 5 需要把mysqlsource 广播出去，作为broadcaststate来使用 ,广播流直接使用broadcaststate广播，需要提供一个mapstatedescriptor
    val broadcastStateDesc: MapStateDescriptor[String, (String, Int)] =
      new MapStateDescriptor[String, (String, Int)]("broadcastState", classOf[String], classOf[(String, Int)])
    val broadcastStream: BroadcastStream[(String, (String, Int))] = mysqlSource.broadcast(broadcastStateDesc)
    // 6 双流的connect  合并流，原来流中数据依然是独立存在
    val connectStream: BroadcastConnectedStream[(String, String, String, Int), (String, (String, Int))] = tupleDs.connect(broadcastStream)
    //7 使用processfunction处理connectstream,自定义BroadcastStateProcessFunction实现在处理事件流数据时获取到广播流中的数据，借助于state实现
    val resDs: DataStream[(String, String, String, Int, String, Int)] = connectStream.process(new MyProcessFunction)
    // 8 打印结果
    resDs.print()
    // 9 启动
    env.execute()

  }
}

//7 使用processfunction处理connectstream,自定义BroadcastStateProcessFunction实现在处理事件流数据时获取到广播流中的数据，借助于state实现
/*
@param <IN1> The input type of the non-broadcast side.事件流中数据类型  (userid,eventime,type,productId)
 * @param <IN2> The input type of the broadcast side. 广播流中数据类型  (userid,(username,userage))
 * @param <OUT> The output type of the operator.  输出的数据类型   (userid,eventime,type,productId,username,userage)
 */

class MyProcessFunction extends BroadcastProcessFunction[(String, String, String, Int), (String, (String, Int)),
  (String, String, String, Int, String, Int)] {
  //broadcaststate描述器
  val broadcastStateDesc: MapStateDescriptor[String, (String, Int)] =
    new MapStateDescriptor[String, (String, Int)]("broadcastState", classOf[String], classOf[(String, Int)])


  //处理事件流中数据的方法  对于广播流数据是只读，不能修改的
  override def processElement(value: (String, String, String, Int), ctx: BroadcastProcessFunction[(String, String, String, Int),
    (String, (String, Int)), (String, String, String, Int, String, Int)]#ReadOnlyContext,
                              out: Collector[(String, String, String, Int, String, Int)]): Unit = {
    //处理事件流中数据如何获取到广播流中的数据呢？借助于state，需要在processBroadCastelement中把广播流数据存入state中，在这个方法中获取数据
    val readOnlyState: ReadOnlyBroadcastState[String, (String, Int)] = ctx.getBroadcastState(broadcastStateDesc)
    //根据userid去state中取出其它数据 ,state中有可能存储该userid数据有可能没有
    val bool = readOnlyState.contains(value._1)
    if (bool) {
      //username,userage
      val tuple: (String, Int) = readOnlyState.get(value._1)
      //补全事件流中的数据
      out.collect((value._1, value._2, value._3, value._4, tuple._1, tuple._2))
    } else {
      //可以丢弃，也可以补null值
    }
  }

  //处理广播流中数据的方法
  override def processBroadcastElement(value: (String, (String, Int)),
                                       ctx: BroadcastProcessFunction[(String, String, String, Int), (String, (String, Int)),
                                         (String, String, String, Int, String, Int)]#Context,
                                       out: Collector[(String, String, String, Int, String, Int)]): Unit = {
    //把广播流中的数据存入state
    //根据mapstate描述器获取broadcaststate数据
    val broadCastState =
    ctx.getBroadcastState(broadcastStateDesc)
    //需要把广播流中的数据存入mapstate中
    broadCastState.put(value._1, value._2)
  }
}

// 4  加载用户配置的mysql数据流 自定义实现读取mysql的source  读取mysql返回数据类型：为了后续根据userid查询方便我们设置读取返回类型：
// (userId,(username,userage))
class MysqlSource extends RichSourceFunction[(String, (String, Int))] {
  var conn: Connection = null
  var ps: PreparedStatement = null
  var flag = true

  //打开mysql连接
  override def open(parameters: Configuration): Unit = {
    conn = DriverManager
      .getConnection("jdbc:mysql://node1:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "123456")
    //准备preparestatement
    var sqlString = "select * from user_info"
    ps = conn.prepareStatement(sqlString)
  }

  override def close(): Unit = {
    if (conn != null) {
      conn.close()
    }
    if (ps != null) {
      ps.close()
    }
  }

  //读取数据并发送出去
  override def run(ctx: SourceFunction.SourceContext[(String, (String, Int))]): Unit = {
    while (flag) {
      val result: ResultSet = ps.executeQuery()
      while (result.next()) {
        val userId: String = result.getString("userID")
        val userName: String = result.getString("userName")
        val userAge: Int = result.getInt("userAge")
        // 收集器发送数据
        ctx.collect((userId, (userName, userAge)))
      }
      //休眠1秒
      TimeUnit.SECONDS.sleep(1)
    }
  }

  //取消方法
  override def cancel(): Unit = {
    flag = false
  }
}
```

总结：

1 使用broadcastprocessfunction对connectedstream进行处理，通过重写两个方法实现在事件流中获取到广播流数据进行处理，processelement:只能读取状态数据,processbroadcastelement：可以修改状态数据



整个案例：

1 了解什么是broadcaststate,在流式计算中我们可以把一个流广播给另外一个流并且带有state的特性

2 这种方式对于我们广播流数据我们可以进行更新操作，事件流可以捕捉到广播流数据的变化。

3 针对这种方式要求广播流的吞吐不能太大，广播流state数据是存储在内存中。