# day01课堂笔记

## flink简介

### flink引入

大数据技术框架发展阶段

总共有四代，mr-->DAG框架（tez）--->Spark流批处理框架，内存计算（伪实时）-->flink流批处理，内存计算（真正的实时计算）

flink vs spark

![image-20200415105119792](assets/image-20200415105119792.png)

### 什么是flink

flink是一个分布式，高性能，随时可用的以及准确的流处理计算框架，

flink可以对**无界数据**（流处理）和**有界数据**（批处理）进行有**状态计算**（flink天生支持状态计算）的分布式，高性能的计算框架。

### flink流处理特性

![image-20200415110408410](assets/image-20200415110408410.png)

### flink的基石

flink的四大基石：**checkpoint,state,time,window**

checkpoint:基于chandy-lamport算法实现分布式计算任务的一致性语义；

state:flink中的状态机制，flink天生支持state,state可以认为程序的中间计算结果或者是历史计算结果；

time:flink中支持基于事件时间和处理时间进行计算，spark streaming只能按照process time进行处理；

基于事件时间的计算我们可以解决数据迟到和乱序等问题。

window:flink提供了更多丰富的window,基于时间，基于数量，session window,同样支持滚动和滑动窗口的计算。

### flink流处理和批处理



流处理：无界，实时性有要求，只需对经过程序的每条数据进行处理

批处理：有界，持久，需要对全部数据进行访问处理；

spark vs flink

spark：spark生态中是把所有的计算都当做批处理，spark streaming中流处理本质上也是批处理（micro batch）;

flink：flink中是把批处理（有界数据集的处理）看成是一个特殊的流处理场景；flink中所有计算都是流式计算；

flink中技术栈

![image-20200415112706997](assets/image-20200415112706997.png)

## flink架构体系

### flink中重要角色

JobManager:类似spark中master，负责资源申请，任务分发，任务调度执行，checkpoint的协调执行；可以搭建HA，双master。

TaskManager:类似spark中的worker，负责任务的执行，基于dataflow(spark中DAG)划分出的task;与jobmanager保持心跳，汇报任务状态。

![image-20200415121624466](assets/image-20200415121624466.png)

### 无界数据和有界数据

无界数据流：数据流是有一个开始但是没有结束；

有界数据流：数据流是有一个明确的开始和结束，数据流是有边界的。

flink处理流批处理的思想是：

flink支持的runtime(core 分布式流计算)支持的是无界数据流，但是对flink来说可以支持批处理，只是从数据流上来说把有界数据流只是无界数据流的一个特例，无界数据流只要添加上边界就是有界数据流。

### flink编程模型

flink提供了四种编程模型，分别应对我们不同的场景：

![image-20200415123517084](assets/image-20200415123517084.png)

flink中四种api可以混合使用，无缝的切换。

从数据结构和api层面比对flink和spark

spark vs flink

![image-20200415123930994](assets/image-20200415123930994.png)

## flink集群搭建

### flink的安装模式

三种：

local:单机模式，尽量不使用

standalone:flink自带集群，资源管理由flink集群管理

flink on yarn: 把资源管理交给yarn实现。

![image-20200415124521431](assets/image-20200415124521431.png)

安装环境准备：

jdk1.8及以上版本，免密登录；

flink的安装包：

flink 1.7.2版本，从资料中获取安装包

#### local模式 很少使用

a 上传安装包然后解压到指定目录,注意修改所属用户和用户组

```shell
tar -zxvf flink-1.7.2-bin-hadoop27-scala_2.11.tgz 
mv flink-1.7.2 flink
chown -R root:root flink
```

b 去flink的bin目录下启动shell交互式窗口

```shell
bin/start-scala-shell.sh local
```

c 提交一个任务

```scala
benv.readTextFile("/root/words.txt").flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1).print()
```



启动scala-shell的现象flink准备了benv,senv,分别是批处理和流处理程序入口对象

![image-20200415141047021](assets/image-20200415141047021.png)

单节点的flink集群

a 直接启动

```shell
bin/start-cluster.sh
```

验证这两个进程是否存在：

![image-20200415141609336](assets/image-20200415141609336.png)

c flink web ui

```shell
http://node1:8081
```

![image-20200415142136876](assets/image-20200415142136876.png)

d 提交任务到flink 单节点集群:统计/root/words.txt中的单词数量，（准备数据文件）

```shell
 /export/servers/flink/bin/flink run /export/servers/flink/examples/batch/WordCount.jar --input /root/words.txt --output /root/out2
```

注意：

自己练习如果来回切换模式时可能会遇到提交任务报错的情况：

如失败需删除之前的运行信息

rm -rf /tmp/.yarn-properties-root

e 停止集群

```shell
bin/stop-cluster.sh
```



####  standalone模式

原理：

![image-20200415150822314](assets/image-20200415150822314.png)

a 修改配置文件 conf/flink-conf.yaml

```pro
jobmanager.rpc.address: node1
jobmanager.rpc.port: 6123
jobmanager.heap.size: 1024
taskmanager.heap.size: 1024
taskmanager.numberOfTaskSlots: 2
taskmanager.memory.preallocate: false
parallelism.default: 1
jobmanager.web.port: 8081
taskmanager.tmp.dirs: /export/servers/flink/tmp
web.submit.enable: true
```

b 修改master文件 conf/master

```pro
node1:8081
```

c 修改conf目录下slave文件

```pro
node1
node2
node3
```

d 配置hadoop_conf_dir到/etc/profile中，是flink on yarn的时候使用

e 分发flink目录到其它节点

```shell
 scp -r /export/servers/flink node2:/export/servers/flink
 scp -r /export/servers/flink node3:/export/servers/flink
scp -r /etc/profile node2:/etc/profile
 scp -r /etc/profile node3:/etc/profile
```

f 启动集群

```shell
bin/start-cluster.sh 停止 bin/stop-cluster.sh
```

单独启动jobmanager或者taskmanager

```shell
bin/jobmanager.sh start/stop
bin/taskmanager.sh start/stop
```

h提交任务到standalone集群

```shell
/export/servers/flink/bin/flink run  /export/servers/flink/examples/batch/WordCount.jar 
--input hdfs://node1:8020/wordcount/input/words.txt --output hdfs://node1:8020/wordcount/output/result.txt  --parallelism 2
```

注意：使用的数据文件是hdfs上，不能是本地文件路径，因为会找不到文件。

##### standalone HA集群搭建

解决standalone集群的单点故障问题，所以搭建HA集群。

原理：

![image-20200415153743317](assets/image-20200415153743317.png)

引入zookeeper来完成双主节点，主从切换工作。

具体步骤：

a 停止原先standalone集群

```shell
bin/stop-cluster.sh
```

b 修改conf/flink-conf.yaml

```pro
state.backend: filesystem
state.backend.fs.checkpointdir: hdfs://node1:8020/flink-checkpoints
high-availability: zookeeper
high-availability.storageDir: hdfs://node1:8020/flink/ha/
high-availability.zookeeper.quorum: node1:2181,node2:2181,node3:2181
high-availability.zookeeper.client.acl: open
```

配置的解释：

```pro
#开启HA，使用文件系统作为快照存储
state.backend: filesystem

#启用检查点，可以将快照保存到HDFS
state.backend.fs.checkpointdir: hdfs://node1:8020/flink-checkpoints

#使用zookeeper搭建高可用
high-availability: zookeeper

# 存储JobManager的元数据到HDFS
high-availability.storageDir: hdfs://node1:8020/flink/ha/

# 配置ZK集群地址
high-availability.zookeeper.quorum: node1:2181,node2:2181,node3:2181

# 默认是 open，如果 zookeeper security 启用了更改成 creator
high-availability.zookeeper.client.acl: open

# 设置savepoints 的默认目标目录(可选)
# state.savepoints.dir: hdfs://namenode-host:port/flink-checkpoints

# 用于启用/禁用增量 checkpoints 的标志
# state.backend.incremental: false
```

c 配置master

```pro
node1:8081
node2:8081
```

d 分发master,flink-conf.yaml

e 在node2节点上，修改flink-conf.yaml中jobmanager.rpc.address: node2

f 启动HA集群

```shell
bin/start-cluster.sh
```

h 测试

杀死active的jobmanager,然后看standby是否会切换为active状态。

### 重点：flink on yarn

flink on yarn 企业生产环境运行flink任务大多数的选择

好处：集群资源由yarn集群统一调度和管理，提高利用率，flink中jobmanager的高可用操作就由yarn集群来管理实现。

准备工作：

主要是在yarn-site.xml中配置关闭内存校验

```pro
<property>
    <name>yarn.nodemanager.pmem-check-enabled</name>
    <value>false</value>
</property>
<property>
    <name>yarn.nodemanager.vmem-check-enabled</name>
    <value>false</value>
</property>
```

否则flink任务可能会因为内存超标而被yarn集群主动杀死

flink on yarn 两种模式

![image-20200415172546708](assets/image-20200415172546708.png)

#### session 会话模式

![image-20200415172832111](assets/image-20200415172832111.png)

使用yarn-session.sh命令申请资源初始化一个flink集群

```shell
bin/yarn-session.sh -n 2 -tm 800 -s 1 -d
```

\# -n 表示申请2个容器，这里指的就是多少个taskmanager

\# -s 表示每个TaskManager的slots数量

\# -tm 表示每个TaskManager的内存大小

\# -d 表示以后台程序方式运行

使用yarn-session.sh --help 查看可用参数：

```pro
Usage:
   Required
     -n,--container <arg>   Number of YARN container to allocate (=Number of Task Managers)
   Optional
     -D <property=value>             use value for given property
     -d,--detached                   If present, runs the job in detached mode
     -h,--help                       Help for the Yarn session CLI.
     -id,--applicationId <arg>       Attach to running YARN session
     -j,--jar <arg>                  Path to Flink jar file
     -jm,--jobManagerMemory <arg>    Memory for JobManager Container with optional unit (default: MB)
     -m,--jobmanager <arg>           Address of the JobManager (master) to which to connect. Use this flag to connect to a different JobManager than the one specified in the configuration.
     -n,--container <arg>            Number of YARN container to allocate (=Number of Task Managers)
     -nl,--nodeLabel <arg>           Specify YARN node label for the YARN application
     -nm,--name <arg>                Set a custom name for the application on YARN
     -q,--query                      Display available YARN resources (memory, cores)
     -qu,--queue <arg>               Specify YARN queue.
     -s,--slots <arg>                Number of slots per TaskManager
     -sae,--shutdownOnAttachedExit   If the job is submitted in attached mode, perform a best-effort cluster shutdown when the CLI is terminated abruptly, e.g., in response to a user interrupt, such
                                     as typing Ctrl + C.
     -st,--streaming                 Start Flink in streaming mode
     -t,--ship <arg>                 Ship files in the specified directory (t for transfer)
     -tm,--taskManagerMemory <arg>   Memory per TaskManager Container with optional unit (default: MB)
     -yd,--yarndetached              If present, runs the job in detached mode (deprecated; use non-YARN specific option instead)
     -z,--zookeeperNamespace <arg>   Namespace to create the Zookeeper sub-paths for high availability mode
```



yarn集群中运行的任务：

![image-20200415173428942](assets/image-20200415173428942.png)

提交任务

flink run

```shell
/export/servers/flink/bin/flink run /export/servers/flink/examples/batch/WordCount.jar
```

停止 flink on yarn 会话模式中的flink集群

```shell
yarn application -kill appid
```

会话模式这种方式的优缺点：

缺点：1 会一直有一个程序运行在yarn集群中，不管有没有任务提交执行，浪费资源，

优点：flink 集群环境是提前准备好的不需要为每个作业单独创建flink环境

适用场景：大量的小作业的时候可以考虑使用这种方式

#### job分离模式



flink run -m yarn-cluster --help;可用参数：

```pro
Options for yarn-cluster mode:
     -d,--detached                        If present, runs the job in detached
                                          mode
     -m,--jobmanager <arg>                Address of the JobManager (master) to
                                          which to connect. Use this flag to
                                          connect to a different JobManager than
                                          the one specified in the
                                          configuration.
     -sae,--shutdownOnAttachedExit        If the job is submitted in attached
                                          mode, perform a best-effort cluster
                                          shutdown when the CLI is terminated
                                          abruptly, e.g., in response to a user
                                          interrupt, such as typing Ctrl + C.
     -yD <property=value>                 use value for given property
     -yd,--yarndetached                   If present, runs the job in detached
                                          mode (deprecated; use non-YARN
                                          specific option instead)
     -yh,--yarnhelp                       Help for the Yarn session CLI.
     -yid,--yarnapplicationId <arg>       Attach to running YARN session
     -yj,--yarnjar <arg>                  Path to Flink jar file
     -yjm,--yarnjobManagerMemory <arg>    Memory for JobManager Container with
                                          optional unit (default: MB)
     -yn,--yarncontainer <arg>            Number of YARN container to allocate
                                          (=Number of Task Managers)
     -ynl,--yarnnodeLabel <arg>           Specify YARN node label for the YARN
                                          application
     -ynm,--yarnname <arg>                Set a custom name for the application
                                          on YARN
     -yq,--yarnquery                      Display available YARN resources
                                          (memory, cores)
     -yqu,--yarnqueue <arg>               Specify YARN queue.
     -ys,--yarnslots <arg>                Number of slots per TaskManager
     -yst,--yarnstreaming                 Start Flink in streaming mode
     -yt,--yarnship <arg>                 Ship files in the specified directory
                                          (t for transfer)
     -ytm,--yarntaskManagerMemory <arg>   Memory per TaskManager Container with
                                          optional unit (default: MB)
     -yz,--yarnzookeeperNamespace <arg>   Namespace to create the Zookeeper
                                          sub-paths for high availability mode
     -z,--zookeeperNamespace <arg>        Namespace to create the Zookeeper
                                          sub-paths for high availability mode
```



直接提交任务到yarn即可：

```shell
bin/flink run -m yarn-cluster -yn 2 -yjm 1024 -ytm 1024 /export/servers/flink/examples/batch/WordCount.jar
```

yjm:jobmanager内存

ytm：taskmanager内存

ys：taskmanager slot

yn:taskmanger数量



提交任务之后会在yarn集群按照我们的配置初始化一个flink集群，运行我们提交的作业，作业执行完成之后就释放资源关闭掉flink集群，把资源还给yarn集群。

总结：

优点：随到随用，只有任务需要运行时才会开启flink集群；运行完就关闭释放资源，资源利用更合理；

缺点：对于小作业不太友好，

适用场景：适合大作业，长时间运行的大作业。

## flink 运行架构

### flink的编程模型

![image-20200415212320252](assets/image-20200415212320252.png)

### flink中的并行流

![image-20200415213039762](assets/image-20200415213039762.png)

flink中streamdataflow实际是并行化的，

operator并行化也就是有多个并行度，每个并行度就是一个operator subtask;

stream 并行化，会产生stream partition;

flink中operator之间数据是如何分发的？

两种模式：

one to one:一对一模式，上下游算子并行度一致并且数据没有类似shuffle的分发，就保持上游每个streampartition中数据的特性（排序）传递给下游某个分区。

redistributing:重新分区，类似spark中的shuffle操作，数据会在上下游算子不同的subtask中分散。

#### flink中的task和operator chain

![image-20200415215251110](assets/image-20200415215251110.png)

flink中把onetoone的operator可以合并为一个operator chain,operator chain他的某个并行度就是一个subtask，

flink中真正调度的任务就是operator chain的subtask.

#### flink 调度和执行

![image-20200415215707073](assets/image-20200415215707073.png)

jobclient:用户编写的代码，flink的客户端封装好的提交任务的客户端；

主要作用：提交任务，不是flink内部的一个角色。接收用户编写的代码，创建streamdataflow，提交给jobmanager，接收任务的执行结果并返回给客户；

jobmanager:负责接收任务，对任务进行优化，并调度和执行任务；主要由调度器和checkpoint coordinator（ck协调器）

taskmanger:从jobmanager中接收task,部署到自己的slot中并执行，tm实际执行任务都是以线程执行（更轻量级），

tm中有配置好的slot,每个slot都可以执行task.

#### slot(槽)和slot sharing（槽共享）

slot:是flink中从资源层面进行调度的单位，

特点：slot是会平均划分当前tm中内存，flink程序的最大并行度就是所有tm中的slot的数量，（我们flink控制可以接收的任务数量就是通过slot数量来实现）

slot数量如何确定：保持和tm中的cpu核数一样，保证任务执行的性能。

slot实际是任务执行的真正角色。

slot sharing:槽共享，每个slot都可以接收当前作业的不同的子任务，这样充分利用了当前所有slot来提高并行度。

![image-20200415221557613](assets/image-20200415221557613.png)

## flink程序入门案例

使用scala代码来编写flink程序，虽然flink的源码是java但是也有部分scala代码（scala与java代码混编），使用scala编写程序会比较简洁方便。

1 创建project，创建的是父子工程，pom依赖都在父工程中

2 准备一个log4j.properties

```pro
log4j.rootLogger=INFO, console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{HH:mm:ss,SSS} %-5p %-60c %x - %m%n
```

3 编写wordcount代码

步骤：

1. 获得一个execution environment，

2. 加载/创建初始数据，

3. 指定这些数据的转换，

4. 指定将计算结果放在哪里，

5. 触发程序执行



参考代码：

```scala
package cn.itcast.flink.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem

/*
使用flink批处理进行单词计数
 */
object WordCountDemo {
  def main(args: Array[String]): Unit = {
    /*
    1.获得一个execution environment，
    2.加载/创建初始数据，
    3.指定这些数据的转换，
    4.指定将计算结果放在哪里，
    5.触发程序执行
     */
    //  1.获得一个execution environment， 批处理程序入口对象
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //设置全局并行度为1，
    env.setParallelism(1)
    // 2.加载/创建初始数据
    val sourceDs: DataSet[String] = env.fromElements("Apache Flink is an open source platform for " +
      "distributed stream and batch data processing",
      "Flink’s core is a streaming dataflow engine that provides data distribution")
    // 大致思路：对每行语句按照空格进行切分，切分之后组成（单词，1）tuple,按照单词分组最后进行聚合计算
    // 3.指定这些数据的转换， transformation
    val wordsDs: DataSet[String] = sourceDs.flatMap(_.split(" "))
    //(单词，1)
    val wordAndOneDs: DataSet[(String, Int)] = wordsDs.map((_, 1))
    val groupDs: GroupedDataSet[(String, Int)] = wordAndOneDs.groupBy(0)
    //聚合
    val aggDs: AggregateDataSet[(String, Int)] = groupDs.sum(1)
    // 4.指定将计算结果放在哪里，
    aggDs.writeAsText("hdfs://node1:8020/wc/out1", FileSystem.WriteMode.OVERWRITE)
    //关于默认的并行度：默认获取的是当前机器的cpu核数是8，所以有8个结果文件，
    // 5 触发程序执行
    env.execute()
  }
}

```



提交任务到flink集群或者on yarn模式运行

1 打包程序

2 上传程序到linux中

3 on yarn模式，使用flink run 命令提交任务

```shell
flink run -m yarn-cluster -yn 2 -yjm 1024 -ytm 1024 -c cn.itcast.flink.batch.WordCountDemo /root/wc.jar
```



