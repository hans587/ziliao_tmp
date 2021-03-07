# flink day02课堂笔记

## datasource

批处理中常见是两类source 

### 基于集合

1.使用env.fromElements()支持Tuple，自定义对象等复合形式。

2.使用env.fromCollection()支持多种Collection的具体类型

3.使用env.generateSequence()支持创建基于Sequence的DataSet

参考代码：

```scala
package cn.itcast.batch.source

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/*
演示flink中dataset的常见datasource
 */
object DataSourceDemo {
  def main(args: Array[String]): Unit = {
    /*
    dataset api中datasource主要有两类
    1.基于集合
    2.基于文件
     */
    //1 获取executionenviroment
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 2 source操作
    // 2.1     1.基于集合
    /*
    1.使用env.fromElements()支持Tuple，自定义对象等复合形式。
    2.使用env.fromCollection()支持多种Collection的具体类型
    3.使用env.generateSequence()支持创建基于Sequence的DataSet
     */
    // 使用env.fromElements()
    val eleDs: DataSet[String] = env.fromElements("spark", "hadoop", "flink")

    // 使用env.fromCollection()
    val collDs: DataSet[String] = env.fromCollection(Array("spark", "hadoop", "flink"))
    //使用env.generateSequence()
    val seqDs: DataSet[Long] = env.generateSequence(1, 9)

    // 3 转换 可以没有转换
    //4 sink 输出
    eleDs.print()
    collDs.print()
    seqDs.print()

    // 5 启动  在批处理中： 如果sink操作是'count()', 'collect()', or 'print()',最后不需要执行execute操作，否则会报错
    //    env.execute()
  }

}

```

完整版本：

```scala
package cn.itcast.batch.source

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/*
基于集合创建dataset 完整版
 */
object DataSourceDemo2 {
  def main(args: Array[String]): Unit = {
    //获取env
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    //0.用element创建DataSet(fromElements)
    val ds0: DataSet[String] = env.fromElements("spark", "flink")
    ds0.print()

    //1.用Tuple创建DataSet(fromElements)
    val ds1: DataSet[(Int, String)] = env.fromElements((1, "spark"), (2, "flink"))
    ds1.print()

    //2.用Array创建DataSet
    val ds2: DataSet[String] = env.fromCollection(Array("spark", "flink"))
    ds2.print()

    //3.用ArrayBuffer创建DataSet
    val ds3: DataSet[String] = env.fromCollection(ArrayBuffer("spark", "flink"))
    ds3.print()

    //4.用List创建DataSet
    val ds4: DataSet[String] = env.fromCollection(List("spark", "flink"))
    ds4.print()

    //5.用ListBuffer创建DataSet
    val ds5: DataSet[String] = env.fromCollection(ListBuffer("spark", "flink"))
    ds5.print()

    //6.用Vector创建DataSet
    val ds6: DataSet[String] = env.fromCollection(Vector("spark", "flink"))
    ds6.print()

    //7.用Queue创建DataSet
    val ds7: DataSet[String] = env.fromCollection(mutable.Queue("spark", "flink"))
    ds7.print()

    //8.用Stack创建DataSet
    val ds8: DataSet[String] = env.fromCollection(mutable.Stack("spark", "flink"))
    ds8.print()

    //9.用Stream创建DataSet(Stream相当于lazy List，避免在中间过程中生成不必要的集合)
    val ds9: DataSet[String] = env.fromCollection(Stream("spark", "flink"))
    ds9.print()

    //10.用Seq创建DataSet
    val ds10: DataSet[String] = env.fromCollection(Seq("spark", "flink"))
    ds10.print()

    //11.用Set创建DataSet
    val ds11: DataSet[String] = env.fromCollection(Set("spark", "flink"))
    ds11.print()

    //12.用Iterable创建DataSet
    val ds12: DataSet[String] = env.fromCollection(Iterable("spark", "flink"))
    ds12.print()

    //13.用ArraySeq创建DataSet
    val ds13: DataSet[String] = env.fromCollection(mutable.ArraySeq("spark", "flink"))
    ds13.print()

    //14.用ArrayStack创建DataSet
    val ds14: DataSet[String] = env.fromCollection(mutable.ArrayStack("spark", "flink"))
    ds14.print()

    //15.用Map创建DataSet
    val ds15: DataSet[(Int, String)] = env.fromCollection(Map(1 -> "spark", 2 -> "flink"))
    ds15.print()

    //16.用Range创建DataSet
    val ds16: DataSet[Int] = env.fromCollection(Range(1, 9))
    ds16.print()

    //17.用fromElements创建DataSet
    val ds17: DataSet[Long] = env.generateSequence(1, 9)
    ds17.print()
  }
}


```





### 基于文件

1. 读取本地文件数据 readTextFile

2. 读取HDFS文件数据

3. 读取CSV文件数据

4. 读取压缩文件

5. 遍历目录

   

参考代码：

```scala
package cn.itcast.batch.source

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/*
演示flink 批处理基于文件创建dataset
 */
object DataSourceDemo3 {
  def main(args: Array[String]): Unit = {
    //1 获取env
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 2 基于文件来创建dataset
    //2.1读取本地文本文件
    val wordsDs: DataSet[String] = env.readTextFile("E:/data/words.txt")
    // 2.2读取hdfs文件
    val hdfsDs: DataSet[String] = env.readTextFile("hdfs://node1:8020/wordcount/input/words.txt")
    // 2.3读取csv文件
    //读取csv文件需要准备一个case class
    case class Subject(id: Int, name: String)
    val subjectDs: DataSet[Subject] = env.readCsvFile[Subject]("E:/data/subject.csv")
    // 2.4 读取压缩文件
    val compressDs: DataSet[String] = env.readTextFile("E:/data/wordcount.txt.gz")
    // 2.5 遍历读取文件夹数据
    val conf = new Configuration()
    conf.setBoolean("recursive.file.enumeration",true)
    val folderDs: DataSet[String] = env.readTextFile("E:/data/wc/").withParameters(conf)
    //打印输出结果
//    wordsDs.print()
//    print("------------------------------------------")
//    hdfsDs.print()
//    println("=====================")
//    subjectDs.print()
//    println("=====================")
//    compressDs.print()
//    println("===============")
    folderDs.print()
  }
}

```

注意：

1 读取压缩文件，对于某些压缩文件flink可以直接读取，不能并行读取

![image-20200416014354417](assets/image-20200416014354417.png)

2 如果读取的是文件夹，想要遍历读取需要设置属性；recursive.file.enumeration进行递归读取



## transforma

### map&mappartition

参考代码：

```scala
package cn.itcast.batch.transformation

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/*
演示flink map与mappartition的operator
 */
/*
需求：
示例
使用map操作，将以下数据转换为一个scala的样例类。
"1,张三", "2,李四", "3,王五", "4,赵六"
 */
object MapAndMapPartitionDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2 source 加载数据
    val sourceDs: DataSet[String] = env.fromCollection(List("1,张三", "2,李四", "3,王五", "4,赵六"))

    // 3 转换操作
    // 3.1 定义case class
    case class Student(id: Int, name: String)
    // 3.2 map操作
    val stuDs: DataSet[Student] = sourceDs.map(
      line => {
        val arr: Array[String] = line.split(",")
        Student(arr(0).toInt, arr(1))
      }
    )
    // 3.3 mappartition操作
    val stuDs2: DataSet[Student] = sourceDs.mapPartition(
      
      iter => { //迭代器
        //todo 做一些昂贵的动作，比如开启连接
        //遍历迭代器数据转为case class类型然后返回
        iter.map(
          it => {
            val arr: Array[String] = it.split(",")
            Student(arr(0).toInt, arr(1))
          }
        )
        //todo 做一些昂贵的动作，关闭连接
      }
    )

    // 4 输出 (如果直接打印无需进行启动，执行execute)
    stuDs.print()
    println("====================")
    stuDs2.print()
    // 5 执行
  }
}

```

总结：

map与mappartition最终效果实际是一样的，但是对于mappartition可以让我们有机会对整个分区的数据看做一个整体进行处理，此外还给我们创建了针对当前分区只需做一次的昂贵动作的机会。





### flatMap

![image-20200416020516644](assets/image-20200416020516644.png)

针对需求如果是数据变多的时候就要考虑是不是要使用flatmap进行操作，注意flatMap返回值类型要是一个可迭代类型；

```scal
package cn.itcast.batch.transformation

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/*
演示flink flatMap的operator
 */
/*
需求：
示例
分别将以下数据，转换成国家、省份、城市三个维度的数据。
将以下数据
    张三,中国,江西省,南昌市
    李四,中国,河北省,石家庄市
转换为
    (张三,中国)
    (张三,中国,江西省)
    (张三,中国,江西省,南昌市)
    (李四,中国)
    (李四,中国,河北省)
    (李四,中国,河北省,石家庄市)
 */
object FlatMapDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2 source 加载数据
    val sourceDs: DataSet[String] = env.fromCollection(List(
      "张三,中国,江西省,南昌市",
      "李四,中国,河北省,石家庄市"
    ))

    // 3 转换操作 使用flatMap:分为map和flat,先执行map操作针对每个数据切分，切分后按照要求组成所谓元组数据（3）装入list中，最后执行flat操作去掉
    //list的外壳
    val faltMapDs: DataSet[Product with Serializable] = sourceDs.flatMap(
      line => {
        // 3.1对数据进行切分操作
        val arr: Array[String] = line.split(",")
        // 3.2 组装数据装入list中
        List(
          //(张三,中国)
          (arr(0), arr(1)),
          // (张三,中国,江西省)
          (arr(0), arr(1), arr(2)),
          // (张三,中国,江西省,南昌市)
          (arr(0), arr(1), arr(2), arr(3))
        )
      }
    )


    // 4 输出 (如果直接打印无需进行启动，执行execute)
    faltMapDs.print()
    // 5 执行
  }
}

```

### filter

对数据进行过滤操作，保留下来结果为true的数据

参考代码：

```scala
package cn.itcast.batch.transformation

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/*
演示flink filter的operator
 */
/*
需求：
示例：
过滤出来以下以长度>4的单词。
"hadoop", "hive", "spark", "flink"
 */
object FilterDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2 source 加载数据
    val wordsDs: DataSet[String] = env.fromCollection(List("hadoop", "hive", "spark", "flink"))

    // 3 转换操作 使用filter过滤单词长度大于4的单词
    val moreThan4Words: DataSet[String] = wordsDs.filter(_.length > 4)

    // 4 输出 (如果直接打印无需进行启动，执行execute)
    moreThan4Words.print()
    // 5 执行
  }
}

```

### reduce操作

reduce聚合操作的算子，

注意针对groupby之后的数据流我们可以使用reduce进行聚合，不用考虑按照那种方式分组，reduce都可以实现；

但是如果想要使用sum（）进行聚合前面分组指定key必须是按照索引才可以。

参考代码：

```sacala
package cn.itcast.batch.transformation

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/*
演示flink filter的operator
 */
/*
需求：
示例1
请将以下元组数据，使用reduce操作聚合成一个最终结果
 ("java" , 1) , ("java", 1) ,("java" , 1)
将上传元素数据转换为("java",3)
示例2
请将以下元组数据，下按照单词使用groupBy进行分组，再使用reduce操作聚合成一个最终结果
("java" , 1) , ("java", 1) ,("scala" , 1)
转换为
("java", 2), ("scala", 1)
 */
object ReduceDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2 source 加载数据
    //    val wordsDs = env.fromCollection(List(("java", 1), ("java", 1), ("java", 1)))
    val wordsDs = env.fromCollection(List(("java", 1), ("java", 1), ("scala", 1)))
    //    // 3 转换操作 使用reduce计算单词次数
    //    val resultDs: DataSet[(String, Int)] = wordsDs.reduce(
    //      (w1, w2) => { // w1是一个初始值，第一次的时候是：（单词，0），之后都是之前的累加结果
    //        //3.1 进行次数累加
    //        (w1._1, w1._2 + w2._2)
    //      }
    //    )

    //    // 3 转换操作，需要对单词进行分组，分组之后再进行次数的累加
    val groupDs: GroupedDataSet[(String, Int)] = wordsDs.groupBy(_._1)
    // 3.2 进行次数累加
    val resultDs: DataSet[(String, Int)] = groupDs.reduce(
      (w1, w2) => { // w1是一个初始值，第一次的时候是：（单词，0），之后都是之前的累加结果
        //3.1 进行次数累加
        (w1._1, w1._2 + w2._2)
      }
    )
    // 3 转换操作，按照索引分组，使用sum操作
    //    wordsDs.groupBy(0).reduce(
    //      (w1, w2) => { // w1是一个初始值，第一次的时候是：（单词，0），之后都是之前的累加结果
    //                //3.1 进行次数累加
    //                (w1._1, w1._2 + w2._2)
    //              }
    //    ).print()
    // 这种方式不允许，会报错：ava.lang.UnsupportedOperationException: Aggregate does not support grouping with KeySelector functions, yet.
    //    wordsDs.groupBy(_._1).sum(1).print()
    // 4 输出 (如果直接打印无需进行启动，执行execute)
    resultDs.print()
    // 5 执行
  }
}

```



### reduceGroup

原理分析：

![image-20200416025256248](assets/image-20200416025256248.png)

参考代码：

```scala
package cn.itcast.batch.transformation

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/*
演示flink filter的operator
 */
/*
需求：

示例
请将以下元组数据，下按照单词使用groupBy进行分组，再使用reduce操作聚合成一个最终结果
("java" , 1) , ("java", 1) ,("scala" , 1)
转换为
("java", 2), ("scala", 1)
 */
object ReduceGroupDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2 source 加载数据
    //    val wordsDs = env.fromCollection(List(("java", 1), ("java", 1), ("java", 1)))
    val wordsDs = env.fromCollection(List(("java", 1), ("java", 1), ("scala", 1)))

    // 3 转换 使用reducegroup实现单词计数
    val groupDs: GroupedDataSet[(String, Int)] = wordsDs.groupBy(_._1)
    val resultDs: DataSet[(String, Int)] = groupDs.reduceGroup(
      iter => {
        //参数是一个迭代器
        iter.reduce(//再对迭代器进行reduce聚合操作
          (w1, w2) => (w1._1, w1._2 + w2._2)
        )
      }
    )

    // 4 输出 (如果直接打印无需进行启动，执行execute)
    resultDs.print()
    // 5 执行
  }
}

```



### aggregate操作

aggregate只能作用于元组类型的数据，并且对分组方式有要求只能是按照索引或者字段名称方式分组的聚合计算。

参考代码：

```scala
package cn.itcast.batch.transformation

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/*
演示flink filter的operator
 */
/*
需求：

示例
请将以下元组数据，下按照单词使用groupBy进行分组，再使用reduce操作聚合成一个最终结果
("java" , 1) , ("java", 1) ,("scala" , 1)
转换为
("java", 2), ("scala", 1)
 */
object AggregateDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2 source 加载数据
    //    val wordsDs = env.fromCollection(List(("java", 1), ("java", 1), ("java", 1)))
    val wordsDs = env.fromCollection(List(("java", 1), ("java", 1), ("scala", 1)))

    // 3 转换 使用aggregate实现单词计数
//   val groupDs: GroupedDataSet[(String, Int)] = wordsDs.groupBy(_._1) //aggregation聚合方式不支持这种分组方式
    val groupDs: GroupedDataSet[(String, Int)] = wordsDs.groupBy(0)
    val resultDs: AggregateDataSet[(String, Int)] = groupDs.aggregate(Aggregations.SUM,1)

    // 4 输出 (如果直接打印无需进行启动，执行execute)
    resultDs.print()
    // 5 执行
  }
}

```

### distinct

对数据进行去重操作，可以指定对某个字段进行去重

参考代码：

```scala
package cn.itcast.batch.transformation

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/*
演示flink distinct的operator
 */
/*
需求：

示例
请将以下元组数据，对数据进行去重操作
("java" , 1) , ("java", 1) ,("scala" , 1)
转换为
("java", 2), ("scala", 1)
 */
object DistinctDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2 source 加载数据
    val wordsDs = env.fromCollection(List(("java", 1), ("java", 2), ("scala", 1)))

    // 3 转换 使用distinct实现去重
//    wordsDs.distinct().print() //是对整个元组进行去重
    wordsDs.distinct(0).print()  //指定按照某个字段进行去重操作

    // 4 输出 (如果直接打印无需进行启动，执行execute)
//    resultDs.print()
    // 5 执行
  }
}

```

### join操作

类似于sql中的inner join，只展示join成功的数据。

参考代码：

```scala
package cn.itcast.batch.transformation

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/*
演示flink join的operator
 */
/*
需求：
示例
有两个csv文件，有一个为score.csv，一个为subject.csv，分别保存了成绩数据以及学科数据。
需要将这两个数据连接到一起，然后打印出来。
 */
//定义case class
// 成绩  学生id,学生的姓名，学科id,分数
case class Score(id: Int, name: String, subjectId: Int, score: Double)

//学科 学科id,学科名称
case class Subject(id: Int, name: String)

object JoinDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2 source 加载数据
    val scoreDs: DataSet[Score] = env.readCsvFile[Score]("E:\\data\\score.csv")
    val sbujectDs: DataSet[Subject] = env.readCsvFile[Subject]("E:\\data\\subject.csv")
    // 3 转换 使用join实现两个dataset关联
    val joinDs: JoinDataSet[Score, Subject] = scoreDs.join(sbujectDs).where(2).equalTo(0)
    // 4 输出 (如果直接打印无需进行启动，执行execute)
    joinDs.print()
    // 5 执行
  }
}

```

### leftouterjoin

类似表的左外关联

参考代码：

```scala
package cn.itcast.batch.transformation

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/*
演示flink LeftOuterJoin的operator
 */
/*
需求：用户dataset左关联城市数据
示例
请将以下元组数据(用户id,用户姓名)
(1, "zhangsan") , (2, "lisi") ,(3 , "wangwu")
元组数据(用户id,所在城市)
(1, "beijing"), (2, "shanghai"), (4, "guangzhou")
返回如下数据：
(3,wangwu,null)
(1,zhangsan,beijing)
(2,lisi,shanghai)
 */


object LeftOuterJoinDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2 source 加载数据
    //2.1用户数据
    val userDs: DataSet[(Int, String)] = env.fromCollection(List((1, "zhangsan"), (2, "lisi"), (3, "wangwu")))
    //2.2城市数据
    val cityDs: DataSet[(Int, String)] = env.fromCollection(List((1, "beijing"), (2, "shanghai"), (4, "guangzhou")))
    // 3 转换 使用userDs leftouterjoin cityDs
    /*
    
      OPTIMIZER_CHOOSES：将选择权交予Flink优化器；
      BROADCAST_HASH_FIRST：广播第一个输入端，同时基于它构建一个哈希表，而第二个输入端作为探索端，选择这种策略的场景是第一个输入端规模很小；
      BROADCAST_HASH_SECOND：广播第二个输入端并基于它构建哈希表，第一个输入端作为探索端，选择这种策略的场景是第二个输入端的规模很小；
      REPARTITION_HASH_FIRST：该策略会导致两个输入端都会被重分区，但会基于第一个输入端构建哈希表。该策略适用于第一个输入端数据量小于
      第二个输入端的数据量，但这两个输入端的规模仍然很大，优化器也是当没有办法估算大小，没有已存在的分区以及排序顺序可被使用时系统默认采用的策略；
      REPARTITION_HASH_SECOND：该策略会导致两个输入端都会被重分区，但会基于第二个输入端构建哈希表。
      该策略适用于两个输入端的规模都很大，但第二个输入端的数据量小于第一个输入端的情况；
      REPARTITION_SORT_MERGE：输入端被以流的形式进行连接并合并成排过序的输入。该策略适用于一个或两个输入端都已排过序的情况；

     */
   //我们一般如果不明确数据集情况，就使用OPTIMIZER_CHOOSES
    val leftJoinAssigner: JoinFunctionAssigner[(Int, String), (Int, String)] = userDs.leftOuterJoin(cityDs,
      JoinHint.OPTIMIZER_CHOOSES).where(0).equalTo(0)
    //3.1 使用apply方法解析JoinFunctionAssigner中的数据处理逻辑
    val resultDs: DataSet[(Int, String, String)] = leftJoinAssigner.apply(
      (left, right) => {
        if (right == null) { //cityds中结果为null
          //返回的数据
          (left._1, left._2, "null")
        } else {
          //返回的数据
          (left._1, left._2, right._2)
        }
      }
    )

    // 4 输出 (如果直接打印无需进行启动，执行execute)
    resultDs.print()
    // 5 执行
  }
}

```

注意：

1 leftjoin返回结果不是dataset而是leftjoinassginer，需要我们通过apply进行逻辑处理的实现；

2 关join的策略问题，优先使用joinhint.OPTIMIZER_CHOOSES

joinhit策略：

```pro
 OPTIMIZER_CHOOSES：将选择权交予Flink优化器；
 BROADCAST_HASH_FIRST：广播第一个输入端，同时基于它构建一个哈希表，而第二个输入端作为探索端，选择这种策略的场景是第一个输入端规模很小；
 BROADCAST_HASH_SECOND：广播第二个输入端并基于它构建哈希表，第一个输入端作为探索端，选择这种策略的场景是第二个输入端的规模很小；
 REPARTITION_HASH_FIRST：该策略会导致两个输入端都会被重分区，但会基于第一个输入端构建哈希表。该策略适用于第一个输入端数据量小于第二个输入端的数据量，但这两个输入端的规模仍然很大，优化器也是当没有办法估算大小，没有已存在的分区以及排序顺序可被使用时系统默认采用的策略；
 REPARTITION_HASH_SECOND：该策略会导致两个输入端都会被重分区，但会基于第二个输入端构建哈希表。该策略适用于两个输入端的规模都很大，但第二个输入端的数据量小于第一个输入端的情况；
 REPARTITION_SORT_MERGE：输入端被以流的形式进行连接并合并成排过序的输入。该策略适用于一个或两个输入端都已排过序的情况；
```





### rightouterjoin

参考代码：

```scala
package cn.itcast.batch.transformation

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/*
演示flink RightOuterJoin的operator
 */
/*
需求：用户dataset左关联城市数据
示例
请将以下元组数据(用户id,用户姓名)
(1, "zhangsan") , (2, "lisi") ,(3 , "wangwu")
元组数据(用户id,所在城市)
(1, "beijing"), (2, "shanghai"), (4, "guangzhou")
返回如下数据：
(3,wangwu,null)
(1,zhangsan,beijing)
(2,lisi,shanghai)
 */


object RighterOuterJoinDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2 source 加载数据
    //2.1用户数据
    val userDs: DataSet[(Int, String)] = env.fromCollection(List((1, "zhangsan"), (2, "lisi"), (3, "wangwu")))
    //2.2城市数据
    val cityDs: DataSet[(Int, String)] = env.fromCollection(List((1, "beijing"), (2, "shanghai"), (4, "guangzhou")))
    // 3 转换 使用userDs leftouterjoin cityDs
    /*

      OPTIMIZER_CHOOSES：将选择权交予Flink优化器；
      BROADCAST_HASH_FIRST：广播第一个输入端，同时基于它构建一个哈希表，而第二个输入端作为探索端，选择这种策略的场景是第一个输入端规模很小；
      BROADCAST_HASH_SECOND：广播第二个输入端并基于它构建哈希表，第一个输入端作为探索端，选择这种策略的场景是第二个输入端的规模很小；
      REPARTITION_HASH_FIRST：该策略会导致两个输入端都会被重分区，但会基于第一个输入端构建哈希表。该策略适用于第一个输入端数据量小于
      第二个输入端的数据量，但这两个输入端的规模仍然很大，优化器也是当没有办法估算大小，没有已存在的分区以及排序顺序可被使用时系统默认采用的策略；
      REPARTITION_HASH_SECOND：该策略会导致两个输入端都会被重分区，但会基于第二个输入端构建哈希表。
      该策略适用于两个输入端的规模都很大，但第二个输入端的数据量小于第一个输入端的情况；
      REPARTITION_SORT_MERGE：输入端被以流的形式进行连接并合并成排过序的输入。该策略适用于一个或两个输入端都已排过序的情况；

     */
    //我们一般如果不明确数据集情况，就使用OPTIMIZER_CHOOSES
    val leftJoinAssigner: JoinFunctionAssigner[(Int, String), (Int, String)] = userDs.rightOuterJoin(cityDs,
      JoinHint.OPTIMIZER_CHOOSES).where(0).equalTo(0)
    //3.1 使用apply方法解析JoinFunctionAssigner中的数据处理逻辑
    val resultDs: DataSet[(Int, String, String)] = leftJoinAssigner.apply(
      (left, right) => {
        if (left == null) { //cityds中结果为null
          //返回的数据
          (right._1, right._2, "null")
        } else {
          //返回的数据
          (right._1, right._2, left._2)
        }
      }
    )

    // 4 输出 (如果直接打印无需进行启动，执行execute)
    resultDs.print()
    // 5 执行
  }
}

```

### fullOuterJoin

参考代码

```scala
package cn.itcast.batch.transformation

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/*
演示flink FullOuterJoin的operator
 */
/*
需求：用户dataset FullOuterJoin城市数据
示例
请将以下元组数据(用户id,用户姓名)
(1, "zhangsan") , (2, "lisi") ,(3 , "wangwu")
元组数据(用户id,所在城市)
(1, "beijing"), (2, "shanghai"), (4, "guangzhou")
返回如下数据：
(3,wangwu,null)
(1,zhangsan,beijing)
(2,lisi,shanghai)
 */


object FullOuterJoinDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2 source 加载数据
    //2.1用户数据
    val userDs: DataSet[(Int, String)] = env.fromCollection(List((1, "zhangsan"), (2, "lisi"), (3, "wangwu")))
    //2.2城市数据
    val cityDs: DataSet[(Int, String)] = env.fromCollection(List((1, "beijing"), (2, "shanghai"), (4, "guangzhou")))
    // 3 转换 使用userDs leftouterjoin cityDs
    /*

      OPTIMIZER_CHOOSES：将选择权交予Flink优化器；
      BROADCAST_HASH_FIRST：广播第一个输入端，同时基于它构建一个哈希表，而第二个输入端作为探索端，选择这种策略的场景是第一个输入端规模很小；
      BROADCAST_HASH_SECOND：广播第二个输入端并基于它构建哈希表，第一个输入端作为探索端，选择这种策略的场景是第二个输入端的规模很小；
      REPARTITION_HASH_FIRST：该策略会导致两个输入端都会被重分区，但会基于第一个输入端构建哈希表。该策略适用于第一个输入端数据量小于
      第二个输入端的数据量，但这两个输入端的规模仍然很大，优化器也是当没有办法估算大小，没有已存在的分区以及排序顺序可被使用时系统默认采用的策略；
      REPARTITION_HASH_SECOND：该策略会导致两个输入端都会被重分区，但会基于第二个输入端构建哈希表。
      该策略适用于两个输入端的规模都很大，但第二个输入端的数据量小于第一个输入端的情况；
      REPARTITION_SORT_MERGE：输入端被以流的形式进行连接并合并成排过序的输入。该策略适用于一个或两个输入端都已排过序的情况；

     */
    //我们一般如果不明确数据集情况，就使用OPTIMIZER_CHOOSES
    val leftJoinAssigner: JoinFunctionAssigner[(Int, String), (Int, String)] = userDs.fullOuterJoin(cityDs,
      JoinHint.REPARTITION_SORT_MERGE).where(0).equalTo(0)
    //3.1 使用apply方法解析JoinFunctionAssigner中的数据处理逻辑
    val resultDs: DataSet[(Int, String, String)] = leftJoinAssigner.apply(
      (left, right) => {
        if (left == null) { //cityds中结果为null
          //返回的数据
          (right._1,  "null",right._2)
        } else if (right == null) {
          (left._1,left._2,  "null")
        }  else {
          //返回的数据
          (left._1, left._2, right._2)
        }
      }
    )

    // 4 输出 (如果直接打印无需进行启动，执行execute)
    resultDs.print()
    // 5 执行
  }
}

```

### union 

针对两个dataset去并集，注意结果不去重，并且要求两个数据集的数据类型必须一致。

参考代码：

```sca
package cn.itcast.batch.transformation

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

/*
演示flink union的operator
 */
/*
需求：
示例
将以下数据进行取并集操作
数据集1
"hadoop", "hive", "flume"
数据集2
"hadoop", "hive", "spark"
 */

object UnionDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2 source 加载数据
    val ds1: DataSet[String] = env.fromCollection(List("hadoop",
      "hive",
      "flume"))
    val ds2: DataSet[String] = env.fromCollection(List("hadoop",
      "hive",
      "azkaban"))
//val ds2: DataSet[Int] = env.fromCollection(List(1,
//  2,
//  3))
    // union算子要求两个ds中的数据类型必须一致！！
    // 3 转换 使用union获取两个ds的并集
    val unionDs: DataSet[String] = ds1.union(ds2)

    // 4 输出 (如果直接打印无需进行启动，执行execute)
    unionDs.print()
    // 5 执行
  }
}

```

### rebalance

rabalance可以对数据集进行数据的均匀分布，内部使用round robin(轮询策略)进行数据分发。

![image-20200416043506052](assets/image-20200416043506052.png)

rebalance 参考代码：

```sca
package cn.itcast.batch.transformation

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

/*
演示flink rebalance的operator
 */
/*
需求：
演示rebalance分区均衡数据
 */
object RebalanceDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2 source 加载数据
    val sourceDs: DataSet[Long] = env.generateSequence(0, 100)
    //过滤掉部分数据保证倾斜现象明显
    val filterDs: DataSet[Long] = sourceDs.filter(_ > 8)


    // 3 转换操作 使用filter过滤单词长度大于4的单词
    // 3.1 如何观测到每个分区的数据量？使用richmapfunciton来观察数据
    val subTaskDs: DataSet[(Long, Long)] = filterDs.map(
      new RichMapFunction[Long, (Long, Long)] { //rich为代表的富函数可以获取到运行的上下文对象
        //自己定义map逻辑，
        override def map(value: Long): (Long, Long) = {
          //获取到当前任务编号信息（以此来代表分区）
          val subtask: Long = getRuntimeContext.getIndexOfThisSubtask
          (subtask, value)
        }
      }
    )
    // 3.2使用rebalance解决数据倾斜问题
    val rebalanceDs: DataSet[Long] = filterDs.rebalance()

    val rebDs: DataSet[(Long, Long)] = rebalanceDs.map(
      new RichMapFunction[Long, (Long, Long)] { //rich为代表的富函数可以获取到运行的上下文对象
        //自己定义map逻辑，
        override def map(value: Long): (Long, Long) = {
          //获取到当前任务编号信息（以此来代表分区）
          val subtask: Long = getRuntimeContext.getIndexOfThisSubtask
          (subtask, value)
        }
      }
    )
    // 4 输出 (如果直接打印无需进行启动，执行execute)
    subTaskDs.print()
    println("================================")
    rebDs.print()
    // 5 执行
  }
}

```

总结：

reblance通过轮询的方式大致保证每个分区的数据量时均衡分布（不一定完全均衡）。

技术点：

1 通过subtask编号来代指分区，需要通过上下文件对象获取到subtask编号

2 算子普通方法中无法获取运行时上下文，需要通过富函数来获取，富函数就是flink提供的拥有更丰富权限的一类方法；

![image-20200416050129921](assets/image-20200416050129921.png)

### flink dataset 分区策略

hash分区：根据key的hash值进行分区

range分区：根据key的范围进行分区的划分

参考代码：

```scala
package cn.itcast.batch.transformation

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem

import scala.collection.mutable
import scala.util.Random

/*
演示flink dataset的分区策略
 */


object PartitionDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 演示效果明显，设置并行度为2，设置全局并行度为2
    env.setParallelism(2)
    //2 source 加载数据
    val datas = new mutable.MutableList[(Int, Long, String)]
    datas.+=((1, 1L, "Hello"))
    datas.+=((2, 2L, "Hello"))
    datas.+=((3, 2L, "Hello"))
    datas.+=((4, 3L, "Hello"))
    datas.+=((5, 3L, "Hello"))
    datas.+=((6, 3L, "hehe"))
    datas.+=((7, 4L, "hehe"))
    datas.+=((8, 4L, "hehe"))
    datas.+=((9, 4L, "hehe"))
    datas.+=((10, 4L, "hehe"))
    datas.+=((11, 5L, "hehe"))
    datas.+=((12, 5L, "hehe"))
    datas.+=((13, 5L, "hehe"))
    datas.+=((14, 5L, "hehe"))
    datas.+=((15, 5L, "hehe"))
    datas.+=((16, 6L, "hehe"))
    datas.+=((17, 6L, "hehe"))
    datas.+=((18, 6L, "hehe"))
    datas.+=((19, 6L, "hehe"))
    datas.+=((20, 6L, "hehe"))
    datas.+=((21, 6L, "hehe"))
    val orginalDs: DataSet[(Int, Long, String)] = env.fromCollection(Random.shuffle(datas))

    // 3 转换 使用分区规则进行分区
    //3.1 hash分区
    val hashDs: DataSet[(Int, Long, String)] = orginalDs.partitionByHash(_._3)
    //3.2 range 分区
    val rangeDs: DataSet[(Int, Long, String)] = orginalDs.partitionByRange(_._1)
    //对分区进行排序 sortpartition
    val sortDs: DataSet[(Int, Long, String)] = hashDs.sortPartition(_._2, Order.ASCENDING)

    // 4 输出 (如果直接打印无需进行启动，执行execute)
    hashDs.writeAsText("e:/data/partitionbyhash/", FileSystem.WriteMode.OVERWRITE)
    rangeDs.writeAsText("e:/data/partitionbyrange", FileSystem.WriteMode.OVERWRITE)

    sortDs.writeAsText("e:/data/sortpartition", FileSystem.WriteMode.OVERWRITE)
    // 5 执行
    env.execute()
  }
}

```

使用partitionbyhash可以对数据集按照指定字段的hash值进行数据分分散，

partitionbyrange可以对数据集按照指定字段的范围进行划分；

sortpartition就是分区内的数据按照指定字段以及指定规则进行排序操作

### minby/maxby

求取最小或者最大值

参考代码：

```scala
package cn.itcast.batch.transformation

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable
import scala.util.Random
import org.apache.flink.api.scala._

/*
演示flink minby,maxby操作
 */
/*
计算每个学科下最大和最小成绩
 */

object MinMaxDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 演示效果明显，设置并行度为2，设置全局并行度为2
    val scores = new mutable.MutableList[(Int, String, Double)]
    scores.+=((1, "yuwen", 90.0))
    scores.+=((2, "shuxue", 20.0))
    scores.+=((3, "yingyu", 30.0))
    scores.+=((4, "wuli", 40.0))
    scores.+=((5, "yuwen", 50.0))
    scores.+=((6, "wuli", 60.0))
    scores.+=((7, "yuwen", 70.0))
    val scoreDs = env.fromCollection(Random.shuffle(scores))

    // 3 转换  计算每个学科下最大和最小成绩
    val groupDs: GroupedDataSet[(Int, String, Double)] = scoreDs.groupBy(1)
    //调用min
    val aggMinDs: AggregateDataSet[(Int, String, Double)] = groupDs.min(2)
//调用minby
    val minByDs: DataSet[(Int, String, Double)] = groupDs.minBy(2)
    // 4 输出 (如果直接打印无需进行启动，执行execute)
    aggMinDs.print()
    println("====================")
    minByDs.print()
    // 5 执行
//    env.execute()
  }
}

```

注意：

min也可以计算最小值，但是返回的数据是包含最小值字段的数据，但是有可能其它字段是不正确的，所以想要获取最小值要使用minby,max与maxby同理。

### cross 笛卡尔积

参考代码：

```scala
package cn.itcast.batch.transformation

import org.apache.flink.api.scala.{ExecutionEnvironment, _}

import scala.collection.mutable
import scala.util.Random

/*
演示flink cross笛卡尔积
 */
/*

 */

object CrossDemo {
  def main(args: Array[String]): Unit = {

    // 1 获取执行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2 source 加载数据
    val students = new mutable.MutableList[(Int, String)]
    //学生
    students.+=((1, "张三"))
    students.+=((2, "李四"))
    students.+=((3, "王五"))
    students.+=((4, "赵六"))

    val subjects = new mutable.MutableList[(Int, String)]
    //课程
    subjects.+=((1, "Java"))
    subjects.+=((2, "Python"))
    subjects.+=((3, "前端"))
    subjects.+=((4, "大数据"))
    val stuDs = env.fromCollection(Random.shuffle(students))
    val subjectDs = env.fromCollection(Random.shuffle(subjects))
    // 3 转换 进行笛卡尔积操作,类似偏函数（scala）
    val resDs: DataSet[(Int, String, Int, String)] = stuDs.cross(subjectDs) {
      (item1, item2) => {
        (item1._1, item1._2, item2._1, item2._2)
      }
    }


    // 4 输出 (如果直接打印无需进行启动，执行execute)
    resDs.print()
    // 5 执行
  }
}

```

## sink

参考代码

```scala
package cn.itcast.batch.sink

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

/*
演示flink dataset 中的sink operator
 */
object SinkDemo {
  def main(args: Array[String]): Unit = {
    //1 创建入口对象，获取运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    //2 source
    val wordsDs: DataSet[String] = env.fromElements("spark", "hadoop", "flink")

    // 3 转换

    // 4 sink （如果是print无需执行下面的execute）

    // 4.1  基于集合
    //标准输出
//    wordsDs.print()
//    println("==========================")
//    //错误输出
//    wordsDs.printToErr() //高亮显示
//    //collect 到本地
//    println("==========================")
//
//    println(wordsDs.collect())

    // 4.2 基于文件，保存结果到文件, 可以直接调整operator的并行度
    wordsDs.setParallelism(1).writeAsText("e:/data/sink/words")
    // 5 执行
    env.execute()
  }
}

```

注意：

最后保存数据到文件时，如果只有一个并行度最终会保存为一个文件，如果是多个并行度最后是在文件中生成多个并行度对应的文件。



## 执行模式

flink中批处理运行环境介绍，我们查看executionenviroment的类中有很多获取enviroment的方法：

![image-20200416071607550](assets/image-20200416071607550.png)

![image-20200416072052422](assets/image-20200416072052422.png)



flink中常见enviroment

1 localEnviroment:通过createlocalEnviroment创建获取，是比较完善和完整的一个虚拟集群，相对来说开销比较大；因此建议使用getexecutionenviroment来获取一个相对来轻量一些的运行环境（一直使用，这种基于所处场景可以是本地也可以是集群环境）

2 collectionenviroment:开销比较低，但是不完整而且局限性

总结：建议使用getexecutionenviroment来获取即可！！

演示创建不同env:

参考代码：

```scala
package cn.itcast.batch.env

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
/*
演示下不同env创建
 */
object FlinkEnvDemo {
  def main(args: Array[String]): Unit = {

    // 1 localenviroment 开销昂贵
//    val env = ExecutionEnvironment.createLocalEnvironment()
    //2 collectionenviroment 局限性太大，
//    val env = ExecutionEnvironment.createCollectionsEnvironment
//    // 3 executionenviroment
    val env = ExecutionEnvironment.getExecutionEnvironment //推荐使用这种方式创建
    val ds1 = env.fromElements(List(1,2,3))
    ds1.print()
  }
}

```



远程集群环境

1 打成jar包之后使用 flink run命令进行提交（建议使用这种方式）

2 使用createremoteenviroment方式

参考代码：

```scala
package cn.itcast.batch.env

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/*
演示下通过idea提交远程集群
 */
object FlinRemoteEnvDemo {
  def main(args: Array[String]): Unit = {

    //创建远程集群运行环境，需要能连接到集群，连接到jobmanger的地址，ip:jobmanager地址，port:jbomanger端口，jar:当前程序所打成的jar路径
    val env: ExecutionEnvironment = ExecutionEnvironment.createRemoteEnvironment("node1",
      8081,"E:\\flink\\flink_video\\day01\\code\\flink_learning\\day02\\target\\original-day02-1.0-SNAPSHOT.jar"
      )
//读取hdfs上的文件
    val resDs: DataSet[String] = env.readTextFile("hdfs://node1:8020/wordcount/input/words.txt")

    resDs.print()
  }
}

```

## flink 广播变量

广播变量的作用：

1 就是把之前不使用广播变量会导致的内存消耗增大，使用广播变量会降低tm的内存压力，

2 减轻shuffle，因为数据存在tm的内存中，进行join操作不需要跨节点通过网络传输，直接从内存中获取即可。

![image-20200416081843690](assets/image-20200416081843690.png)

注意点：

1 广播变量数据不应该太大；

2 广播变量数据最好是不需要频繁更新的数据，广播变量最好是只读的。

flink中的广播变量如何使用？

![image-20200416082058838](assets/image-20200416082058838.png)



flink中广播变量代码执行流程：

![image-20200416085647452](assets/image-20200416085647452.png)



完整代码：

```sacala
package cn.itcast.batch.broadcast

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/*
使用广播变量实现该需求
需求
创建一个成绩数据集(学生ID,学科,成绩): List( (1, "语文", 50),(2, "数学", 70), (3, "英文", 86))
再创建一个学生数据集(学生ID,姓名): List((1, "张三"), (2, "李四"), (3, "王五")),并将该数据，发布到广播
通过广播获取到学生姓名并将数据转换为(学生姓名,学科,成绩): List( ("张三", "语文", 50),("李四", "数学", 70), ("王五", "英文", 86))
 */
object BroadCastDemo {
  def main(args: Array[String]): Unit = {
    //1 运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment //推荐使用这种方式创建

    //2 source 2. 分别创建两个数据集
    import org.apache.flink.api.scala._
    // 2.1学生数据集，广播该数据集
    val studentDataSet: DataSet[(Int, String)] = env.fromCollection(List((1, "张三"), (2, "李四"), (3, "王五")))
    //2.2 成绩数据集
    val scoreDataSet: DataSet[(Int, String, Int)] = env.fromCollection(List((1, "语文", 50), (2, "数学", 70), (3, "英文", 86)))

    // 3 转换
    // 3.1 使用RichMapFunction对成绩数据集进行map转换
    // 泛型：in,out: in:map处理的数据类型：(Int, String, Int)，out：map处理之后输出的数据集:("张三", "语文", 50)
    var resDs = scoreDataSet.map(new RichMapFunction[(Int, String, Int), (String, String, Int)] {
      // 3.3.4定义成员变量来接收获取的广播数据
      var stuMap: Map[Int, String] = null
      // 3.3  获取广播变量
      // open方法是rich函数中另外一个比较重要和常用的方法，初始化方法，执行一次，获取广播变量操作再此处执行
      override def open(parameters: Configuration): Unit = {
        // 3.3.1 根据名称获取广播变量,需要限定获取的广播变量数据类型：
        val stuList: util.List[(Int, String)] = getRuntimeContext.getBroadcastVariable[(Int, String)]("student")
        // 3.3.2 list转为map数据
        import scala.collection.JavaConverters._
        // 3.3.3 把获取到的广播数据转为map,key:学生id，value：学生姓名
        stuMap = stuList.asScala.toMap
      }
      // 3.4  根据stumap查询到学生姓名然后返回数据；map处理业务逻辑：value:接收到的一个数据
      override def map(score: (Int, String, Int)): (String, String, Int) = { //每条数据执行一次map方法，获取广播变量的方法不适合在此处执行
        // 3.4.1 获取成绩中的学生id
        val stuId = score._1
        //3.4.2 根据学生id去广播变量map中获取到学生姓名
        val stuName: String = stuMap.getOrElse(stuId, "null")
        // 3.4.3 组装数据并返回
        (stuName, score._2, score._3)
      }

      // 3.2 广播学生数据
    }) withBroadcastSet(studentDataSet, "student")

    // 4 sink
    resDs.print()
    //5 excute 执行
  }
}

```

总结：

用法：

1 需要在获取广播的operator后使用withbroadcast进行广播

2 获取广播的operator必须使用richfunction然后重写open方法获取广播变量

3 编写自己的业务



## flink累加器

累加器用法

![image-20200416090341651](assets/image-20200416090341651.png)

参考代码：

```scala
package cn.itcast.batch.accumulator

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem

/*
flink中累加器使用演示
需求：

遍历下列数据, 打印出数据总行数
   "aaaa","bbbb","cccc","dddd"
 */
object AccumulatorDemo {
  def main(args: Array[String]): Unit = {
    
    //1 运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 2 source 加载本地集合
    val wordsDs: DataSet[String] = env.fromCollection(List("aaaa", "bbbb", "cccc", "dddd"))
    //3 转换 不能使用普通方法，要是richfunction,因为需要使用上下午注册累加器
    val accDs: DataSet[String] = wordsDs.map(
      // 3.1 in: String out: String ;不处理数据只是统计数量而已
      new RichMapFunction[String, String] {
        //3.2 创建累加器
        private var numLines = new IntCounter()
        // 对比：定义一个int类型变量
        var num = 0
        override def open(parameters: Configuration): Unit = {
          //3.3 注册累加器
          getRuntimeContext.addAccumulator("numLines", numLines)
        }
        //3.4使用累加器统计数据条数
        override def map(value: String): String = {
          numLines.add(1) //累加器中增加1
          num += 1
          println(num)
          value
        }
      }
    ).setParallelism(2)
    // 4 输出 保存到文件中，因为需要执行env.execute动作，不能直接打印
    accDs.writeAsText("e:/data/acc", FileSystem.WriteMode.OVERWRITE)
    // 5 启动执行
    val result: JobExecutionResult = env.execute()
    // 6 获取累加器的值
    val accRes = result.getAccumulatorResult[Int]("numLines")
    println("acc结果值：" + accRes)
  }
}

```

总结：

flink中累加器的用法：

1 创建一个累加器 new IntCounter,longCounter

2 使用运行上下文件进行注册该累加器

3 累加器.add方法

4 获取累加器的值，需要通过env.execute的返回结果resut获取

```scala
result.getAccumulatorResult[Int]("numLines") //根据注册时的名称获取
```

广播变量：把一批数据发送到taskmanger端

累加器：是进行全局计数操作，



## flink分布式缓存

与广播变量区别：

![image-20200416094524510](assets/image-20200416094524510.png)

用法：

1 注册文件：env.registerCacheFile(path,name)

2  获取文件：getrumtimeContext.getdistributecache.getfile(name);使用richfunction才能获取；

![image-20200416100356067](assets/image-20200416100356067.png)

参考代码

```scala
package cn.itcast.batch.distributecache

import java.io.File

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.io.Source

/*
使用分布式缓存技术实现该需求
需求
创建一个成绩数据集(学生ID,学科,成绩): List( (1, "语文", 50),(2, "数学", 70), (3, "英文", 86))
注册一个学生数据集(学生ID,姓名): List((1, "张三"), (2, "李四"), (3, "王五"))为分布式缓存文件
通过分布式缓存文件获取到学生姓名并将数据转换为(学生姓名,学科,成绩): List( ("张三", "语文", 50),("李四", "数学", 70), ("王五", "英文", 86))
 */
object DistributeCacheDemo {
  def main(args: Array[String]): Unit = {
    //1 运行环境
    val env = ExecutionEnvironment.getExecutionEnvironment //推荐使用这种方式创建

    //2 source  分别创建成绩数据集
    import org.apache.flink.api.scala._

    //2.1成绩数据集
    val scoreDataSet: DataSet[(Int, String, Int)] = env.fromCollection(List((1, "语文", 50), (2, "数学", 70), (3, "英文", 86)))
    //2.2 把学生数据注册为分布式缓存文件,文件路径，缓存文件名称（别名）
    env.registerCachedFile("hdfs://node1:8020/distribute_cache_student", "cache_student")
    // 3 转换
    // 3.1 使用RichMapFunction对成绩数据集进行map转换
    // 泛型：in,out: in:map处理的数据类型：(Int, String, Int)，out：map处理之后输出的数据集:("张三", "语文", 50)
    val resDs: DataSet[(String, String, Int)] = scoreDataSet.map(new RichMapFunction[(Int, String, Int), (String, String, Int)] {
      //3.3 定义一个map成员变量
      var stuMap: Map[Int, String] = null
      //3.2通过上下文件获取缓存文件
      override def open(parameters: Configuration): Unit = {
        val file: File = getRuntimeContext.getDistributedCache.getFile("cache_student")
        //3.2.1对文件进行读取和解析，方便根据学生id查找数据
        val tuples: Iterator[(Int, String)] = Source.fromFile(file).getLines().map(
          line => {
            val arr = line.split(",")
            (arr(0).toInt, arr(1)) //学生id,学生姓名
          }
        )
        //得到一个id作为key，姓名是value的map
        stuMap = tuples.toMap
      }
      //3.4 根据map解析出学生的姓名然后返回结果
      override def map(score: (Int, String, Int)): (String, String, Int) = {
        // 3.4.1 获取成绩中的学生id
        val stuId = score._1
        //3.4.2 根据学生id去广播变量map中获取到学生姓名
        val stuName: String = stuMap.getOrElse(stuId, "null")
        // 3.4.3 组装数据并返回
        (stuName, score._2, score._3)
      }
    })


    // 4 sink
    resDs.print()
    //5 excute 执行
  }
}

```



广播变量和分布式缓存：

广播变量适合的是：小数据量，更新不频繁的数据，

分布式缓存：适合你需要分发的数据是存储在文件中的；

## flink程序并行度的设置

flink程序有四种设置并行度的方式

优先级：

算子级别  >env级别>client 级别（flink run -p n）>系统默认（flink-conf.yaml配置）

建议：

1 建议不设置算子并行度，如果算子并行度发生变化，造成数据的shuffle;

2 不建议使用系统默认配置，配置太过于死板了，修改起来比较麻烦；

3 可以设置env级别或者client级别。