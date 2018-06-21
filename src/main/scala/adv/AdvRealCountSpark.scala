package adv

import java.util.{Date, Properties}

import com.aura.conf.ConfigurationManager
import com.aura.constant.Constants
import com.aura.jdbc.MysqlPool
import com.aura.utils.DateUtils
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by Administrator on 2017/8/14.
  */
object AdvRealCountSpark {
  def main(args: Array[String]): Unit = {
    /**
      * 第一步：
      *    先从kafka里面读取到广告点击日志数据
      *    [
    K: ClassTag,
    V: ClassTag,
    KD <: Decoder[K]: ClassTag,
    VD <: Decoder[V]: ClassTag] (
      ssc: StreamingContext,
      kafkaParams: Map[String, String],
      topics: Set[String]
      */
    val conf=new SparkConf().setAppName(Constants.SPARK_APP_NAME_ADV).setMaster("local")
    val ssc = new StreamingContext(conf,Seconds(5))
    val kafkaParams= Map("metadata.broker.list" -> ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST))
    val topics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS).split(",").toSet
    //k,v  k偏移量的信息  v 才使我们需要的数据
    val logDStream: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
      .map(_._2)

    //获取sparksession
    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()



    /**
      * 第二步：
      *    进行黑名单过滤，也就是说需要对日志进行过滤
      *    过滤出来了有效的数据，后面的计算操作，就针对这个有效数据进行操作
      */
    val filterDStream = fliterBlackList(logDStream)
    /**
      * 第三步：
      *   动态的生成一个黑名单
      */
    generaterDynamicBlackList(filterDStream,spark)
    /**
      * 第四步：
      *  实时统计各省份各城市点击广告的点击量
      *  updateSteteBykey
      */
    val provinceCityAdvClickCount = calculatorCityAdvClickCount(filterDStream)
    /**
      * date_province_city_adv  count  updateSteteByKey
      *      date provnice city advid count  -> table
      *
      *      row_number() over( partition by provnice order by count)
      *
      *第五步：
      *   实时统计[各省份]点击广告的点击量  TopN
      *
      */
    calculatorProvinceAdvCount(provinceCityAdvClickCount,spark)

    /**
      * 第六步：
      *  实时统计每天每个广告最近一个小时的滑动窗口的点击趋势
      */
    realCountAdvClickByWindow(filterDStream)
  }

  /**
    *对数据进行黑名单过滤：
    * 1）真实项目里面需要 从MySQL或者是redis数据库里面去获取黑名单。
    *   a Mysql
    *   b Redis（如果可以大家就回答是redis）
    *     建议用redis
    * @param logDStream
    * @return
    */
  def fliterBlackList(logDStream: DStream[String]):DStream[String]={
    //mysql 读取的数据，把黑名单作为广播变量放进来。
    //userid long
    val filterDStream: DStream[String] = logDStream.transform(rdd => {
      //1L 2L 3L我们模拟的是userid  黑名单的用户ID 号
      val blackList = List((1L, true), (2L, true), (3L, true))
      val blackListRDD: RDD[(Long, Boolean)] = rdd.sparkContext.parallelize(blackList)
      /**
        * timestamp,province,city,userid,advid
        */
      val userid_log_RDD = rdd.map(log => {
        val fields = log.split(",")
        (fields(3).toLong, log)
      })

      val joinRDD: RDD[(Long, (String, Option[Boolean]))] = userid_log_RDD.leftOuterJoin(blackListRDD)
      //true=tupble._2._2.isEmpty
      val filterRDD = joinRDD.filter(tuple => {
        tuple._2._2.isEmpty
      }).map(tuple => tuple._2._1)

      filterRDD

    })

    filterDStream

  }

  /**
    * 首先我们思考一下：
    * 黑名单规则：【某天】 【某个用户】 对【某个广告】的点击次数超过100次，那么这个用户就是黑名单
    *    1）如果我今天对这个广告点击50次，明天又对这个广告点击50次，这个不是黑名单
    *    2）我今天对广告A点击了90,对广告B点击了20次  这个也是不是黑名单
    *    3）看不同的公司：
    *        比如用户A  今天是黑名单，
    *                   那么明天他算不算黑名单
    *   4）过滤完黑名单以后，需要把数据持久化到数据库（MySQL，redis，hbase）
    *
    *   timestamp,province,city,userid,advid
    *
    *   spark 1.6
    */
  def generaterDynamicBlackList(filterDStream: DStream[String],spark:SparkSession): Unit ={
    /**
      * 步骤一：统计出来每天每个用户每个广告的点击次数
      *
      * reduceBykey : 算出来这一批数据里面的
      * (date_userid_advid,1) yyyyMMdd
      * timestamp,province,city,userid,advid
      */
    val date_userid_advID_RDD: DStream[(String, Long)] = filterDStream.map(log => {
      val fields = log.split(",")
      val date = DateUtils.formatDateKey(new Date(fields(0).toLong))
      val userID = fields(3).toLong
      val advID = fields(4).toLong
      (date + "_" + userID + "_" + advID, 1L)
    })

    val dua_dsteam: DStream[(String, Long)] = date_userid_advID_RDD.reduceByKey((_+_))
    //date_userid_advid,count
    //把统计出来的数据 存储到MySQL的临时表
     dua_dsteam.foreachRDD( rdd =>{

      rdd.foreachPartition( partition =>{
        val connection = MysqlPool.getJdbcCoon()
        partition.foreach( recored =>{
          val date_userid_advid = recored._1
          val fields = date_userid_advid.split("_")
          val count = recored._2.toLong
          val sql=
            s"""
               insert into tmp_click_count values(${fields(0)},${fields(1).toLong},${fields(2).toLong},${count},)
            """
          connection.createStatement().execute(sql)

        })

        MysqlPool.releaseConn(connection)
      } )
    })

    /**
      *
      * spark.read.format("jdbc") tmp_click_count
      *
      * 通过tmp_click_count 获取黑名单
      *
      * select userid
      * (
      * select
      *   sum(click_count) count,date,userid,advid
      * from
      *  tmp_click_count
      * group by
      *   date,userid,advid
      *   ) tmp
      *   where tmp.count > 100
      *
      *   因为有些用户 可能既是这个广告的黑名单，又是那个广告的黑名单，所以需要去重
      */

    val df = spark.read.format("jdbc")
      .option("url", ConfigurationManager.getProperty(Constants.JDBC_URL))
      .option("user", ConfigurationManager.getProperty(Constants.JDBC_USER))
      .option("password", ConfigurationManager.getProperty(Constants.JDBC_PASSWORD))
      .option("dbtable", "tmp_click_count")
      .load()

     df.createOrReplaceTempView("tmp_click_count")

    val sql=
      """
         SELECT
                userid
         FROM
           (
         SELECT
               SUM(click_count) count,date,userid,advid
         FROM
              tmp_click_count
         GROUP BY
          date,userid,advid
           ) tmp
         WHERE
              tmp.count >= 100
      """
    //把黑名单持久化到MySQL数据库里面
     spark.sql(sql).distinct().write.mode(SaveMode.Overwrite)
      .jdbc("","black_list",new Properties())

  }

  /**
    *
    * 实时统计【每天】【各省份】的【各城市】【广告】点击量
    *
    * UpdateStateBykey
    *
    * @param logDStream
    */
  def calculatorCityAdvClickCount(logDStream: DStream[String]):  DStream[(String, Long)] ={
    /**
      * timestamp,province,city,userid,advid
      */
    val date_province_city_advid_1: DStream[(String, Long)] = logDStream.map(log => {
      val fields = log.split(",")
      val date = DateUtils.formatDateKey(new Date(fields(0).toLong))
      val province = fields(1).toLong
      val city = fields(2).toLong
      val advid = fields(4).toLong
      (date + "_" + province + "_" + city + "_" + advid, 1L)
    })
    /**
      *   updateFunc: (Seq[V], Option[S]) => Option[S]
      */
    def updateFunc(values:Seq[Long], status: Option[Long]):Option[Long]={
      val currentCount = values.sum
      val lastCount = status.getOrElse(0)
      Some(currentCount+lastCount)
    }

    /**
      * 实时统计每天各省各城市广告的点击次数
      */
    val resultDStream: DStream[(String, Long)] = date_province_city_advid_1.updateStateByKey(updateFunc)
    /**
      * 把结果持久化到数据库，
      * 让JavaEE的程序员去展示
      */
    resultDStream.foreachRDD( rdd =>{
      rdd.foreachPartition( partition =>{
        partition.foreach( recored =>{
         val sql=
           """

           """
        })
      })
    })
    resultDStream
  }

  /**
    * 实时统计各省份 广告点击  TopN
    * @param cityCount
    * @param spark
    */
  def calculatorProvinceAdvCount(cityCount:DStream[(String, Long)],spark:SparkSession): Unit = {
    cityCount.transform(rdd => {
      val date_province_adv_count = rdd.map(tuple => {
        val date_provnice_city_adv = tuple._1
        val fields = date_provnice_city_adv.split("_")
        val date = fields(0)
        val province = fields(1).toLong
        val city = fields(2).toLong
        val adv = fields(3)

        val count = tuple._2
        (date + "_" + province + "_" + adv, 1L)
      }).reduceByKey((_ + _))
      //date province  adv  count
      val rowRDD: RDD[Row] = date_province_adv_count.map(tuple => {
        val date_province_adv = tuple._1
        val fields = date_province_adv.split("_")
        val count = tuple._2.toLong
        Row(fields(0), fields(1).toLong, fields(2).toLong, count)
      })
      val schema = StructType(
        StructField("date", StringType, true) ::
          StructField("province", LongType, true) ::
          StructField("advid", LongType, true) ::
          StructField("count", LongType, true) :: Nil
      )

      val df = spark.createDataFrame(rowRDD, schema)
      df.createOrReplaceTempView("temp_date_province_adv_count")
      val sql =
        """
           SELECT
                 date,province,advid,count
           FROM
           (
           SELECT
                 date,province,advid,count,
                 ROW_NUMBER() OVER(PARTITION BY provnice order by count desc) rank
           FROM
                 temp_date_province_adv_count
             ) tmp
           WHERE tmp.rank <= 10

        """
      val top10 = spark.sql(sql)
      /**
        * 然后把这个结果持久化到数据库即可
        */
      ///////
      null
    })

  }

  /**
    * 实时 每天 每隔20秒统计最近一小时内，这些广告每分钟的点击趋势
    * 201701010101_advid,1L
    * 统计
    *
    * @param filterDStream
    */
  def realCountAdvClickByWindow(filterDStream: DStream[String]): Unit ={
    /**timestamp,province,city,userid,advid
      *
      * 重点就是key如何设计：
      * date adv
      */

    filterDStream.map( log =>{
      val fields = log.split(",")
     val y_day_hour_min = DateUtils.formatTimeMinute(new Date( fields(0).toLong))
      val advid = fields(4).toLong
      (y_day_hour_min+"_"+advid,1L)
    }).reduceByKeyAndWindow((a:Long,b:Long) => a+b,Minutes(60),Seconds(20))
      .foreachRDD( rdd =>{
        rdd.foreachPartition( partition =>{
         partition.foreach( record =>{
           /////持久化到数据库即可
         })
        })
      })

  }





}
