package product

import com.alibaba.fastjson.{JSON, JSONObject}
import com.aura.conf.ConfigurationManager
import com.aura.constant.Constants
import com.aura.dao.factory.DAOFactory
import com.aura.utils.ParamUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by Administrator on 2017/8/11.
  */
object AreaHotProductTotalSpark {
  def main(args: Array[String]): Unit = {
    /**
      * 第一步：创建一个sparkSession
      *
      */
    val spark = SparkSession.builder()
      .appName(Constants.SPARK_APP_NAME_PRODUCT)
      .config("spark.sql.warehouse.dir", "hdfs://hadoop1:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    spark.udf.register("concact_long_string",(city_id:Long,city_name:String,delimiter:String) =>{
      city_id+delimiter+city_name
    })

    spark.udf.register("groupdistinct",GroupDistinctUDAF)
    spark.udf.register("get_json_object",(str:String,s:String) =>{//str:extends_info   s:product_status
      val parseObject = JSON.parseObject(str)
      ParamUtils.getParam(parseObject,s)
    })

    /**第二步：获取参数（起始时间和结束时间）
      */
    val taskID = ParamUtils.getTaskIdFromArgs(args)
    val taskDao = DAOFactory.getTaskDAO
    val task = taskDao.findById(taskID)
    val taskObject: JSONObject = ParamUtils.getTaskParam(task)
    val startDate = ParamUtils.getParam(taskObject,Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskObject,Constants.PARAM_END_DATE)
    /**
      * 第三步：根据时间访问去hive表种去获取用户行为数据
      * user_visit_action  (city_id)
      *
      * (k,v)
      * (k,v) -> cityid,row
      */
    val userActionRDD: RDD[(Long, Row)] = getUserActionRDDByDateRange(spark,startDate,endDate)
    /**
      * 第四步： 从MySQL数据库里面去获取城市信息表
      */
    val cityInfoRDD = getCity2CityInfoRDD(spark)
    /**
      * 第五步：第三步和第四步的数据进行join 操作
      *  --> dataframe
      */
    val action_city_RDD = generateTempTable(spark,userActionRDD,cityInfoRDD)

    /**
      * 第六步：
      * 先统计出来各城市商品统计
      * select
      *     area,city_id,click_product_id,count(*) click_count
      * from
      *    action_city
      * group by
      *    area,city_id,click_product_id
      *
      * 各区域的每个商品的点击次数
      *
      *  * select
      *     area,click_product_id,count(*) click_count
      *     groupdistinct(concact_long_string(city_id,city_name,":"))
      * from
      *    action_city
      * group by
      *    area,click_product_id

      *  city_id,city_name   udf
      *  1,beijing             1:beijing
      *  2,shanghai            2:shanghai
      *  3,shenzheng           3:shenzheng
      *
      * 1:beijing
      * 1:beijing           udaf   (1:beijing,2shanghai,3zhenzheng)
      * 2:shanghai
      * 2:shanghai
      * 2:shanghai
      */
    aeraProcuctCountTotal(spark)
    /**
      *
      * 分析:
      *
      *
      * tmp_area_product_click_id_count
      *
      *  city_area,
      *  click_product_id
      *  count(*) click_count,
      *  city_info
      *
      * 第七步：把上一步统计出来的结果与商品表进行jion
      *
      */
    areaProductCountJoinProduct(spark)

    /**
      * 第八步：TopN
      */
    getTop10(spark)

    /**
      *第九步： 把结果持久化到数据库
      */


    spark.stop()
  }

  /**
    * 根据日期范围获取用户行为数据
    * @param spark  sparksession
    * @param startDate  开始日期
    * @param endDate  结束日期
    * @return  RDD(cityid，row)
    *
    * 思考：
    *   点击，下单，支付
    *   null
    *   ‘null’ 'NULL'
    *   */
  def getUserActionRDDByDateRange(spark: SparkSession,
                                  startDate:String,endDate:String):RDD[(Long,Row)]={
    val sql=
      s"""
        select
              city_id,click_product_id
        from
              user_visit_action
        where
              date >= ${startDate} and date <= ${endDate} and
              click_product_id is not null and
              click_product_id != 'null' and
              click_product_id != 'NULL' and
              click_product_id != ''
      """
    val df = spark.sql(sql)
    //返回去的是Type(城市id,row)
    df.rdd.map( row =>(row.getLong(0),row))
  }

  /**
    * 从MySQL数据库里面去获取城市信息表
    * @param spark
    * @return  cityid,row
    */
  def getCity2CityInfoRDD(spark:SparkSession):RDD[(Long,Row)] ={
    val df = spark.read.format("jdbc")
      .option("url", ConfigurationManager.getProperty(Constants.JDBC_URL))
      .option("user", ConfigurationManager.getProperty(Constants.JDBC_USER))
      .option("password", ConfigurationManager.getProperty(Constants.JDBC_PASSWORD))
      .option("dbtable", "city_info")
      .load()
    df.rdd.map( row =>(row.getLong(0),row))
  }

  /**
    * 用户行为表和城市信息表进行join
    * @param spark
    * @param useractionRDD 用户行为数据
    * @param city2cityInfoRDD 城市信息详细数据
    *
    *
    */
  def generateTempTable(spark:SparkSession,
                        useractionRDD:RDD[(Long,Row)],city2cityInfoRDD:RDD[(Long,Row)]): Unit ={
    /**
      * (Long, (Row, Row))
      * Long : CityId
      * Row: 用户行为的数据 cityid,click_product_id
      * Row:城市信息数据  cityid,city_name,area
      *
      * cityid,click_product_id,city_name,area
      */
    val joinRDD: RDD[(Long, (Row, Row))] = useractionRDD.join(city2cityInfoRDD)

    val rowRDD = joinRDD.map(tuple => {
      val cityID = tuple._1
      val click_product_id = tuple._2._1.getLong(1)
      val city_name = tuple._2._2.getString(1)
      val area = tuple._2._2.getString(2)
      Row(cityID, click_product_id, city_name, area)
    })
    val schame=StructType(
      StructField("city_id",LongType,true)::
        StructField("click_product_id",LongType,true)::
        StructField("city_name",StringType,true)::
        StructField("city_area",StringType,true):: Nil
    )
    val df = spark.createDataFrame(rowRDD,schame)
      df.createOrReplaceTempView("action_city")
  }

  /**
    * 统计各区域的各商品点击次数
    * @param spark
    */
  def aeraProcuctCountTotal(spark:SparkSession): Unit ={
    val sql=
      """
        select
           city_area,click_product_id,count(*) click_count,
           groupdistinct(concact_long_string(city_id,city_name,':')) city_info
        from
           action_city
         group by
           city_area,click_product_id
      """
    val df = spark.sql(sql)
    df.createOrReplaceTempView("tmp_area_product_click_id_count")
  }

  /**
    * 各区域商品点击次数表  join  商品信息表
    */
  def areaProductCountJoinProduct(spark:SparkSession): Unit ={
     val sql=
       """
          SELECT
            tc.city_area,
            tc.click_product_id,
            tc.click_count,
            tc.city_info,
            pi.product_name,
          if( get_json_object(pi.extends_info,'product_status') = 0,'自营商品','第三方商品') status
          FROM
             tmp_area_product_click_id_count  tc
          JOIN
             product_info pi
          ON
             tc.click_product_id = pi.prodcut_id
       """
    val df = spark.sql(sql)
    df.createOrReplaceTempView("area_product_count")
  }

  /**
    *
    * 分析：
    *  tc.city_area,
    *  tc.click_product_id,
       tc.click_count,
       tc.city_info,
       pi.product_name,
      status
    * 获取各区域热门商品
    *
    * 华北，华东  -》 A级
    * 华南，华中   -》 B级
    * @param spark
    */
  def getTop10(spark:SparkSession): Unit ={
    val sql=
      """
        SELECT
          CASE
            WHEN city_area='华北' or city_area='华东' then 'A级'
            WHEN city_area='华南' or city_area='华中' then 'B级'
            WHEN city_area='西北' or city_area='西南' then 'C级'
            WHEN city_area='东北' then 'D级'
            ELSE 'E级'
          END area_level,
          click_product_id,click_count,city_info,product_name,status
         FROM
         (
         SELECT
            city_area,click_product_id,click_count,city_info,product_name,status
            ROW_NUMBER() OVER(PARTITION BY city_area ORDER BY click_count desc) rank
         FROM
           area_product_count
           ) tmp
         WHERE tmp.rank <= 10

      """
    val df = spark.sql(sql)

    df.createOrReplaceTempView("area_click_product_top10")

  }

  /**
    * 持久化到数据库
    * @param spark
    */
  def persistResult(spark:SparkSession): Unit ={
    spark.read.table("area_click_product_top10").rdd.foreachPartition(
      partition =>{
        //
      }

    )
  }



}
