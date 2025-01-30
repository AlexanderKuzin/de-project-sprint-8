from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, to_json, col, lit, struct, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

# настройки security для кафки
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': f'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
    'kafka.bootstrap.servers': 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091',
}

postgresql_settings_restaurants = {
    'user': 'student',
    'password': 'de-student',
    'url': 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
    'driver': 'org.postgresql.Driver',
    'dbtable': 'subscribers_restaurants', 
    'schema': 'public',
}

postgresql_settings_feedback = {
    'user': 'jovyan',
    'password': 'jovyan',
    'url': 'jdbc:postgresql://localhost:5432/de',
    'driver': 'org.postgresql.Driver',
    'dbtable': 'subscribers_feedback', 
    'schema': 'public',
}

TOPIC_IN = 'student.topic.cohort29.kuzinaa_in'
TOPIC_OUT = 'student.topic.cohort29.kuzinaa_out'

def spark_init() -> SparkSession:
    return (SparkSession.builder \
            .appName("RestaurantSubscribeStreamingService") \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.jars.packages", spark_jars_packages) \
            .getOrCreate())

def restaurant_read_stream(spark: SparkSession) -> DataFrame:
    return (spark.readStream \
            .format('kafka') \
            .options(**kafka_security_options) \
            .option('subscribe', TOPIC_IN) \
            .load())

def transform(df: DataFrame) -> DataFrame:
    incomming_message_schema = StructType([
        StructField("restaurant_id", StringType(), True),
        StructField("adv_campaign_id", StringType(), True),
        StructField("adv_campaign_content", StringType(), True),
        StructField("adv_campaign_owner", StringType(), True),
        StructField("adv_campaign_owner_contact", StringType(), True),
        StructField("adv_campaign_datetime_start", LongType(), True),
        StructField("adv_campaign_datetime_end", LongType(), True),
        StructField("datetime_created", LongType(), True),
    ])

    # Добавляем колонку с текущим временем в датафрейм и фильтруем по ней
    return (df.select(from_json(col("value").cast("string"), incomming_message_schema).alias("parsed_key_value")) \
                .select(col("parsed_key_value.restaurant_id"),
                    col("parsed_key_value.adv_campaign_id"),
                    col("parsed_key_value.adv_campaign_content"),
                    col("parsed_key_value.adv_campaign_owner"),
                    col("parsed_key_value.adv_campaign_owner_contact"),
                    col("parsed_key_value.adv_campaign_datetime_start"),
                    col("parsed_key_value.adv_campaign_datetime_end"),
                    col("parsed_key_value.datetime_created"),
                    ) \
                .withColumn('trigger_datetime_created', lit(int(round(datetime.utcnow().timestamp()))).cast(LongType())) \
                .where((col("adv_campaign_datetime_start") < col("trigger_datetime_created")) & (col("adv_campaign_datetime_end") > col("trigger_datetime_created"))))

def read_subscribers(spark: SparkSession) -> DataFrame:
    return (spark.read \
            .format('jdbc') \
            .options(**postgresql_settings_restaurants) \
            .load())

def join(filtered_df, subscribers_df) -> DataFrame:
    # убираем дубликаты с заданной границей устаревших данных в 60 минут
    df = filtered_df.join(subscribers_df, on="restaurant_id", how="inner") \
        .select(
            'restaurant_id', 
            'adv_campaign_id', 
            'adv_campaign_content', 
            'adv_campaign_owner', 
            'adv_campaign_owner_contact', 
            'adv_campaign_datetime_start', 
            'adv_campaign_datetime_end',
            'datetime_created', 
            'client_id',
            'trigger_datetime_created'
            ) \
        .withColumn('timestamp', 
                    from_unixtime(col('trigger_datetime_created'), "yyyy-MM-dd' 'HH:mm:ss.SSS").cast(TimestampType())) \
        .dropDuplicates(['restaurant_id', 'adv_campaign_id', 'client_id']) \
        .withWatermark("timestamp", "60 minutes") \
        .drop('timestamp')

    return df       
    
# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
    
    # записываем df в PostgreSQL с полем feedback
    postgres_df = df.withColumn('feedback', lit(None).cast(StringType()))
    postgres_df.write \
        .mode('append') \
        .format('jdbc') \
        .options(**postgresql_settings_feedback) \
        .option('autoCommit', 'true') \
        .save()
        
    # создаём df для отправки в Kafka. Сериализация в json.
    kafka_df = df.select(to_json(struct(col('*'))).alias('value'))
    
    # отправляем сообщения в результирующий топик Kafka без поля feedback
    kafka_df.write \
        .outputMode("append") \
        .format('kafka') \
        .options(**kafka_security_options) \
        .option('topic', TOPIC_OUT) \
        .option('truncate', False) \
        .save()
        
    # очищаем память от df
    df.unpersist()   
    
    
# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = spark_init()

# читаем из топика Kafka сообщения с акциями от ресторанов
restaurant_read_stream_df = restaurant_read_stream(spark)

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = transform(restaurant_read_stream_df)

# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = read_subscribers(spark)

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid).
result_df = join(filtered_read_stream_df, subscribers_restaurant_df)

# запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination() 