from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.libs import typed_lit
from streamingallpipeline.streaming.main.one.this.config.ConfigStore import *
from streamingallpipeline.streaming.main.one.this.udfs.UDFs import *

def dest_qa_kafka_steraming(spark: SparkSession, in0: DataFrame):
    df1 = in0.select([to_json(struct("*")).cast('string').alias("value")])
    writer = df1.writeStream\
                 .format("kafka")\
                 .option("checkpointLocation", "dbfs:/tmp/e2e/streaming_qa_kafka/uitesting")
    writer\
        .options(
          **{
            "kafka.sasl.jaas.config": "kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required username=\"ehtbzofh\" password=\"ehtbzofh\";",
            "kafka.sasl.mechanism": "SCRAM-SHA-256",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.bootstrap.servers": "dory-01.srvs.cloudkafka.com:9094,dory-02.srvs.cloudkafka.com:9094,dory-03.srvs.cloudkafka.com:9094",
            "topic": "ehtbzofh-test-topic-1-target",
          }
        )\
        .start()
