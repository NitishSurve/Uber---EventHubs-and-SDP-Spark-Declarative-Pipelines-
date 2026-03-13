from pyspark import pipelines as dp
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Event Hubs configuration
EH_NAMESPACE                    = "uberevents2397"
EH_NAME                         = "ubertopics"

EH_CONN_STR = "Endpoint=sb://uberevents2397.servicebus.windows.net/;SharedAccessKeyName=ReadPolicy;SharedAccessKey=Z13zMbbTioN0z8a40DgwHOGhk6iHqfwOY+AEhMupxAU=;EntityPath=ubertopics"
# EH_CONN_STR                     = spark.conf.get("connection_string")
# Kafka Consumer configuration

KAFKA_OPTIONS = {
  "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
  "subscribe"                : EH_NAME,
  "kafka.sasl.mechanism"     : "PLAIN",
  "kafka.security.protocol"  : "SASL_SSL",
  "kafka.sasl.jaas.config"   : f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{EH_CONN_STR}\";",
  "kafka.request.timeout.ms" : 10000,
  "kafka.session.timeout.ms" : 10000,
  "maxOffsetsPerTrigger"     : 10000,
  "failOnDataLoss"           : True,
  "startingOffsets"          : 'earliest'
}

@dp.table
def rides_raw():
    df = spark.readStream.format("kafka") \
        .options(**KAFKA_OPTIONS) \
            .load()

    #Converting Values to String
    df = df.withColumn("rides",(col("value").cast("string")))
    return df
