package org.moqui.kafka

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.CreateTopicsResult
import org.apache.kafka.clients.admin.DescribeTopicsResult
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.moqui.impl.context.ExecutionContextImpl
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.concurrent.ExecutionException

Logger logger = LoggerFactory.getLogger("org.moqui.impl.sendEmailTemplate")
ExecutionContextImpl ec = context.ec

if (dataFeedId) def topicName = dataFeedId
Properties props = new Properties()
props.put("bootstrap.servers", "localhost:9092")

AdminClient kafkaAdminClient = AdminClient.create(props)

DescribeTopicsResult topic = kafkaAdminClient.describeTopics(Collections.singletonList(dataFeedId))

try{
    topicDescription = topic.topicNameValues().get(dataFeedId).get()
    logger.info("Topic Name : " + topicDescription)
} catch (ExecutionException e) {
    // exit early for almost all exceptions
    if (! (e.getCause() instanceof UnknownTopicOrPartitionException)) {
        e.printStackTrace()
        throw e
    }

    // if we are here, topic doesn't exist
    logger.info("Topic " + dataFeedId + " does not exist. Going to create it now")

    CreateTopicsResult newTopic = kafkaAdminClient.createTopics(Collections.singletonList(new NewTopic(dataFeedId, Optional<Integer>.empty(), Optional<Short>.empty())))

    logger.info("Created Topic : " + dataFeedId)
}

kafkaAdminClient.close(Duration.ofSeconds(5))

props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

producer = new KafkaProducer<String, String>(props);
ObjectMapper objectMapper = new ObjectMapper()
for (Map document in documentList) {
    try {
        documentString = objectMapper.writeValueAsString(document);
    } catch (JsonProcessingException e) {
        e.printStackTrace();
    }
    ProducerRecord<String, String> record = new ProducerRecord<>(dataFeedId, document._id, documentString)

    try {
        producer.send(record)
    } catch (Exception e) {
        e.printStackTrace()
    }

    logger.info("Sent Kafka Message : Topic Name = " + dataFeedId + ", Key = " + document._id + ", Value = " + documentString)

    producer.close()
}


