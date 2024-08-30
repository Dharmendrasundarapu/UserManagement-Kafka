package com.amzur.controller

import com.amzur.entity.Users
import io.micronaut.configuration.kafka.annotation.KafkaListener
import io.micronaut.configuration.kafka.annotation.Topic
import org.slf4j.Logger
import org.slf4j.LoggerFactory



@KafkaListener(groupId = "sireesha-consumer")
class UserMessage {
    private static final Logger LOG= LoggerFactory.getLogger(UserMessage.class)
    @Topic("Siri-topic")
    def mahesh(def users)
    {
        LOG.info("Message received ${users}")
    }
}
