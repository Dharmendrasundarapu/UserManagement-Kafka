package com.amzur.service

import com.amzur.entity.UserEntity
import io.micronaut.configuration.kafka.annotation.KafkaClient
import io.micronaut.configuration.kafka.annotation.Topic


@KafkaClient
interface UserService {
    @Topic("Siri-topic")
    def add(def userEntity)

}