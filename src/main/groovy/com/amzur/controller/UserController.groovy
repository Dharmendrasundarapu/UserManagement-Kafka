package com.amzur.controller

import com.amzur.entity.UserEntity
import com.amzur.model.UserRequest
import com.amzur.service.UserService
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.Status
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.scheduling.TaskExecutors
import io.micronaut.scheduling.annotation.ExecuteOn
import jakarta.inject.Inject




@Controller("/users")
class UserController {

    @Inject
    UserService userService

    @Inject
    @Client("http://localhost:9091")
    HttpClient httpClient

    @ExecuteOn(TaskExecutors.BLOCKING)
    @Post
    @Status(HttpStatus.CREATED)
    def createUser(@Body UserRequest userRequest)
    {
        try {

            HttpResponse<UserEntity> response = httpClient.toBlocking().exchange(
                    HttpRequest.POST("/user", userRequest),
                    UserEntity
            )
            if ((response.status==HttpStatus.CREATED  && response.body())) {
                  UserEntity savedUser=response.body()

                  if (userService.add(savedUser))
                {
                    return HttpResponse.ok("sent user details successfully through kafka")
                }
                else {
                    return HttpResponse.serverError("Unable to send your object")
                }
            }
            else {
                return  HttpResponse.status(response.status).body("Failed to process user object in to other microservice")
            }
        }
        catch (Exception e)
        {
            return HttpResponse.serverError("An error occured ${e.message}")
        }

    }
}
