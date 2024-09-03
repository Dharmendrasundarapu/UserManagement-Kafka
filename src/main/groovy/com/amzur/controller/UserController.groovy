package com.amzur.controller

import com.amzur.entity.UserEntity
import com.amzur.model.UserRequest
import com.amzur.service.UserService
import io.micronaut.core.type.Argument
import io.micronaut.http.HttpRequest
import io.micronaut.http.HttpResponse
import io.micronaut.http.HttpStatus
import io.micronaut.http.annotation.Body
import io.micronaut.http.annotation.Controller
import io.micronaut.http.annotation.Delete
import io.micronaut.http.annotation.Get
import io.micronaut.http.annotation.PathVariable
import io.micronaut.http.annotation.Post
import io.micronaut.http.annotation.Put
import io.micronaut.http.annotation.Status
import io.micronaut.http.client.HttpClient
import io.micronaut.http.client.annotation.Client
import io.micronaut.scheduling.TaskExecutors
import io.micronaut.scheduling.annotation.ExecuteOn
import jakarta.inject.Inject
import org.apache.kafka.common.protocol.types.Field.Str


@Controller("/users")
class UserController {

    @Inject
    UserService userService

    @Inject
    @Client("http://localhost:9091")
    HttpClient httpClient

    @ExecuteOn(TaskExecutors.BLOCKING)
    @Post("/data")
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
    @ExecuteOn(TaskExecutors.BLOCKING)
    @Get
    @Status(HttpStatus.CREATED)
    def getAllUsers()
    {
        try{
            HttpResponse<List<UserEntity>> responses=httpClient.toBlocking().exchange(
                    HttpRequest.GET("/user"),
                    Argument.listOf(UserEntity)

            )
            if(responses.status()==HttpStatus.OK && responses.body())
            {
                def users=responses.body()
                return  HttpResponse.ok(users)
            }
            else
            {
                return  HttpResponse.status(responses.status).body("Failed to get user object ")
            }
        }
        catch (Exception e)
        {
            return HttpResponse.serverError("An error occured ${e.message}")
        }

    }
    @ExecuteOn(TaskExecutors.BLOCKING)
    @Get("/{id}")
    @Status(HttpStatus.CREATED)
    def getById(Long id)
    {
        try {
            HttpResponse<UserEntity>response=httpClient.toBlocking().exchange(
                    HttpRequest.GET("/user/${id}"),
                    UserEntity
            )
            if(response.status()==HttpStatus.OK && response.body())
            {
                def users=response.body()
                return  HttpResponse.ok(users)
            }
            else
            {
                return  HttpResponse.status(response.status).body("Failed to get user object ")
            }
        }
        catch (Exception e)
        {
            return HttpResponse.serverError("An error occured ${e.message}")
        }
    }
    @ExecuteOn(TaskExecutors.BLOCKING)
    @Post   ("/login")
    @Status(HttpStatus.CREATED)
    def userLogIn(@Body UserRequest userRequest)
    {
        try
        {
            HttpResponse<UserEntity> response=httpClient.toBlocking().exchange(
                    HttpRequest.POST("/user/login",userRequest),
                    UserEntity
            )
            if(response.status()==HttpStatus.OK && response.body())
            {
                def user=response.body()
                return HttpResponse.ok(user)
            }
            else {
                return  HttpResponse.status(response.status).body("Failed to get user object ")
            }
        }
        catch (Exception e)
        {
            return HttpResponse.serverError("An error occured ${e.message}")
        }

    }
    @ExecuteOn(TaskExecutors.BLOCKING)
    @Delete("/delete/{id}")
    @Status(HttpStatus.CREATED)
    def deleteUser(@PathVariable Long id)
    {
        try {

        HttpResponse<?>response=httpClient.toBlocking().exchange(
                HttpRequest.DELETE("/user/${id}")
        )
            if(response.status()==HttpStatus.NO_CONTENT)
            {

                return  HttpResponse.noContent()
            }
            else
            {
                return  HttpResponse.status(response.status).body("Failed to get user object ")
            }
        }
        catch (Exception e)
        {
            return HttpResponse.serverError("An error occured ${e.message}")
        }
    }
    @ExecuteOn(TaskExecutors.BLOCKING)
    @Put("/update/{id}")
    @Status(HttpStatus.CREATED)
    def updateBooks(@PathVariable Long id,@Body UserRequest userRequest)
    {
        try{
        HttpResponse<UserEntity>responses=httpClient.toBlocking().exchange(
                HttpRequest.PUT("/user/${id}",userRequest),
                UserEntity
        )
            if ((responses.status==HttpStatus.CREATED  && responses.body())) {
                UserEntity savedUser=responses.body()

                if (userService.add(savedUser))
                {
                    return HttpResponse.ok("sent user details successfully through kafka")
                }
                else {
                    return HttpResponse.serverError("Unable to send your object")
                }
            }
            else {
                return  HttpResponse.status(responses.status).body("Failed to process user object in to other microservice")
            }
        }
        catch (Exception e)
        {
            return HttpResponse.serverError("An error occured ${e.message}")
        }
    }
}
