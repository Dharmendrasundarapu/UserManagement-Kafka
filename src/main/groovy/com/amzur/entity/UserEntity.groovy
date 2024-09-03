package com.amzur.entity

import io.micronaut.core.annotation.Introspected
import io.micronaut.serde.annotation.Serdeable


@Introspected
@Serdeable

class UserEntity {
    Long id
    String name
    String address
    Long phoneNumber
    String email
    String password

    UserEntity(Long id,String name, String address, Long phoneNumber, String email, String password) {
        this.id=id
        this.name = name
        this.address = address
        this.phoneNumber = phoneNumber
        this.email = email
        this.password = password
    }


    @Override
    public String toString() {
        return "UserEntity{" +
                "name='" + name + '\'' +
                ", address='" + address + '\'' +
                ", phoneNumber=" + phoneNumber +
                ", email='" + email + '\'' +
                ", password='" + password + '\'' +
                '}';
    }
}
