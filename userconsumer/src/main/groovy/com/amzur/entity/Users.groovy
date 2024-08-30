package com.amzur.entity

import io.micronaut.core.annotation.Introspected
import io.micronaut.serde.annotation.Serdeable

@Introspected
@Serdeable
class Users {
    String name
    String address
    Long phoneNumber
    String email
    String password

    Users(String name, String address, Long phoneNumber, String email, String password) {
        this.name = name
        this.address = address
        this.phoneNumber = phoneNumber
        this.email = email
        this.password = password
    }


    @Override
    public String toString() {
        return "Users{" +
                "name='" + name + '\'' +
                ", address='" + address + '\'' +
                ", phoneNumber=" + phoneNumber +
                ", email='" + email + '\'' +
                ", password='" + password + '\'' +
                '}';
    }
}
