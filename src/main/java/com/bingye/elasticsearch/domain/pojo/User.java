package com.bingye.elasticsearch.domain.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class User {

    private String id;

    private String name;

    private int age;

    private String desc;

}
