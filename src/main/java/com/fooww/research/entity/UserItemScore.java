package com.fooww.research.entity;

import lombok.Data;

/**
 * author:zwy
 * Date:2020-01-05
 * Time:15:25
 */
@Data
public class UserItemScore {

    private Long user;

    private Long item;

    private Float score;
}
