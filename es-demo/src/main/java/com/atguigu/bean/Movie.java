package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhangjie
 * @create 2020-07-11 10:52
 * @description
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Movie {
    private String movie_id;
    private String movie_name;
}
