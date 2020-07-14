package com.atguigu.flume.interceptor;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author zhangjie
 * @create 2020-06-25 15:59
 * @description
 */
public class GetDate {
    public static void main(String[] args) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//        System.out.println(System.currentTimeMillis());
        System.out.println(sdf.format(new Date(1592216464862L)));
    }
}
