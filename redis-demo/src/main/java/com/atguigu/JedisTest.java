package com.atguigu;

import redis.clients.jedis.Jedis;

import java.util.Set;

/**
 * @author zhangjie
 * @create 2020-07-13 19:52
 * @description
 */
public class JedisTest {
    public static void main(String[] args) {

        // 创建jedis客户端
        Jedis jedis = new Jedis("hadoop102", 6379);

        // ping一下
        String ping = jedis.ping();

        // 打印结果
        System.out.println(ping);

        System.out.println("Git Git");

        Set<String> lll = jedis.smembers("lll");
        System.out.println(lll);

        // 关闭连接
        jedis.close();
    }
}
