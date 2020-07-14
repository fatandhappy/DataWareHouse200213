package com.atguigu;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

/**
 * @author zhangjie
 * @create 2020-07-13 19:59
 * @description
 */
public class JedisPoolTest {
    public static void main(String[] args) {

        // 创建jedis连接池
        JedisPool jedisPool = new JedisPool("hadoop102", 6379);

        // 从连接池获取数据
        Jedis jedis = jedisPool.getResource();

        // 测试链接
        System.out.println(jedis.ping());

        // 归还连接
        jedisPool.close();
    }
}
