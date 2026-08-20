package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;


import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {

    private StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    private static final Long BEGIN_TIMESTAMP = 1735689600L;
    private static final int COUNT_BITS = 32;

    public Long nextId(String keyPrefix) {
        // 1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        Long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        Long timestamp = nowSecond - BEGIN_TIMESTAMP;

        // 2.生成序列号
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        Long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);
        // 3.拼接并返回
        return timestamp << COUNT_BITS | count;
    }

    /*
    public static void main(String[] args) {
        LocalDateTime now = LocalDateTime.of(2025,1,1,0,0,0);
        Long second = now.toEpochSecond(ZoneOffset.UTC);
        System.out.println(second);
    }
     */
}
