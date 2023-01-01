package com.apache.dolphinscheduler.sdk.utils;

import cn.hutool.core.lang.Snowflake;
import cn.hutool.core.util.IdUtil;

/*
 * @Author Administrator
 * @Date 2023/1/1
 **/
public class SnowflakeIdUtils {

    private static final Snowflake snowflake = IdUtil.getSnowflake();

    public static Long generateId(){
        return snowflake.nextId();
    }

    public static String generateIdStr(){
        return Long.toString(snowflake.nextId());
    }
}
