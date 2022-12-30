package com.apache.dolphinscheduler.sdk;

import java.util.Objects;

/**
 * @author ysear
 * @date 2022/12/30
 */
public class ParameterInfo {

    /**
     * 名称
     */
    final String name;

    /**
     * 类型
     */
    String type;

    /**
     * 是否加密
     */
    boolean encode;

    /**
     * 必须参数
     */
    boolean require;

    /**
     * 默认值
     */
    Object defaultValue;

    public ParameterInfo(String name) {
        this.name = name;
    }

    public ParameterInfo(String name, boolean encode) {
        this.name = name;
        this.encode = encode;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public boolean isEncode() {
        return encode;
    }

    public void setEncode(boolean encode) {
        this.encode = encode;
    }

    public boolean isRequire() {
        return require;
    }

    public void setRequire(boolean require) {
        this.require = require;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ParameterInfo that = (ParameterInfo) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
