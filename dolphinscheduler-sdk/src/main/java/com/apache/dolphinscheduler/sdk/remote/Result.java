package com.apache.dolphinscheduler.sdk.remote;

import java.util.Map;

/**
 * @author ysear
 * @date 2022/12/30
 */
public class Result {

    /**
     * Code编码
     */
    Integer code;

    /**
     * 消息体
     */
    String msg;

    /**
     * 数据集
     */
    Map<String, Object> data;

    /**
     * 失败标记
     */
    Boolean failed;

    /**
     * 成功标记
     */
    Boolean success;

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }

    public Boolean getFailed() {
        return failed;
    }

    public void setFailed(Boolean failed) {
        this.failed = failed;
    }

    public Boolean getSuccess() {
        return success;
    }

    public void setSuccess(Boolean success) {
        this.success = success;
    }

    @Override
    public String toString() {
        return "Result{" +
                "code=" + code +
                ", msg='" + msg + '\'' +
                ", data=" + data +
                ", failed=" + failed +
                ", success=" + success +
                '}';
    }
}
