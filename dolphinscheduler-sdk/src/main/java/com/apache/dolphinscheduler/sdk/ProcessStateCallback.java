package com.apache.dolphinscheduler.sdk;

/**
 * @author ysear
 * @date 2022/12/30
 */
public interface ProcessStateCallback {

    /**
     * 回掉函数
     * @param command
     */
    public void callback(ProcessInstanceStateCommand command);


    /**
     * 完成
     */
    default void finish(){

    }
}
