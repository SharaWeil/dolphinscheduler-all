package com.apache.dolphinscheduler.sdk;

import org.apache.dolphinscheduler.remote.command.ProcessInstanceStateCommand;

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
