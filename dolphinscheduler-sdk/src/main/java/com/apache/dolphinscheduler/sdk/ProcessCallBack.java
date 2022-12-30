package com.apache.dolphinscheduler.sdk;

/**
 * @author ysear
 * @date 2022/12/30
 */
public interface ProcessCallBack {
    /**
     *  任务完成
     * @param command
     */
    public void processFinished(ProcessInstanceStateCommand command);


    /**
     *  task完成
     * @param command
     */
    public void taskFinished(ProcessInstanceStateCommand command);

}