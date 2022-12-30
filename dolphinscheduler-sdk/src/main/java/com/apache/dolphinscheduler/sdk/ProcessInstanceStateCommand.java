package com.apache.dolphinscheduler.sdk;

import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;

import java.io.Serializable;

/**
 * @author ysear
 * @date 2022/12/30
 */
public class ProcessInstanceStateCommand implements Serializable {

    private String id;

    private int processInstanceId;

    private int taskInstanceId;

    public ProcessInstanceStateCommand() {
        super();
    }

    public ProcessInstanceStateCommand(
            int processInstanceId,
            int taskInstanceId
    ) {
        this.id = String.format("%d-%d",
                processInstanceId,
                taskInstanceId);

        this.processInstanceId = processInstanceId;
        this.taskInstanceId = taskInstanceId;
    }


    public Command convert2Command(CommandType commandType) {
        Command command = new Command();
        command.setType(commandType);
        byte[] body = JSONUtils.toJsonByteArray(this);
        command.setBody(body);
        return command;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getProcessInstanceId() {
        return processInstanceId;
    }

    public void setProcessInstanceId(int processInstanceId) {
        this.processInstanceId = processInstanceId;
    }

    public int getTaskInstanceId() {
        return taskInstanceId;
    }

    public void setTaskInstanceId(int taskInstanceId) {
        this.taskInstanceId = taskInstanceId;
    }

    @Override
    public String toString() {
        return "ProcessInstanceStateCommand{" +
                "key='" +  + '\'' +
                ", processInstanceId=" + processInstanceId +
                ", taskInstanceId=" + taskInstanceId +
                '}';
    }
}
