package com.apache.dolphinscheduler.sdk;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import org.apache.commons.lang.StringUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.command.TaskEventChangeCommand;
import org.apache.dolphinscheduler.remote.processor.NettyRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author ysear
 * @date 2022/12/30
 */
public class ProcessInstanceStateProcessor implements NettyRequestProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessInstanceStateProcessor.class);

    private final static Map<ProcessStateCallback, String> CM = new ConcurrentHashMap<>();

    @Override
    public void process(Channel channel, Command command) {
        try {
            Preconditions.checkArgument(CommandType.PROCESS_INSTANCE_STATE == command.getType(),
                    String.format("invalid command type: %s", command.getType()));

            ProcessInstanceStateCommand processInstanceStateCommand = JSONUtils.parseObject(command.getBody(), ProcessInstanceStateCommand.class);


            for (Map.Entry<ProcessStateCallback, String> entry : CM.entrySet()) {
                if (processInstanceStateCommand.getId().equals(entry.getValue()) || "__ALL__".equals(entry.getValue())){
                    entry.getKey().callback(processInstanceStateCommand);
                }
            }
        }catch (Exception e){
            LOGGER.error("数据处理异常: ",e);
        }
    }

    /**
     *  添加监听器
     * @param callback 监听
     * @param id 任务实例ID
     */
    public static void addListener(ProcessStateCallback callback,String id){
        CM.put(callback,id);
    }

    /**
     *  移除监听
     * @param id
     */
    public static void removeListener(String id){
        ProcessStateCallback callback = null;
        for (Map.Entry<ProcessStateCallback, String> entry : CM.entrySet()) {
            if (StringUtils.equals(entry.getValue(),id)){
                callback = entry.getKey();
                break;
            }
        }
        if (null != callback){
            CM.remove(callback,id);
            callback.finish();
        }
    }


}
