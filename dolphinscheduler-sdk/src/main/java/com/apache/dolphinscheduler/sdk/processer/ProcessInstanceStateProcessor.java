package com.apache.dolphinscheduler.sdk.processer;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import org.apache.commons.lang.StringUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.command.ProcessInstanceStateCommand;
import org.apache.dolphinscheduler.remote.processor.NettyRequestProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.NonNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author ysear
 * @date 2022/12/30
 */
public class ProcessInstanceStateProcessor implements NettyRequestProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessInstanceStateProcessor.class);

    private final static Map<String,ProcessStateCallback> SUB_ONE = new ConcurrentHashMap<>();


    private final static Map<String,ProcessStateCallback> SUB_ALL = new ConcurrentHashMap<>();

    @Override
    public void process(Channel channel, Command command) {
        try {
            Preconditions.checkArgument(CommandType.PROCESS_INSTANCE_STATE == command.getType(),
                    String.format("invalid command type: %s", command.getType()));

            ProcessInstanceStateCommand processInstanceStateCommand = JSONUtils.parseObject(command.getBody(), ProcessInstanceStateCommand.class);

            ProcessInstanceStateCommand.ConsumerType type = processInstanceStateCommand.getConsumerType();
            if (type.equals(ProcessInstanceStateCommand.ConsumerType.SUBSCRIBE_ONE)){
                for (Map.Entry<String,ProcessStateCallback> entry : SUB_ONE.entrySet()) {
                    if (processInstanceStateCommand.getId().equals(entry.getKey())){
                        entry.getValue().callback(processInstanceStateCommand);
                    }
                }
            }else {
                SUB_ALL.forEach((k, v)->{
                    v.callback(processInstanceStateCommand);
                });
            }
        }catch (Exception e){
            LOGGER.error("??????????????????: ",e);
        }
    }

    /**
     *  ???????????????
     * @param callback ??????
     * @param id ????????????ID
     */
    public static void addListener(ProcessStateCallback callback,String id,ProcessInstanceStateCommand.ConsumerType subType){
        switch (subType){
            case SUBSCRIBE_ALL:{
                SUB_ALL.put(id,callback);
                break;
            }
            case SUBSCRIBE_ONE:{
                SUB_ONE.put(id,callback);
                break;
            }
            default:
                SUB_ONE.put(id,callback);
        }
    }

    /**
     *  ????????????
     * @param id
     * @param consumerType
     */
    public static void removeListener(@NonNull String id, ProcessInstanceStateCommand.ConsumerType consumerType){
        ProcessStateCallback callback = null;
        Map<String,ProcessStateCallback> removeMaps =null;
        switch (consumerType){
            case SUBSCRIBE_ONE:{
                removeMaps = SUB_ONE;
                break;
            }
            case SUBSCRIBE_ALL:{
                removeMaps = SUB_ALL;
                break;
            }
            default:
        }

        if (null == removeMaps){
            return;
        }

        for (Map.Entry<String,ProcessStateCallback> entry : removeMaps.entrySet()) {
            if (StringUtils.equals(entry.getKey(),id)){
                callback = entry.getValue();
                break;
            }
        }
        if (null != callback){
            removeMaps.remove(id,callback);
        }
    }


}
