package com.apache.dolphinscheduler.sdk.remote;

import com.apache.dolphinscheduler.sdk.processer.ProcessInstanceStateProcessor;
import com.google.common.net.HostAndPort;
import io.netty.channel.Channel;
import org.apache.dolphinscheduler.remote.NettyRemotingClient;
import org.apache.dolphinscheduler.remote.command.Command;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.config.NettyClientConfig;
import org.apache.dolphinscheduler.remote.exceptions.RemotingException;
import org.apache.dolphinscheduler.remote.utils.Host;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author ysear
 * @date 2022/12/30
 */
public class DsRpcClient implements AutoCloseable{

    private final Logger logger = LoggerFactory.getLogger(DsRpcClient.class);

    /**
     * channels
     */
    private final ConcurrentHashMap<Host, Channel> channels = new ConcurrentHashMap<>(128);

    /**
     * ds master 通信客户端
     */
    private NettyRemotingClient remotingClient;

    public DsRpcClient() {
        connection();
    }

    private void connection() {
        if (null == remotingClient){
            this.remotingClient = new NettyRemotingClient(new NettyClientConfig());
        }
        this.remotingClient.registerProcessor(CommandType.PROCESS_INSTANCE_STATE,new ProcessInstanceStateProcessor());

    }


    @Override
    public void close() throws Exception {
        logger.info("Worker rpc client closing");
        remotingClient.close();
        logger.info("Worker rpc client closed");
    }

    public void send(final Host host, final Command command) throws RemotingException {
        remotingClient.send(host,command);
    }

    public void send(final String hostAndPort, final Command command) throws RemotingException {
        HostAndPort hostAndPort1 = HostAndPort.fromString(hostAndPort);
        remotingClient.send(new Host(hostAndPort1.getHost(),hostAndPort1.getPort()),command);
    }
}
