package com.apache.dolphinscheduler.sdk;


import com.google.common.net.HostAndPort;
import org.apache.commons.lang.StringUtils;
import org.apache.dolphinscheduler.remote.NettyRemotingClient;
import org.apache.dolphinscheduler.remote.command.CommandType;
import org.apache.dolphinscheduler.remote.config.NettyClientConfig;
import org.apache.dolphinscheduler.remote.utils.Host;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author ysear
 * @date 2022/12/30
 */

public class DsClient implements AutoCloseable{

    private static final Logger LOGGER = LoggerFactory.getLogger(DsClient.class);

    private DolphinSchedulerProperties dsProperties;

    /**
     *  token
     */
    private String token;


    /**
     * ds调用服务
     */
    private final DsRemoteApiService dsRemoteApiService;

    private final DsRpcClient client;


    public DsClient(DolphinSchedulerProperties dolphinSchedulerProperties) {
        this.dsProperties = dolphinSchedulerProperties;
        this.token = dolphinSchedulerProperties.getToken();
        dsRemoteApiService = new DsClientFactory().newInstance(DsRemoteApiService.class,dsProperties.getUrl());
        client = new DsRpcClient();
    }


    /**
     * 提交任务
     *
     * @param projectCode 项目编码
     * @param taskCode 任务编码
     * @param scheduleTime 任务定时管理
     * @param failureStrategy 任务状态 CONTINUE
     * @param taskDependType TASK_ONLY,TASK_POST
     * @param startNodeList 开始启动的任务节点 ID
     * @param processInstancePriority MEDIUM
     * @param startParams 一般是一个 JSON String。如：{"xxx":"GGG"}
     * @return
     */
    public Result submitTask(@NonNull String projectCode,
                             @NonNull String taskCode,
                             String scheduleTime,
                             String failureStrategy,
                             String warningType,
                             int warningGroupId,
                             String execType,
                             String startNodeList,
                             String taskDependType,
                             String runMode,
                             String processInstancePriority,
                             String workerGroup,
                             String environmentCode,
                             String startParams,
                             String expectedParallelismNumber,
                             int dryRun) {
        scheduleTime = Optional.ofNullable(StringUtils.trimToNull(scheduleTime)).orElse("");
        processInstancePriority = Optional.ofNullable(StringUtils.trimToNull(processInstancePriority)).orElse("MEDIUM");
        warningType = Optional.ofNullable(StringUtils.trimToNull(warningType)).orElse("NONE");
        failureStrategy = Optional.ofNullable(StringUtils.trimToNull(failureStrategy)).orElse("CONTINUE");
        execType = Optional.ofNullable(StringUtils.trimToNull(execType)).orElse("");
        startNodeList = Optional.ofNullable(StringUtils.trimToNull(startNodeList)).orElse("");
        taskDependType = Optional.ofNullable(StringUtils.trimToNull(taskDependType)).orElse("TASK_POST");
        expectedParallelismNumber = Optional.ofNullable(StringUtils.trimToNull(expectedParallelismNumber)).orElse("");
        runMode = Optional.ofNullable(StringUtils.trimToNull(runMode)).orElse("RUN_MODE_SERIAL");
        int dryRunX = Math.max(0, dryRun);

        return this.submit(projectCode, taskCode, scheduleTime, failureStrategy,
                warningType, warningGroupId, execType, startNodeList, taskDependType, runMode,
                processInstancePriority, workerGroup, environmentCode, startParams,
                expectedParallelismNumber, dryRunX);
    }


    private Result submit(String projectCode,
                          String taskCode,
                          String scheduleTime,
                          String failureStrategy,
                          String warningType,
                          int warningGroupId,
                          String execType,
                          String startNodeList,
                          String taskDependType,
                          String runMode,
                          String processInstancePriority,
                          String workerGroup,
                          String environmentCode,
                          String startParams,
                          String expectedParallelismNumber,
                          int dryRun) {
        return dsRemoteApiService.submitTask(token, projectCode, taskCode, scheduleTime, failureStrategy,
                warningType, warningGroupId, execType, startNodeList, taskDependType, runMode,
                processInstancePriority, workerGroup, environmentCode, startParams,
                expectedParallelismNumber, dryRun);
    }

    /**
     * 提交一次性任务，并支持回调
     * @param projectCode
     * @param taskCode
     * @param warningGroupId
     * @param workerGroup
     * @param environmentCode
     * @param startParams
     * @param callback 回调函数
     * @param await 需要等待的超时时间
     * @param timeUnit 时间单位
     *
     * @return
     */
    public void submitTask(@NonNull String projectCode,
                           @NonNull String taskCode,
                           int warningGroupId,
                           String workerGroup,
                           String environmentCode,
                           String startParams,
                           final ProcessCallBack callback,
                           long await,
                           TimeUnit timeUnit
    ){

        String scheduleTime = "";
        String processInstancePriority = "MEDIUM";
        String warningType = "NONE";
        String failureStrategy = "CONTINUE";
        String execType = "";
        String startNodeList = "";
        String taskDependType = "TASK_POST";
        String expectedParallelismNumber = "";
        String runMode = "RUN_MODE_SERIAL";
        String hostAndPort;

        final ArrayResult master = getAllMaster();
        if(master == null || master.getData() == null || master.getData().isEmpty()) {
            throw new IllegalArgumentException("没有可用的 dolphinscheduler master。");
        }

        Map<String, Object> masterInfo = (Map)master.getData().get(0);
        String zkpath = (String) masterInfo.get("zkDirectory");
        if(StringUtils.isBlank(zkpath)) {
            throw new IllegalArgumentException("没有可用的 dolphinscheduler master。");
        }
        // 默认的 那个 master
        hostAndPort = StringUtils.trimToNull(new Path(zkpath).getName());
        if(StringUtils.isBlank(hostAndPort)) {
            throw new IllegalArgumentException("没有可用的 dolphinscheduler master。");
        }

        //提交任务
        Result result = submitTask(projectCode, taskCode, scheduleTime, failureStrategy,
                warningType, warningGroupId, execType, startNodeList, taskDependType, runMode,
                processInstancePriority, workerGroup, environmentCode, startParams,
                expectedParallelismNumber, 0);

        if (result.getCode() != 0){
            LOGGER.error("提交任务失败");
            throw new RuntimeException("提交任务失败: "+result.getMsg());
        }
        Map<String, Object> data = result.getData();
        String commandId = "123";

        // 等待接收
        final CountDownLatch latch = new CountDownLatch(1);

        // 通过master的rpc接口，发送订阅这个command id的事件
        ProcessInstanceStateCommand stateCommand = new ProcessInstanceStateCommand();
        stateCommand.setId(commandId);
        try {
            HostAndPort hostAndPort1 = HostAndPort.fromString(hostAndPort);
            client.send(new Host(hostAndPort1.getHost(),hostAndPort1.getPort()),stateCommand.convert2Command(CommandType.PROCESS_INSTANCE_STATE));

            ProcessStateCallback processStateCallback = new ProcessStateCallback() {
                @Override
                public void callback(ProcessInstanceStateCommand command) {
                    // 根据不同的状态调用不同的callBack方法
                }
                @Override
                public void finish() {
                    latch.countDown();
                }
            };
            ProcessInstanceStateProcessor.addListener(processStateCallback,commandId);

            // 根据等待超时事件计算
            if(await <= 0 || timeUnit == null) {
                latch.await();
            } else {
                // 等待一定的时间，超时则通过
                latch.await(await, timeUnit);
            }
        }catch (Exception e){
            e.printStackTrace();
        }


    }


    /**
     * 获取所有的 master
     * @return
     */
    public ArrayResult getAllMaster() {
        return dsRemoteApiService.listMaster(token);
    }

    /**
     * 获取所有的 worker
     * @return
     */
    public ArrayResult getAllWorker() {
        return dsRemoteApiService.getAllWorker(token);
    }

    /**
     * 获取当前所有的 workers group 分组
     * @param searchVal
     * @param page
     * @param pageSize
     * @return
     */
    public ArrayResult getWorkerGroups(String searchVal,
                                       int page,
                                       int pageSize) {
        searchVal = Optional.ofNullable(StringUtils.trimToNull(searchVal)).orElse("");
        page = Math.max(1, page);
        pageSize = pageSize <= 0 ? 10 : pageSize;
        return dsRemoteApiService.getWorkerGroups(token, searchVal, page, pageSize);
    }

    /**
     * 获取当前所有的 env 环境变量
     * @param searchVal
     * @param page
     * @param pageSize
     * @return
     */
    public ArrayResult getAllEnvs(String searchVal,
                                  int page,
                                  int pageSize){
        searchVal = Optional.ofNullable(StringUtils.trimToNull(searchVal)).orElse("");
        page = Math.max(1, page);
        pageSize = pageSize <= 0 ? 10 : pageSize;
        return dsRemoteApiService.getAllEnvs(token, searchVal, page, pageSize);
    }

    @Override
    public void close() throws Exception {

    }
}
