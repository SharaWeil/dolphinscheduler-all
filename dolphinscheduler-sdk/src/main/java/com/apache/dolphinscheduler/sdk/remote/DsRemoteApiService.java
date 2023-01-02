package com.apache.dolphinscheduler.sdk.remote;

import feign.Body;
import feign.Headers;
import feign.Param;
import feign.RequestLine;

/**
 * @author ysear
 * @date 2022/12/30
 */
public interface DsRemoteApiService {

    @RequestLine("POST /login?userName={userName}&userPassword={userPassword}")
    Result login(@Param(value = "userName") String userName,
                 @Param(value = "userPassword") String userPassword);


    /**
     *  提交任务
     * @param key
     * @return
     */
    @RequestLine("POST /projects/{projectCode}/executors/start-process-instance")
    @Headers({"Content-Type: application/x-www-form-urlencoded", "token: {token}"})
    Result submitTask(String key);



    /**
     * 提交任务
     *
     * @param token Session Id
     * @param projectCode 项目编码
     * @param taskCode 任务编码
     * @param scheduleTime 任务定时管理
     * @param failureStrategy 任务状态 CONTINUE
     * @param startParams 一般是一个 JSON String。如：{"xxx":"GGG"}
     * @return
     */
    @RequestLine("POST /projects/{projectCode}/executors/start-process-instance")
    @Headers({"Content-Type: application/x-www-form-urlencoded", "token: {token}"})
    Result submitTask(@Param(value = "token") String token,
                      @Param(value = "projectCode") String projectCode,
                      @Param(value = "processDefinitionCode") String taskCode,
                      @Param(value = "scheduleTime", encoded = true) String scheduleTime,
                      @Param(value = "failureStrategy") String failureStrategy,
                      @Param(value = "warningType") String warningType,
                      @Param(value = "warningGroupId") int warningGroupId,
                      @Param(value = "execType") String execType,
                      @Param(value = "startNodeList") String startNodeList,
                      @Param(value = "taskDependType") String taskDependType,
                      @Param(value = "runMode") String runMode,
                      @Param(value = "processInstancePriority") String processInstancePriority,
                      @Param(value = "workerGroup") String workerGroup,
                      @Param(value = "environmentCode") String environmentCode,
                      @Param(value = "startParams") String startParams,
                      @Param(value = "expectedParallelismNumber") String expectedParallelismNumber,
                      @Param(value = "dryRun") int dryRun);



    /**
     * 获取当前所有的 master。
     * @param token
     * @return
     */
    @RequestLine("GET /monitor/masters")
    @Headers({"Content-Type: application/x-www-form-urlencoded", "token: {token}"})
    ArrayResult listMaster(@Param(value = "token") String token);




    /**
     * 获取当前所有的 workers。
     * @param token
     * @return
     */
    @RequestLine("GET /monitor/workers")
    @Headers({"Content-Type: application/x-www-form-urlencoded", "token: {token}"})
    ArrayResult getAllWorker(@Param(value = "token") String token);


    /**
     * 获取当前所有的 workers group 分组
     * @param token
     * @param searchVal
     * @param page
     * @param pageSize
     * @return
     */
    @RequestLine("GET /worker-groups?pageSize={pageSize}&pageNo={page}&searchVal={searchVal}")
    @Headers({"Content-Type: application/x-www-form-urlencoded", "token: {token}"})
    ArrayResult getWorkerGroups(@Param(value = "token") String token,
                                @Param(value = "searchVal") String searchVal,
                                @Param(value = "page") int page,
                                @Param(value = "pageSize") int pageSize);

    /**
     * 获取当前所有的 env 环境变量
     * @param token
     * @param searchVal
     * @param page
     * @param pageSize
     * @return
     */
    @RequestLine("GET /environment/list-paging?pageSize={pageSize}&pageNo={page}&searchVal={searchVal}")
    @Headers({"Content-Type: application/x-www-form-urlencoded", "token: {token}"})
    ArrayResult getAllEnvs(@Param(value = "token") String token,
                           @Param(value = "searchVal") String searchVal,
                           @Param(value = "page") int page,
                           @Param(value = "pageSize") int pageSize);



}
