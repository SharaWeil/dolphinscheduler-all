package com.apache.dolphinscheduler.sdk.configuration;

import com.apache.dolphinscheduler.sdk.DsClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author ysear
 * @date 2022/12/30
 */
@Configuration(
        proxyBeanMethods = false
)
@ConditionalOnClass(DsClient.class)
@EnableConfigurationProperties(DolphinSchedulerProperties.class)
public class DolphinSchedulerAutoConfiguration {

    public DolphinSchedulerProperties dolphinSchedulerProperties;

    public DolphinSchedulerAutoConfiguration(DolphinSchedulerProperties dolphinSchedulerProperties) {
        this.dolphinSchedulerProperties = dolphinSchedulerProperties;
    }

    @Bean(destroyMethod = "shutDown")
    @ConditionalOnMissingBean
    public DsClient dsClient(){
        return new DsClient(dolphinSchedulerProperties);
    }
}
