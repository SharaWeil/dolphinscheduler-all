package com.apache.dolphinscheduler.sdk;

import com.alibaba.fastjson.JSON;
import com.google.common.net.MediaType;
import feign.*;
import feign.codec.Decoder;
import feign.codec.Encoder;
import feign.httpclient.ApacheHttpClient;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.slf4j.LoggerFactory;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.web.multipart.MultipartFile;
import org.apache.http.entity.mime.*;
import org.apache.http.entity.mime.content.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.Type;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author ysear
 * @date 2022/12/30
 */
public class DsClientFactory implements MethodInterceptor{

    private static org.slf4j.Logger log = LoggerFactory.getLogger(DsClientFactory.class);

    private final CloseableHttpClient httpClient;

    private String restfulUrl;

    static final Pattern VAR_PATTERN = Pattern.compile("(\\{\\s*(\\w|\\.|-|_)+\\s*\\})");

    public DsClientFactory() {
        httpClient = HttpClientBuilder.create().build();
    }

    public <T> T newInstance(Class<T> apiClazz, String url) {
        T target = Feign.builder().client(new ApacheHttpClient(httpClient))
                .logger(new Logger.JavaLogger())
                .logLevel(Logger.Level.FULL)
                .encoder(feignEncoder())
                .decoder(feignDecoder())
                .target(apiClazz, url);

        ProxyFactory proxyFactory = new ProxyFactory();
        proxyFactory.addAdvice(this);
        proxyFactory.setTarget(target);

        return target;
    }

    /**
     * 解码器实现
     * @return
     */
    public static Decoder feignDecoder() {
        // JSON
        return (Response response, Type type) -> {
            Response.Body body = response.body();
            if (response.status() == 404) {
                return Util.emptyValueOf(type);
            }
            if (body == null) {
                return null;
            }
            if (byte[].class.equals(type)) {
                return Util.toByteArray(response.body().asInputStream());
            }
            if (String.class == type) {
                return new String(Util.toByteArray(response.body().asInputStream()), StandardCharsets.UTF_8);
            }

            if(Number.class.isAssignableFrom(type.getClass())) {
                // 数字类型
                String bodyStr = new String(Util.toByteArray(response.body().asInputStream()), StandardCharsets.UTF_8);
                return NumberUtils.createNumber(bodyStr).intValue();
            }

            // JSON 类型或者为对象类型
            String bodyStr = new String(Util.toByteArray(response.body().asInputStream()), StandardCharsets.UTF_8);
            return JSON.parseObject(bodyStr, type);
        };
    }


    /**
     * 接口发送时编码器
     * @return
     */
    public static Encoder feignEncoder() {
        // JSON
        return (Object object, Type bodyType, RequestTemplate template) -> {
            if(object == null) {
                return;
            } else if (bodyType == String.class) {
                template.body((String)object);
                return;
            } else if (bodyType == byte[].class) {
                template.body(Request.Body.encoded((byte[]) object, StandardCharsets.UTF_8));
                return;
            } else if (object instanceof Number) {
                // 数字类型
                template.body(String.valueOf(object));
                return;
            }

            // 根据类型，分别不同处理
            MediaType contentType = getContentTypeValue(template.headers());
            // 这里获取了我们设置的header类型，也就是默认的application/json
            if(MediaType.JSON_UTF_8.is(contentType)) {
                // 复杂类型，对象类型 转为 JSON 发送
                template.body(JSON.toJSONString(object));
            } else if (object instanceof Map) {
                StringBuilder bodyData = new StringBuilder();
                Map<String, Object> data = (Map) object;
                int i = 0;
                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    if (i > 0) {
                        bodyData.append("&");
                    }
                    i++;
                    bodyData.append(entry.getKey());
                    bodyData.append("=");
                    if (entry.getValue() == null) {
                        continue;
                    }
                    final String value = entry.getValue() instanceof String ? (String) entry.getValue() : String.valueOf(entry.getValue());
                    if (value.contains(":") || value.contains("{") || value.contains("[") || value.contains("&")) {
                        try {
                            bodyData.append(URLEncoder.encode(value, "UTF-8"));
                        } catch (UnsupportedEncodingException e) {

                        }
                    } else {
                        bodyData.append(value);
                    }
                }
                template.body(bodyData.toString());
            }
        };
    }


    /**
     * 获取该 参数的名称，@Param 注解名称，若没有选择字段名称
     *
     * @param parameter
     * @param param
     * @return
     */
    ParameterInfo getFormName(Parameter parameter, Annotation[] param) {
        if (param == null || param.length == 0) {
            return new ParameterInfo(parameter.getName());
        }

        // 选择 param 的名称
        Param params = (Param) param[0];
        return new ParameterInfo(params.value(), params.encoded());
    }

    /**
     * 处理返回类型
     * @param headers
     * @return
     */
    public static MediaType getContentTypeValue(Map<String, Collection<String>> headers) {
        final Collection<String> collection = headers.get("Content-Type");
        if(collection == null || collection.isEmpty()) {
            return MediaType.parse("application/x-www-form-urlencoded");
        }
        String contentType = Optional.ofNullable(StringUtils.trimToNull(new ArrayList<String>(collection).get(0))).orElse("application/x-www-form-urlencoded");
        return MediaType.parse(contentType);
    }

    @Nullable
    @Override
    public Object invoke(@Nonnull MethodInvocation invocation) throws Throwable {
        Object[] arguments = invocation.getArguments();
        boolean isMutilForm = false;
        for (Object argument : arguments) {
            // 文件
            isMutilForm = argument instanceof File || argument instanceof MultipartFile;
            if (isMutilForm) {
                // 有一个是文件的，则是上传
                break;
            }
        }
        Object result = null;
        if (!isMutilForm) {
            // 普通表单
            result = invocation.proceed();
        } else {
            // 文件上传
            result = new HashMap<>();
            Method method = invocation.getMethod();
            RequestLine requestLine = method.getAnnotation(RequestLine.class);
            Headers headers = method.getAnnotation(Headers.class);
            if (requestLine != null) {
                String[] split = requestLine.value().split("\\s+");

                // 上传文件一般为 POST，PUT 两种
                String httpMethod = StringUtils.trimToEmpty(split[0]);
                String url = restfulUrl + split[1];
                Map<ParameterInfo, Object> form = new HashMap<>();
                Parameter[] parameters = method.getParameters();
                Annotation[][] annotations = method.getParameterAnnotations();
                int i = 0;
                for (Parameter parameter : parameters) {
                    final ParameterInfo parameterInfo = getFormName(parameter, annotations[i]);
                    Object objVal = invocation.getArguments()[i];
                    if(objVal != null && parameterInfo.isEncode()) {
                        String encodeObjVal = objVal instanceof String ? (String) objVal : String.valueOf(objVal);
                        form.put(parameterInfo, URLEncoder.encode(encodeObjVal, "UTF-8"));
                    } else {
                        form.put(parameterInfo, objVal);
                    }
                    i++;
                }

                // 提交表单
                result = executeRest(httpMethod, url, form, headers, method.getReturnType());
            }
        }
        return result;
    }

    /**
     * 根据 String 的 ${field} 按照 map 中的参数替换，组成一个新的 SQL 返回。
     * @param valStr 支持 ${field} 的变量替换
     * @param params 参数，如果没有参数则会原路返回
     * @return 返回新的变量替换的 SQL
     */
    public static String getFullString(String valStr, Map<String, String> params){
        if(valStr == null) {
            return null;
        }
        final Matcher matcher = VAR_PATTERN.matcher(valStr);
        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            String group = matcher.group();
            String field = StringUtils.trim(group.substring(1, group.length() - 1));
            String val = params.get(field);
            if(val == null) {
                continue;
            }
            int start = val.indexOf("{");
            int end = val.indexOf("}");
            if(start >= 0 && end > start) {
                val = getFullString(val, params);
            }
            matcher.appendReplacement(sb, val);
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    /**
     * 提交表单
     *
     * @param httpMethod HTTP POST，PUT
     * @param url 连接信息
     * @param form 提交表单，携带文件
     * @param headers Header 信息
     * @return
     */
    public <T> T executeRest(String httpMethod,
                             String url,
                             Map<ParameterInfo, Object> form,
                             Headers headers,
                             Class<T> returnType) {
        final HashMap<String, String> params = new HashMap<>();
        for (Map.Entry<ParameterInfo, Object> entry : form.entrySet()) {
            String v = (entry.getValue() instanceof String ? (String) entry.getValue() : String.valueOf(entry.getValue()));
            params.put(entry.getKey().getName(), v);
        }

        // url 中有 {} 变量替换的情况
        String newUrl = getFullString(url, params);
        // 请求函数区别
        HttpEntityEnclosingRequestBase request = "PUT".equalsIgnoreCase(httpMethod) ? new HttpPut(newUrl) : new HttpPost(newUrl);
        MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create().setMode(HttpMultipartMode.RFC6532);
        multipartEntityBuilder.setCharset(StandardCharsets.UTF_8);
        // 设置编码
        multipartEntityBuilder.seContentType(ContentType.create("multipart/form-data", StandardCharsets.UTF_8));

        InputStream in = null;
        try {
            log.info("form: " + form);
            if(headers != null && headers.value().length > 0) {
                for (String header : headers.value()) {
                    final String[] split = header.split(":\\s");
                    if (split.length >= 2) {
                        String contentValue = StringUtils.trimToEmpty(split[1]);
                        if(contentValue.startsWith("{") && contentValue.endsWith("}")) {
                            Object v = form.get(new ParameterInfo(
                                    StringUtils.trimToEmpty(contentValue.substring(1, contentValue.length() - 1))));
                            contentValue = v instanceof String ? (String)v : String.valueOf(v);
                        }
                        log.info("add header: {}: {}", split[0], contentValue);
                        request.setHeader(split[0], contentValue);
                    }
                }
            } else {
                request.setHeader("sessionId", (String) form.get("sessionId"));
            }

            for (Map.Entry<ParameterInfo, Object> entry : form.entrySet()) {
                if (entry.getValue() instanceof File || entry.getValue() instanceof MultipartFile) {
                    if (entry.getValue() instanceof File) {
                        multipartEntityBuilder.addPart(entry.getKey().getName(), new FileBody((File) entry.getValue()));
                        //String name = ((File) entry.getValue()).getName();
                        //multipartEntityBuilder.addPart("name", new StringBody(name, ContentType.MULTIPART_FORM_DATA));
                    } else {
                        MultipartFile uploadFile = (MultipartFile) entry.getValue();
                        in = uploadFile.getInputStream();
                        try {
                            String name = ((MultipartFile) entry.getValue()).getOriginalFilename();
                            multipartEntityBuilder.addPart(entry.getKey().getName(), new InputStreamBody(in, ContentType.MULTIPART_FORM_DATA, name));
                            multipartEntityBuilder.addTextBody("name", name);
                        } finally {
                            in.close();
                        }
                    }
                } else {
                    multipartEntityBuilder.addTextBody(entry.getKey().getName(),
                            (entry.getValue() instanceof String ? (String) entry.getValue() : String.valueOf(entry.getValue())));
                }
            }

            request.setEntity(multipartEntityBuilder.build());
            try(CloseableHttpResponse response = httpClient.execute(request);) {
                if (response.getStatusLine().getStatusCode() == 200) {
                    final HttpEntity entity = response.getEntity();
                    String json = EntityUtils.toString(entity, StandardCharsets.UTF_8);
                    log.info(json);
                    EntityUtils.consumeQuietly(entity);
                    return JSON.parseObject(json, returnType);
                } else {
                    log.error("execute {} error.", request.getURI());
                }
            }
        } catch (IOException e) {
            log.error("execute rest api error.", e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                }
            }
        }
        return null;
    }
}
