package com.apache.dolphinscheduler.sdk.remote;

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
        this.restfulUrl = url;
        ProxyFactory proxyFactory = new ProxyFactory();
        proxyFactory.addAdvice(this);
        proxyFactory.setTarget(target);

        return (T)proxyFactory.getProxy();
    }

    /**
     * ???????????????
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
                // ????????????
                String bodyStr = new String(Util.toByteArray(response.body().asInputStream()), StandardCharsets.UTF_8);
                return NumberUtils.createNumber(bodyStr).intValue();
            }

            // JSON ???????????????????????????
            String bodyStr = new String(Util.toByteArray(response.body().asInputStream()), StandardCharsets.UTF_8);
            return JSON.parseObject(bodyStr, type);
        };
    }


    /**
     * ????????????????????????
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
                // ????????????
                template.body(String.valueOf(object));
                return;
            }

            // ?????????????????????????????????
            MediaType contentType = getContentTypeValue(template.headers());
            // ??????????????????????????????header???????????????????????????application/json
            if(MediaType.JSON_UTF_8.is(contentType)) {
                // ??????????????????????????? ?????? JSON ??????
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
     * ????????? ??????????????????@Param ??????????????????????????????????????????
     *
     * @param parameter
     * @param param
     * @return
     */
    ParameterInfo getFormName(Parameter parameter, Annotation[] param) {
        if (param == null || param.length == 0) {
            return new ParameterInfo(parameter.getName());
        }

        // ?????? param ?????????
        Param params = (Param) param[0];
        return new ParameterInfo(params.value(), params.encoded());
    }

    /**
     * ??????????????????
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
            // ??????
            isMutilForm = argument instanceof File || argument instanceof MultipartFile;
            if (isMutilForm) {
                // ????????????????????????????????????
                break;
            }
        }
        Object result = null;
        if (!isMutilForm) {
            // ????????????
            result = invocation.proceed();
        } else {
            // ????????????
            result = new HashMap<>();
            Method method = invocation.getMethod();
            RequestLine requestLine = method.getAnnotation(RequestLine.class);
            Headers headers = method.getAnnotation(Headers.class);
            if (requestLine != null) {
                String[] split = requestLine.value().split("\\s+");

                // ????????????????????? POST???PUT ??????
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

                // ????????????
                result = executeRest(httpMethod, url, form, headers, method.getReturnType());
            }
        }
        return result;
    }

    /**
     * ?????? String ??? ${field} ?????? map ??????????????????????????????????????? SQL ?????????
     * @param valStr ?????? ${field} ???????????????
     * @param params ?????????????????????????????????????????????
     * @return ??????????????????????????? SQL
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
     * ????????????
     *
     * @param httpMethod HTTP POST???PUT
     * @param url ????????????
     * @param form ???????????????????????????
     * @param headers Header ??????
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

        // url ?????? {} ?????????????????????
        String newUrl = getFullString(url, params);
        // ??????????????????
        HttpEntityEnclosingRequestBase request = "PUT".equalsIgnoreCase(httpMethod) ? new HttpPut(newUrl) : new HttpPost(newUrl);
        MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create().setMode(HttpMultipartMode.RFC6532);
        multipartEntityBuilder.setCharset(StandardCharsets.UTF_8);
        // ????????????
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
