package com.zzr.es.test;

import com.alibaba.fastjson.JSON;
import com.zzr.es.util.ESUtil;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhaozhirong on 2019/4/22.
 */
public class Main {
    public static void main(String[] args) throws Exception {
//        createIndex();
//        createDocument();
//        catApi();
//        getDocument();

//        queryByField();
//        System.out.println(ESUtil.findAllDocument("test-index","test"));
        Map<String,Object> params = new HashMap<>();
        params.put("user","zzr");
        System.out.println(ESUtil.findDocumentByField("test-index","test",params));
    }


    /**
     * 创建文档
     * @throws Exception
     */
    public static void createDocument()throws Exception{
        RestClient restClient = RestClient.builder(new HttpHost("localhost",9200)).build();
        String method = "PUT";
        String endpoint = "/test-index/test/3";
        Map<String,Object> map = new HashMap<>();
        map.put("user","zzr");
        map.put("address","zhangbasi");
        map.put("age","22");
        HttpEntity entity = new NStringEntity(JSON.toJSONString(map), ContentType.APPLICATION_JSON);

        Request request =  new Request(method,endpoint);
        request.setEntity(entity);
        Response response = restClient.performRequest(request);
        System.out.println(EntityUtils.toString(response.getEntity()));
    }

    public static void getDocument() throws IOException {
        RestClient restClient = RestClient.builder(new HttpHost("localhost",9200)).build();
        String method = "GET";
        String endpoint = "/test-index/test/1";
        Response response = restClient.performRequest(new Request(method,endpoint));
        System.out.println(EntityUtils.toString(response.getEntity()));
    }

    public static void queryAll() throws IOException {
        RestClient restClient = RestClient.builder(new HttpHost("localhost",9200)).build();
        String method = "GET";
        String endPoint = "test-index/test/_search";
        Map<String,Object> param = new HashMap<>();
        param.put("query",new HashMap<String,Object>().put("match_all",new HashMap<>()));
        Request request = new Request(method,endPoint);
        Response response = restClient.performRequest(request);
        System.out.println(EntityUtils.toString(response.getEntity()));
    }

    public static void queryByField() throws IOException {
        RestClient restClient = RestClient.builder(new HttpHost("localhost",9200)).build();
        String method = "POST";
        String endPoint = "test-index/test/_search";
        Map<String,Object> param = new HashMap<>();
        param.put("query",new HashMap<String,Object>().put("match",new HashMap<String,Object>().put("user","kimchyzzr")));
        HttpEntity httpEntity = new NStringEntity(JSON.toJSONString(param),ContentType.APPLICATION_JSON);
        Request request = new Request(method,endPoint);
        request.setEntity(httpEntity);
        Response response = restClient.performRequest(request);
        System.out.println(EntityUtils.toString(response.getEntity()));
    }
}
