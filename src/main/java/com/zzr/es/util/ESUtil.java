package com.zzr.es.util;

import com.alibaba.fastjson.JSON;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhaozhirong on 2019/4/22.
 * 文档是不可变的：他们不能被修改，只能被替换
 */
public class ESUtil {

    private static RestClient getRestClient(){
       return RestClient.builder(new HttpHost("es1",9201)).build();
    }

    /**
     * 删除索引
     * @param index
     * @throws IOException
     */
    public static void deleteIndex(String index) throws IOException {
        String method = "DELETE";
        getRestClient().performRequest(new Request(method,index));
    }

    /**
     * 创建索引
     * @param index
     * @throws IOException
     */
    public static void createIndex(String index) throws IOException {
        String method = "PUT";
        getRestClient().performRequest(new Request(method,index));
    }

    /**
     * 带参数创建索引
     * @param index
     * @param params
     * @throws IOException
     */
    public static void createIndexByParams(String index, Map<String,Object> params) throws IOException {
        String method = "PUT";
        HttpEntity entity = new NStringEntity(JSON.toJSONString(params), ContentType.APPLICATION_JSON);
        Request request = new Request(method,index);
        request.setEntity(entity);
        getRestClient().performRequest(request);
    }

    /**
     * 创建文档，需要自己控制id
     * @throws Exception
     */
    public static void createDocumentById(String index, String type,String id, Map<String,Object> params)throws Exception{
        HttpEntity entity = new NStringEntity(JSON.toJSONString(params), ContentType.APPLICATION_JSON);
        Request request =  getRequestForCreateDocumentById(index,type,id);
        request.setEntity(entity);
        getRestClient().performRequest(request);
    }

    /**
     * 创建文档，系统自动生成id
     * @throws Exception
     */
    public static void createDocument(String index, String type,Map<String,Object> params)throws Exception{
        HttpEntity entity = new NStringEntity(JSON.toJSONString(params), ContentType.APPLICATION_JSON);
        StringBuffer stringBuffer = new StringBuffer();
        String endPoint = stringBuffer.append("/").append(index).append("/").append(type).append("/").toString();
        Request request = new Request("PUT",endPoint);
        request.setEntity(entity);
        getRestClient().performRequest(request).getEntity();
    }

    /**
     * 删除文档
     * @param index
     * @param type
     * @param id
     * @throws Exception
     */
    public static void deleteDocument(String index, String type,String id)throws Exception{
        StringBuffer stringBuffer = new StringBuffer();
        String endPoint = stringBuffer.append("/").append(index).append("/").append(type).append("/").append(id).toString();
        Request request = new Request("DELETE",endPoint);
        getRestClient().performRequest(request);
    }

    /**
     * 根据id更新文档，实际是创建文档，elastic无更新功能
     * @param index
     * @param type
     * @param id
     * @param params
     * @throws Exception
     */
    public static void updateDocumentById(String index, String type,String id, Map<String,Object> params)throws Exception{
        HttpEntity entity = new NStringEntity(JSON.toJSONString(params), ContentType.APPLICATION_JSON);
        Request request =  getRequestForCreateDocumentById(index,type,id);
        request.setEntity(entity);
        getRestClient().performRequest(request);
    }


    /**
     * 根据id更新文档，实际是创建文档，elastic无更新功能
     * @param index
     * @param type
     * @param id
     * @param params
     * @throws Exception
     */
    public static void updateDocumentFieldById(String index, String type,String id, Map<String,Object> params)throws Exception{
        HttpEntity entity = new NStringEntity(JSON.toJSONString(params), ContentType.APPLICATION_JSON);
        StringBuffer stringBuffer = new StringBuffer();
        String endPoint = stringBuffer.append("/").append(index).append("/").append(type).append("/").append(id).append("/").append("_update").toString();
        System.out.println(endPoint);
        Request request = new Request("POST",endPoint);
        request.setEntity(entity);
        getRestClient().performRequest(request);
    }

    /**
     * 更加id查询文档
     * @param index
     * @param type
     * @param id
     * @return
     * @throws IOException
     */
    public static String findDocumentById(String index, String type, String id) throws IOException {
        StringBuffer stringBuffer = new StringBuffer();
        String endPoint = stringBuffer.append("/").append(index).append("/").append(type).append("/").append(id).toString();
        Request request = new Request("GET",endPoint);
        Response response = getRestClient().performRequest(request);
        return EntityUtils.toString(response.getEntity());
    }

    /**
     * 查询所有文档
     * @param index
     * @param type
     * @return
     * @throws IOException
     */
    public static String findAllDocument(String index,String type) throws IOException {
        Response response = getRestClient().performRequest(getRequestForQueryDocument(index,type));
        return EntityUtils.toString(response.getEntity());
    }

    /**
     * 根据条件查询文档
     * @param index
     * @param type
     * @param params
     * @return
     * @throws IOException
     */
    public static String findDocumentByField(String index, String type, Map<String,Object> params) throws IOException {
        HttpEntity entity = new NStringEntity(JSON.toJSONString(params), ContentType.APPLICATION_JSON);
        Request request = getRequestForQueryDocument(index,type);
        request.setEntity(entity);
        Response response = getRestClient().performRequest(request);
        return EntityUtils.toString(response.getEntity());
    }


    /**
     * 判断文档是否存在
     * @param index
     * @param type
     * @param id
     * @return
     * @throws Exception
     */
    public static Boolean existDocument(String index, String type,String id)throws Exception{
        StringBuffer stringBuffer = new StringBuffer();
        String endPoint = stringBuffer.append("/").append(index).append("/").append(type).append("/").append(id).toString();
        Request request = new Request("HEAD",endPoint);
        Response response = getRestClient().performRequest(request);
        return 200 == response.getStatusLine().getStatusCode();
    }


    private static Request getRequestForQueryDocument(String index,String type){
        StringBuffer stringBuffer = new StringBuffer();
        String endPoint = stringBuffer.append("/").append(index).append("/").append(type).append("/_search").toString();
        Request request = new Request("POST",endPoint);
        return request;
    }

    private static Request getRequestForCreateDocumentById(String index,String type,String id){
        StringBuffer stringBuffer = new StringBuffer();
        String endPoint = stringBuffer.append("/").append(index).append("/").append(type).append("/").append(id).toString();
        Request request = new Request("PUT",endPoint);
        return request;
    }
}
