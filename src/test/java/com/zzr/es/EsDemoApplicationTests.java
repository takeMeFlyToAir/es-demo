package com.zzr.es;

import com.alibaba.fastjson.JSON;
import com.zzr.es.util.ESUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class EsDemoApplicationTests {

	public static final String index = "website";
	public static final String type = "blog";

	@Test
	public void deleteIndex() throws Exception {
		ESUtil.deleteIndex(index);
	}

	@Test
	public void createIndex() throws Exception {
		ESUtil.createIndex(index);
	}

	@Test
	public void createIndexByParams() throws IOException {
		Map<String,Object> params = new HashMap<>();
		Map<String,Object> settings = new HashMap<>();
		settings.put("number_of_shards",3);
		settings.put("number_of_replicas",1);
		params.put("settings",settings);
		ESUtil.createIndexByParams("book",params);
	}


	@Test
	public void createDocumentById() throws Exception {
		Map<String,Object> params = new HashMap<>();
		params.put("title","My first blog entry");
		params.put("text","Just trying this out...");
		params.put("date","2014/01/01");
		ESUtil.createDocumentById(index,type,"123",params);
	}

	@Test
	public void createDocument() throws Exception {
		Map<String,Object> params = new HashMap<>();
		params.put("title","My first blog entry");
		params.put("text","Just trying this out...");
		params.put("date","2014/01/01");
		ESUtil.createDocument(index,type,params);
	}

	@Test
	public void deleteDocument() throws Exception {
		ESUtil.deleteDocument(index,type,"89");
	}


	@Test
	public void updateDocument() throws Exception {
		Map<String,Object> params = new HashMap<>();
		params.put("first_name","345");
		params.put("last_name","345");
		params.put("age","345");
		params.put("about","345");
		List<String> list = new ArrayList<>();
		list.add("345");
		params.put("interests",list);
		ESUtil.updateDocumentById(index,type,"4",params);
	}

	@Test
	public void updateDocumentFieldById() throws Exception {
		Map<String,Object> params = new HashMap<>();
		params.put("title","zhangbasi");
		ESUtil.updateDocumentFieldById(index,type,"123",params);
	}


	@Test
	public void findAllDocument() throws Exception {
		Integer cc = null;
		Integer dd = 0;
		System.out.println(dd.toString().equals(cc));
	}

	@Test
	public void findDocumentByField() throws Exception {
		Map<String,Object> field = new HashMap<>();
		Map<String,Object> terms = new HashMap<>();
		Map<String,Object> all_interests = new HashMap<>();
		Map<String,Object> aggs = new HashMap<>();

		field.put("field","interests.keyword");
		terms.put("terms",field);
		all_interests.put("all_interests",terms);
		aggs.put("aggs",all_interests);
		System.out.println("===============");
		System.out.println(JSON.toJSONString(aggs));
		System.out.println(ESUtil.findDocumentByField(index,type,aggs));
	}

	@Test
	public void existDocument() throws Exception {
		List<User> users= null;
		List<Integer> list = users.stream().map(User::getId).collect(Collectors.toList());
		System.out.println(list);
	}
}
