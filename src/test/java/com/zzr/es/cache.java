package com.zzr.es;

import com.google.common.cache.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhaozhirong on 2019/5/5.
 */
public class cache {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        LoadingCache<String, String> cache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(10, TimeUnit.SECONDS)
                .removalListener(new MyRemovalListener())
                .build(new CacheLoader<String, String>() {
                    @Override
                    public String load(String s) throws Exception {
                        System.out.println("load");
                        return "zzr";
                    }
                });

        System.out.println(cache.get("ddd"));
        System.out.println(cache.get("ddd"));
        Thread.sleep(15000);
        System.out.println(cache.get("ddd"));
        System.out.println(cache.get("ddd"));
    }

    private static class MyRemovalListener implements RemovalListener<String,String>{

        @Override
        public void onRemoval(RemovalNotification<String, String> removalNotification) {
            System.out.println("key==="+removalNotification.getKey());
            System.out.println("value==="+removalNotification.getValue());
        }
    }
}
