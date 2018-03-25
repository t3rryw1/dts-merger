package com.cozystay.model;

import com.aliyun.drc.clusterclient.message.ClusterMessage;

import java.util.LinkedList;
import java.util.List;

public class SyncTaskBuilder {
    private static List<String> sourceList = new LinkedList<>();
    public static SyncTaskImpl build(ClusterMessage message, String source) {
        //TODO: reformat message to SyncTaskImpl
        return null;
    }


    public static void addSource(String name) {
        sourceList.add(name);
    }
}
