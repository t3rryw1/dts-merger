package com.cozystay.datasource;

import com.aliyun.drc.client.message.DataMessage;
import com.aliyun.drc.clusterclient.message.ClusterMessage;
import com.cozystay.SyncMain;
import com.cozystay.db.schema.SchemaRuleCollection;
import com.cozystay.model.SyncTask;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.IOException;

import java.util.Properties;

public class TestMessageParser {
    @Before
    public void setUp() {
        System.out.println("set up " + this.getClass().getName());
    }


    @After
    public void tearDown(){
        System.out.println("tear down " + this.getClass().getName());
    }

}
