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

    @Test
    public void TestCreateTaskByMessageParser() throws IOException {
        DataMessage.Record record = new DataMessage.Record();
        record.setType(DataMessage.Record.Type.UPDATE);
        ClusterMessage message = new ClusterMessage(record, null);
        Properties prop = new Properties();
        prop.load(SyncMain.class.getResourceAsStream("/test-filter.properties"));
        SchemaRuleCollection rules = SchemaRuleCollection.loadRules(prop);
        SyncTask task = DTSMessageParser.parseMessage(message, "source1", rules);
    }

    @After
    public void tearDown(){
        System.out.println("tear down " + this.getClass().getName());
    }

}
