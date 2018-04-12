package com.cozystay.model;

import com.cozystay.SyncMain;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.*;

public class TestSchemaRuleCollection {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws IOException {
        System.out.println("set up " + this.getClass().getName());
    }

    @Test
    public void testLoadNormalString() {
        SchemaRuleCollection.FilterRule rule = SchemaRuleCollection.parseFilter("galaxy.listings.updated_at");
        Assert.assertEquals(rule.getDatabaseName(), "galaxy");
        Assert.assertEquals(rule.getTableName(), "listings");
        Assert.assertEquals(rule.getFieldName(), "updated_at");
    }

    @Test
    public void testLoadIllegalString() {
        thrown.expect(IllegalArgumentException.class);
        thrown.expectMessage("Wrong filter format, must follow db.table.field format");
        SchemaRuleCollection.FilterRule rule1 = SchemaRuleCollection.parseFilter("*.updated_at");
    }


    @Test
    public void testLoadWildcardString() {
        SchemaRuleCollection.FilterRule rule1 = SchemaRuleCollection.parseFilter("*.*.updated_at");
        Assert.assertNull(rule1.getDatabaseName());
        Assert.assertNull(rule1.getTableName());
        Assert.assertEquals(rule1.getFieldName(), "updated_at");

        SchemaRuleCollection.FilterRule rule2 = SchemaRuleCollection.parseFilter("galaxy_eadu.*.updated_at");
        Assert.assertEquals(rule2.getDatabaseName(), "galaxy_eadu");
        Assert.assertNull(rule2.getTableName());
        Assert.assertEquals(rule2.getFieldName(), "updated_at");
    }

    private SchemaRuleCollection loadProperties() throws IOException {
        Properties prop = new Properties();
        prop.load(SyncMain.class.getResourceAsStream("/test-filter.properties"));
        return SchemaRuleCollection.loadRules(prop);

    }

    @Test
    public void testLoadFromProperties() throws IOException {
        SchemaRuleCollection ruleList = loadProperties();
        Assert.assertEquals(ruleList.filterRules.size(), 3);
        Assert.assertEquals(ruleList.filterRules.get(0).getFieldName(), "created_at");
        Assert.assertEquals(ruleList.filterRules.get(1).getFieldName(), "user_notes");
        Assert.assertEquals(ruleList.filterRules.get(2).getFieldName(), "updated_at");
    }

    private SyncOperation buildOperationWithTwoItems(String dbName, String tableName, String fieldName1, String fieldName2) {
        SyncOperation.SyncItem item1 = new SyncOperation.SyncItem<>(fieldName1, "val1", "val2");
        SyncOperation.SyncItem item2 = new SyncOperation.SyncItem<>(fieldName2, "val1", "val2");
        List<SyncOperation.SyncItem> items = new ArrayList<>(Arrays.asList(item1, item2));
        SyncTask task = new SyncTaskImpl("id-123", dbName, tableName);
        List<String> sources = new ArrayList<>(Arrays.asList("source1", "source2"));

        return new SyncOperationImpl(task,
                SyncOperation.OperationType.UPDATE,
                items,
                "source1",
                sources,
                new Date());

    }

    @Test
    public void testMatchSyncItem(){
        SchemaRuleCollection.FilterRule rule1 = SchemaRuleCollection.parseFilter("galaxy_eadu.calendar.user_notes");
        SchemaRuleCollection.FilterRule rule2 = SchemaRuleCollection.parseFilter("*.*.updated_at");
        SyncOperation operation = buildOperationWithTwoItems("galaxy_eadu",
                "calendar",
                "user_notes",
                "updated_at");
        SyncOperation.SyncItem item1 = operation.getSyncItems().get(0);
        SyncOperation.SyncItem item2 = operation.getSyncItems().get(1);
        Assert.assertTrue(rule1.match(item1,operation.getTask()));
        Assert.assertTrue(rule2.match(item2,operation.getTask()));

    }


    @Test
    public void testFilterSyncOperation() throws IOException {
        SchemaRuleCollection ruleList = loadProperties();
        Assert.assertFalse(ruleList.filter(buildOperationWithTwoItems("galaxy",
                "listings",
                "created_at",
                "updated_at")));

        Assert.assertTrue(ruleList.filter(buildOperationWithTwoItems("galaxy_eadu",
                "any_table",
                "created_at",
                "updated_at")));

        Assert.assertTrue(ruleList.filter(buildOperationWithTwoItems("galaxy_eadu",
                "calendar",
                "user_notes",
                "updated_at")));

        Assert.assertFalse(ruleList.filter(buildOperationWithTwoItems("galaxy_eadu",
                "calendar",
                "other_note",
                "updated_at")));


    }


    @After
    public void tearDown() {
//        System.out.println("tear down " + this.getClass().getName());
    }
}
