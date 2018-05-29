package com.cozystay.datasource;


import org.junit.After;
import org.junit.Before;

public class TestMessageParser {
    @Before
    public void setUp() {
        System.setProperty("COZ_MERGE_HOME", "..");
        System.out.println("set up " + this.getClass().getName());
    }


    @After
    public void tearDown(){
        System.out.println("tear down " + this.getClass().getName());
    }

}
