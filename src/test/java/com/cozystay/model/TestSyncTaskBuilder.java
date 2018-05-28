package com.cozystay.model;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestSyncTaskBuilder {
    @Before
    public void setUp(){
        System.out.println("set up " + this.getClass().getName());
    }

    @After
    public void tearDown(){
        System.out.println("tear down " + this.getClass().getName());
    }
}
