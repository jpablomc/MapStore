/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.transformers.impl;

import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.transformers.exception.UnTransformableException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Pablo
 */
public class ArrayTransformerTest {
        Integer[] colInt;

    public ArrayTransformerTest() {
        colInt = new Integer[]{1,2,3,5,8,13,21,34,0};
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
    }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    /**
     * Test of toStore method, of class ArrayTransformer.
     */
    @Test
    public void testToStore() throws Exception {
        System.out.println("toStore");
        ArrayTransformer instance = new ArrayTransformer();
        instance.toStore(colInt);
    }

    /**
     * Test of toObject method, of class ArrayTransformer.
     */
    @Test
    public void testToObject() throws UnTransformableException {
        System.out.println("toObject");
        ArrayTransformer instance = new ArrayTransformer();
        MapStoreItem item = instance.toStore(colInt);
        Object[] result = instance.toObject(item);
        assertArrayEquals(colInt, result);
    }

}