/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.transformers.impl;

import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.transformers.exception.UnTransformableException;
import java.util.ArrayList;
import java.util.Collection;
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
public class CollectionTransformerTest {

    private Collection<Integer> colInt;

    public CollectionTransformerTest() {
        colInt = new ArrayList<Integer>();
        colInt.add(Integer.valueOf(1));
        colInt.add(Integer.valueOf(1));
        colInt.add(Integer.valueOf(2));
        colInt.add(Integer.valueOf(3));
        colInt.add(Integer.valueOf(5));
        colInt.add(Integer.valueOf(8));
        colInt.add(Integer.valueOf(13));
        colInt.add(Integer.valueOf(21));
        colInt.add(Integer.valueOf(34));
        colInt.add(Integer.valueOf(0));

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
     * Test of toStore method, of class CollectionTransformer.
     */
    @Test
    public void testToStore() throws Exception {
        System.out.println("toStore");
        CollectionTransformer instance = new CollectionTransformer();
        MapStoreItem result = instance.toStore(colInt);
    }

    /**
     * Test of toObject method, of class CollectionTransformer.
     */
    @Test
    public void testToObject() throws UnTransformableException {
        System.out.println("toObject");
        CollectionTransformer instance = new CollectionTransformer();
        MapStoreItem item = instance.toStore(colInt);
        Collection expResult = colInt;
        Collection<Integer> result = (Collection<Integer>)instance.toObject(item);
        assertEquals(expResult, result);
    }
}