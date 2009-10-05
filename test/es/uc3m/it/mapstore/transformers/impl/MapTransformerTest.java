/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.transformers.impl;

import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.transformers.exception.UnTransformableException;
import java.util.HashMap;
import java.util.Map;
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
public class MapTransformerTest {
    private Map mapa;

    public MapTransformerTest() {
        mapa = new HashMap();
        mapa.put(Integer.valueOf(10),Long.valueOf(50));
        mapa.put("PRUEBA","VALORPRUEBA");
        mapa.put(Thread.currentThread(),new int[]{5,6});
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
     * Test of toStore method, of class MapTransformer.
     */
    @Test
    public void testToStore() throws Exception {
        System.out.println("toStore");
        Map object = mapa;
        MapTransformer instance = new MapTransformer();
        MapStoreItem result = instance.toStore(object);
    }

    /**
     * Test of toObject method, of class MapTransformer.
     */
    @Test
    public void testToObject() throws UnTransformableException {
        System.out.println("toObject");
        Map object = mapa;
        MapTransformer instance = new MapTransformer();
        MapStoreItem item = instance.toStore(object);
        Map expResult = mapa;
        Map result = instance.toObject(item);
        assertEquals(expResult, result);
    }

}