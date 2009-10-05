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
public class MapStringTransformerTest {
    private Map<String,Object> mapa;


    public MapStringTransformerTest() {
        mapa = new HashMap<String,Object>();
        mapa.put("1",Long.valueOf(50));
        mapa.put("PRUEBA","VALORPRUEBA");
        mapa.put("2",new int[]{5,6});
        mapa.put("2",Thread.currentThread());
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
     * Test of toStore method, of class MapStringTransformer.
     */
    @Test
    public void testToStore() throws Exception {
        System.out.println("toStore");
        Map object = mapa;
        MapStringTransformer instance = new MapStringTransformer();
        instance.toStore(object);
    }

    /**
     * Test of toObject method, of class MapStringTransformer.
     */
    @Test
    public void testToObject() throws UnTransformableException {
        System.out.println("toObject");
        Map object = mapa;
        MapStringTransformer instance = new MapStringTransformer();
        MapStoreItem item = instance.toStore(object);
        Map expResult = mapa;
        Map result = instance.toObject(item);
        assertEquals(expResult, result);
    }

}