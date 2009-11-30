/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.transformers.factory.impl;

import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.transformers.MapStoreTransformer;
import es.uc3m.it.mapstore.transformers.impl.ArrayTransformer;
import es.uc3m.it.mapstore.transformers.impl.CollectionTransformer;
import es.uc3m.it.mapstore.transformers.impl.MapTransformer;
import es.uc3m.it.mapstore.transformers.impl.MapStoreItemTransformer;
import es.uc3m.it.mapstore.transformers.impl.DefaultTransformer;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
public class DefaultTransformerFactoryTest {

    public DefaultTransformerFactoryTest() {
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
     * Test of getFactory method, of class DefaultTransformerFactory.
     */
    @Test
    public void testGetFactory_Object() {
        System.out.println("getFactory");

        List<Integer> lista= new ArrayList<Integer>();
        Set<String> set = new HashSet<String>();
        Object[] array = new GregorianCalendar[3];
        Map<Object,Object> mapa = new HashMap<Object,Object>();
        MapStoreItem i = new MapStoreItem();
        Object obj = new Date();

        DefaultTransformerFactory instance = new DefaultTransformerFactory();
        
        MapStoreTransformer expResult1 = new CollectionTransformer();
        MapStoreTransformer expResult2 = new CollectionTransformer();
        MapStoreTransformer expResult3 = new ArrayTransformer();
        MapStoreTransformer expResult4 = new MapTransformer();
        MapStoreTransformer expResult5 = new MapStoreItemTransformer();
        MapStoreTransformer expResult6 = new DefaultTransformer();

        MapStoreTransformer result1 = instance.getFactory(lista);
        MapStoreTransformer result2 = instance.getFactory(set);
        MapStoreTransformer result3 = instance.getFactory(array);
        MapStoreTransformer result4 = instance.getFactory(mapa);
        MapStoreTransformer result5 = instance.getFactory(i);
        MapStoreTransformer result6 = instance.getFactory(obj);

        assertEquals(expResult1.getClass(), result1.getClass());
        assertEquals(expResult2.getClass(), result2.getClass());
        assertEquals(expResult3.getClass(), result3.getClass());
        assertEquals(expResult4.getClass(), result4.getClass());
        assertEquals(expResult5.getClass(), result5.getClass());
        assertEquals(expResult6.getClass(), result6.getClass());

    }

    /**
     * Test of getFactory method, of class DefaultTransformerFactory.
     */
    @Test
    public void testGetFactory_Class() {
        System.out.println("getFactory");

        List<Integer> lista= new ArrayList<Integer>();
        Set<String> set = new HashSet<String>();
        Double[] array = new Double[3];
        Map<Object,Object> mapa = new HashMap<Object,Object>();
        MapStoreItem i = new MapStoreItem();
        Object obj = new Date();

        DefaultTransformerFactory instance = new DefaultTransformerFactory();

        MapStoreTransformer expResult1 = new CollectionTransformer();
        MapStoreTransformer expResult2 = new CollectionTransformer();
        MapStoreTransformer expResult3 = new ArrayTransformer();
        MapStoreTransformer expResult4 = new MapTransformer();
        MapStoreTransformer expResult5 = new MapStoreItemTransformer();
        MapStoreTransformer expResult6 = new DefaultTransformer();

        MapStoreTransformer result1 = instance.getFactory(lista.getClass());
        MapStoreTransformer result2 = instance.getFactory(set.getClass());
        MapStoreTransformer result3 = instance.getFactory(array.getClass());
        MapStoreTransformer result4 = instance.getFactory(mapa.getClass());
        MapStoreTransformer result5 = instance.getFactory(i.getClass());
        MapStoreTransformer result6 = instance.getFactory(obj.getClass());

        assertEquals(expResult1.getClass(), result1.getClass());
        assertEquals(expResult2.getClass(), result2.getClass());
        assertEquals(expResult3.getClass(), result3.getClass());
        assertEquals(expResult4.getClass(), result4.getClass());
        assertEquals(expResult5.getClass(), result5.getClass());
        assertEquals(expResult6.getClass(), result6.getClass());
    }

}