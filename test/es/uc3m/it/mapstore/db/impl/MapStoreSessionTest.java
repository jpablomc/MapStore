/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.impl;

import es.uc3m.it.mapstore.bean.MapStoreItem;
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
public class MapStoreSessionTest {

    public MapStoreSessionTest() {
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
     * Test of getSession method, of class MapStoreSession.
     */
//    @Test
//    public void testGetSession() {
//        System.out.println("getSession");
//        MapStoreSession expResult = null;
//        MapStoreSession result = MapStoreSession.getSession();
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }

    /**
     * Test of isClosed method, of class MapStoreSession.
     */
    @Test
    public void testIsClosed() {
        System.out.println("isClosed");
        MapStoreSession instance = MapStoreSession.getSession();
        boolean expResult = false;
        boolean result = instance.isClosed();
        assertEquals(expResult, result);
        expResult = true;
        instance.close();
        result = instance.isClosed();
        assertEquals(expResult, result);
    }

    /**
     * Test of close method, of class MapStoreSession.
     */
    @Test
    public void testClose() {
        System.out.println("close");
        MapStoreSession instance = MapStoreSession.getSession();
        assertFalse(instance.isClosed());
        instance.close();
        assertTrue(instance.isClosed());
    }

    /**
     * Test of beginTransaction method, of class MapStoreSession.
     */
    @Test
    public void testBeginTransaction() {
        MapStoreItem item = generateMapTStoreItemExample();
        System.out.println("beginTransaction");
        MapStoreSession instance = MapStoreSession.getSession();
        instance.beginTransaction();
        instance.save(item);
        instance.commit();
        instance.close();
    }

    private MapStoreItem generateMapTStoreItemExample() {
        MapStoreItem item = new MapStoreItem();
        item.setId(1);
        item.setName("Prueba");
        item.setType("Any");
        item.setProperty("prop1", Long.valueOf("10"));
        item.setProperty("prop2", Integer.valueOf("10"));
        return item;
    }

//    /**
//     * Test of commit method, of class MapStoreSession.
//     */
//    @Test
//    public void testCommit() {
//        System.out.println("commit");
//        MapStoreSession instance = null;
//        instance.commit();
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of rollback method, of class MapStoreSession.
//     */
//    @Test
//    public void testRollback() {
//        System.out.println("rollback");
//        MapStoreSession instance = null;
//        instance.rollback();
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of save method, of class MapStoreSession.
//     */
//    @Test
//    public void testSave() {
//        System.out.println("save");
//        MapStoreItem item = null;
//        MapStoreSession instance = null;
//        Serializable expResult = null;
//        Serializable result = instance.save(item);
//        assertEquals(expResult, result);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of update method, of class MapStoreSession.
//     */
//    @Test
//    public void testUpdate() {
//        System.out.println("update");
//        MapStoreItem item = null;
//        MapStoreSession instance = null;
//        instance.update(item);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }
//
//    /**
//     * Test of delete method, of class MapStoreSession.
//     */
//    @Test
//    public void testDelete() {
//        System.out.println("delete");
//        MapStoreItem item = null;
//        MapStoreSession instance = null;
//        instance.delete(item);
//        // TODO review the generated test code and remove the default call to fail.
//        fail("The test case is a prototype.");
//    }

}