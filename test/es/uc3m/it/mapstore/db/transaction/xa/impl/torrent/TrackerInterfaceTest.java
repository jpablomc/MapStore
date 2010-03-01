/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.torrent;

import java.io.File;
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
public class TrackerInterfaceTest {

    public TrackerInterfaceTest() {
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
     * Test of getInstance method, of class TrackerInterface.
     */
    @Test
    public void testGetInstance() {
        System.out.println("getInstance");
        File downloads = new File("d");
        File uploads = new File("u");
        TrackerInterface result = TrackerInterface.getInstance(downloads, uploads);
        assertNotNull(result);
        result.shutdown();
    }

    /**
     * Test of downloadTorrent method, of class TrackerInterface.
     */
    @Test
    public void testDownloadTorrent() {
        System.out.println("downloadTorrent");
        File f = new File("F:\\PFC\\prueba\\0_orig.torrent");
        File downloads = new File("d");
        File uploads = new File("u");
        TrackerInterface instance = TrackerInterface.getInstance(downloads, uploads);
        instance.downloadTorrent(f);
        
    }

    /**
     * Test of uploadTorrent method, of class TrackerInterface.
     */
    //@Test
    public void testUploadTorrent() {
        System.out.println("uploadTorrent");
        File f = null;
        TrackerInterface instance = null;
        instance.uploadTorrent(f);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

}