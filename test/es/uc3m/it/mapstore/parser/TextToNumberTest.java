/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.parser;

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
public class TextToNumberTest {

    public TextToNumberTest() {
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
     * Test of toText method, of class TextToNumber.
     */
    @Test
    public void testToText() {
        long l = -999999999999999L;
        while (true) {
            String aux = TextToNumber.toText(l);
            System.out.println(l + " - " + aux);
            l++;
        }
    }

}