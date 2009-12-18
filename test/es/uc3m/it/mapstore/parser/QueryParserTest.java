/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.parser;

import es.uc3m.it.mapstore.bean.MapStoreCondition;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Pablo
 */
public class QueryParserTest {

    public QueryParserTest() {
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
     * Test of queryToConditions method, of class QueryParser.
     */
    @Test
    public void testQueryToConditions() {
        System.out.println("queryToConditions");
        String[] queries = new String[]{
//            "propiedad = valor",
//            "propiedad = 12",
//            "propiedad = 12.2553",
//            "propiedad = \"frase\"",
//            "propiedad = \"Esto es una frase\"",
//            "propiedad = [1,2]",
//            "propiedad = [1 ,2]",
//            "propiedad = [abc ,cde]",
//            "propiedad = [11 ,22]",
//            "propiedad = [\"add asd\" ,\"eee s\"]",
//            "prp1 <= 01/01/2007 prp2 > bbb AND prp4 != vvv OR prp6 != ddd OR pro7 ~ eee",
//            "prp1 <= 01/01/2007 AND prp2 > bbb AND prp4 != vvv OR prp6 != ddd AND pro7 ~ eee",
//            "(prp1= pepito) -> {ANY, 1}",
//            "(_TYPE =  es.uc3m.it.mapstore.web.beans.DataType AND _NAME = GeoPoint) -> {[DATATYPE,a] ,3}", // AND _TYPE = es.uc3m.it.mapstore.web.beans.DataObject"
            "(_TYPE =  es.uc3m.it.mapstore.web.beans.DataType AND _NAME = GeoPoint) -> {[DATATYPE,a] ,[<--] ,3}" // AND _TYPE = es.uc3m.it.mapstore.web.beans.DataObject"
        };
        for (String query: queries) {
            System.out.println(query);
            MapStoreCondition result = QueryParser.queryToConditions(query);


        }
    }

}