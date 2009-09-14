/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.dialect;

import es.uc3m.it.mapstore.db.MapStoreDialect;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 *
 * @author Pablo
 */
public class SQLDialect implements MapStoreDialect {

    @Override
    public boolean isCreated(Object o) {
        List<String> tables = (List<String>) o;
        String[] expectedTables = new String[] {"INTEGERS","LONGS"};
        return tables.containsAll(Arrays.asList(expectedTables));
    }

    @Override
    public List<Object> initializeDataBase() {
        throw new UnsupportedOperationException("Must be implemented for specific databse");
    }

    @Override
    public String create(long id, String key, Object value) {
        String aux;
        if (value instanceof Integer) {
            aux = generateInsertorUpdateSQL(id, key, (Integer)value,false);
        } else if (value instanceof Long) {
            aux = generateInsertorUpdateSQL(id, key, (Long)value,false);
        } else {
            aux =  generateInsertorUpdateSQL(id, key, value,false);
        }
        return aux;
    }

    @Override
    public Serializable update(long id, String key, Object value) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Serializable delete(long id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private String generateInsertorUpdateSQL(long id , String property, Integer value,boolean update) {
        String sql;
        if (!update) {
            sql = "INSERT INTO INTEGERS(ID,PROPERTY,VALUE) VALUES("+id+", '"+ property + "', " + value+")";
        } else {
            sql = "UPDATE INTEGERS SET VALUE = " + value + " WHERE ID = " + id + " AND PROPERTY = " + property + ";";
        }
        return sql;
    }

    private String generateInsertorUpdateSQL(long id , String property, Long value,boolean update) {
        String sql;
        if (!update) {
            sql = "INSERT INTO LONGS(ID,PROPERTY,VALUE) VALUES("+id+", '"+ property + "', " + value+")";
        } else {
            sql = "UPDATE LONGS SET VALUE = " + value + " WHERE ID = " + id + " AND PROPERTY = " + property + ";";
        }
        return sql;
    }

    private String generateInsertorUpdateSQL(long id , String property, Float value,boolean update) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private String generateInsertorUpdateSQL(long id , String property, Double value,boolean update) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private String generateInsertorUpdateSQL(long id , String property, Date value,boolean update) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private String generateInsertorUpdateSQL(long id , String property, Object value,boolean update) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public List<Object> getAll() {
        List<Object> statements = new ArrayList<Object>();
        statements.add("SELECT * FROM INTEGERS");
        statements.add("SELECT * FROM LONGS");
        return statements;
    }
}
