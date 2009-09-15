/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.dialect;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Pablo
 */
public class DerbyDialect extends SQLDialect{
    @Override
    public List<Object> initializeDataBase() {
        List<Object> statements = new ArrayList<Object>();
        statements.add("CREATE TABLE INTEGERS(ID BIGINT NOT NULL, PROPERTY VARCHAR(50) NOT NULL, VALUE INT)");
        statements.add("CREATE INDEX INTEGERS_VALUE ON INTEGERS (VALUE)");
        statements.add("ALTER TABLE INTEGERS ADD PRIMARY KEY(ID,PROPERTY)");
        statements.add("CREATE TABLE LONGS(ID BIGINT NOT NULL, PROPERTY VARCHAR(50) NOT NULL, VALUE BIGINT)");
        statements.add("CREATE INDEX LONGS_VALUE ON LONGS (VALUE)");
        statements.add("ALTER TABLE LONGS ADD PRIMARY KEY(ID,PROPERTY)");
        statements.add("CREATE TABLE NAME(ID BIGINT NOT NULL, TYPE VARCHAR(50) NOT NULL, NAME VARCHAR(50) NOT NULL)");
        statements.add("ALTER TABLE LONGS ADD PRIMARY KEY(ID)");
        statements.add("CREATE UNIQUE INDEX NAME_TYPE_NAME ON NAME (TYPE,NAME)");
        return statements;
    }
}
