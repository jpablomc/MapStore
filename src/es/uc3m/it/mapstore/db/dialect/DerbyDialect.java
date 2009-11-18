/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.dialect;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;

/**
 *
 * @author Pablo
 */
public class DerbyDialect extends SQLDialect{
    @Override
    public List<Object> initializeDataBase() {
        List<Object> statements = new ArrayList<Object>();
        statements.add("CREATE TABLE INTEGERS(ID BIGINT NOT NULL, VERSION BIGINT NOT NULL, PROPERTY VARCHAR(50) NOT NULL, VALUE INT)");
        statements.add("CREATE INDEX INTEGERS_VALUE ON INTEGERS (VALUE)");
        statements.add("ALTER TABLE INTEGERS ADD PRIMARY KEY(ID,VERSION,PROPERTY)");

        statements.add("CREATE TABLE LONGS(ID BIGINT NOT NULL, VERSION BIGINT NOT NULL, PROPERTY VARCHAR(50) NOT NULL, VALUE BIGINT)");
        statements.add("CREATE INDEX LONGS_VALUE ON LONGS (VALUE)");
        statements.add("ALTER TABLE LONGS ADD PRIMARY KEY(ID, VERSION, PROPERTY)");

        statements.add("CREATE TABLE FLOATS(ID BIGINT NOT NULL, VERSION BIGINT NOT NULL, PROPERTY VARCHAR(50) NOT NULL, VALUE REAL)");
        statements.add("CREATE INDEX FLOATS_VALUE ON FLOATS (VALUE)");
        statements.add("ALTER TABLE FLOATS ADD PRIMARY KEY(ID,VERSION,PROPERTY)");

        statements.add("CREATE TABLE DOUBLES(ID BIGINT NOT NULL, VERSION BIGINT NOT NULL, PROPERTY VARCHAR(50) NOT NULL, VALUE DOUBLE)");
        statements.add("CREATE INDEX DOUBLES_VALUE ON DOUBLES (VALUE)");
        statements.add("ALTER TABLE DOUBLES ADD PRIMARY KEY(ID,VERSION,PROPERTY)");

        statements.add("CREATE TABLE DATES(ID BIGINT NOT NULL, VERSION BIGINT NOT NULL, PROPERTY VARCHAR(50) NOT NULL, VALUE TIMESTAMP)");
        statements.add("CREATE INDEX DATES_VALUE ON DATES (VALUE)");
        statements.add("ALTER TABLE DATES ADD PRIMARY KEY(ID, VERSION, PROPERTY)");

        statements.add("CREATE TABLE INTEGERS_LIST(ID BIGINT NOT NULL, VERSION BIGINT NOT NULL, PROPERTY VARCHAR(50) NOT NULL, POSITION INT NOT NULL, VALUE INT)");
        statements.add("CREATE INDEX INTEGERS_LIST_VALUE ON INTEGERS_LIST (VALUE)");
        statements.add("ALTER TABLE INTEGERS_LIST ADD PRIMARY KEY(ID,VERSION,PROPERTY,POSITION)");

        statements.add("CREATE TABLE LONGS_LIST(ID BIGINT NOT NULL, VERSION BIGINT NOT NULL, PROPERTY VARCHAR(50) NOT NULL, POSITION INT NOT NULL, VALUE BIGINT)");
        statements.add("CREATE INDEX LONGS_LIST_VALUE ON LONGS_LIST (VALUE)");
        statements.add("ALTER TABLE LONGS_LIST ADD PRIMARY KEY(ID, VERSION, PROPERTY,POSITION)");

        statements.add("CREATE TABLE FLOATS_LIST(ID BIGINT NOT NULL, VERSION BIGINT NOT NULL, PROPERTY VARCHAR(50) NOT NULL, POSITION INT NOT NULL, VALUE REAL)");
        statements.add("CREATE INDEX FLOATS_LIST_VALUE ON FLOATS_LIST (VALUE)");
        statements.add("ALTER TABLE FLOATS_LIST ADD PRIMARY KEY(ID,VERSION,PROPERTY,POSITION)");

        statements.add("CREATE TABLE DOUBLES_LIST(ID BIGINT NOT NULL, VERSION BIGINT NOT NULL, PROPERTY VARCHAR(50) NOT NULL, POSITION INT NOT NULL, VALUE DOUBLE)");
        statements.add("CREATE INDEX DOUBLES_LIST_VALUE ON DOUBLES_LIST (VALUE)");
        statements.add("ALTER TABLE DOUBLES_LIST ADD PRIMARY KEY(ID,VERSION,PROPERTY,POSITION)");

        statements.add("CREATE TABLE DATES_LIST(ID BIGINT NOT NULL, VERSION BIGINT NOT NULL, PROPERTY VARCHAR(50) NOT NULL, POSITION INT NOT NULL, VALUE TIMESTAMP)");
        statements.add("CREATE INDEX DATES_LIST_VALUE ON DATES_LIST (VALUE)");
        statements.add("ALTER TABLE DATES_LIST ADD PRIMARY KEY(ID, VERSION, PROPERTY,POSITION)");



        statements.add("CREATE TABLE NAME(ID BIGINT NOT NULL, TYPE VARCHAR(50) NOT NULL, NAME VARCHAR(50) NOT NULL)");
        statements.add("ALTER TABLE NAME ADD PRIMARY KEY(ID)");
        statements.add("CREATE UNIQUE INDEX NAME_TYPE_NAME ON NAME (TYPE,NAME)");
        return statements;
    }

    protected String generateInsertorUpdateSQL(long id , long version, String property, Date date,boolean update) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String value = df.format(date);
        String sql;
        if (!update) {
            sql = "INSERT INTO DATES(ID,VERSION,PROPERTY,VALUE) VALUES("+id+", "+version +", '"+ property + "', '" + value+"')";
        } else {
            sql = "UPDATE DATES SET VALUE = " + value + " WHERE ID = " + id + " AND VERSION = "+ version +" AND PROPERTY = '" + property + "';";
        }
        return sql;
    }

    @Override
    protected String convertDate(Date d) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return df.format(d);
    }

    @Override
    protected String convertNumber(Number n) {
        return n.toString();
    }

    @Override
    protected String generateInsertorUpdateSQLForList(long id, long version, String property, long order, Date value, boolean update) {
        String sql;
        if (!update) {
            sql = "INSERT INTO INTEGERS_LIST(ID,VERSION,POSITION,PROPERTY,VALUE) VALUES(" + id + ", " + version + ", " + order + ", '" + property + "', " + value + ")";
        } else {
            sql = "UPDATE INTEGERS_LIST SET VALUE = " + value + " WHERE ID = " + id + " AND VERSION = " + version + " AND POSITION = " + order + " AND PROPERTY = '" + property + "';";
        }
        return sql;
    }

}
