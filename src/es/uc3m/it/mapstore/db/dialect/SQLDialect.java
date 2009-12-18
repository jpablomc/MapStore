/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package es.uc3m.it.mapstore.db.dialect;

import es.uc3m.it.mapstore.bean.MapStoreBasicCondition;
import es.uc3m.it.mapstore.db.MapStoreDialect;
import java.io.Serializable;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Pablo
 */
public abstract class SQLDialect implements MapStoreDialect {

    @Override
    public boolean isCreated(Object o) {
        List<String> tables = (List<String>) o;
        String[] expectedTables = new String[]{"INTEGERS", "LONGS", "DATES", "NAME"};
        return tables.containsAll(Arrays.asList(expectedTables));
    }

    @Override
    public List<Object> initializeDataBase() {
        throw new UnsupportedOperationException("Must be implemented for specific databse");
    }

    @Override
    public String create(long id, long version, String key, Object value) {
        String aux;
        if (value instanceof Integer) {
            aux = generateInsertorUpdateSQL(id, version, key, (Integer) value, false);
        } else if (value instanceof Long) {
            aux = generateInsertorUpdateSQL(id, version, key, (Long) value, false);
        } else if (value instanceof Float) {
            aux = generateInsertorUpdateSQL(id, version, key, (Float) value, false);
        } else if (value instanceof Double) {
            aux = generateInsertorUpdateSQL(id, version, key, (Double) value, false);
        } else if (value instanceof Date) {
            aux = generateInsertorUpdateSQL(id, version, key, (Date) value, false);
        } else {
            aux = generateInsertorUpdateSQL(id, version, key, value, false);
        }
        return aux;
    }

    @Override
    public String createList(long id, long version, long order, String key, Object value) {
        String aux;
        if (value instanceof Integer) {
            aux = generateInsertorUpdateSQLForList(id, version, key, order, (Integer) value, false);
        } else if (value instanceof Long) {
            aux = generateInsertorUpdateSQLForList(id, version, key, order, (Long) value, false);
        } else if (value instanceof Float) {
            aux = generateInsertorUpdateSQLForList(id, version, key, order, (Float) value, false);
        } else if (value instanceof Double) {
            aux = generateInsertorUpdateSQLForList(id, version, key, order, (Double) value, false);
        } else if (value instanceof Date) {
            aux = generateInsertorUpdateSQLForList(id, version, key, order, (Date) value, false);
        } else {
            aux = generateInsertorUpdateSQLForList(id, version, key, order, value, false);
        }
        return aux;
    }

    public String getQueryForCondition(MapStoreBasicCondition cond) {
        Object value = cond.getValue();
        String sql;
        if (value instanceof List) {
            sql = getQueryForCondition((List) value);
        } else if (value instanceof Number) {
            sql = getQueryForCondition((Number) value);
        } else if (value instanceof Date) {
            sql = getQueryForCondition((Date) value);
        } else {
            throw new IllegalArgumentException("Type is unsupported");
        }
        String operator = processOperator(cond);
        return sql.replaceAll("%CONDITION%", operator);
    }

    public String insertTypeName(long id, String type, String name) {
        return "INSERT INTO NAME(ID,TYPE,NAME) VALUES(" + id + ", '" + type + "', '" + name + "')";
    }

    @Override
    public Serializable update(long id, long version, String key, Object value) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Serializable delete(long id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private String generateInsertorUpdateSQL(long id, long version, String property, Integer value, boolean update) {
        String sql;
        if (!update) {
            sql = "INSERT INTO INTEGERS(ID,VERSION,PROPERTY,VALUE) VALUES(" + id + ", " + version + ", '" + property + "', " + value + ")";
        } else {
            sql = "UPDATE INTEGERS SET VALUE = " + value + " WHERE ID = " + id + " AND VERSION = " + version + " AND PROPERTY = " + property + ";";
        }
        return sql;
    }

    private String generateInsertorUpdateSQL(long id, long version, String property, Long value, boolean update) {
        String sql;
        if (!update) {
            sql = "INSERT INTO LONGS(ID,VERSION,PROPERTY,VALUE) VALUES(" + id + ", " + version + ", '" + property + "', " + value + ")";
        } else {
            sql = "UPDATE LONGS SET VALUE = " + value + " WHERE ID = " + id + " AND VERSION = " + version + " AND PROPERTY = " + property + ";";
        }
        return sql;
    }

    private String generateInsertorUpdateSQL(long id, long version, String property, Float value, boolean update) {
        String sql;
        if (!update) {
            sql = "INSERT INTO FLOATS(ID,VERSION,PROPERTY,VALUE) VALUES(" + id + ", " + version + ", '" + property + "', " + value + ")";
        } else {
            sql = "UPDATE FLOATS SET VALUE = " + value + " WHERE ID = " + id + " AND VERSION = " + version + " AND PROPERTY = " + property + ";";
        }
        return sql;
    }

    private String generateInsertorUpdateSQL(long id, long version, String property, Double value, boolean update) {
        String sql;
        if (!update) {
            sql = "INSERT INTO DOUBLES(ID,VERSION,PROPERTY,VALUE) VALUES(" + id + ", " + version + ", '" + property + "', " + value + ")";
        } else {
            sql = "UPDATE DOUBLES SET VALUE = " + value + " WHERE ID = " + id + " AND VERSION = " + version + " AND PROPERTY = " + property + ";";
        }
        return sql;
    }

    protected String generateInsertorUpdateSQL(long id, long version, String property, Date date, boolean update) {
        throw new UnsupportedOperationException("Generic class does not support dates.");
    }

    private String generateInsertorUpdateSQL(long id, long version, String property, Object value, boolean update) {
        throw new UnsupportedOperationException("Object type is not supported.");
    }

    public List<Object> getAll() {
        List<Object> statements = new ArrayList<Object>();
        statements.add("SELECT * FROM INTEGERS ORDER BY ID,PROPERTY");
        statements.add("SELECT * FROM LONGS ORDER BY ID,PROPERTY");
        statements.add("SELECT * FROM DATES ORDER BY ID,PROPERTY");
        statements.add("SELECT * FROM FLOATS ORDER BY ID,PROPERTY");
        statements.add("SELECT * FROM DOUBLES ORDER BY ID,PROPERTY");
        statements.add("SELECT * FROM INTEGERS_LIST ORDER BY ID,PROPERTY,POSITION");
        statements.add("SELECT * FROM LONGS_LIST ORDER BY ID,PROPERTY,POSITION");
        statements.add("SELECT * FROM DATES_LIST ORDER BY ID,PROPERTY,POSITION");
        statements.add("SELECT * FROM FLOATS_LIST ORDER BY ID,PROPERTY,POSITION");
        statements.add("SELECT * FROM DOUBLES_LIST ORDER BY ID,PROPERTY,POSITION");
        statements.add("SELECT * FROM NAME ORDER BY ID");
        return statements;
    }

    @Override
    public String getByTypeName(String type, String name) {
        return "SELECT ID FROM NAME WHERE TYPE = '" + type + "' AND NAME = '" + name + "'";
    }

    @Override
    public String getByType(String type) {
        return "SELECT ID FROM NAME WHERE TYPE = '" + type + "' ORDER BY NAME";
    }


    private String getQueryForCondition(Number value) {
        String sql;
        if (value instanceof Integer) {
            sql = "SELECT ID,VERSION FROM INTEGERS WHERE %CONDITION% UNION " +
                    "SELECT ID,VERSION FROM LONGS WHERE %CONDITION% UNION " +
                    "SELECT ID,VERSION FROM FLOATS WHERE %CONDITION% UNION " +
                    "SELECT ID,VERSION FROM DOUBLES WHERE %CONDITION% UNION" +
                    "SELECT ID,VERSION FROM INTEGERS_LIST WHERE %CONDITION% UNION " +
                    "SELECT ID,VERSION FROM LONGS_LIST WHERE %CONDITION% UNION " +
                    "SELECT ID,VERSION FROM FLOATS_LIST WHERE %CONDITION% UNION " +
                    "SELECT ID,VERSION FROM DOUBLES_LIST WHERE %CONDITION%";

        } else if (value instanceof Long) {
            sql = "";
            if ((Long) value <= Integer.MAX_VALUE && (Long) value >= Integer.MIN_VALUE) {
                sql += "SELECT ID,VERSION FROM INTEGERS WHERE %CONDITION% UNION "+ 
                        "SELECT ID,VERSION FROM INTEGERS_LIST WHERE %CONDITION% UNION ";
            }
            sql += "SELECT ID,VERSION FROM LONGS WHERE %CONDITION% UNION " + 
                    "SELECT ID,VERSION FROM FLOATS WHERE %CONDITION% UNION " +
                    "SELECT ID,VERSION FROM DOUBLES WHERE %CONDITION% UNION" +
                    "SELECT ID,VERSION FROM LONGS_LIST WHERE %CONDITION% UNION " +
                    "SELECT ID,VERSION FROM FLOATS_LIST WHERE %CONDITION% UNION " +
                    "SELECT ID,VERSION FROM DOUBLES_LIST WHERE %CONDITION%";

        } else if (value instanceof Float) {
            sql = "";
            Float aux = ((Float) value).longValue() - (Float) value;
            if (aux == 0) {
                Long l = ((Float) value).longValue();
                if (l <= Integer.MAX_VALUE && l >= Integer.MIN_VALUE) {
                sql += "SELECT ID,VERSION FROM INTEGERS WHERE %CONDITION% UNION "+
                        "SELECT ID,VERSION FROM INTEGERS_LIST WHERE %CONDITION% UNION ";
                }
                sql += "SELECT ID,VERSION FROM LONGS WHERE %CONDITION% UNION " +
                        "SELECT ID,VERSION FROM LONGS WHERE %CONDITION% UNION ";
            }
            sql += "SELECT ID,VERSION FROM FLOATS WHERE %CONDITION% UNION " +
                    "SELECT ID,VERSION FROM DOUBLES WHERE %CONDITION% UNION " +
                    "SELECT ID,VERSION FROM FLOATS_LIST WHERE %CONDITION% UNION " +
                    "SELECT ID,VERSION FROM DOUBLES_LIST WHERE %CONDITION%";
        } else if (value instanceof Double) {
            sql = "";
            Double aux = ((Double) value).longValue() - (Double) value;
            if (aux == 0) {
                Long l = ((Double) value).longValue();
                if (l <= Integer.MAX_VALUE && l >= Integer.MIN_VALUE) {
                sql += "SELECT ID,VERSION FROM INTEGERS WHERE %CONDITION% UNION "+
                        "SELECT ID,VERSION FROM INTEGERS_LIST WHERE %CONDITION% UNION ";
                }
                sql += "SELECT ID,VERSION FROM LONGS WHERE %CONDITION% UNION " +
                        "SELECT ID,VERSION FROM LONGS WHERE %CONDITION% UNION ";
            }
            aux = ((Double) value).floatValue() - (Double) value;
            if (aux == 0) {
                sql += "SELECT ID,VERSION FROM FLOATS WHERE %CONDITION% UNION " +
                        "SELECT ID,VERSION FROM FLOATS_LIST WHERE %CONDITION% UNION ";
            }
            sql += "SELECT ID,VERSION FROM DOUBLES WHERE %CONDITION% UNION " +
                    "SELECT ID,VERSION FROM DOUBLES_LIST WHERE %CONDITION%";
        } else {
            throw new IllegalArgumentException("Unssupported type");
        }
        return sql;
    }

    private String getQueryForCondition(Date value) {
        return "SELECT ID,VERSION FROM DATES WHERE %CONDITION% UNION " +
                "SELECT ID,VERSION FROM DATES_LIST WHERE %CONDITION%";
    }

    private String getQueryForCondition(List values) {
        boolean canBeRepresentedAsInteger = true;
        boolean canBeRepresentedAsLong = true;
        boolean canBeRepresentedAsDouble = true;
        boolean canBeRepresentedAsFloat = true;
        boolean canBeRepresentedAsDate = true;
        for (Object o : values) {
            if (o instanceof Number) {
                canBeRepresentedAsDate = false;
                if (o instanceof Long) {
                    Long l = (Long) o;
                    if (l > Integer.MAX_VALUE || l < Integer.MIN_VALUE) {
                        canBeRepresentedAsInteger = false;
                    }
                } else if (o instanceof Float) {
                    Float f = (Float) o;
                    Float aux = new Float(f.longValue());
                    if (f - aux != 0) {
                        canBeRepresentedAsInteger = false;
                        canBeRepresentedAsLong = false;
                    } else {
                        Long l = aux.longValue();
                        if (l > Integer.MAX_VALUE || l < Integer.MIN_VALUE) {
                            canBeRepresentedAsInteger = false;
                        }
                    }
                } else if (o instanceof Double) {
                    Double f = (Double) o;
                    Double aux = new Double(f.longValue());
                    if (f - aux != 0) {
                        canBeRepresentedAsInteger = false;
                        canBeRepresentedAsLong = false;
                    } else {
                        Long l = aux.longValue();
                        if (l > Integer.MAX_VALUE || l < Integer.MIN_VALUE) {
                            canBeRepresentedAsInteger = false;
                        }
                    }
                    Float aux2 = new Float(f.floatValue());
                    if (f - aux2 != 0) {
                        canBeRepresentedAsFloat = false;
                    }
                }
            } else if (o instanceof Date) {
                canBeRepresentedAsDouble = false;
                canBeRepresentedAsFloat = false;
                canBeRepresentedAsInteger = false;
                canBeRepresentedAsLong = false;
            } else {
                canBeRepresentedAsDate = false;
                canBeRepresentedAsDouble = false;
                canBeRepresentedAsFloat = false;
                canBeRepresentedAsInteger = false;
                canBeRepresentedAsLong = false;
            }
        }
        StringBuffer sb = new StringBuffer();
        if (canBeRepresentedAsDate) {
            sb.append("SELECT ID,VERSION FROM DATES WHERE %CONDITION% UNION").
                    append("SELECT ID,VERSION FROM DATES_LIST WHERE %CONDITION%");
        }
        if (canBeRepresentedAsInteger) {
            sb.append("SELECT ID,VERSION FROM INTEGERS WHERE %CONDITION% UNION ").
                    append("SELECT ID,VERSION FROM INTEGERS_LIST WHERE %CONDITION% UNION ");
        }
        if (canBeRepresentedAsLong) {
            sb.append("SELECT ID,VERSION FROM LONGS WHERE %CONDITION% UNION ")
                    .append("SELECT ID,VERSION FROM LONGS_LIST WHERE %CONDITION% UNION ");
        }
        if (canBeRepresentedAsFloat) {
            sb.append("SELECT ID,VERSION FROM FLOATS WHERE %CONDITION% UNION ").
                    append("SELECT ID,VERSION FROM FLOATS_LIST WHERE %CONDITION% UNION ");
        }
        if (canBeRepresentedAsDouble) {
            sb.append("SELECT ID,VERSION FROM DOUBLES WHERE %CONDITION%").append("SELECT ID,VERSION FROM DOUBLES_LIST WHERE %CONDITION%");
        }
        if (sb.length() == 0) {
            throw new IllegalArgumentException("Type is not supported");
        }
        return sb.toString();
    }

    private String processOperator(MapStoreBasicCondition cond) {
        Object o = cond.getValue();
        String sql = cond.getProperty() + " ";
        if (o instanceof Number) {
            sql = processOperatorNumber(cond);
        } else if (o instanceof Date) {
            sql = processOperatorDate(cond);
        } else if (o instanceof List) {
            sql = processOperatorList(cond);
        } else {
            throw new IllegalArgumentException("Unsupported type");
        }
        return sql;
    }

    private String processOperatorList(MapStoreBasicCondition cond) {
        String sql = null;
        switch (cond.getOperator()) {
            case MapStoreBasicCondition.OP_BETWEEN:
                sql = processOperatorBetween(cond);
                break;
            case MapStoreBasicCondition.OP_IN:
                sql = processOperatorIn(cond);
                break;
            default:
                throw new IllegalArgumentException("Operator not supported for List");
        }
        return sql;
    }

    private String processOperatorBetween(MapStoreBasicCondition cond) {
        Object o = cond.getValue();
        String sql = null;
        if (!(o instanceof List)) {
            throw new IllegalArgumentException("Type is not supported. Between requieres that a List object is provided.");
        }
        List list = (List) o;
        if (list.size() != 2) {
            throw new IllegalArgumentException("Type is not supported. Between requieres that a 2 element List.");
        }
        Object a = list.get(0);
        Object b = list.get(1);
        if (a instanceof Number && b instanceof Number) {
            if (a instanceof Comparable && b instanceof Comparable) {
                Comparable ca = (Comparable) a;
                Comparable cb = (Comparable) b;
                if (ca.compareTo(cb) <= 0) {
                    sql = "PROPERTY = '" + cond.getProperty() + "' AND VALUE BETWEEN " + convertNumber((Number) a) + " AND " + convertNumber((Number) b);
                } else {
                    sql = "PROPERTY = '" + cond.getProperty() + "' AND VALUE BETWEEN " + convertNumber((Number) b) + " AND " + convertNumber((Number) a);
                }
            }
        } else if (a instanceof Date && b instanceof Date) {
            Date da = (Date) a;
            Date db = (Date) b;
            if (da.compareTo(db) <= 0) {
                sql = "PROPERTY = '" + cond.getProperty() + "' AND VALUE BETWEEN '" + convertDate(da) + "' AND '" + convertDate(db) + "'";
            } else {
                sql = "PROPERTY = '" + cond.getProperty() + "' AND VALUE BETWEEN '" + convertDate(db) + "' AND '" + convertDate(da) + "'";
            }
        }
        return sql;
    }

    private String processOperatorIn(MapStoreBasicCondition cond) {
        Object o = cond.getValue();
        if (!(o instanceof List)) {
            throw new IllegalArgumentException("Type is not supported. IN requieres that a List object is provided.");
        }
        List list = (List) o;
        if (list.size() < 2) {
            throw new IllegalArgumentException("Type is not supported. IN requieres at least 2 element.");
        }
        StringBuffer sb = new StringBuffer("PROPERTY = '" + cond.getProperty() + "' AND VALUE IN (");
        int initLen = sb.length();
        boolean isNumberList = false;
        boolean isDateList = false;
        for (Object aux : list) {
            if (sb.length() > initLen) {
                sb.append((","));
            }
            if (aux instanceof Number) {
                sb.append(convertNumber((Number) aux));
                isNumberList = true;
            } else if (aux instanceof Date) {
                sb.append("'").append(convertDate((Date) aux)).append("'");
                isNumberList = true;
            } else {
                throw new IllegalArgumentException("Type is not supported. List must contain Number or Date objects");
            }
            if (isNumberList && isDateList) {
                throw new IllegalArgumentException("Can not combine Date and Number on IN search");
            }
        }
        sb.append(")");
        return sb.toString();
    }

    private String processOperatorNumber(MapStoreBasicCondition cond) {
        String sql = "PROPERTY = '" + cond.getProperty() + "' AND VALUE ";
        Object o = cond.getValue();
        if (!(o instanceof Number)) {
            throw new IllegalArgumentException("Type is not supportes. Must be a Number");
        }
        Number n = (Number) o;
        switch (cond.getOperator()) {
            case MapStoreBasicCondition.OP_BIGGEROREQUALSTHAN:
                sql += ">= " + convertNumber(n);
                break;
            case MapStoreBasicCondition.OP_BIGGERTHAN:
                sql += "> " + convertNumber(n);
                break;
            case MapStoreBasicCondition.OP_EQUALS:
                sql += "= " + convertNumber(n);
                break;
            case MapStoreBasicCondition.OP_LESSOREQUALSTHAN:
                sql += "<= " + convertNumber(n);
                break;
            case MapStoreBasicCondition.OP_LESSTHAN:
                sql += "< " + convertNumber(n);
                break;
            case MapStoreBasicCondition.OP_NOTEQUALS:
                sql += "<> " + convertNumber(n);
                break;
            default:
                throw new IllegalArgumentException("operator is not supported for this type");
        }
        return sql;
    }

    private String processOperatorDate(MapStoreBasicCondition cond) {
        String sql = "PROPERTY = '" + cond.getProperty() + "' AND VALUE ";
        Object o = cond.getValue();
        if (!(o instanceof Date)) {
            throw new IllegalArgumentException("Type is not supportes. Must be a Date");
        }
        Date d = (Date) o;
        //Determinar hora... si esta es 0:0:0:000 asumimos que es busqueda en el día para equals o a partir del dia anterior o siguiente para las comparaciones
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy"); //Da igual el formato en tanto que solo recuperemos el día sin hora.
        boolean porFechaSolo = false;
        try {
            //Da igual el formato en tanto que solo recuperemos el día sin hora.
            Date sinHora = sdf.parse(sdf.format(d));
            if (d.compareTo(sinHora) == 0) {
                porFechaSolo = true;
            }
        } catch (ParseException ex) {
            //CASO IMPOSIBLE
            Logger.getLogger(SQLDialect.class.getName()).log(Level.SEVERE, null, ex);
        }
        switch (cond.getOperator()) {
            case MapStoreBasicCondition.OP_BIGGEROREQUALSTHAN:
                //En ambos casos es busqueda a partir de la fecha dada
                sql += ">= " + convertDate(d);
                break;
            case MapStoreBasicCondition.OP_BIGGERTHAN:
                //En este caso si es por fecha solo hay que buscar a partir del dis siguiente incluido
                if (porFechaSolo) {
                    Calendar cal = new GregorianCalendar();
                    cal.setTime(d);
                    cal.add(Calendar.DATE, 1);
                    sql += ">= " + convertDate(cal.getTime());
                } else {
                    sql += "> " + convertDate(d);
                }
                break;
            case MapStoreBasicCondition.OP_EQUALS:
                //En este caso si es por fecha solo hay que buscar en todo el día.
                if (porFechaSolo) {
                    Calendar cal = new GregorianCalendar();
                    cal.setTime(d);
                    cal.add(Calendar.DATE, 1);
                    cal.add(Calendar.MILLISECOND, -1);
                    sql += "BETWEEN '" + convertDate(d) + "' AND '" + convertDate(cal.getTime()) + "'";
                } else {
                    sql += "= '" + convertDate(d) + "'";
                }
                break;
            case MapStoreBasicCondition.OP_LESSOREQUALSTHAN:
                //En este caso si es por fecha solo hay que buscar hasta el dia siguiente no incluido
                if (porFechaSolo) {
                    Calendar cal = new GregorianCalendar();
                    cal.setTime(d);
                    cal.add(Calendar.DATE, 1);
                    sql += "< " + convertDate(cal.getTime());
                } else {
                    sql += "<= " + convertDate(d);
                }
                break;
            case MapStoreBasicCondition.OP_LESSTHAN:
                //En ambos casos es busqueda hasta la fecha dada
                sql += ">= " + convertDate(d);
                break;
            case MapStoreBasicCondition.OP_NOTEQUALS:
                //En este caso si es por fecha solo hay que buscar en todo el día.
                if (porFechaSolo) {
                    Calendar cal = new GregorianCalendar();
                    cal.setTime(d);
                    cal.add(Calendar.DATE, 1);
                    cal.add(Calendar.MILLISECOND, -1);
                    sql += "NOT BETWEEN '" + convertDate(d) + "' AND '" + convertDate(cal.getTime()) + "'";
                } else {
                    sql += "<> '" + convertDate(d) + "'";
                }
                break;
            default:
                throw new IllegalArgumentException("operator is not supported for this type");
        }
        return sql;
    }

    protected abstract String convertDate(Date d);

    protected abstract String convertNumber(Number n);

    private String generateInsertorUpdateSQLForList(long id, long version, String property, long order, Integer value, boolean update) {
        String sql;
        if (!update) {
            sql = "INSERT INTO INTEGERS_LIST(ID,VERSION,POSITION,PROPERTY,VALUE) VALUES(" + id + ", " + version + ", " + order + ", '" + property + "', " + value + ")";
        } else {
            sql = "UPDATE INTEGERS_LIST SET VALUE = " + value + " WHERE ID = " + id + " AND VERSION = " + version + " AND POSITION = " + order + " AND PROPERTY = '" + property + "';";
        }
        return sql;
    }

    private String generateInsertorUpdateSQLForList(long id, long version, String property, long order, Long value, boolean update) {
        String sql;
        if (!update) {
            sql = "INSERT INTO LONGS_LIST(ID,VERSION,POSITION,PROPERTY,VALUE) VALUES(" + id + ", " + version + ", " + order + ", '" + property + "', " + value + ")";
        } else {
            sql = "UPDATE LONGS_LIST SET VALUE = " + value + " WHERE ID = " + id + " AND VERSION = " + version + " AND POSITION = " + order + " AND PROPERTY = '" + property + "';";
        }
        return sql;
    }

    private String generateInsertorUpdateSQL(long id, long version, String property, long order, Float value, boolean update) {
        String sql;
        if (!update) {
            sql = "INSERT INTO FLOATS_LIST(ID,VERSION,POSITION,PROPERTY,VALUE) VALUES(" + id + ", " + version + ", " + order + ", '" + property + "', " + value + ")";
        } else {
            sql = "UPDATE FLOATS_LIST SET VALUE = " + value + " WHERE ID = " + id + " AND VERSION = " + version + " AND POSITION = " + order + " AND PROPERTY = '" + property + "';";
        }
        return sql;
    }

    private String generateInsertorUpdateSQL(long id, long version, String property, long order, Double value, boolean update) {
        String sql;
        if (!update) {
            sql = "INSERT INTO DOUBLES_LIST(ID,VERSION,POSITION,PROPERTY,VALUE) VALUES(" + id + ", " + version + ", " + order + ", '" + property + "', " + value + ")";
        } else {
            sql = "UPDATE DOUBLES_LIST SET VALUE = " + value + " WHERE ID = " + id + " AND VERSION = " + version + " AND POSITION = " + order + " AND PROPERTY = '" + property + "';";
        }
        return sql;
    }



    protected String generateInsertorUpdateSQLForList(long id, long version, String property, long order, Date value, boolean update) {
        throw new UnsupportedOperationException("Generic class does not support dates.");    }

    private String generateInsertorUpdateSQLForList(long id, long version, String key, long order, Object value, boolean b) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
