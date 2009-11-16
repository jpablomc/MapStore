/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.bean;

/**
 *
 * @author Pablo
 */
public class MapStoreCondition {
    public static final int OP_EQUALS = 0;
    public static final int OP_SIMILARITY = 1;
    public static final int OP_BIGGERTHAN = 2;
    public static final int OP_BIGGEROREQUALSTHAN = 3;
    public static final int OP_LESSTHAN = 4;
    public static final int OP_LESSOREQUALSTHAN = 5;
    public static final int OP_NOTEQUALS = 6;
    public static final int OP_IN = 7;
    public static final int OP_BETWEEN = 8;
    public static final int OP_RELATED = 9;
    public static final int OP_PHRASE = 10;


    public String property;
    public Object value;
    public int operator;

    public MapStoreCondition(String property, Object value,int operator) {
        this.property = property;
        this.value = value;
        this.operator = operator;
    }

    public String getProperty() {
        return property;
    }

    public void setProperty(String property) {
        this.property = property;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Class getType() {
        return value.getClass();
    }

    public int getOperator() {
        return operator;
    }

    public void setOperator(int operator) {
        this.operator = operator;
    }

    
}
