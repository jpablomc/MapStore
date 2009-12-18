/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.bean;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Pablo
 */
public class MapStoreBasicCondition implements MapStoreCondition {
    public String property;
    public Object value;
    public int operator;

    public MapStoreBasicCondition(String property, Object value,int operator) {
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

    @Override
    public Set<MapStoreCondition> getRequieredConditions() {
        return new HashSet<MapStoreCondition>();
    }

    @Override
    public void setResultsForCondition(MapStoreCondition cond, MapStoreResult r) {
    }

    @Override
    public boolean hasDependencies() {
        return false;
    }

    @Override
    public void debugPrint(int depth) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0;i<depth;i++) { sb.append("\t");}
        sb.append("MapStoreBasicCOndition: Property: " + property + " operator: " + operator + " value: " + value);
        System.out.println(sb.toString());
    }

    
}
