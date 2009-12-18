/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.bean;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Pablo
 */
public class MapStoreComplexCondition implements MapStoreCondition {

    private MapStoreCondition subject;
    private MapStoreTraverserDescriptor predicate;
    private int operator;

    private MapStoreResult subjectResults;


    private static String SUBJECT_KEY = "SUBJECT";

    public MapStoreComplexCondition(MapStoreCondition subject, MapStoreTraverserDescriptor predicate, int operator) {
        this.subject = subject;
        this.predicate = predicate;
        this.operator = operator;
    }

    @Override
    public boolean hasDependencies() {
        return (subject != null || predicate != null);
    }

    @Override
    public Set<MapStoreCondition> getRequieredConditions() {
        Set<MapStoreCondition> map = new HashSet<MapStoreCondition>();
        map.add(subject);
        return map;
    }

    @Override
    public void setResultsForCondition(MapStoreCondition cond, MapStoreResult r) {
        if (cond.equals(subject)) {
            subject = null;
            subjectResults = r;
        }
    }

    @Override
    public void debugPrint(int depth) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0;i<depth;i++) { sb.append("\t");}
        sb.append("MapStoreComplexCondition: operator: " + operator + " value: " + predicate);
        System.out.println(sb.toString());
        subject.debugPrint(depth+1);        
    }

    public MapStoreBasicCondition convertToBasicCondition() {
        if (subject != null) throw new IllegalStateException("Subject condition has not been solved");
        predicate.setInitialNodes(subjectResults.getIds());
        return new MapStoreBasicCondition(null, predicate, operator);
    }


}