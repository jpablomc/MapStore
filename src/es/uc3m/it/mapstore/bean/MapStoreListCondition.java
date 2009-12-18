/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.bean;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Pablo
 */
public class MapStoreListCondition implements MapStoreCondition {

    private Set<MapStoreCondition> conditions;
    private Map<MapStoreCondition,MapStoreResult> results;
    private boolean andList;

    public MapStoreListCondition(boolean andList) {
        conditions = new HashSet<MapStoreCondition>();
        results = new HashMap<MapStoreCondition, MapStoreResult>();
        this.andList = andList;
    }

    public void addCondition(MapStoreCondition newCondition) {
        conditions.add(newCondition);
    }

    public void removeCondition(MapStoreCondition newCondition) {
        conditions.remove(newCondition);
    }

    public boolean isAndList() {
        return andList;
    }

    @Override
    public boolean hasDependencies() {
        return conditions.size()>0;
    }

    @Override
    public Set<MapStoreCondition> getRequieredConditions() {
        return new HashSet<MapStoreCondition>(conditions);
    }

    @Override
    public void setResultsForCondition(MapStoreCondition cond, MapStoreResult r) {
        if (conditions.contains(cond)) {
            conditions.remove(cond);
            results.put(cond, r);
        }
    }

    @Override
    public void debugPrint(int depth) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0;i<depth;i++) { sb.append("\t");}
        System.out.println(sb.toString() + "STARTS LIST:");
        for (MapStoreCondition cond : conditions) {
            cond.debugPrint(depth+1);
            if (isAndList()) System.out.println(sb.toString() + "AND");
            else System.out.println(sb.toString() + "OR");
        }
        System.out.println(sb.toString() + "END LIST:");
    }

    public MapStoreResult getResults() {
        if (conditions.size() > 0) throw new IllegalStateException("Not all the conditions have been solved");
        MapStoreResult toReturn = null;
        for (MapStoreResult res : results.values()) {
            if (toReturn == null) toReturn = res;
            else {
                if (isAndList()) toReturn.and(res);
                else toReturn.or(res);
            }
        }
        return toReturn;
    }
}
