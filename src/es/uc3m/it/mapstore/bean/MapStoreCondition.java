/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.bean;

import java.util.Set;

/**
 *
 * @author Pablo
 */
public interface MapStoreCondition {
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

    public boolean hasDependencies();
    public Set<MapStoreCondition> getRequieredConditions();
    public void setResultsForCondition(MapStoreCondition cond, MapStoreResult r);

    public void debugPrint(int depth);
    
}
