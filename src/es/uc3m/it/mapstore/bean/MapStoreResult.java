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
public class MapStoreResult {
    Map<Integer,Set<Integer>> results;

    public void addIdVersion(int id, int version) {
        Set<Integer> vers = getVersionsForId(id);
        if (vers == null) {
            vers = new HashSet<Integer>();
            results.put(id, vers);
        }
        vers.add(version);
    }

    public void addIdVersion(int id, Set<Integer> versions) {
        Set<Integer> vers = getVersionsForId(id);
        if (vers == null) {
            vers = new HashSet<Integer>();
            results.put(id, vers);
        }
        vers.addAll(versions);
    }


    public MapStoreResult() {
        results = new HashMap<Integer,Set<Integer>>();
    }

    public void and(MapStoreResult other) {
        removeNonCommonId(other);
        removeNonCommonVersions();
    }

    public void or(MapStoreResult other) {
        for (Integer id : other.getIds()) {
            Set<Integer> vers1 = this.getVersionsForId(id);
            Set<Integer> vers2 = other.getVersionsForId(id);
            if (vers1 == null) results.put(id, vers2);
            else vers1.addAll(vers2);
        }
    }

    public Set<Integer> getIds() {
        return results.keySet();
    }

    public Set<Integer> getVersionsForId(int id) {
        return results.get(id);
    }


    private void removeNonCommonId(MapStoreResult other) {
        //Primero eliminamos los ids no comunes
        Set<Integer> ids1 = this.getIds();
        Set<Integer> ids2 = other.getIds();
        ids1.retainAll(ids2); //Estos son los ids comunes... por tanto los que hay que mantener
        //Calculamos el complementario
        Set<Integer> toRemove = this.getIds();
        toRemove.removeAll(ids1);
        for (Integer id : toRemove) {
            results.remove(id);
        }
    }

    private void removeNonCommonVersions() {
        for (Integer id : getIds()) {
            Set<Integer> ver1 = this.getVersionsForId(id);
            Set<Integer> ver2 = this.getVersionsForId(id);
            //Solo nos quedamos los comunes
            ver1.retainAll(ver2);
        }
    }
}
