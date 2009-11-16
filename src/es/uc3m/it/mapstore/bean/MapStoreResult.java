/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.bean;

import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Pablo
 */
public class MapStoreResult {
    private int id;
    private Set<Integer> versions;

    public MapStoreResult(int id) {
        this.id = id;
        versions = new HashSet<Integer>();
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public Set<Integer> getVersions() {
        return versions;
    }

    public void addVersion(int version) {
        versions.add(version);
    }

    public void removeVersion(int version) {
        versions.remove(version);
    }

}
