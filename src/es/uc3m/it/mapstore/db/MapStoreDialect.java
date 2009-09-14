/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db;

import java.io.Serializable;
import java.util.List;

/**
 *
 * @author Pablo
 */
public interface MapStoreDialect {
    public boolean isCreated(Object o);
    public List<Object> initializeDataBase();
    public Serializable create(long id, String property, Object value);
    //public Serializable recover();
    public Serializable update(long id, String property, Object value);
    public Serializable delete(long id);
}
