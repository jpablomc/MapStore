/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db;

import es.uc3m.it.mapstore.bean.MapStoreItem;
import java.io.Serializable;

/**
 *
 * @author Pablo
 */
public interface PersistenceDB {
    public Serializable executeCreate(MapStoreItem item);
    public MapStoreItem executeRecover(long id);
    public void executeUpdate(MapStoreItem item);
    public void executeDelete(MapStoreItem item);

}
