/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa;

import es.uc3m.it.mapstore.bean.MapStoreItem;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Pablo
 */
public interface PersistenceManagerWrapper extends ResourceManagerWrapper {
    public List<MapStoreItem> recoverById(Set<Integer> ids);
    public Map<Integer,Map<Integer,MapStoreItem>> recoverByIdVersion(Map<Integer,Set<Integer>> request);
    public int getNewId();
    public int getNewVersion(int id);
}
