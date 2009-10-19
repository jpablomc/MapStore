/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa;

import es.uc3m.it.mapstore.bean.MapStoreItem;
import java.util.List;

/**
 *
 * @author Pablo
 */
public interface PersistenceManagerWrapper extends ResourceManagerWrapper {
    public List<MapStoreItem> recoverById(List<Long> ids);
    public long getNewId();
    public long getNewVersion(long id);
}
