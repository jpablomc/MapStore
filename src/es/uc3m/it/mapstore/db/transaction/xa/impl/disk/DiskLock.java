/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.disk;

import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Pablo
 */
public class DiskLock {
    private static Map<Long,Set<Long>> idMap = new HashMap<Long,Set<Long>>();

    public static synchronized void acquireLock(long id,long version) throws SQLException {
        Set<Long> versionSet = idMap.get(id);
        if (versionSet == null) {
            versionSet = new HashSet<Long>();
            idMap.put(id, versionSet);
        }
        if (versionSet.contains(version)) throw new MapStoreRunTimeException("Item currently locked");
        versionSet.add(version);
    }

    public static synchronized void releaseLock(long id,long version) throws SQLException {
        Set<Long> versionSet = idMap.get(id);
        if (versionSet == null || !versionSet.contains(version)) throw new MapStoreRunTimeException("Item is not locked");
        versionSet.remove(version);
        if (versionSet.isEmpty()) idMap.remove(id);
    }
    

}
