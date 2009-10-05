/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.disk;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Pablo
 */
public class DiskLock {
    private static Set<Long> ids = new HashSet<Long>();

    public static synchronized void acquireLock(long id) throws SQLException {
        if (ids.contains(id)) throw new SQLException("Object "+ id + " is already locked");
        ids.add(id);
    }

    public static synchronized void releaseLock(long id) throws SQLException {
        if (!ids.contains(id)) throw new SQLException("Object "+ id + " is not locked");
        ids.remove(id);
    }

}
