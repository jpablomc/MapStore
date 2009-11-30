/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa;

import es.uc3m.it.mapstore.bean.MapStoreCondition;
import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.bean.MapStoreResult;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.sql.XAConnection;
import javax.transaction.Transaction;

/**
 *
 * @author Pablo
 */
public interface ResourceManagerWrapper {
public XAConnection getXAConnection();
public String getType() throws MapStoreRunTimeException;
public String getName() throws MapStoreRunTimeException;
public void start(Properties prop) throws MapStoreRunTimeException;
public void create(MapStoreItem item, Transaction t);
public void update(MapStoreItem item, MapStoreItem old, Transaction t);
public void delete(MapStoreItem item, MapStoreItem old,Transaction t);
public Map<Integer,MapStoreResult> findByConditions(List<MapStoreCondition> cond, int flag, Date fecha);
public void getAll();
public boolean canFindByNameType();
public Integer findByNameType(String name, String type);
public Set<Integer> findByType(String type);
public void shutdown();
}
