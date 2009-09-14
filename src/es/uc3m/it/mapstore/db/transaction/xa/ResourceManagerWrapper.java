/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa;

import es.uc3m.it.mapstore.bean.MapStoreCondition;
import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import java.util.List;
import java.util.Properties;
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
public void update(MapStoreItem item, Transaction t);
public void delete(long id,Transaction t);
public List<Long> findByConditions(List<MapStoreCondition> cond);
public void getAll();
}
