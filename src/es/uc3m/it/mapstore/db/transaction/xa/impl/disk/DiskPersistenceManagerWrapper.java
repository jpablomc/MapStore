/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.disk;

import es.uc3m.it.mapstore.bean.MapStoreBasicCondition;
import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.bean.MapStoreResult;
import es.uc3m.it.mapstore.db.transaction.xa.PersistenceManagerWrapper;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.XAConnection;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

/**
 *
 * @author Pablo
 */
public class DiskPersistenceManagerWrapper implements PersistenceManagerWrapper{
    private DiskXADataSource ds;
    private IdGenerator idgen;

    @Override
    public void create(MapStoreItem item,Transaction t) {
        DiskXAResource res=null;
        Exception e = null;
        boolean enlisted = false;
        try {
            DiskXAConnection xaconn = ds.getXAConnection();
            res = xaconn.getXAResource();
            t.enlistResource(res);
            enlisted = true;
            DiskConnection conn = xaconn.getConnection();
            conn.storeNew(item);
            t.delistResource(res, XAResource.TMSUCCESS);
            enlisted = false;
        } catch (IOException ex) {
            e= ex;
            Logger.getLogger(DiskPersistenceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (RollbackException ex) {
            e= ex;
            Logger.getLogger(DiskPersistenceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalStateException ex) {
            e= ex;
            Logger.getLogger(DiskPersistenceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SystemException ex) {
            e= ex;
            Logger.getLogger(DiskPersistenceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            e= ex;
            Logger.getLogger(DiskPersistenceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (e != null) {
            if (enlisted) try {
                t.delistResource(res, XAResource.TMFAIL);
            } catch (IllegalStateException ex) {
                Logger.getLogger(DiskPersistenceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
            } catch (SystemException ex) {
                Logger.getLogger(DiskPersistenceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
            }
            throw new MapStoreRunTimeException(e);
        }
    }

    @Override
    public void update(MapStoreItem item,MapStoreItem old,Transaction t) {
        DiskXAResource res=null;
        Exception e = null;
        boolean enlisted = false;
        try {
            DiskXAConnection xaconn = ds.getXAConnection();
            res = xaconn.getXAResource();
            t.enlistResource(res);
            enlisted = true;
            DiskConnection conn = xaconn.getConnection();
            conn.store(item);
            t.delistResource(res, XAResource.TMSUCCESS);
            enlisted = false;
        } catch (IOException ex) {
            e= ex;
            Logger.getLogger(DiskPersistenceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (RollbackException ex) {
            e= ex;
            Logger.getLogger(DiskPersistenceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalStateException ex) {
            e= ex;
            Logger.getLogger(DiskPersistenceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SystemException ex) {
            e= ex;
            Logger.getLogger(DiskPersistenceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            e= ex;
            Logger.getLogger(DiskPersistenceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (e != null) {
            if (enlisted) try {
                t.delistResource(res, XAResource.TMFAIL);
            } catch (IllegalStateException ex) {
                Logger.getLogger(DiskPersistenceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
            } catch (SystemException ex) {
                Logger.getLogger(DiskPersistenceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
            }
            throw new MapStoreRunTimeException(e);
        }
    }

    @Override
    public void delete(MapStoreItem item,MapStoreItem old,Transaction t) {
        update(item, old, t);
    }

    @Override
    public List<MapStoreItem> recoverById(Set<Integer> ids) {
        try {
            DiskConnection conn = ds.getXAConnection().getConnection();
            return conn.getById(ids);
        } catch (SQLException ex) {
            Logger.getLogger(DiskPersistenceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

    @Override
    public void start(Properties prop) throws MapStoreRunTimeException {
        String directory = prop.getProperty("directory");
        if (directory == null) directory = (new File("")).getAbsolutePath() +
                System.getProperty("file.separator") + "db" +
                System.getProperty("file.separator") + "persistence";
        ds = new DiskXADataSource(directory);
        idgen = new IdGenerator(directory);
    }

    @Override
    public int getNewId() {
        return idgen.getNewId();
    }

    @Override
    public int getNewVersion(int id) {
        return idgen.getNewVersion(id);
    }

    @Override
    public XAConnection getXAConnection() {
        return ds.getXAConnection();
    }

    @Override
    public String getType() throws MapStoreRunTimeException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getName() throws MapStoreRunTimeException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public MapStoreResult findByConditions(List<MapStoreBasicCondition> cond, int flag, Date fecha) {
        throw new UnsupportedOperationException("This resource can not permorm searches");
    }

    @Override
    public boolean canFindByNameType() {
        return false;
    }

    @Override
    public Integer findByNameType(String name, String type) {
        throw new UnsupportedOperationException("This resource can not permorm searches");
    }

    @Override
    public void getAll() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<Integer, Map<Integer, MapStoreItem>> recoverByIdVersion(Map<Integer, Set<Integer>> request) {
        DiskConnection conn = null;
        try {
            conn = ds.getXAConnection().getConnection();
            Map<Integer, Map<Integer, MapStoreItem>> results = new HashMap<Integer, Map<Integer, MapStoreItem>>();
            for (int id : request.keySet()) {
                Set<Integer> versiones = request.get(id);
                Map<Integer, MapStoreItem> items = new HashMap<Integer, MapStoreItem>();
                for (int version : versiones) {
                    MapStoreItem item = conn.getByIdVersion(id, version);
                    items.put(version, item);
                }
                results.put(id, items);
            }
            conn.close();
            return results;
        } catch (SQLException ex) {
            Logger.getLogger(DiskPersistenceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } finally {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException ex) {
                Logger.getLogger(DiskPersistenceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
                throw new MapStoreRunTimeException(ex);
            }
        }
    }

    @Override
    public void shutdown() {
        //Empty
    }

    @Override
    public Set<Integer> findByType(String type) {
        throw new UnsupportedOperationException("Operation is not supported");
    }


}
