/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.disk;

import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.db.transaction.xa.PersistenceManagerWrapper;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
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
    private String path;

    @Override
    public void create(MapStoreItem item,Transaction t) {
        DiskXAResource res=null;
        Exception e;
        boolean enlisted = false;
        try {
            DiskXAConnection xaconn = ds.getXAConnection();
            res = xaconn.getXAResource();
            DiskConnection conn = xaconn.getConnection();
            t.enlistResource(res);
            enlisted = true;
            conn.store(item);
            t.delistResource(res, XAResource.TMSUCCESS);
            enlisted = false;
            throw new UnsupportedOperationException("Not supported yet.");
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
    public void update(MapStoreItem item,Transaction t) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void delete(long id,Transaction t) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<MapStoreItem> recoverById(List<Long> ids) {
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
        ds = new DiskXADataSource();
    }


}
