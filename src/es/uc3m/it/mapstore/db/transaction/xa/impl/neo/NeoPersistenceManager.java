/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.neo;

import org.neo4j.impl.nioneo.xa.NeoPersistenceSource;
import es.uc3m.it.mapstore.config.MapStoreConfig;
import es.uc3m.it.mapstore.db.transaction.TransactionManagerWrapper;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.transaction.TransactionManager;
import org.neo4j.impl.core.LockReleaser;
import org.neo4j.impl.core.PropertyIndexManager;
import org.neo4j.impl.nioneo.store.NeoStore;
import org.neo4j.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.impl.nioneo.xa.NioNeoDbPersistenceSource;
import org.neo4j.impl.persistence.IdGenerator;
import org.neo4j.impl.persistence.PersistenceManager;
import org.neo4j.impl.persistence.PersistenceModule;
import org.neo4j.impl.transaction.LockManager;

/**
 *
 * @author Pablo
 */
public class NeoPersistenceManager {
    public static void init(PersistenceModule pm) {
        TransactionManager t = getTransactionManager();
        LockManager lm = getLockManager(t);
        String dir = (new File("")).getAbsolutePath() + System.getProperty("file.separator") + "db" + System.getProperty("file.separator") + "neo";
        String fileData = dir + System.getProperty("file.separator") + "mapstore";
        String fileLog = dir + System.getProperty("file.separator") + "log";
        createNeoStore(fileData);

        LockReleaser lr = getLockReleaser(lm, t);
        NeoStoreXaDataSource ds = getXaDataSource(fileData, fileLog, lm, lr);
        NioNeoDbPersistenceSource ps = new NeoPersistenceSource(t, ds, dir);
        pm.start(t, ps);

        IdGenerator idg = getIdGenerator();
        PropertyIndexManager pim = getPropertyIndexManager(t, pm.getPersistenceManager(), idg,lr);
        
        
        ds.setBranchId("neo".getBytes());
        

    }

    private static void createPath(File f) {
        File aux = f.getParentFile();
        if (!aux.exists()) createPath(aux);
        f.mkdir();
    }

    private static void createNeoStore(String fileData) {
        try {
            File f = new File(fileData);
            if (!f.exists()) {
                createPath(f.getParentFile());
            }
            NeoStore.createStore(fileData);
        } catch (IllegalStateException e) {
            //Already created
        }
    }

    private static IdGenerator getIdGenerator() {
        try {
            Constructor c = IdGenerator.class.getDeclaredConstructor(new Class[0]);
            c.setAccessible(true);
            IdGenerator idg = (IdGenerator) c.newInstance(new Object[0]);
            return idg;
        } catch (InstantiationException ex) {
            Logger.getLogger(NeoPersistenceManager.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(NeoPersistenceManager.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (IllegalArgumentException ex) {
            Logger.getLogger(NeoPersistenceManager.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (InvocationTargetException ex) {
            Logger.getLogger(NeoPersistenceManager.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (NoSuchMethodException ex) {
            Logger.getLogger(NeoPersistenceManager.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (SecurityException ex) {
            Logger.getLogger(NeoPersistenceManager.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

    private static LockManager getLockManager(TransactionManager t) {
        LockManager lm = new LockManager(t);
        return lm;
    }

    private static LockReleaser getLockReleaser(LockManager lm, TransactionManager t) {
            LockReleaser lr = new LockReleaser(lm, t);
            return lr;
    }

    private static PropertyIndexManager getPropertyIndexManager(TransactionManager t,
            PersistenceManager pm, IdGenerator idg,LockReleaser lr) {
        try {
            Class[] types = {TransactionManager.class, PersistenceManager.class, IdGenerator.class};
            Object[] params = {t, pm, idg};
            Constructor c = PropertyIndexManager.class.getDeclaredConstructor(types);
            c.setAccessible(true);
            PropertyIndexManager propertyIndexManager = (PropertyIndexManager) c.newInstance(params);
            Class[] types2 = {PropertyIndexManager.class};
            Object[] params2 = {propertyIndexManager};
            Method m = lr.getClass().getDeclaredMethod("setPropertyIndexManager", types2);
            m.setAccessible(true);
            m.invoke(lr, params2);
            return propertyIndexManager;
        } catch (InstantiationException ex) {
            Logger.getLogger(NeoPersistenceManager.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(NeoPersistenceManager.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (IllegalArgumentException ex) {
            Logger.getLogger(NeoPersistenceManager.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (InvocationTargetException ex) {
            Logger.getLogger(NeoPersistenceManager.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (NoSuchMethodException ex) {
            Logger.getLogger(NeoPersistenceManager.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (SecurityException ex) {
            Logger.getLogger(NeoPersistenceManager.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

    private static TransactionManager getTransactionManager() {
        TransactionManager t = ((TransactionManagerWrapper) MapStoreConfig.getInstance().getObject(MapStoreConfig.TXMANAGER)).getTransactionManager();
        return t;
    }

    private static NeoStoreXaDataSource getXaDataSource(String fileData, String fileLog, LockManager lm, LockReleaser lr){
        try {
            NeoStoreXaDataSource ds = new NeoStoreXaDataSource(fileData, fileLog, lm, lr);
            return ds;
        } catch (IOException ex) {
            Logger.getLogger(NeoPersistenceManager.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (InstantiationException ex) {
            Logger.getLogger(NeoPersistenceManager.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }
}
