/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.impl;

import es.uc3m.it.mapstore.db.transaction.*;
import es.uc3m.it.mapstore.config.MapStoreConfig;
import es.uc3m.it.mapstore.db.transaction.xa.PersistenceManagerWrapper;
import es.uc3m.it.mapstore.db.transaction.xa.ResourceManagerWrapper;
import es.uc3m.it.mapstore.db.transaction.xa.impl.neo.NeoXAResourceWrapper;
import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import org.neo4j.impl.transaction.TxModule;


/**
 *
 * @author Pablo
 */
public class NeoTransactionManagerWrapper implements TransactionManagerWrapper {
    private static TxModule tm;
    private Map<Transaction,List<ResourceManagerWrapper>> registered;

    @Override
    public void init() {
        Properties props = MapStoreConfig.getInstance().getTransactionManagerProperties();
        if (tm == null) {
            String directory = props.getProperty("directory");
            if (directory == null) directory = new File("").getAbsolutePath();
            tm = new TxModule(directory);
            registerDataSources();
            tm.start();
            registered = new HashMap<Transaction,List<ResourceManagerWrapper>>();
        }
    }

    private void registerDataSources() {
        Collection<ResourceManagerWrapper> resources = MapStoreConfig.getInstance().getXAResourceLookup();
        PersistenceManagerWrapper pm = MapStoreConfig.getInstance().getPersistenceResourceLookup();
        for (ResourceManagerWrapper r : resources) {
            Map<String,Object> parameters = new HashMap<String,Object>();
            parameters.put(NeoXAResourceWrapper.WRAPPED_OBJECT, r);
            tm.registerDataSource(r.getName(), NeoXAResourceWrapper.class.getName(),
                    r.getName().getBytes(), parameters);
        }
        Map<String,Object> parameters = new HashMap<String,Object>();
        parameters.put(NeoXAResourceWrapper.WRAPPED_OBJECT, pm);
        tm.registerDataSource("persistence", NeoXAResourceWrapper.class.getName(),
                "persistence".getBytes(), parameters);
    }

    @Override
    public void rollback() throws IllegalStateException, SecurityException, SystemException {
        tm.getTxManager().rollback();
    }

    @Override
    public void begin() throws NotSupportedException, SystemException {
        tm.getTxManager().begin();
    }

    @Override
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException{
        Transaction t = tm.getTxManager().getTransaction();
        tm.getTxManager().getTransaction().commit();
    }

    @Override
    public int getStatus() throws SystemException {
        return tm.getTxManager().getStatus();
    }

    @Override
    public TransactionManager getTransactionManager() {
        return tm.getTxManager();
    }




}
