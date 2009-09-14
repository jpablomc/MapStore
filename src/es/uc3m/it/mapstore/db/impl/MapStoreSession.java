/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.impl;

import es.uc3m.it.mapstore.bean.MapStoreCondition;
import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.config.MapStoreConfig;
import es.uc3m.it.mapstore.db.transaction.TransactionManagerWrapper;
import es.uc3m.it.mapstore.db.transaction.xa.ResourceManagerWrapper;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import es.uc3m.it.mapstore.transformers.MapStoreTransformer;
import es.uc3m.it.mapstore.transformers.exception.UnTransformableException;
import es.uc3m.it.mapstore.transformers.factory.TransformerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;


/**
 *
 * @author Pablo
 */
public class MapStoreSession implements es.uc3m.it.mapstore.db.MapStoreSession {

    private TransactionManagerWrapper tm;
    private boolean closed;
    private Map<Object,MapStoreItem> cache;

    private static Map<Thread,MapStoreSession> sessions;

    private MapStoreSession() {
        //Establecer el gestor de transacciones. Este se recupera del fichero de configuración.
        tm = (TransactionManagerWrapper) MapStoreConfig.getInstance().getObject(MapStoreConfig.TXMANAGER);
        tm.init();
        closed = false;
        cache = new HashMap<Object,MapStoreItem>();
    }

    private static void startup() {
        sessions = new HashMap<Thread,MapStoreSession>();
    }

    public static MapStoreSession getSession() {
        if (sessions == null) startup();
        MapStoreSession s = sessions.get(Thread.currentThread());
        if (s == null || s.closed) {
            s = new MapStoreSession();
            sessions.put(Thread.currentThread(), s);
        }
        return s;
    }
    
    public boolean isClosed() {
        return closed;
    }

    public void close() {
        closed = true;
        int status = getTransactionStatus();
        switch (status) {
            case Status.STATUS_ACTIVE:
            case Status.STATUS_PREPARED:
            case Status.STATUS_PREPARING:
            case Status.STATUS_UNKNOWN:
                try {
                    tm.rollback();
                } catch (IllegalStateException ex) {
                    Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
                    throw new MapStoreRunTimeException("Can not close session: Transaction can not be rolled back");
                } catch (SecurityException ex) {
                    Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
                    throw new MapStoreRunTimeException("Can not close session: Transaction can not be rolled back");
                } catch (SystemException ex) {
                    Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
                    throw new MapStoreRunTimeException("Can not close session: Transaction can not be rolled back");
                }
                Logger.getLogger(MapStoreSession.class.getName()).log(Level.WARNING, "Closed session with active transaction");
                break;
        }        
    }

    @Override
    public void beginTransaction() {
        if (this.isClosed()) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, "Attemp to open transaction on closed session");
            throw new MapStoreRunTimeException("Can not create transaction: Session already closed ");
        }
        try {
            tm.begin();
        } catch (NotSupportedException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException("Can not create transaction: Operation unsupported",ex);
        } catch (SystemException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException("Can not obtain transaction",ex);
        }
    }

    private void checkValidStateForOperations() throws MapStoreRunTimeException {
        if (this.isClosed()) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, "Attemp to create item on closed session");
            throw new MapStoreRunTimeException("ttemp to create item on closed session");
        }
        int status = getTransactionStatus();
        if (status == Status.STATUS_NO_TRANSACTION) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, "Attemp to create item without transaction");
            throw new MapStoreRunTimeException("Attemp to create item without transaction");
        } else if (status != Status.STATUS_ACTIVE) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, "Error while creating item: Transaction is not on valid state");
            throw new MapStoreRunTimeException("Error while creating item: Transaction is not on valid state");
        }
    }

    public void commit() throws MapStoreRunTimeException {
        Exception e = null;
        try {
            tm.commit();
        } catch (RollbackException ex) {
            e = ex;
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
        } catch (HeuristicMixedException ex) {
            e = ex;
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
        } catch (HeuristicRollbackException ex) {
            e = ex;
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SecurityException ex) {
            e = ex;
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalStateException ex) {
            e = ex;
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SystemException ex) {
            e = ex;
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (e != null) throw new MapStoreRunTimeException(e);
    }

    public void rollback() {
        try {
            tm.rollback();
        } catch (IllegalStateException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SecurityException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SystemException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public Long save(MapStoreItem item) {
        try {
            checkValidStateForOperations();
            item.setId(initializateId());
            long idItem = item.getId();
            Collection<ResourceManagerWrapper> resources = MapStoreConfig.getInstance().getXAResourceLookup();
            Transaction t = tm.getTransactionManager().getTransaction();
            for (ResourceManagerWrapper r : resources) {
                r.create(item, t);
            }
            return idItem;
        } catch (SystemException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

    private static int id;

    private long initializateId() {
        //TODO: Cambiar esto por algo que use BBDD. SINCRONIZAR!!!!!
        id++;
        return id;
    }


    private boolean processProperty(String property) {
        //TODO: ESTABLECER REGLA PARA IGNORAR PROPIEDADES INTERNAS DEL OBJETO
        return true;
    }

    public void update(MapStoreItem item) {
        checkValidStateForOperations();
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void delete(MapStoreItem item) {
        checkValidStateForOperations();
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private int getTransactionStatus() {
        try {
            return tm.getStatus();
        } catch (SystemException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException("Can not retrieve transaction status", ex);
        }
    }

    public MapStoreItem find(Object o) {
        MapStoreItem item;
        //Buscar en la cache de la sesión
        item = cache.get(o);
        //Si no esta en la cache (no es de la sesión actual... buscar en la BBDD por nombre y tipo
        if (item == null) {
            try {
                item = findByNameType(o);
            } catch (UnTransformableException ex) {
                Logger.getLogger(MapStoreSession.class.getName()).log(Level.WARNING, null, ex);
            }
            if (item != null) cache.put(o,item);
        }
        return item;
    }

    public MapStoreItem findByNameType(Object o) throws UnTransformableException {
        TransformerFactory tf = MapStoreConfig.getInstance().getTransformerFactory();
        MapStoreTransformer t = tf.getFactory(o);
        MapStoreItem item = t.toStore(o);
        String name = item.getName();
        String type = item.getType();
        return findByNameType(name, type);
    }

    public MapStoreItem findByNameType(String name,String type) {
        MapStoreCondition c1 = new MapStoreCondition(MapStoreItem.NAME, name);
        MapStoreCondition c2 = new MapStoreCondition(MapStoreItem.TYPE, type);
        MapStoreItem toReturn;
        List<MapStoreItem> items = findByConditions(new MapStoreCondition[]{c1,c2}, CONJUNCTIVE_SEARCH);
        if (items.isEmpty()) toReturn = null;
        else if (items.size() == 1) toReturn = items.get(0);
        else throw new MapStoreRunTimeException("Non unique result found by name-type");
        return toReturn;
    }

    public final static int CONJUNCTIVE_SEARCH = 0;
    public final static int DISJUNCTIVE_SEARCH = 1;

    public List<MapStoreItem> findByConditions(MapStoreCondition[] conditions,int flag) {
        List<Long> items=null;
        Map<Class,List<MapStoreCondition>> mapa = new HashMap<Class, List<MapStoreCondition>>();
        for (MapStoreCondition c: conditions) {
            Class clazz = c.getClass();
            List<MapStoreCondition> list = mapa.get(clazz);
            if (list == null) {
                list = new ArrayList<MapStoreCondition>();
                mapa.put(clazz,list);
            }
            list.add(c);
        }
        for (Class c : mapa.keySet()) {
            List<MapStoreCondition> cond = mapa.get(c);
            List<ResourceManagerWrapper> resources = MapStoreConfig.getInstance().getXaResourceLookupForClass(c);
            ResourceManagerWrapper r = BalanceLoad.getLessLoadedAndIncrease(resources, cond.size());
            BalanceLoad.decreaseLoad(r, cond.size());
            List<Long> aux = r.findByConditions(cond);
            if (items == null) items = aux;
            else {
                switch (flag) {
                    case CONJUNCTIVE_SEARCH:
                        items.retainAll(aux);
                        break;
                    case DISJUNCTIVE_SEARCH:
                        items.addAll(aux);
                        break;
                }
            }
        }
        return recoverById(items);
    }

    private List<MapStoreItem> recoverById(List<Long> ids) {
        //List<PersistenceManagerWrapper> perList =
        return null;
    }

    public void getAll() {
        Collection<ResourceManagerWrapper> resources = MapStoreConfig.getInstance().getXAResourceLookup();
        for (ResourceManagerWrapper res : resources) {
            res.getAll();
        }
    }

}
