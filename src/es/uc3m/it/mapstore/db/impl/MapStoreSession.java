/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.impl;

import es.uc3m.it.mapstore.bean.MapStoreCondition;
import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.bean.MapStoreResult;
import es.uc3m.it.mapstore.config.MapStoreConfig;
import es.uc3m.it.mapstore.db.transaction.TransactionManagerWrapper;
import es.uc3m.it.mapstore.db.transaction.xa.PersistenceManagerWrapper;
import es.uc3m.it.mapstore.db.transaction.xa.ResourceManagerWrapper;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import es.uc3m.it.mapstore.transformers.MapStoreTransformer;
import es.uc3m.it.mapstore.transformers.exception.UnTransformableException;
import es.uc3m.it.mapstore.transformers.factory.TransformerFactory;
import es.uc3m.it.util.ReflectionUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private Date initDate;
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
            initDate = new Date();
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

    private Integer save(MapStoreItem item) {
        try {
            checkValidStateForOperations();
            item.setId(initializateId());
            item.setVersion(0);
            item.setRecordDate(initDate);
            int idItem = item.getId();
            Collection<ResourceManagerWrapper> resources = MapStoreConfig.getInstance().getXAResourceLookup();
            PersistenceManagerWrapper pm = MapStoreConfig.getInstance().getPersistenceResourceLookup();
            Transaction t = tm.getTransactionManager().getTransaction();
            for (ResourceManagerWrapper r : resources) {
                r.create(item, t);
            }
            pm.create(item, t);
            return idItem;
        } catch (SystemException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

    public Integer saveAnonymous(Object o, String name) {
        try {
            MapStoreItem i = MapStoreConfig.getInstance().getTransformerFactory().getFactory(o).toStore(o);
            if (i.getName() == null) i.setName(name);
            return save(i);
        } catch (UnTransformableException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

    public Integer save(Object o) {
        try {
            MapStoreItem i = MapStoreConfig.getInstance().getTransformerFactory().getFactory(o).toStore(o);
            return save(i);
        } catch (UnTransformableException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

    private int initializateId() {
        PersistenceManagerWrapper pm = MapStoreConfig.getInstance().getPersistenceResourceLookup();
        return pm.getNewId();
    }

    private int getNextVersion(int id) {
        PersistenceManagerWrapper pm = MapStoreConfig.getInstance().getPersistenceResourceLookup();
        Integer version = pm.getNewVersion(id);
        return version.intValue();
    }


    private boolean processProperty(String property) {
        //TODO: ESTABLECER REGLA PARA IGNORAR PROPIEDADES INTERNAS DEL OBJETO
        return true;
    }
    public void update(Object o) {
        try {
            MapStoreItem i = MapStoreConfig.getInstance().getTransformerFactory().getFactory(o).toStore(o);
            update(i);
        } catch (UnTransformableException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

    private void update(MapStoreItem item) {
        try {
            checkValidStateForOperations();
            MapStoreItem old = findByNameType(item.getName(), item.getType());
            item.setId(old.getId());
            item.setVersion(getNextVersion(old.getId()));
            item.setRecordDate(initDate);
            Collection<ResourceManagerWrapper> resources = MapStoreConfig.getInstance().getXAResourceLookup();
            PersistenceManagerWrapper pm = MapStoreConfig.getInstance().getPersistenceResourceLookup();
            Transaction t = tm.getTransactionManager().getTransaction();
            for (ResourceManagerWrapper r : resources) {
                r.update(item, old,  t);
            }
            pm.update(item, old, t);
        } catch (SystemException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

    private final static String[] NONDELETABLEPROPERTIES = {MapStoreItem.ID,MapStoreItem.NAME,MapStoreItem.RECORDDATE,MapStoreItem.TYPE,MapStoreItem.VERSION};

    public void delete(int id) {
            MapStoreItem i = recoverById(id);
            delete(i);
    }

    private void delete(MapStoreItem item) {
        try {
            List<String> nonDeletable = Arrays.asList(NONDELETABLEPROPERTIES);
            checkValidStateForOperations();
            MapStoreItem old = findByNameType(item.getName(), item.getType());
            item.setId(old.getId());
            item.setVersion(getNextVersion(old.getId()));
            item.setRecordDate(initDate);
            Set<String> clone = new HashSet<String>(item.getProperties().keySet());
            for (String aux: clone) {
                if (!nonDeletable.contains(aux)) {
                    item.setProperty(aux, null);
                }
            }
            Collection<ResourceManagerWrapper> resources = MapStoreConfig.getInstance().getXAResourceLookup();
            PersistenceManagerWrapper pm = MapStoreConfig.getInstance().getPersistenceResourceLookup();
            Transaction t = tm.getTransactionManager().getTransaction();
            for (ResourceManagerWrapper r : resources) {
                r.delete(item, old, t);
            }
            pm.delete(item, old, t);
        } catch (SystemException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }    }

    private int getTransactionStatus() {
        try {
            return tm.getStatus();
        } catch (SystemException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException("Can not retrieve transaction status", ex);
        }
    }

    public MapStoreItem findAnonymous(Object o,String name) {
        MapStoreItem item;
        //Buscar en la cache de la sesión
        item = cache.get(o);
        //Si no esta en la cache (no es de la sesión actual... buscar en la BBDD por nombre y tipo
        if (item == null) {
            try {
                TransformerFactory tf = MapStoreConfig.getInstance().getTransformerFactory();
                MapStoreTransformer t = tf.getFactory(o);
                item = t.toStore(o);
                if (item.getName() == null) {
                    item.setName(name);
                }
                item = findByNameType(item.getName(), item.getType());
                if (item != null) {
                    cache.put(o, item);
                }
            } catch (UnTransformableException ex) {
                Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
                throw new MapStoreRunTimeException(ex);
            }

        }
        return item;
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
        MapStoreItem toReturn = null;
        if (name != null && type != null) {
            toReturn = findByNameType(name, type);
        }
        return toReturn;
    }

    public MapStoreItem findByNameType(String name,String type) {
        Collection<ResourceManagerWrapper> resources = MapStoreConfig.getInstance().getXAResourceLookup();
        List<ResourceManagerWrapper> resByName = new ArrayList<ResourceManagerWrapper>();
        for(ResourceManagerWrapper r : resources) {
            if (r.canFindByNameType()) resByName.add(r);
        }
        ResourceManagerWrapper r = BalanceLoad.getLessLoadedAndIncrease(resByName, 2);
        Integer id = r.findByNameType(name, type);
        BalanceLoad.decreaseLoad(r, 2);
        MapStoreItem toReturn = null;
        if (id != null) toReturn = recoverById(id);
        return toReturn;
    }

    public final static int CONJUNCTIVE_SEARCH = 0;
    public final static int DISJUNCTIVE_SEARCH = 1;

    public List<MapStoreItem> findByConditions(MapStoreCondition[] conditions,int flag,Date fecha) {
        Map<Integer,MapStoreResult> items=null;
        Map<Class,List<MapStoreCondition>> mapa = new HashMap<Class, List<MapStoreCondition>>();
        for (MapStoreCondition c: conditions) {
            Class clazz = null;
            if (c.getValue() instanceof Collection) {
                clazz = ReflectionUtils.determineGenericType((Collection)c.getValue());
            } else clazz = c.getType();
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
            Map<Integer,MapStoreResult> aux = r.findByConditions(cond,flag,fecha);
            if (items == null) {
                items = aux;
            } else {
                switch (flag) {
                    case CONJUNCTIVE_SEARCH:
                        //Primero borramos los documentos no comunes
                        //acumulados va a tener los elementos a mantener
                        Set<Integer> acumulados = items.keySet();
                        //acumulados2 va a tener los elementos a eliminar
                        Set<Integer> acumulados2 = items.keySet();
                        Set<Integer> newResults = aux.keySet();
                        acumulados.retainAll(newResults);
                        acumulados2.removeAll(acumulados);
                        for (Integer toDelete : acumulados2) {
                            items.remove(toDelete);
                        }
                        //En el mapa ahora solo quedan los documentos comunes... debemos tratar las versiones
                        for (Integer id : acumulados) {
                            MapStoreResult oldResults = items.get(id);
                            MapStoreResult searchResults = aux.get(id);
                            oldResults.getVersions().retainAll(searchResults.getVersions());
                        }
                        break;
                    case DISJUNCTIVE_SEARCH:
                        //Añadimos todos los elementos
                        for (Integer id : aux.keySet()) {
                            MapStoreResult oldResults = items.get(id);
                            MapStoreResult searchResults = aux.get(id);
                            if (oldResults == null) items.put(id,searchResults);
                            else oldResults.getVersions().addAll(searchResults.getVersions());
                        }                        
                        break;
                }
            }
            BalanceLoad.decreaseLoad(r, cond.size());
        }
        List<MapStoreItem> toReturn = new ArrayList<MapStoreItem>();
        if (fecha != null) {
            Map<Integer,Set<Integer>> request = new HashMap<Integer, Set<Integer>>();
            for (Integer i: items.keySet()) {
                MapStoreResult res = items.get(i);
                Set<Integer> versionsNeeded = new HashSet<Integer>();
                for (Integer ver : res.getVersions()) {
                    versionsNeeded.add(ver);
                    versionsNeeded.add(ver+1);
                }
                request.put(i, versionsNeeded);
            }
            Map<Integer,Map<Integer,MapStoreItem>> toFilter = recoverByIdVersion(request);
            for (Integer i: items.keySet()) {
                MapStoreResult res = items.get(i);
                Map<Integer,MapStoreItem> versions = toFilter.get(i);
                for (Integer currentVer :res.getVersions()) {
                    MapStoreItem current = versions.get(currentVer);
                    MapStoreItem next = versions.get(currentVer+1);
                    if (current.getRecordDate().compareTo(fecha) <= 0 && next.getRecordDate().after(fecha)) {
                        toReturn.add(current);
                    }
                }
            }
        }else {
            List<MapStoreItem> toFilter = recoverById(items.keySet()); //Va a recuperar la ultima versión
            for (MapStoreItem item : toFilter) {
                int id = item.getId();
                int vers = item.getVersion();
                MapStoreResult res = items.get(id);
                if (res.getVersions().contains(vers)) {
                    toReturn.add(item);
                }
            }
        }
        return toReturn;
    }

    private Map<Integer,Map<Integer,MapStoreItem>> recoverByIdVersion(Map<Integer,Set<Integer>> request) {
        PersistenceManagerWrapper pm = MapStoreConfig.getInstance().getPersistenceResourceLookup();
        return pm.recoverByIdVersion(request);
    }

    private List<MapStoreItem> recoverById(Set<Integer> ids) {
        PersistenceManagerWrapper pm = MapStoreConfig.getInstance().getPersistenceResourceLookup();
        return pm.recoverById(ids);
    }

    private MapStoreItem recoverById(Integer id) {
        PersistenceManagerWrapper pm = MapStoreConfig.getInstance().getPersistenceResourceLookup();
        Set<Integer> aux = new HashSet<Integer>();
        aux.add(id);
        List<MapStoreItem> items = pm.recoverById(aux);
        return items.get(0);
    }

    public void getAll() {
        Collection<ResourceManagerWrapper> resources = MapStoreConfig.getInstance().getXAResourceLookup();
        for (ResourceManagerWrapper res : resources) {
            res.getAll();
        }
    }

}
