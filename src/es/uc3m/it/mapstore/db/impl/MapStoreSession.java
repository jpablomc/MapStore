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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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

    private boolean needsUpdateArray(Object[] newObject, Object[] oldObject){
        boolean needsUpdate;
        //Los objetos son arrays se convierten a listas y se comparan
        List c1 = Arrays.asList(newObject);
        List c2 = Arrays.asList(oldObject);
        needsUpdate = needsUpdateList(c1, c2);
        return needsUpdate;
    }


    /**
     *
     * Compara dos objetos de tipo Collection para comprobar si son iguales. No comprueba el orden de los elementos
     * Requiere implementar equals
     *
     * @param newObject
     * @param oldObject
     * @param needsUpdate
     * @return
     * @throws UnTransformableException
     */
    private boolean needsUpdateCollection(Collection newObject, Collection oldObject) {
        boolean needsUpdate = false;;
        //Los objetos son colecciones
        Collection c1 = newObject;
        Collection c2 = oldObject;
        if ((c1.size() != c2.size()) || (!c1.containsAll(c2))) {
            needsUpdate = true;
        } 
        return needsUpdate;
    }


    /**
     *
     * Compara dos objetos tipo lista para ver si son iguales. Se distingue de needsUpdateCollection de que comprueba el orden
     * Los objetos serán comparados utilizando equals
     *
     * @param newObject El nuevo objeto a guardar en BBDD
     * @param oldObject El objeto en BBDD
     * @param needsUpdate Acumulado de comprobaciones
     * @return Devuelve si detecta que debe ser actualizado
     * @throws UnTransformableException
     */
    private boolean needsUpdateList(List newObject, List oldObject){
        boolean needsUpdate = false;
        //Los objetos son listas
        List c1 = newObject;
        List c2 = oldObject;
        if (c1.size() != c2.size()) {
            needsUpdate = true;
        } else {
            //Comparamos los contenidos de la lista
            int i = 0;
            while (!needsUpdate && i < c1.size()) {
                //Recuperamos los valores acomparar
                Object newValue = c1.get(i);
                Object oldValue = c2.get(i);
                //Tenemos dos casos a evaluar, ahora es nulo, o por equals
                //Caso es nulo... si el antiguo no es nulo hay que actualizar
                if (newValue == null) {
                    if (oldValue != null) needsUpdate = true;
                } else {
                    //El equals se deberá implementarse en el objeto
                    if (!newValue.equals(oldValue)) needsUpdate = true;
                }
                i++;
            }
        }
        return needsUpdate;
    }

    private boolean needsUpdateMap(Object newObject, Object oldObject) {
        boolean needsUpdate = false;
        Map m1 = (Map) newObject;
        Map m2 = (Map) oldObject;
        if (!m1.equals(m2)) {
            needsUpdate = true;
        }
        return needsUpdate;
    }

    private int[] save(MapStoreItem item) {
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
            return new int[] {item.getId(),item.getVersion()};
        } catch (SystemException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

    public int[] saveAnonymous(Object o, String name) {
        try {
            MapStoreItem i = MapStoreConfig.getInstance().getTransformerFactory().getFactory(o).toStore(o);
            if (i.getName() == null) i.setName(name);
            return save(i);
        } catch (UnTransformableException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

    public int[] save(Object o){
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

    public int[] update(Object o) {
        try {
            MapStoreItem i = MapStoreConfig.getInstance().getTransformerFactory().getFactory(o).toStore(o);
            return update(i);
        } catch (UnTransformableException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

    public int[] updateAnonymous(Object o,String name) {
        try {
            MapStoreItem i = MapStoreConfig.getInstance().getTransformerFactory().getFactory(o).toStore(o);
            if (i.getName() == null) i.setName(name);
            return update(i);
        } catch (UnTransformableException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }


    private int[] update(MapStoreItem item) {
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
            return new int[]{item.getId(),item.getVersion()};
        } catch (SystemException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

    private final static String[] NONDELETABLEPROPERTIES = {MapStoreItem.ID,MapStoreItem.NAME,MapStoreItem.RECORDDATE,MapStoreItem.TYPE,MapStoreItem.VERSION};

    public void delete(Object obj) {
        try {
            MapStoreItem i = findByNameType(obj);
            if (i != null) delete(i);
        } catch (UnTransformableException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

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

    public List<MapStoreItem> findByType(String type) {
        Collection<ResourceManagerWrapper> resources = MapStoreConfig.getInstance().getXAResourceLookup();
        List<ResourceManagerWrapper> resByName = new ArrayList<ResourceManagerWrapper>();
        for(ResourceManagerWrapper r : resources) {
            if (r.canFindByNameType()) resByName.add(r);
        }
        ResourceManagerWrapper r = BalanceLoad.getLessLoadedAndIncrease(resByName, 1);
        Set<Integer> id = r.findByType(type);
        BalanceLoad.decreaseLoad(r, 1);
        List<MapStoreItem> toReturn = null;
        if (id != null) toReturn = recoverById(id);
        return toReturn;
    }


    public <T> T findByNameType(String name, Class<T> c) {
        try {
            MapStoreItem item = findByNameType(name, c.getName());
            T toReturn = null;
            if (item != null) {
                MapStoreTransformer<T> trans = MapStoreConfig.getInstance().getTransformerFactory().getFactory(c);
                toReturn = trans.toObject(item);
            }
            return toReturn;
        } catch (UnTransformableException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

    public <T> List<T> findByType(Class<T> c) {
        try {
            List<MapStoreItem> items = findByType(c.getName());
            List<T> toReturn = new ArrayList<T>();
            if (items != null && !items.isEmpty()) {
                MapStoreTransformer<T> trans = MapStoreConfig.getInstance().getTransformerFactory().getFactory(c);
                for (MapStoreItem i :items) {
                toReturn.add(trans.toObject(i));
                }
            }
            return toReturn;
        } catch (UnTransformableException ex) {
            Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

    public final static int CONJUNCTIVE_SEARCH = 0;
    public final static int DISJUNCTIVE_SEARCH = 1;

    public <T> List<T> findByConditions(Class<T> a,MapStoreCondition[] conditions,int flag,Date fecha) {
        List<T> results = new ArrayList<T>();
        List<MapStoreItem> items = findByConditions(conditions, flag, fecha);
        MapStoreTransformer<T> trans = MapStoreConfig.getInstance().getTransformerFactory().getFactory(a);
        for (MapStoreItem item : items) {
            try {
                results.add(trans.toObject(item));
            } catch (UnTransformableException ex) {
                Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
                throw new MapStoreRunTimeException(ex);
            }
        }
        return results;
    }

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

    public <T> T recoverByIdVersion(int id, int version, Class<T> clazz) {
        Map<Integer,Set<Integer>> mapa= new HashMap<Integer,Set<Integer>>();
        Set<Integer> set = new HashSet<Integer>();
        set.add(version);
        mapa.put(id, set);
        Map<Integer,Map<Integer,MapStoreItem>> result = recoverByIdVersion(mapa);
        MapStoreItem item = result.get(id).get(version);
        T toReturn = null;
        if (item != null) {
            MapStoreTransformer<T> trans = MapStoreConfig.getInstance().getTransformerFactory().getFactory(clazz);
            try {
                toReturn = trans.toObject(item);
            } catch (UnTransformableException ex) {
                Logger.getLogger(MapStoreSession.class.getName()).log(Level.SEVERE, null, ex);
                throw new MapStoreRunTimeException(ex);
            }
        }
        return toReturn;
    }


    public void getAll() {
        Collection<ResourceManagerWrapper> resources = MapStoreConfig.getInstance().getXAResourceLookup();
        for (ResourceManagerWrapper res : resources) {
            res.getAll();
        }
    }

    /**
     *
     * Comprueba si el objeto requiere ser salvado en BBDD. Utiliza el método
     * equals por lo que este debe estar implementado.
     *
     * @param newObject El objeto que queremos actualiar
     * @return Devuelve si es necesario actualizar
     * @throws UnTransformableException
     */
    public boolean needsUpdate(Object newObject, String name){
        boolean needsUpdate = false;
        MapStoreItem newItem;
        try {
            newItem = MapStoreConfig.getInstance().getTransformerFactory().getFactory(newObject).toStore(newObject);
        } catch (UnTransformableException ex) {
            throw new MapStoreRunTimeException(ex);
        }
        if (newItem.getName() != null) name = newItem.getName();
        Object oldObject = findByNameType(name, newObject.getClass());
        //Procesamos el objeto según el tipo.... (Lista,Array, Coleccion, Mapa, Tipo Complejo
        //Se distinguen las listas de las colecciones ya que estas deben respetar el orden
        //No hay tipos basicos ya que estos se actualizan siempre
        //Caso Lista...
        if (oldObject == null) needsUpdate = true;
        else if (newObject instanceof List) {
            needsUpdate = needsUpdateList((List)newObject, (List)oldObject);
        } else if (newObject instanceof Collection) {
            needsUpdate = needsUpdateCollection((Collection)newObject, (Collection)oldObject);
        } else if (newObject.getClass().isArray()) {
            needsUpdateArray((Object[])newObject, (Object[])oldObject);
        } else if (newObject instanceof Map) {
            needsUpdate = needsUpdateMap(newObject, oldObject);
        } else if (!newObject.equals(oldObject)) {
            needsUpdate = true;
        }
        return needsUpdate;
    }

    public void shutdown() {
        Collection<ResourceManagerWrapper> res = MapStoreConfig.getInstance().getXAResourceLookup();
        for (ResourceManagerWrapper r: res) {
            r.shutdown();
        }
        PersistenceManagerWrapper pm = MapStoreConfig.getInstance().getPersistenceResourceLookup();
        pm.shutdown();
    }

}
