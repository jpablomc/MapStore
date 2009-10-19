/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl;

import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.config.MapStoreConfig;
import es.uc3m.it.mapstore.db.transaction.xa.ResourceManagerWrapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Pablo
 */
public abstract class ResourceManagerlImpl implements ResourceManagerWrapper{

    protected List<String> getPropertiesToProcess(MapStoreItem item) {
        List<String> props = new ArrayList<String>();
        Map<Class,List<ResourceManagerWrapper>> mapa = new HashMap<Class,List<ResourceManagerWrapper>>();
        for (String prop :item.getProperties().keySet()) {
            if (processable(prop)) {
                Object value = item.getProperty(prop);
                List<ResourceManagerWrapper> lista = mapa.get(value.getClass());
                if (lista == null) {
                    lista = MapStoreConfig.getInstance().getXaResourceLookupForClass(value.getClass());
                    mapa.put(value.getClass(),lista);
                }
                if (lista.contains(this)) props.add(prop);
            }
        }
        return props;
    }

    protected List<String> getMapPropertiesToProcess(MapStoreItem item) {
        List<String> props = new ArrayList<String>();
        Map<Class,List<ResourceManagerWrapper>> mapa = new HashMap<Class,List<ResourceManagerWrapper>>();
        for (String prop :item.getProperties().keySet()) {
            if (processable(prop)) {
                Object value = item.getProperty(prop);
                if (value instanceof Map) {
                    Class clazz  = determineGenericType(((Map)value).values());
                    List<ResourceManagerWrapper> lista = mapa.get(clazz);
                    if (lista == null) {
                        lista = MapStoreConfig.getInstance().getXaResourceLookupForClass(clazz);
                        mapa.put(clazz,lista);
                    }
                    if (lista.contains(this)) props.add(prop);
                }

            }
        }
        return props;
    }

    protected List<String> getListPropertiesToProcess(MapStoreItem item) {
        List<String> props = new ArrayList<String>();
        Map<Class,List<ResourceManagerWrapper>> mapa = new HashMap<Class,List<ResourceManagerWrapper>>();
        for (String prop :item.getProperties().keySet()) {
            if (processable(prop)) {
                Object value = item.getProperty(prop);
                if (value instanceof Collection) {
                    Class clazz  = determineGenericType((Collection) value);
                    List<ResourceManagerWrapper> lista = mapa.get(clazz);
                    if (lista == null) {
                        lista = MapStoreConfig.getInstance().getXaResourceLookupForClass(clazz);
                        mapa.put(clazz,lista);
                    }
                    if (lista.contains(this)) props.add(prop);
                }

            }
        }
        return props;
    }

    protected List<String> getArrayPropertiesToProcess(MapStoreItem item) {
        List<String> props = new ArrayList<String>();
        Map<Class,List<ResourceManagerWrapper>> mapa = new HashMap<Class,List<ResourceManagerWrapper>>();
        for (String prop :item.getProperties().keySet()) {
            if (processable(prop)) {
                Object value = item.getProperty(prop);
                if (value.getClass().isArray()) {
                    Collection col = Arrays.asList(value);
                    Class clazz  = determineGenericType(col);
                    List<ResourceManagerWrapper> lista = mapa.get(clazz);
                    if (lista == null) {
                        lista = MapStoreConfig.getInstance().getXaResourceLookupForClass(clazz);
                        mapa.put(clazz,lista);
                    }
                    if (lista.contains(this)) props.add(prop);
                }

            }
        }
        return props;
    }

    private Class determineGenericType(Collection<? extends Object> col) {
        Class toReturn = null;
        Set<Class> clazz = null;
        List<Class> clazzInterfaz = null;
        for (Object o: col) {
            if (o != null) {
                Set<Class> tmpClazz = new HashSet<Class>();
                Class c = o.getClass();
                while (c != null) {
                    tmpClazz.add(c);
                    c = c.getSuperclass();
                }
                if (clazz == null) clazz = tmpClazz;
                else clazz.retainAll(tmpClazz);
                List<Class> tmpInterfaz = Arrays.asList(c.getInterfaces());
                if (clazzInterfaz == null) clazzInterfaz = tmpInterfaz;
                else clazzInterfaz.retainAll(tmpInterfaz);
            }
        }
        //clazz contiene todas las superclases comunes... determinar la más especifica de todas
        for (Class c : clazz) {
            if (toReturn == null) toReturn = c;
            else if (toReturn.isAssignableFrom(c)) toReturn = c;
        }
        //No hay clase común (solo Object) mirar herencia en interfaz
        if (toReturn.equals(Object.class)) {
            Set<Class> toDelete = new HashSet<Class>();
            for (int i = 0;i<clazzInterfaz.size()-1;i++) {
                for (int j= i+1;j<clazzInterfaz.size();j++) {
                    Class c1 = clazzInterfaz.get(i);
                    Class c2 = clazzInterfaz.get(j);
                    if (!c1.isAssignableFrom(c2) && !c2.isAssignableFrom(c1)) {
                        toDelete.add(c1);
                        toDelete.add(c2);
                    }
                }
            }
            clazzInterfaz.removeAll(toDelete);
            //Solo quedan las que estan en herencia
            for (Class c : clazz) {
                if (toReturn == null) toReturn = c;
                else if (toReturn.isAssignableFrom(c)) toReturn = c;
            }
        }
        return toReturn;
    }
    

    protected boolean processable(String prop) {
        boolean toProcess = true;
        if (MapStoreItem.VERSION.equals(prop)) toProcess = false;
        return toProcess;
    }
}
