/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.transformers.impl;

import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.bean.annotations.Name;
import es.uc3m.it.mapstore.bean.annotations.Type;
import es.uc3m.it.mapstore.db.impl.LazyObject;
import es.uc3m.it.mapstore.transformers.MapStoreTransformer;
import es.uc3m.it.mapstore.transformers.exception.UnTransformableException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Pablo
 */
public class DefaultTransformer implements MapStoreTransformer<Object>{

    @Override
    public MapStoreItem toStore(Object object) throws UnTransformableException {
        MapStoreItem item = new MapStoreItem();
        //Recuperamos los atributos
        Field[] attributtes = object.getClass().getDeclaredFields();
        SortedMap<Integer,String> names = new TreeMap<Integer,String>();
        String type = null;
        for (Field attrib : attributtes) {
            if (Modifier.isStatic(attrib.getModifiers())) continue; //Ignoramos los estaticos
            //Para cada atibuto recuperamos el nombre y el valor
            String key = attrib.getName();
            Object value=null;
            boolean errorOnRecovery = false;
            try {
                //Primero intentamos la recuperación por el getter
                value = recoverPropertyByMethod(object, key);
            } catch (Exception ex) {
                errorRecoveringPropertyByMethod(ex,key);
                errorOnRecovery = true;
            }
            if (errorOnRecovery) {
                try {
                    //Si fallara accedemos directamente al atributo
                    value = recoverPropertyByField(attrib, object);
                    errorOnRecovery = false;
                } catch (Exception ex) {
                    errorRecoveringPropertyByField(ex, key);
                }
            }
            if (errorOnRecovery) {
                //TODO: Internacionalizar message
                throw new UnTransformableException("Object can not be converted." +
                        " Reason: DefaultTransformer unable to obtain value for property " + key);
            }
            item.setProperty(key, value);
            //Procesar anotaciones
            Name n = attrib.getAnnotation(Name.class);
            if (n != null) {
                names.put(n.order(),(String)value);
            }
            Type t = attrib.getAnnotation(Type.class);
            if (t != null) {
                type = (String)value;
            }
        }
        //Procesamos name
        StringBuffer sb = new StringBuffer();
        for (Integer i : names.keySet()) {
            sb.append(names.get(i));                    
        }
        item.setName(sb.toString());
        //Procesamos type
        if (type == null) type = object.getClass().getName();
        item.setType(type);
        item.setDataClass(object.getClass().getName());
        return item;
    }

    @Override
    public Object toObject(MapStoreItem item) throws UnTransformableException {
        Class clazz;
        try {
            clazz = Class.forName(item.getDataClass());
        } catch (ClassNotFoundException ex) {
            throw new UnTransformableException("Class not found: " + item.getDataClass(),ex);
        }
        Object a;
        try {
            a = clazz.newInstance();
        } catch (InstantiationException ex) {
            throw new UnTransformableException("Class can not be instantiated: " + item.getDataClass(),ex);
        } catch (IllegalAccessException ex) {
            throw new UnTransformableException("Illegal access: " + item.getDataClass(),ex);
        }

        Field[] fAux = clazz.getDeclaredFields();
        Map<String,Field> fields = new HashMap<String,Field>();
        for (Field f :fAux) {
            fields.put(f.getName(),f);
        }

        for (String prop : item.getProperties().keySet()) {
            Field f = fields.get(prop);
            Object value = null;
            if (f != null) {
                //En este caso no es una referencia sino el valor
                value = item.getProperty(prop);
            } else {
                if (prop.startsWith(MapStoreItem.NONPROCESSABLE)) {
                    //En este caso puede ser una referencia
                    String aux = prop.substring(MapStoreItem.NONPROCESSABLE.length());
                    f = fields.get(aux);
                    if (f != null) {
                        //En este caso es una referencia cargaremos el proxy
                        Class classLazy = f.getType();
                        String[] tmp = ((String)item.getProperty(prop)).split("_");
                        value = LazyObject.newInstance(Integer.valueOf(tmp[0]), Integer.valueOf(tmp[1]), classLazy);
                    }
                }
            }
            if (f != null && value != null) {
                try {
                    f.setAccessible(true);
                    f.set(a, value);
                } catch (IllegalArgumentException ex) {
                    throw new UnTransformableException(ex);
                } catch (IllegalAccessException ex) {
                    throw new UnTransformableException(ex);
                }
            }
        }
        return a;
    }


    private Object recoverPropertyByField(Field attrib, Object object) throws IllegalArgumentException, IllegalAccessException {
        return attrib.get(object);
    }

    private Object recoverPropertyByMethod(Object object, String  key) throws SecurityException, NoSuchMethodException, InvocationTargetException, IllegalArgumentException, IllegalAccessException  {
        String aux = "get" + String.valueOf(key.charAt(0)).toUpperCase()+key.substring(1);
        Method m = object.getClass().getMethod(aux);
        return m.invoke(object, new Object[0]);
    }

    private void errorRecoveringPropertyByMethod(Exception ex, String property) {
        if (property == null) property = "";
        //TODO: Internacionalizar el mensaje
        String msg = "Property " + property +" can not be accesed by method";
        Logger.getLogger(DefaultTransformer.class.getName()).log(Level.WARNING, msg, ex);
    }

    private void errorRecoveringPropertyByField(Exception ex, String property) {
        if (property == null) property = "";
        //TODO: Internacionalizar el mensaje
        String msg = "Property " + property +" can not be accesed by property";
        Logger.getLogger(DefaultTransformer.class.getName()).log(Level.WARNING, msg, ex);
    }


}
