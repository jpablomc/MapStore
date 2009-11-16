/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.transformers.impl;

import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.bean.annotations.Name;
import es.uc3m.it.mapstore.transformers.MapStoreTransformer;
import es.uc3m.it.mapstore.transformers.exception.UnTransformableException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
        for (Field attrib : attributtes) {
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
        }
        //Procesamos name
        StringBuffer sb = new StringBuffer();
        for (Integer i : names.keySet()) {
            sb.append(names.get(i));                    
        }
        item.setName(sb.toString());
        //Procesamos type
        item.setType(object.getClass().getName());
        return item;
    }

    @Override
    public Object toObject(MapStoreItem item) {
        return item;
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
