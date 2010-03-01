/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.transformers.impl;

import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.db.impl.LazyObject;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import es.uc3m.it.mapstore.transformers.MapStoreTransformer;
import es.uc3m.it.mapstore.transformers.exception.UnTransformableException;
import es.uc3m.it.util.ReflectionUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Pablo
 */
public class MapTransformer implements MapStoreTransformer<Map> {

    private final static String KEY ="_key_";
    private final static String VALUE ="_value_";
    private final static String KEY_TYPE ="_type_key_";
    private final static String VALUE_TYPE ="_type_value_";

    @Override
    public MapStoreItem toStore(Map object) throws UnTransformableException {
        if (String.class.isAssignableFrom(ReflectionUtils.determineGenericType(object.keySet()))) return new MapStringTransformer().toStore(object);
        MapStoreItem item = new MapStoreItem();
        int index = 0;
        for (Object key : object.keySet()) {
            item.setProperty(KEY+index, key);
            item.setProperty(KEY_TYPE+index, (key != null)?key.getClass().getName():null);
            item.setProperty(VALUE+index, object.get(key));
            Object v = object.get(key);
            item.setProperty(VALUE_TYPE+index, (v!= null)?v.getClass().getName():null);
            index++;        
        }
        if (object.containsKey(MapStoreItem.TYPE)) item.setType((String)object.get(MapStoreItem.TYPE));
        else item.setType(object.getClass().getName());
        item.setDataClass(object.getClass().getName());
        item.setExtra(MapStoreItem.ISMAP);
        item.setProperty(KEY_TYPE, ReflectionUtils.determineGenericType(object.keySet()).getName());
        item.setProperty(VALUE_TYPE, ReflectionUtils.determineGenericType(object.values()).getName());
        return item;
    }

    @Override
    public Map toObject(MapStoreItem item) {
        if (item.getProperty(KEY_TYPE) == null) return new MapStringTransformer().toObject(item);
        try {
            Map map = (Map) Class.forName(item.getDataClass()).newInstance();
            List<String> properties = getPropertiesToProcess(item);
            for (String prop: properties) {
                Object key;
                if (prop.startsWith(MapStoreItem.NONPROCESSABLE_REFERENCE)) {
                    String[] tmp = ((String)item.getProperty(prop)).split("_");
                    Class clazzkey = Class.forName((String)item.getProperty(KEY_TYPE+prop));
                    key = LazyObject.newInstance(Integer.valueOf(tmp[0]), Integer.valueOf(tmp[1]), clazzkey);
                } else key = item.getProperty(prop);
                String valueStr = prop.replaceAll(KEY, VALUE);

                Object value = item.getProperty(valueStr);
                if (value == null) {
                    //En este caso es una referencia
                    valueStr = prop.replaceAll(KEY, MapStoreItem.NONPROCESSABLE_REFERENCE+VALUE);
                    String[] tmp = ((String)item.getProperty(valueStr)).split("_");
                    Class clazzValue = Class.forName((String)item.getProperty(VALUE_TYPE+ prop));
                    value = LazyObject.newInstance(Integer.valueOf(tmp[0]), Integer.valueOf(tmp[1]), clazzValue);
                }
                map.put(key,value);
            }

            return map;
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MapTransformer.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (InstantiationException ex) {
            Logger.getLogger(MapTransformer.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(MapTransformer.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

    private List<String> getPropertiesToProcess(MapStoreItem item) {
        List<String> properties = new ArrayList<String>();
        for (String property: item.getProperties().keySet()) {
            if (property.startsWith(KEY)) properties.add(property);
            else if (property.startsWith(MapStoreItem.NONPROCESSABLE_REFERENCE + KEY)) properties.add(property);
        }
        return properties;

    }


}
