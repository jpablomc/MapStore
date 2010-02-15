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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Pablo
 */
public class MapStringTransformer implements MapStoreTransformer<Map<String, ? extends Object>> {

    private final static String PROCESSED = "_processed_";
    private final static String VALUE_TYPE = "_type_value_";

    @Override
    public MapStoreItem toStore(Map<String, ? extends Object> object) throws UnTransformableException {
        MapStoreItem item = new MapStoreItem();
        StringBuffer sb = new StringBuffer();
        for (String key : object.keySet()) {
            Object value = object.get(key);
            if (value != null) {
                item.setProperty(key, value);
                item.setProperty(VALUE_TYPE + key, object.get(key).getClass().getName());
                sb.append(key).append("|");
            }
        }
        if (item.getType() == null) {
            item.setType(object.getClass().getName());
        }
        item.setDataClass(object.getClass().getName());
        item.setExtra(MapStoreItem.ISMAP);
        item.setProperty(PROCESSED, sb.toString());
        item.setProperty(VALUE_TYPE, ReflectionUtils.determineGenericType(object.values()).getName());

        return item;

    }

    @Override
    public Map<String, ? extends Object> toObject(MapStoreItem item) {
        try {
            Map map = (Map) Class.forName(item.getDataClass()).newInstance();
            String properties = (String) item.getProperty(PROCESSED);
            List<String> props = Arrays.asList(properties.split("\\|"));
            for (String currentProp : item.getProperties().keySet()) {
                if (props.contains(currentProp)) {
                    map.put(currentProp, item.getProperty(currentProp));
                } else if (currentProp.startsWith(MapStoreItem.NONPROCESSABLE)) {
                    //Es una referencia
                    String aux = currentProp.substring(MapStoreItem.NONPROCESSABLE.length()); // Nos quedamos con el nombre original de la propiedad
                    if (props.contains(aux)) {
                        //Es una referencia
                        String[] tmp = ((String) item.getProperty(currentProp)).split("_");
                        Class clazzkey = Class.forName((String) item.getProperty(VALUE_TYPE+aux));
                        Object key = LazyObject.newInstance(Integer.valueOf(tmp[0]), Integer.valueOf(tmp[1]), clazzkey);
                        map.put(aux, key);
                    } else {
                        throw new MapStoreRunTimeException("Hay algo mal"); //Nunca debería pasar
                    }
                }
            }
            return map;
        } catch (InstantiationException ex) {
            throw new MapStoreRunTimeException(ex);
        } catch (IllegalAccessException ex) {
            throw new MapStoreRunTimeException(ex);
        } catch (ClassNotFoundException ex) {
            throw new MapStoreRunTimeException(ex);
        }

    }
}
