/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.transformers.impl;

import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import es.uc3m.it.mapstore.transformers.MapStoreTransformer;
import es.uc3m.it.mapstore.transformers.exception.UnTransformableException;
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

    @Override
    public MapStoreItem toStore(Map object) throws UnTransformableException {
        MapStoreItem item = new MapStoreItem();
        int index = 0;
        for (Object key : object.keySet()) {
            item.setProperty(KEY+index, key);
            item.setProperty(VALUE+index, object.get(key));
            index++;        
        }
        item.setType(object.getClass().getName());
        item.setExtra(MapStoreItem.ISMAP);
        return item;
    }

    @Override
    public Map toObject(MapStoreItem item) {
        try {
            Map map = (Map) Class.forName(item.getType()).newInstance();
            List<String> properties = getPropertiesToProcess(item);
            for (String prop: properties) {
                Object key = item.getProperty(prop);
                String valueStr = prop.replaceAll(KEY, VALUE);
                Object value = item.getProperty(valueStr);
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
        }
        return properties;

    }


}
