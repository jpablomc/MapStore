/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.transformers.impl;

import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import es.uc3m.it.mapstore.transformers.MapStoreTransformer;
import es.uc3m.it.mapstore.transformers.exception.UnTransformableException;
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
public class MapStringTransformer implements MapStoreTransformer<Map<String,? extends Object>> {

    @Override
    public MapStoreItem toStore(Map<String, ? extends Object> object) throws UnTransformableException {
        MapStoreItem item = new MapStoreItem();
        for (String key : object.keySet()) {
            item.setProperty(key, object.get(key));
        }
        item.setType(object.getClass().getName());
        return item;

    }

    @Override
    public Map<String, ? extends Object> toObject(MapStoreItem item) {
            Map map;
        try {
            map = (Map) Class.forName(item.getType()).newInstance();
            Set<String> properties = getPropertiesToProcess(item);
            for (String prop: properties) {
                Object value = item.getProperty(prop);
                map.put(prop,value);
            }
            return map;
        } catch (InstantiationException ex) {
            Logger.getLogger(MapStringTransformer.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(MapStringTransformer.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MapStringTransformer.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

    protected static List<String> NONPROCESSABLEPROPERTIES =
            Arrays.asList(new String[]{MapStoreItem.ID,MapStoreItem.NAME,MapStoreItem.TYPE});

    protected Set<String> getPropertiesToProcess(MapStoreItem item) {
        Set<String> properties = item.getProperties().keySet();
        properties.removeAll(NONPROCESSABLEPROPERTIES);
        return properties;

    }
    

}
