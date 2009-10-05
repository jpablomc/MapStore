/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.transformers.impl;

import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import es.uc3m.it.mapstore.transformers.MapStoreTransformer;
import es.uc3m.it.mapstore.transformers.exception.UnTransformableException;
import es.uc3m.it.util.ReflectionUtils;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Pablo
 */
public class ArrayTransformer implements MapStoreTransformer<Object[]>{

    private static final String PROP_STRING = "_index_";
    private static final String GENERIC_STRING = "_genericType_";

    @Override
    public MapStoreItem toStore(Object[] collection) throws UnTransformableException {
        MapStoreItem item = new MapStoreItem();
        int i = 0;
        for (Object o : collection) {
            item.setProperty(PROP_STRING + i, o);
            i++;
        }
        item.setType(collection.getClass().getComponentType().getName());
        item.setProperty(GENERIC_STRING, ReflectionUtils.determineGenericType(Arrays.asList(collection)).getName());
        return item;
    }

    @Override
    public Object[] toObject(MapStoreItem item) {
        try {
            String clazzName = item.getType();
            //Object[] col = (Object[]) Class.forName(clazzName).newInstance();
            List<Object> aux = new ArrayList<Object>();
            List<String> propertiesToProcess = getPropertiesToProcess(item);
            for (String property : propertiesToProcess) {
                aux.add(item.getProperty(property));
            }
            Object[] col = (Object[]) Array.newInstance(Class.forName(clazzName), 0);
            return aux.toArray(col);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(ArrayTransformer.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }


    protected List<String> getPropertiesToProcess(MapStoreItem item) {
        List<String> properties = new ArrayList<String>();
        for (String property: item.getProperties().keySet()) {
            if (property.startsWith(PROP_STRING)) properties.add(property);
        }
        Collections.sort(properties);
        return properties;

    }

}
