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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Pablo
 */
public class CollectionTransformer implements MapStoreTransformer<Collection<? extends Object>> {

    private static final String PROP_STRING = "_prop_";
    private static final String GENERIC_STRING = "_genericType_";

    @Override
    public MapStoreItem toStore(Collection collection) throws UnTransformableException {
        MapStoreItem item = new MapStoreItem();
        int i = 0;
        for (Object o : collection) {
            item.setProperty(PROP_STRING + i, o);
            i++;
        }
        //Parche para la clase devuelta por Arrays que no es instanciable. Se sustituye por ArrayList que es el tipo que construye
        Class aux = Arrays.asList(new Object[0]).getClass();
        if (collection.getClass().getName().equals(aux.getName())) {
            item.setType(ArrayList.class.getName());
            item.setDataClass(ArrayList.class.getName());
        } else {
            item.setType(collection.getClass().getName());
            item.setDataClass(collection.getClass().getName());
        }
        item.setProperty(GENERIC_STRING, ReflectionUtils.determineGenericType(collection).getName());
        item.setExtra(MapStoreItem.ISCOLLECTION);
        item.setPrefix(PROP_STRING);
        return item;
    }

    @Override
    public Collection toObject(MapStoreItem item) {
        //Notese que en tiempo de ejecución las colecciones carecen de genericos... por lo que no es necesario devolver el objeto con generico
        String clazzName = item.getDataClass();
        try {
            System.out.println(item.getId() + " - " +clazzName);
            Collection col = (Collection) Class.forName(clazzName).newInstance();
            List<String> propertiesToProcess = getPropertiesToProcess(item);
            for (String property : propertiesToProcess) {
                col.add(item.getProperty(property));
            }
            return col;
        } catch (InstantiationException ex) {
            Logger.getLogger(CollectionTransformer.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (IllegalAccessException ex) {
            Logger.getLogger(CollectionTransformer.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(CollectionTransformer.class.getName()).log(Level.SEVERE, null, ex);
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
