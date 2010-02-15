/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.transformers.factory.impl;

import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.transformers.MapStoreTransformer;
import es.uc3m.it.mapstore.transformers.factory.TransformerFactory;
import es.uc3m.it.mapstore.transformers.impl.ArrayTransformer;
import es.uc3m.it.mapstore.transformers.impl.CollectionTransformer;
import es.uc3m.it.mapstore.transformers.impl.DefaultTransformer;
import es.uc3m.it.mapstore.transformers.impl.MapStoreItemTransformer;
import es.uc3m.it.mapstore.transformers.impl.MapStringTransformer;
import es.uc3m.it.mapstore.transformers.impl.MapTransformer;
import es.uc3m.it.mapstore.transformers.impl.StringPropertyTransformer;
import es.uc3m.it.util.ReflectionUtils;
import java.util.Collection;
import java.util.Map;

/**
 *
 * @author Pablo
 */
public class DefaultTransformerFactory implements TransformerFactory {

    @Override
    public MapStoreTransformer getFactory(Object o) {
        MapStoreTransformer tf;
        if (o.getClass().isArray()) tf = new ArrayTransformer();
        else if (o instanceof String) tf = new StringPropertyTransformer();
        else if (o instanceof MapStoreItem) tf = new MapStoreItemTransformer();
        else if (o instanceof Collection) tf = new CollectionTransformer();
        else if (o instanceof Map) {
            Map aux = (Map) o;
            Class c = ReflectionUtils.determineGenericType(aux.keySet());
            if (String.class.isAssignableFrom(c)) tf = new MapStringTransformer();
            else tf = new MapTransformer();
        }
        else tf = new DefaultTransformer();
        return tf;
    }

    @Override
    public MapStoreTransformer getFactory(Class c) {
        MapStoreTransformer tf;
        if (c.isArray()) tf = new ArrayTransformer();
        else if (String.class.isAssignableFrom(c)) tf = new StringPropertyTransformer();
        else if (MapStoreItem.class.isAssignableFrom(c)) tf = new MapStoreItemTransformer();
        else if (Collection.class.isAssignableFrom(c)) tf = new CollectionTransformer();
        else if (Map.class.isAssignableFrom(c)) tf = new MapTransformer();
        else tf = new DefaultTransformer();
        return tf;
    }
}
