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
import es.uc3m.it.mapstore.transformers.impl.MapTransformer;
import es.uc3m.it.mapstore.transformers.impl.StringPropertyTransformer;
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
        if (o instanceof String) tf = new StringPropertyTransformer();
        else if (o instanceof MapStoreItem) tf = new MapStoreItemTransformer();
        else if (o instanceof Collection) tf = new CollectionTransformer();
        else if (o instanceof Map) tf = new MapTransformer();
        else {
            tf = new DefaultTransformer();
        }
        return tf;
    }

}
