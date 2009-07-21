/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.transformers.factory.impl;

import es.uc3m.it.mapstore.transformers.MapStoreTransformer;
import es.uc3m.it.mapstore.transformers.factory.TransformerFactory;
import es.uc3m.it.mapstore.transformers.impl.DefaultTransformer;
import es.uc3m.it.mapstore.transformers.impl.StringPropertyTransformer;

/**
 *
 * @author Pablo
 */
public class DefaultTransformerFactory implements TransformerFactory {

    public MapStoreTransformer getFactory(Object o) {
        MapStoreTransformer tf;
        if (o instanceof String) tf = new StringPropertyTransformer();
        else {
            tf = new DefaultTransformer();
        }
        return tf;
    }

}
