/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.transformers.factory;

import es.uc3m.it.mapstore.transformers.MapStoreTransformer;

/**
 *
 * @author Pablo
 */
public interface TransformerFactory {
    public MapStoreTransformer getFactory(Object o);
    public MapStoreTransformer getFactory(Class o);
}
