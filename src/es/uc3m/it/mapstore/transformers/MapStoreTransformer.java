/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.transformers;

import es.uc3m.it.mapstore.transformers.exception.UnTransformableException;
import es.uc3m.it.mapstore.bean.MapStoreItem;

/**
 *
 * @author Pablo
 */
public interface MapStoreTransformer<T> {
    public MapStoreItem toStore(T object) throws UnTransformableException;
    public T toObject(MapStoreItem item) throws UnTransformableException;
}
