/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.transformers.impl;

import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.transformers.MapStoreTransformer;
import es.uc3m.it.mapstore.transformers.exception.UnTransformableException;

/**
 *
 * @author Pablo
 */
public class MapStoreItemTransformer implements MapStoreTransformer<MapStoreItem>{

    @Override
    public MapStoreItem toStore(MapStoreItem object) throws UnTransformableException {
        return object;
    }

    @Override
    public MapStoreItem toObject(MapStoreItem item) {
        return item;
    }

}
