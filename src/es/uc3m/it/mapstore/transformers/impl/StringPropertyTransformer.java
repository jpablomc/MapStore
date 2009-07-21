/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.transformers.impl;

import es.uc3m.it.mapstore.transformers.*;
import es.uc3m.it.mapstore.bean.MapStoreItem;

/**
 *
 * @author Pablo
 */
public class StringPropertyTransformer implements MapStoreTransformer<String>{

    private static final String STRING_VALUE = "_STRINGVALUE";

    public MapStoreItem toStore(String object) {
        MapStoreItem item = new MapStoreItem();
        //TODO: Revisar lo del nombre por defecto... tal vez incluir alguna
        //cadena previa
        item.setName(object);
        item.setType(String.class.getName());
        item.setProperty(STRING_VALUE,object);
        return item;
    }

    public String toObject(MapStoreItem item) {
        return (String) item.getProperty(STRING_VALUE);
    }

}
