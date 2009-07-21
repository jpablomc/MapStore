/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.transformers.exception;

import es.uc3m.it.mapstore.exception.MapStoreException;

/**
 *
 * @author Pablo
 */
public class UnTransformableException extends MapStoreException{

    public UnTransformableException(String string) {
        super(string);
    }
}
