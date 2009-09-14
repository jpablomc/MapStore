/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.exception;

/**
 *
 * @author Pablo
 */
public class MapStoreRunTimeException extends RuntimeException{

    public MapStoreRunTimeException(Throwable cause) {
        super(cause);
    }

    public MapStoreRunTimeException(String message, Throwable cause) {
        super(message, cause);
    }

    public MapStoreRunTimeException(String msg) {
        super(msg);
    }

}
