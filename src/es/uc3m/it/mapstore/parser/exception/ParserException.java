/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.parser.exception;

import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;

/**
 *
 * @author Pablo
 */
public class ParserException extends MapStoreRunTimeException{
    public ParserException(String msg) {
        super(msg);
    }

    public ParserException(String message, Throwable cause) {
        super(message, cause);
    }

    public ParserException(Throwable cause) {
        super(cause);
    }
}
