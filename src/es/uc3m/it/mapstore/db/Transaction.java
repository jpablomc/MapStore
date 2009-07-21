/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db;

import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;

/**
 *
 * @author Pablo
 */
public interface Transaction {
    public void commit() throws MapStoreRunTimeException;
    public void rollback() throws MapStoreRunTimeException;
}
