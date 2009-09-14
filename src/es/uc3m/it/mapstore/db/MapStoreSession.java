/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db;

import javax.transaction.Transaction;

/**
 *
 * @author Pablo
 */
public interface MapStoreSession {
    public void beginTransaction();
    
}
