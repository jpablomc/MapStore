/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db;

/**
 *
 * @author Pablo
 */
public interface MapStoreSession {
    public Transaction beginTransaction();
}
