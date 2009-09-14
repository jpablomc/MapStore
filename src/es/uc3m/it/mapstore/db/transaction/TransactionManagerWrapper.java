/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

/**
 *
 * @author Pablo
 */
public interface TransactionManagerWrapper {
    public void init();
    public void rollback() throws IllegalStateException, SecurityException, SystemException;
    public void begin() throws NotSupportedException, SystemException;
    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException;
    public int getStatus() throws SystemException;
    public TransactionManager getTransactionManager();
}
