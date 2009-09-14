/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.disk;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 *
 * @author Pablo
 */
public class DiskXAResource implements XAResource{
    private static int DEFAULT_TIMEOUT = 120;

    @Override
    public void commit(Xid arg0, boolean arg1) throws XAException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void end(Xid arg0, int arg1) throws XAException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void forget(Xid arg0) throws XAException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getTransactionTimeout() throws XAException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isSameRM(XAResource arg0) throws XAException {
        boolean isSame = false;
        if (arg0 instanceof DiskXAResource) {
            isSame = (this == arg0);
        }
        return isSame;
    }

    @Override
    public int prepare(Xid arg0) throws XAException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Xid[] recover(int arg0) throws XAException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void rollback(Xid arg0) throws XAException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean setTransactionTimeout(int arg0) throws XAException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void start(Xid arg0, int arg1) throws XAException {
        throw new UnsupportedOperationException("Not supported yet.");
    }


}
