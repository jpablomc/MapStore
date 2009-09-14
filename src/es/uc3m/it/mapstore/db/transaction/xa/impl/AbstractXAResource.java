/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 *
 * @author Pablo
 */
public abstract class AbstractXAResource implements XAResource{
    private static int DEFAULT_TIMEOUT = 120;
    private static Map<Thread,XidImpl> threads = new HashMap<Thread,XidImpl>();;
    private static Map<XidImpl,Set<Thread>> xidActive = new HashMap<XidImpl,Set<Thread>>();;
    private static Set<XidImpl> xidSuspended = new HashSet<XidImpl>();
    private static Set<XidImpl> xidSuccess = new HashSet<XidImpl>();
    private static Set<XidImpl> xidPrepared = new HashSet<XidImpl>();
    private static Set<XidImpl> xidFailed = new HashSet<XidImpl>();
    private static final Semaphore s = new Semaphore(1);


    @Override
    public void commit(Xid arg0, boolean onePhase) throws XAException {
        XidImpl xid = new XidImpl(arg0.getFormatId(), arg0.getGlobalTransactionId(), arg0.getBranchQualifier());
        if (onePhase) {
            doCommit(xid);
        } else {
            if (!xidPrepared.contains(xid)) prepare(xid);
            doCommit(xid);
        }
    }

    @Override
    public void end(Xid arg0, int arg1) throws XAException {

        XidImpl xid = new XidImpl(arg0.getFormatId(), arg0.getGlobalTransactionId(), arg0.getBranchQualifier());
        try {
            s.acquire();
        } catch (InterruptedException ex) {
            Logger.getLogger(AbstractXAResource.class.getName()).log(Level.SEVERE, null, ex);
            throw new XAException(XAException.XAER_RMERR);
        }
        try {
            if (!xidActive.containsKey(xid)) {
                throw new XAException(XAException.XAER_NOTA);
            }
            Set<Thread> thr;
            switch (arg1) {
                case XAResource.TMSUCCESS:
                    xidSuccess.add(xid);
                    thr = xidActive.get(xid);
                    xidActive.remove(xid);
                    for (Thread t: thr) {
                        threads.remove(t);
                    }
                    break;
                case XAResource.TMFAIL:
                    xidFailed.add(xid);
                    thr = xidActive.get(xid);
                    xidActive.remove(xid);
                    for (Thread t: thr) {
                        threads.remove(t);
                    }
                    break;
                case XAResource.TMSUSPEND:
                    xidSuspended.add(xid);
                    thr = xidActive.get(xid);
                    xidActive.remove(xid);
                    for (Thread t: thr) {
                        threads.remove(t);
                    }
                    break;
                default:
                    throw new XAException(XAException.XAER_INVAL);
            }
        } finally {
            s.release();
        }
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
        if (arg0 instanceof AbstractXAResource) {
            isSame = (this == arg0);
        }
        return isSame;
    }

    @Override
    public int prepare(Xid arg0) throws XAException {
        XidImpl xid = new XidImpl(arg0.getFormatId(), arg0.getGlobalTransactionId(), arg0.getBranchQualifier());
        try {
            s.acquire();
        } catch (InterruptedException ex) {
            Logger.getLogger(AbstractXAResource.class.getName()).log(Level.SEVERE, null, ex);
            throw new XAException(XAException.XAER_RMERR);
        }
        try {
            if (!xidSuccess.contains(xid)) {
                if (xidPrepared.contains(xid)) throw new XAException(XAException.XAER_PROTO);
                else throw new XAException(XAException.XAER_NOTA);
            }
            int result = doPrepare(xid);
            xidSuccess.remove(xid);
            xidPrepared.add(xid);
            return result;
        } finally {
            s.release();
        }
    }

    @Override
    public Xid[] recover(int arg0) throws XAException {
        try {
            s.acquire();
        } catch (InterruptedException ex) {
            Logger.getLogger(AbstractXAResource.class.getName()).log(Level.SEVERE, null, ex);
            throw new XAException(XAException.XAER_RMERR);
        }
        try{
            switch(arg0) {
                case XAResource.TMSTARTRSCAN:
                case XAResource.TMENDRSCAN:
                case XAResource.TMNOFLAGS:
                    break;
                default:
                    throw new XAException(XAException.XAER_PROTO);
            }
            return xidPrepared.toArray(new Xid[0]);
        } finally {
            s.release();
        }
        
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
        XidImpl xid = new XidImpl(arg0.getFormatId(), arg0.getGlobalTransactionId(), arg0.getBranchQualifier());
        try {
            s.acquire();
        } catch (InterruptedException ex) {
            Logger.getLogger(AbstractXAResource.class.getName()).log(Level.SEVERE, null, ex);
            throw new XAException(XAException.XAER_RMERR);
        }
        Thread t = Thread.currentThread();
        Set<Thread> thr = new HashSet<Thread>();
        try {
            switch (arg1) {
                case XAResource.TMNOFLAGS:
                    //Comprobar si el hilo no esta asociado a ninguna transaccion
                    if (threads.containsKey(t)) {
                        throw new XAException(XAException.XAER_PROTO);
                    }
                    //Comprobar que la rama no existe aun (no es JOIN)
                    if (xidActive.containsKey(xid) || xidSuspended.contains(xid)) {
                        throw new XAException(XAException.XAER_DUPID);
                    }
                    thr.add(t);
                    xidActive.put(xid,thr);
                    threads.put(t, xid);
                    break;
                case XAResource.TMRESUME:
                    //Comprobar si el hilo no esta asociado a ninguna transaccion
                    if (threads.containsKey(t)) {
                        throw new XAException(XAException.XAER_PROTO);
                    }
                    //Comprobar que el hilo esta suspendido
                    if (!xidSuspended.contains(xid)) {
                        throw new XAException(XAException.XAER_NOTA);
                    }
                    thr.add(t);
                    xidActive.put(xid,thr);
                    threads.put(t, xid);
                    xidSuspended.remove(xid);
                    break;
                case XAResource.TMJOIN:
                    //Comprobar si el hilo no esta asociado a ninguna transaccion
                    if (threads.containsKey(t)) {
                        throw new XAException(XAException.XAER_PROTO);
                    }
                    if (!xidActive.containsKey(xid)) {
                        throw new XAException(XAException.XAER_NOTA);
                    }
                    thr = xidActive.get(xid);
                    thr.add(t);
                    break;
                default:
                    throw new XAException(XAException.XAER_DUPID);
            }
        } finally {
            s.release();
        }
    }

    protected Xid getXidCurrentThread() {
        return threads.get(Thread.currentThread());
    }

    protected abstract int doPrepare(Xid arg0) throws XAException;
    protected abstract void doCommit(Xid arg0) throws XAException;
    protected abstract void doRollback(Xid arg0) throws XAException;


    private class XidImpl implements Xid{
        private int formatId;
        private byte[] globalTransactionId;
        private byte[] branchQualifier;

        public XidImpl(int formatId, byte[] globalTransactionId, byte[] branchQualifier) {
            this.formatId = formatId;
            this.globalTransactionId = new byte[globalTransactionId.length];
            this.branchQualifier = new byte[branchQualifier.length];
            System.arraycopy(globalTransactionId,0 , this.globalTransactionId, 0, globalTransactionId.length);
            System.arraycopy(branchQualifier,0 , this.branchQualifier, 0, branchQualifier.length);
        }

        @Override
        public int getFormatId() {
            return formatId;
        }

        @Override
        public byte[] getGlobalTransactionId() {
            return globalTransactionId;
        }

        @Override
        public byte[] getBranchQualifier() {
            return branchQualifier;
        }

        @Override
        public boolean equals(Object obj) {
            boolean isEquals = false;
            if (obj instanceof Xid) {
                XidImpl other = (XidImpl) obj;
                boolean a = (this.getFormatId() == other.getFormatId());
                boolean b = compareGlobalTransactionID(other);
                boolean c = compareBranchQualifier(other);
                isEquals = a & b & c;
            }
            return isEquals;
        }

        @Override
        public int hashCode() {
            //NO GARANTIZA DISTINTO HASH PARA DISTINTOS OBJETOS
            int a = formatId;
            return a;
        }

        private boolean compareGlobalTransactionID(XidImpl other) {
            byte[] b1 = this.getGlobalTransactionId();
            byte[] b2 = other.getGlobalTransactionId();
            return comparebyteArray(b1, b2);
        }

        private boolean compareBranchQualifier(XidImpl other) {
            byte[] b1 = this.getBranchQualifier();
            byte[] b2 = other.getBranchQualifier();
            return comparebyteArray(b1, b2);
        }

        private boolean comparebyteArray(byte[] b1, byte[] b2) {
            boolean isEquals = false;
            if (b1.length == b2.length) {
                boolean aux = true;
                int i = 0;
                while (aux && i < b1.length) {
                    if (b1[i] != b2[i]) {
                        aux = false;
                    }
                    i++;
                }
                isEquals = aux;
            }
            return isEquals;
        }
    }
}
