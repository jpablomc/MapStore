/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.lucene;

import es.uc3m.it.mapstore.db.transaction.xa.impl.AbstractXAResource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import org.neo4j.impl.transaction.xaframework.XaResource;

/**
 *
 * @author Pablo
 */
public class LuceneXAResource extends AbstractXAResource {
    Map<Xid,Set<LuceneConnection>> connections;

    public LuceneXAResource() {
        connections = new HashMap<Xid, Set<LuceneConnection>>();
    }

    public void addConnection(LuceneConnection conn) {
        Xid xid = getXidCurrentThread();
        if (xid != null) {
            Set<LuceneConnection> conns = connections.get(xid);
            if (conns == null) {
                conns = new HashSet<LuceneConnection>();
                connections.put(xid, conns);
            }
            conns.add(conn);
        }
    }

    @Override
    protected int doPrepare(Xid arg0) throws XAException {
        boolean modifies = false;
        Set<LuceneConnection> conns = connections.get(arg0);
        if (conns != null) {
            for (LuceneConnection conn :conns) {
                try {
                    int i = conn.prepare();
                    switch (i) {
                        case XaResource.XA_OK:
                            modifies = true;
                    }
                } catch (SQLException ex) {
                    Logger.getLogger(LuceneXAResource.class.getName()).log(Level.SEVERE, null, ex);
                    throw new XAException(XAException.XA_RBROLLBACK);
                }
            }
        }
        return (modifies)?XaResource.XA_OK:XaResource.XA_RDONLY;
    }

    @Override
    protected void doCommit(Xid arg0) throws XAException {
        Set<LuceneConnection> conns = connections.get(arg0);
        for (LuceneConnection conn :conns) {
            try {
                conn.commit();
            } catch (SQLException ex) {
                Logger.getLogger(LuceneXAResource.class.getName()).log(Level.SEVERE, null, ex);
                throw new XAException(XAException.XA_RBROLLBACK);
            }
        }
    }

    @Override
    protected void doRollback(Xid arg0) throws XAException {
        Set<LuceneConnection> conns = connections.get(arg0);
        for (LuceneConnection conn :conns) {
            try {
                conn.rollback();
            } catch (SQLException ex) {
                Logger.getLogger(LuceneXAResource.class.getName()).log(Level.SEVERE, null, ex);
                throw new XAException(XAException.XA_RBROLLBACK);
            }
        }
    }
}
