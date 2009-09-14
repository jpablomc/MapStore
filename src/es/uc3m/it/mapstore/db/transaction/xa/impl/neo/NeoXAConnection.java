/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.neo;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import org.neo4j.impl.transaction.xaframework.XaConnection;

/**
 *
 * @author Pablo
 */
public class NeoXAConnection implements XAConnection {
    XaConnection neoXAC;

    public NeoXAConnection(XaConnection neoXAC) {
        this.neoXAC = neoXAC;
    }
    @Override
    public XAResource getXAResource() throws SQLException {
        return neoXAC.getXaResource();
    }

    @Override
    public Connection getConnection() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void close() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addConnectionEventListener(ConnectionEventListener listener) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void removeConnectionEventListener(ConnectionEventListener listener) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void addStatementEventListener(StatementEventListener listener) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void removeStatementEventListener(StatementEventListener listener) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    
}
