/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.lucene;

import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.LockObtainFailedException;

/**
 *
 * @author Pablo
 */
public class LuceneXAConnection implements XAConnection{
    private LuceneXAResource resource;
    private String path;

    public LuceneXAConnection(String path) {
        resource = new LuceneXAResource();
        this.path = path;
    }

    @Override
    public LuceneXAResource getXAResource() throws SQLException {
        return resource;
    }

    @Override
    public LuceneConnection getConnection() throws SQLException {
        try {
            LuceneConnection conn = new LuceneConnection(path);
            resource.addConnection(conn);
            return conn;
        } catch (CorruptIndexException ex) {
            Logger.getLogger(LuceneXAConnection.class.getName()).log(Level.SEVERE, null, ex);
            throw new SQLException(ex);
        } catch (LockObtainFailedException ex) {
            Logger.getLogger(LuceneXAConnection.class.getName()).log(Level.SEVERE, null, ex);
            throw new SQLException(ex);
        } catch (IOException ex) {
            Logger.getLogger(LuceneXAConnection.class.getName()).log(Level.SEVERE, null, ex);
            throw new SQLException(ex);
        }
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
