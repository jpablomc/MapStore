/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.torrent;

import java.sql.SQLException;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;

/**
 *
 * @author Pablo
 */
public class TorrentXAConnection implements XAConnection{
    private TorrentXAResource resource;
    private String path;
    private String host;
    private int port;

    public TorrentXAConnection(String path, String host, int port) {
        resource = new TorrentXAResource(path,host, port);
        this.path = path;
        this.path = path;
        this.host = host;
        this.port = port;
    }

    @Override
    public TorrentXAResource getXAResource() throws SQLException {
        return resource;
    }

    @Override
    public TorrentConnection getConnection() throws SQLException {
        TorrentConnection conn = new TorrentConnection(path, host, port);
        resource.addConnection(conn);
        return conn;
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
