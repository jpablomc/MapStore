/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.torrent;

import java.io.File;
import java.io.PrintWriter;
import java.sql.SQLException;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

/**
 *
 * @author Pablo
 */
public class TorrentXADataSource implements XADataSource{
    String path;
    TorrentXAConnection xaconn;

    public TorrentXADataSource(String path, String subDirTorrent, String subDirFiles, String host, int port) {
        checkPath(path);
        this.path = path;
        xaconn = new TorrentXAConnection(path, host, port);
    }

    @Override
    public TorrentXAConnection getXAConnection(){
        return xaconn;
    }

    @Override
    public XAConnection getXAConnection(String user, String password) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private void checkPath(String path) {
        File f = new File(path);
        if (!f.exists()) {
            checkPath(f.getParent());
            f.mkdir();
        }
    }

}
