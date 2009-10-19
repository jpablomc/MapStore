/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.disk;

import java.io.File;
import java.io.PrintWriter;
import java.sql.SQLException;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

/**
 *
 * @author Pablo
 */
public class DiskXADataSource implements XADataSource{
    String path;
    DiskXAConnection xaconn;

    public DiskXADataSource(String path) {
        checkPath(path);
        this.path = path;
        xaconn = new DiskXAConnection(path);
    }

    @Override
    public DiskXAConnection getXAConnection(){
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
