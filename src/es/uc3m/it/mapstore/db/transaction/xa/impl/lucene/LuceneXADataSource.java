/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.lucene;

import java.io.PrintWriter;
import java.sql.SQLException;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

/**
 *
 * @author Pablo
 */
public class LuceneXADataSource implements XADataSource{
    private LuceneXAConnection xaconn;

    public LuceneXADataSource(String path) {
        this.xaconn = new LuceneXAConnection(path);
    }

    @Override
    public LuceneXAConnection getXAConnection() throws SQLException {
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

}
