/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.neo;

import es.uc3m.it.mapstore.exception.MapStoreException;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import org.neo4j.impl.transaction.xaframework.XaConnection;

/**
 *
 * @author Pablo
 */
public class NeoXAConnectionWrapper implements XaConnection{
    private XAConnection xac;

    public NeoXAConnectionWrapper(XAConnection xac) {
        this.xac = xac;
    }

    @Override
    public XAResource getXaResource() {
        try {
            return xac.getXAResource();
        } catch (SQLException ex) {
            Logger.getLogger(NeoXAConnectionWrapper.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

    @Override
    public void destroy() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
