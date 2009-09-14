/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.neo;

import es.uc3m.it.mapstore.db.transaction.xa.ResourceManagerWrapper;
import java.util.Map;
import org.neo4j.impl.transaction.xaframework.XaConnection;
import org.neo4j.impl.transaction.xaframework.XaDataSource;

/**
 *
 * @author Pablo
 */
public class NeoXAResourceWrapper extends XaDataSource {
    public static final String WRAPPED_OBJECT ="WRAPPED";
    private ResourceManagerWrapper ds;

    @Override
    public XaConnection getXaConnection() {
        return new NeoXAConnectionWrapper(ds.getXAConnection());
    }

    @Override
    public void close() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public NeoXAResourceWrapper( Map<?,?> params ) throws InstantiationException
    {
        super(params);
        ds = (ResourceManagerWrapper) params.get(WRAPPED_OBJECT);
    }
}
