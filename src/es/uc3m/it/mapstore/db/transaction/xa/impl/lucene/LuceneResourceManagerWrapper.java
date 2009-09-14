/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.lucene;

import es.uc3m.it.mapstore.bean.MapStoreCondition;
import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.config.MapStoreConfig;
import es.uc3m.it.mapstore.db.transaction.xa.*;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.XAConnection;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Fieldable;

/**
 *
 * @author Pablo
 */
public class LuceneResourceManagerWrapper implements ResourceManagerWrapper {
    private String type;
    private String name;
    private LuceneXADataSource ds;

    @Override
    public XAConnection getXAConnection() {
        try {
            return ds.getXAConnection();
        } catch (SQLException ex) {
            throw new MapStoreRunTimeException(ex);
        }
    }

    @Override
    public String getType() throws MapStoreRunTimeException {
        return type;
    }

    @Override
    public String getName() throws MapStoreRunTimeException {
        return name;
    }

    @Override
    public void start(Properties prop) throws MapStoreRunTimeException {
        name = prop.getProperty("name");
        type = prop.getProperty("type");
        String directory = prop.getProperty("directory");
        if (directory == null) directory = (new File("")).getAbsolutePath() +
                System.getProperty("file.separator") + "db" +
                System.getProperty("file.separator") + "lucene";
        ds = new LuceneXADataSource(directory);
    }

    @Override
    public void create(MapStoreItem item, Transaction t) {
        LuceneXAResource res=null;
        boolean enlisted = false;
        List<String> props = getPropertiesToProcess(item);
        long id = item.getId();
        try {
            LuceneXAConnection xaconn = ds.getXAConnection();
            res = xaconn.getXAResource();
            enlisted = t.enlistResource(res);
            LuceneConnection conn = xaconn.getConnection();
            for (String property : props) {
                Object o = item.getProperty(property);
                conn.indexNew(id, property, o);
            }
            enlisted = !(t.delistResource(res, XAResource.TMSUCCESS));          
        } catch (RollbackException ex) {
            Logger.getLogger(LuceneResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (IllegalStateException ex) {
            Logger.getLogger(LuceneResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (SystemException ex) {
            Logger.getLogger(LuceneResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (SQLException ex) {
            Logger.getLogger(LuceneResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } finally {
            if(enlisted) {
                try {
                    t.delistResource(res, XAResource.TMFAIL);
                } catch (IllegalStateException ex) {
                    Logger.getLogger(LuceneResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
                    throw new MapStoreRunTimeException(ex);
                } catch (SystemException ex) {
                    Logger.getLogger(LuceneResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
                    throw new MapStoreRunTimeException(ex);
                }
            }
        }
    }

    private List<String> getPropertiesToProcess(MapStoreItem item) {
        List<String> props = new ArrayList<String>();
        Map<Class,List<ResourceManagerWrapper>> mapa = new HashMap<Class,List<ResourceManagerWrapper>>();
        for (String prop :item.getProperties().keySet()) {
            if (processable(prop)) {
                Object value = item.getProperty(prop);
                List<ResourceManagerWrapper> lista = mapa.get(value.getClass());
                if (lista == null) {
                    lista = MapStoreConfig.getInstance().getXaResourceLookupForClass(value.getClass());
                    mapa.put(value.getClass(),lista);
                }
                if (lista.contains(this)) props.add(prop);
            }
        }
        return props;
    }

    private boolean processable(String prop) {
        return true;
    }


    @Override
    public void delete(long id, Transaction t) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<Long> findByConditions(List<MapStoreCondition> cond) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void update(MapStoreItem item, Transaction t) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void getAll() {
        try {
            LuceneConnection conn = ds.getXAConnection().getConnection();
            List<Document> docs = conn.getAll();
            for (Document d : docs) {
                StringBuffer sb = new StringBuffer();
                List<Fieldable> l = d.getFields();
                for (Fieldable f : l) {
                    sb.append(f.name()).append(": ").append(f.stringValue()).append("\t");
                }
                System.out.println(sb.toString());
            }
        } catch (SQLException ex) {
            Logger.getLogger(LuceneResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }



}
