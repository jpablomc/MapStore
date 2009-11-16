/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.lucene;

import es.uc3m.it.mapstore.bean.MapStoreCondition;
import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.bean.MapStoreResult;
import es.uc3m.it.mapstore.db.transaction.xa.impl.ResourceManagerlImpl;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import java.io.File;
import java.sql.SQLException;
import java.util.Date;
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
public class LuceneResourceManagerWrapper extends ResourceManagerlImpl {
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
                conn.indexNew(id, item.getVersion(), property, o);
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

    @Override
    public void delete(MapStoreItem item, MapStoreItem old, Transaction t) {
        create(item, t);
    }

    @Override
    public Map<Integer,MapStoreResult> findByConditions(List<MapStoreCondition> cond, int flag, Date fecha) {
        LuceneConnection conn;
        try {
            conn = ds.getXAConnection().getConnection();
            return conn.findByConditions(cond, flag, fecha);
        } catch (SQLException ex) {
            Logger.getLogger(LuceneResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

    @Override
    public void update(MapStoreItem item, MapStoreItem old, Transaction t) {
        create(item, t);
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

    @Override
    public boolean canFindByNameType() {
        return false;
    }

    @Override
    public Integer findByNameType(String name, String type) {
        throw new UnsupportedOperationException("Resource does not support search by name/type");
    }

}
