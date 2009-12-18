/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.lucene;

import es.uc3m.it.mapstore.bean.MapStoreBasicCondition;
import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.bean.MapStoreResult;
import es.uc3m.it.mapstore.db.transaction.xa.impl.ResourceManagerlImpl;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
        if (directory == null) {
            directory = (new File("")).getAbsolutePath() +
                    System.getProperty("file.separator") + "db" +
                    System.getProperty("file.separator") + "lucene";
        }
        ds = new LuceneXADataSource(directory);
        initDatabase();
    }

    @Override
    public void create(MapStoreItem item, Transaction t) {
        if (item.isCollection() ||item.isArray()) createCollection(item, t);
        else createObject(item, t);
    }

    @Override
    public void delete(MapStoreItem item, MapStoreItem old, Transaction t) {
        create(item, t);
    }

    @Override
    public MapStoreResult findByConditions(List<MapStoreBasicCondition> cond, int flag, Date fecha) {
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

    private void createCollection(MapStoreItem item, Transaction t) {
        //El objeto tendra en name el nombre de la propiedad
        List<String> props = getPropertiesToProcess(item);
        String prefix = item.getPrefix();
        Map<Integer,Object> map = new HashMap<Integer, Object>();
        for (String property: item.getProperties().keySet()) {
            //Si es procesable y es de la lista
            if (property.startsWith(prefix) && props.contains(property)) {
                String order = property.substring(prefix.length());
                int index = Integer.valueOf(order);
                map.put(index, item.getProperty(property));
                props.remove(property);
            }
        }
        //Aqui tendremos dos listas. El objeto map que contiene la lista y  props que contiene el resto de datos a agregar al documento

        LuceneXAResource res = null;
        boolean enlisted = false;
        long id = item.getId();
        try {
            LuceneXAConnection xaconn = ds.getXAConnection();
            res = xaconn.getXAResource();
            enlisted = t.enlistResource(res);
            LuceneConnection conn = xaconn.getConnection();
            //Procesamos los datos que no son de lista
            for (String property : props) {
                Object o = item.getProperty(property);
                conn.indexNew(id, item.getVersion(), property, o);
            }
            //Procesamos la lista
            if (!map.isEmpty()) {
                //El nombre del campo esta en la propiedad name del MapStoreItem en formato "id|propiedad"
                String[] nombre = item.getName().split("\\|");
                //Se crea un wrapper para la colección ya que por defecto el objeto devuelto puede no ser serializable (de hecho no lo es)
                ArrayList aux = new ArrayList(map.values());
                conn.indexNew(id, item.getVersion(), nombre[1], aux);
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
            if (enlisted) {
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

    private void createObject(MapStoreItem item, Transaction t) throws MapStoreRunTimeException {
        LuceneXAResource res = null;
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
            if (enlisted) {
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

    private void initDatabase() {
        try {
            ds.getXAConnection().getConnection().commit();
        } catch (SQLException ex) {
            Logger.getLogger(LuceneResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException("Error initializing Lucene indexes");
        }
    }

    @Override
    public void shutdown() {
        //Empty
    }

    @Override
    public Set<Integer> findByType(String type) {
        throw new UnsupportedOperationException("Operation is not supported");
    }
}
