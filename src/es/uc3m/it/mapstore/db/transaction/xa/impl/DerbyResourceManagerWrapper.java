/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl;

import es.uc3m.it.mapstore.bean.MapStoreCondition;
import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.config.MapStoreConfig;
import es.uc3m.it.mapstore.db.transaction.xa.*;
import es.uc3m.it.mapstore.db.dialect.SQLDialect;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
import org.apache.derby.jdbc.EmbeddedXADataSource40;

/**
 *
 * @author Pablo
 */
public class DerbyResourceManagerWrapper implements ResourceManagerWrapper {
    private EmbeddedXADataSource40 ds;
    private SQLDialect dialect;
    private String type;
    private String name;

    @Override
    public XAConnection getXAConnection() {
        try {
            return ds.getXAConnection();
        } catch (SQLException ex) {
            Logger.getLogger(DerbyResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
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
        Exception e= null;
        ds = new EmbeddedXADataSource40();
        ds.setDatabaseName("db/derby");
        ds.setCreateDatabase("create");
        name = prop.getProperty("name");
        type = prop.getProperty("type");
        Class<SQLDialect> clazz;
        try {
            clazz = (Class<SQLDialect>) Class.forName(prop.getProperty("prop.dialect"));
            try {
                dialect = clazz.newInstance();
            } catch (InstantiationException ex) {
                e = ex;
                Logger.getLogger(DerbyResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IllegalAccessException ex) {
                e = ex;
                Logger.getLogger(DerbyResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (ClassNotFoundException ex) {
            e = ex;
            Logger.getLogger(DerbyResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (e != null) throw new MapStoreRunTimeException(e);
        initDatabase();
    }

    private void initDatabase() {
        try {
            //Se hace fuera de la transacion XA... LA MANIPULACION DE TABLAS NO ES TRANSACCIONAL            
            Connection c = ds.getConnection();
            if (!isDatabaseCreated(c)){
                List<Object> statements = dialect.initializeDataBase();
                for (Object s : statements) {
                    String aux = (String) s;
                    PreparedStatement ps = c.prepareStatement(aux);
                    ps.execute();
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(DerbyResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException("Can not initialize database");
        }
    }

    private boolean isDatabaseCreated(Connection c) {
        List<String> tablas = new ArrayList();
        try {
            ResultSet rs = c.getMetaData().getTables(null, null, null, new String[]{"TABLE"});
            while (rs.next()) {
                tablas.add(rs.getString("TABLE_NAME"));
            }
        } catch (SQLException ex) {
            Logger.getLogger(DerbyResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
        return dialect.isCreated(tablas);
    }

    @Override
    public void create(MapStoreItem item, Transaction t) {
        Exception e=null;
        try {
            boolean result;
            long id = item.getId();
            List<String> props = getPropertiesToProcess(item);
            XAConnection connXA = ds.getXAConnection();
            XAResource r = connXA.getXAResource();
            Connection conn =  connXA.getConnection();
            result = t.enlistResource(r);
            if (!result) throw new MapStoreRunTimeException("Can not enlist resource in transaction");
            for (String property : props) {
                Object value = item.getProperty(property);
                String sql = dialect.create(id, property, value);
                PreparedStatement ps;
                ps = conn.prepareStatement(sql);
                ps.executeUpdate();
            }
            result = t.delistResource(r, XAResource.TMSUCCESS);
            if (!result) throw new MapStoreRunTimeException("Can not delist resource in transaction");
        } catch (RollbackException ex) {
            e=ex;
            Logger.getLogger(DerbyResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IllegalStateException ex) {
            e=ex;
            Logger.getLogger(DerbyResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SystemException ex) {
            e=ex;
            Logger.getLogger(DerbyResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            e=ex;
            Logger.getLogger(DerbyResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
        }
        if (e != null) throw new MapStoreRunTimeException(e);
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

    private boolean processable(String prop) {
        return true;
    }

    @Override
    public void getAll() {
        try {
            Connection c = ds.getConnection();
            List<Object> statements = dialect.getAll();
            for (Object s : statements) {
                String aux = (String) s;
                PreparedStatement ps = c.prepareStatement(aux);
                ResultSet rs = ps.executeQuery();
                //Impriminos query
                System.out.println(aux);
                //Imprimimos cabecera
                int columns = rs.getMetaData().getColumnCount();
                StringBuffer sb = new StringBuffer();
                for (int i = 1; i <= columns; i++) {
                    sb.append(rs.getMetaData().getColumnName(i)).append("\t");
                }
                System.out.println(sb.toString());
                while (rs.next()) {
                    sb = new StringBuffer();
                    for (int i = 1; i <= columns; i++) {
                        sb.append(rs.getObject(i).toString()).append("\t");
                    }
                    System.out.println(sb.toString());
                }
            }
        } catch (SQLException ex) {
            Logger.getLogger(DerbyResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
}
