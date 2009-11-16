/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl;

import es.uc3m.it.mapstore.bean.MapStoreCondition;
import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.bean.MapStoreResult;
import es.uc3m.it.mapstore.db.dialect.SQLDialect;
import es.uc3m.it.mapstore.db.impl.MapStoreSession;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
import org.apache.derby.jdbc.EmbeddedXADataSource40;

/**
 *
 * @author Pablo
 */
public class DerbyResourceManagerWrapper extends ResourceManagerlImpl{
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
            long version = item.getVersion();
            List<String> props = getPropertiesToProcess(item);
            XAConnection connXA = ds.getXAConnection();
            XAResource r = connXA.getXAResource();
            Connection conn =  connXA.getConnection();
            result = t.enlistResource(r);
            if (!result) throw new MapStoreRunTimeException("Can not enlist resource in transaction");
            for (String property : props) {
                Object value = item.getProperty(property);
                String sql = dialect.create(id, version, property, value);
                PreparedStatement ps;
                ps = conn.prepareStatement(sql);
                ps.executeUpdate();
            }
            if (item.getName() != null) {
                String sql = dialect.insertTypeName(id, item.getType(), item.getName());
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

    @Override
    public void delete(MapStoreItem item, MapStoreItem old, Transaction t) {
        update(item, old, t);
    }

    @Override
    public Map<Integer,MapStoreResult> findByConditions(List<MapStoreCondition> cond, int flag, Date fecha) {
        Map<Integer,MapStoreResult> results = null;
        for (MapStoreCondition c : cond) {
            String sql = dialect.getQueryForCondition(c);
            Map<Integer, MapStoreResult> aux;
            try {
                 aux = findByQuery(sql);
            } catch (SQLException ex) {
                Logger.getLogger(DerbyResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
                throw new MapStoreRunTimeException(ex);
            }
            if (results == null) results = aux;
            else {
                switch (flag) {
                    case MapStoreSession.CONJUNCTIVE_SEARCH:
                        //Primero borramos los documentos no comunes
                        //acumulados va a tener los elementos a mantener
                        Set<Integer> acumulados = results.keySet();
                        //acumulados2 va a tener los elementos a eliminar
                        Set<Integer> acumulados2 = results.keySet();
                        Set<Integer> newResults = aux.keySet();
                        acumulados.retainAll(newResults);
                        acumulados2.removeAll(acumulados);
                        for (Integer toDelete : acumulados2) {
                            results.remove(toDelete);
                        }
                        //En el mapa ahora solo quedan los documentos comunes... debemos tratar las versiones
                        for (Integer id : acumulados) {
                            MapStoreResult oldResults = results.get(id);
                            MapStoreResult searchResults = aux.get(id);
                            oldResults.getVersions().retainAll(searchResults.getVersions());
                        }
                        break;
                    case MapStoreSession.DISJUNCTIVE_SEARCH:
                        //Añadimos todos los elementos
                        for (Integer id : aux.keySet()) {
                            MapStoreResult oldResults = results.get(id);
                            MapStoreResult searchResults = aux.get(id);
                            if (oldResults == null) results.put(id,searchResults);
                            else oldResults.getVersions().addAll(searchResults.getVersions());
                        }
                        break;
                }
            }
        }
        return results;
    }

    private Map<Integer,MapStoreResult> findByQuery(String query) throws SQLException {
        Map<Integer,MapStoreResult> results = new HashMap<Integer, MapStoreResult>();
        Connection c = ds.getConnection();
        PreparedStatement ps = c.prepareStatement(query);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            int id = rs.getInt(1);
            int version = rs.getInt(2);
            MapStoreResult msr = results.get(id);
            if (msr == null) {
                msr = new MapStoreResult(id);
                results.put(id, msr);
            }
            msr.addVersion(version);
        }
        return results;
    }

    @Override
    public void update(MapStoreItem item, MapStoreItem old, Transaction t) {
        Exception e=null;
        try {
            boolean result;
            long id = item.getId();
            long version = item.getVersion();
            List<String> props = getPropertiesToProcess(item);
            XAConnection connXA = ds.getXAConnection();
            XAResource r = connXA.getXAResource();
            Connection conn =  connXA.getConnection();
            result = t.enlistResource(r);
            if (!result) throw new MapStoreRunTimeException("Can not enlist resource in transaction");
            for (String property : props) {
                Object value = item.getProperty(property);
                String sql = dialect.create(id, version, property, value);
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

    @Override
    public boolean canFindByNameType() {
        return true;
    }

    @Override
    public Integer findByNameType(String name, String type) {
        try {
            Connection c = ds.getConnection();
            String statement = dialect.getByTypeName(type, name);
            PreparedStatement ps = c.prepareStatement(statement);
            ResultSet rs = ps.executeQuery();
            if (rs.getMetaData().getColumnCount() != 1) {
                throw new MapStoreRunTimeException("Invalid results for find by type/name. Can not determine column with ID");
            }
            boolean exist = rs.next();
            Integer id = null;
            if (exist) {
                id = rs.getInt(1);
                if (rs.next()) throw new MapStoreRunTimeException("Invalid results for find by type/name. Multiple results found");
            }
            return id;
        } catch (SQLException ex) {
            Logger.getLogger(DerbyResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }
   
}
