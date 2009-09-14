/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.disk;

import es.uc3m.it.mapstore.bean.MapStoreItem;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Pablo
 */
public class DiskConnection implements Connection {
    private String path;
    private Map<File,ByteArrayOutputStream> data;
    private List<File> dataDelete;
    private boolean autocommit;

    public DiskConnection(String path) {
        this.path = path;
        data = new HashMap<File,ByteArrayOutputStream>();
        dataDelete = new ArrayList<File>();
        autocommit = false;
    }

    @Override
    public Statement createStatement() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        autocommit = true;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return autocommit;
    }

    @Override
    public void commit() throws SQLException {
        for (File f : data.keySet()) {
            try {
                ByteArrayOutputStream bos = data.get(f);
                bos.writeTo(new FileOutputStream(f));
            } catch (FileNotFoundException ex) {
                Logger.getLogger(DiskConnection.class.getName()).log(Level.SEVERE, null, ex);
                throw new SQLException(ex);
            } catch (IOException ex) {
                Logger.getLogger(DiskConnection.class.getName()).log(Level.SEVERE, null, ex);
                throw new SQLException(ex);
            }
        }
        for (File f : dataDelete) {
            f.delete();
        }
    }

    @Override
    public void rollback() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void close() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getCatalog() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }


    public void storeNew(MapStoreItem i) throws SQLException, IOException {
        long id = i.getId();
        File f = getLastVersion(id);
        if (f != null) throw new SQLException("Item already exist");
        MapStoreItem toRecord = eliminateNonPrimitive(i);
        serialize(getInitialVersion(id), toRecord);
    }

    public void store(MapStoreItem i) throws SQLException, IOException {
        long id = i.getId();
        File f = getNextVersion(id);
        if (f == null) throw new SQLException("Item can not be updated: Item does not exist");
        MapStoreItem toRecord = eliminateNonPrimitive(i);
        serialize(f, toRecord);
    }

    public void delete(long id) throws SQLException {
        List<File> files = getAllVersions(id);
        if (files.isEmpty()) throw new SQLException("Item can not be deleted: Item does not exist");
        for (File aux : files) {
            if (!aux.canWrite()) throw new SQLException("Item can not be deleted: User has no rights to delete");
        }
        if (getAutoCommit()) {
            for (File aux: files) {
                boolean result = aux.delete();
                if (!result) throw new SQLException("Can not delete item: File can not be deleted");
            }
        } else {
            dataDelete.addAll(files);
        }
    }

    public List<MapStoreItem> getById(List<Long> ids) throws SQLException {
        List<MapStoreItem> items = new ArrayList<MapStoreItem>();
        for (Long id : ids) {
            try {
                File f = getLastVersion(id);
                MapStoreItem item = deserialize(f);
                items.add(item);
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(DiskConnection.class.getName()).log(Level.SEVERE, null, ex);
                throw new SQLException(ex);
            } catch (FileNotFoundException ex) {
                Logger.getLogger(DiskConnection.class.getName()).log(Level.SEVERE, null, ex);
                throw new SQLException(ex);
            } catch (IOException ex) {
                Logger.getLogger(DiskConnection.class.getName()).log(Level.SEVERE, null, ex);
                throw new SQLException(ex);
            }
        }
        return items;
    }

    private static final Class[] SUPPORTED_TYPES = new Class[]{Float.class,
        Double.class,Integer.class,Long.class,Byte.class,Character.class,
        Date.class,String.class};

    private MapStoreItem deserialize(File f) throws ClassNotFoundException, FileNotFoundException, IOException {
        ObjectInputStream in = null;
        MapStoreItem item = null;
        try {
            in = new ObjectInputStream(new FileInputStream(f));
            // Deserialize the object
            item = (MapStoreItem) in.readObject();
            in.close();
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(DiskConnection.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        } catch (FileNotFoundException ex) {
            Logger.getLogger(DiskConnection.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        } catch (IOException ex) {
            Logger.getLogger(DiskConnection.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        } finally {
            try {
                in.close();
            } catch (IOException ex) {
                Logger.getLogger(DiskConnection.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return item;
    }

    private MapStoreItem eliminateNonPrimitive(MapStoreItem item) {
        List<Class> types = Arrays.asList(SUPPORTED_TYPES);
        MapStoreItem toRecord = new MapStoreItem();
        for (String prop : item.getProperties().keySet()) {
            Object value = item.getProperty(prop);
            if (value != null) {
                if (types.contains(value.getClass())) {
                    toRecord.setProperty(prop, value);
                }
            }
        }
        return toRecord;
    }

    private void serialize(File f, MapStoreItem toRecord) throws SQLException, IOException {
        if (getAutoCommit()) {
            ObjectOutput out = new ObjectOutputStream(new FileOutputStream(f));
            out.writeObject(toRecord);
            out.close();
        } else {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos);
            out.writeObject(toRecord);
            out.close();
            data.put(f, bos);
        }
    }

    private List<File> getAllVersions(long id) {
        File f = new File(getPath(id));
        return Arrays.asList(f.listFiles());
    }

    private File getInitialVersion(long id) {
        return new File(getPath(id) + System.getProperty("file.separator") +
                "0");
    }

    private File getLastVersion(long id) {
        List<File> files = getAllVersions(id);
        File aux = null;
        long vMax = Long.MIN_VALUE;
        for (File f : files) {
            String version = f.getName();
            long v = Long.valueOf(version);
            if (aux == null || v>vMax) {
                aux = f;
                vMax = v;
            }
        }
        return aux;
    }

    private File getNextVersion(long id) {
        List<File> files = getAllVersions(id);
        long vMax = Long.MIN_VALUE;
        for (File f : files) {
            String version = f.getName();
            long v = Long.valueOf(version);
            if (v>vMax) {
                vMax = v;
            }
        }
        File aux = null;
        if (vMax>Long.MIN_VALUE) aux = new File(getPath(id)+
                System.getProperty("file.separator") + (vMax+1));
        return aux;
    }

    private String getPath(long id) {
        String sep = System.getProperty("file.separator");
        StringBuffer sb = new StringBuffer(Long.toHexString(id));
        StringBuffer zeros = new StringBuffer();
        while (zeros.length()+sb.length()<16) zeros.append("0");
        String aux = zeros.toString() + sb.toString();
        return path + sep + aux.substring(0, 1) + sep + aux.substring(2, 3)
                + sep + aux.substring(4, 5) + sep + aux.substring(6, 7)
                + sep + aux.substring(8, 9) + sep + aux.substring(10, 11)
                + sep + aux.substring(12, 13) + sep + aux.substring(14, 15);
    }
}
