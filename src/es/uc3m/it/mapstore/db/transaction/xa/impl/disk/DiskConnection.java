/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.disk;

import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.db.transaction.xa.impl.AbstractConnection;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.transaction.xa.XAResource;

/**
 *
 * @author Pablo
 */
public class DiskConnection extends AbstractConnection {
    private String path;
    private Map<File,ByteArrayOutputStream> data;
    private List<File> dataDelete;
    private Map<Long,Set<Long>> locked;
    private boolean autocommit;
    List<DiskOperation> operations;
    private boolean closed;

    public DiskConnection(String path) {
        this.path = path;
        data = new HashMap<File,ByteArrayOutputStream>();
        dataDelete = new ArrayList<File>();
        autocommit = false;
        locked = new HashMap<Long,Set<Long>>();
        operations = new ArrayList<DiskOperation>();
        closed = false;
    }

    public int prepare() throws SQLException {
        for (File f : data.keySet()) {
            if (f.exists()) throw new SQLException("Error while writing to disk");
        }
        for (File f : dataDelete) {
            if (f.exists()) throw new SQLException("Error while writing to disk");
        }
        return (data.keySet().size()+dataDelete.size()>0)?XAResource.XA_OK:XAResource.XA_RDONLY;
    }

    @Override
    public void commit() throws SQLException {
        for (File f : data.keySet()) {
            try {
                checkPath(f.getParent());
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
        for (Long id: locked.keySet()) {
            for (Long version : locked.get(id)) {
                DiskLock.releaseLock(id,version);
            }
        }
    }

    private void acquireLock(long id,long version) throws SQLException {
        DiskLock.acquireLock(id,version);
        Set<Long> versions = locked.get(id);
        if (versions == null) {
            versions = new HashSet<Long>();
            locked.put(id, versions);
        }
        versions.add(version);
    }

    public void storeNew(MapStoreItem i) throws SQLException, IOException {
        long id = i.getId();
        long version = i.getVersion();
        acquireLock(id,version);
        String file = getPath(id) + System.getProperty("file.separator") + version;
        File f = new File(file);
        if (f.exists()) throw new SQLException("Item already exist");
        MapStoreItem toRecord = eliminateNonPrimitive(i);
        serialize(f, toRecord);
        operations.add(new DiskOperation(DiskOperation.CREATE, new Object[]{toRecord}));
    }

    public void store(MapStoreItem i) throws SQLException, IOException {
        long id = i.getId();
        long version = i.getVersion();
        acquireLock(id,version);
        String file = getPath(id) + System.getProperty("file.separator") + version;
        File f = new File(file);
        if (f.exists()) throw new SQLException("Item already exist");
        MapStoreItem toRecord = eliminateNonPrimitive(i);
        serialize(f, toRecord);
        operations.add(new DiskOperation(DiskOperation.UPDATE, new Object[]{toRecord}));
    }

    public void delete(MapStoreItem i) throws SQLException, IOException {
        long id = i.getId();
        long version = i.getVersion();
        acquireLock(id,version);
        String file = getPath(id) + System.getProperty("file.separator") + version;
        File f = new File(file);
        if (f.exists()) throw new SQLException("Item already exist");
        MapStoreItem toRecord = eliminateNonPrimitive(i);
        serialize(f, toRecord);
        operations.add(new DiskOperation(DiskOperation.DELETE, new Object[]{toRecord}));
    }

    public List<MapStoreItem> getById(Set<Integer> ids) throws SQLException {
        List<MapStoreItem> items = new ArrayList<MapStoreItem>();
        if (ids != null) {
            for (Integer id : ids) {
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

//    private File getInitialVersion(long id) {
//        return new File(getPath(id) + System.getProperty("file.separator") +
//                "0");
//    }

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
/*
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
*/
    private String getPath(long id) {
        String sep = System.getProperty("file.separator");
        StringBuffer sb = new StringBuffer(Long.toHexString(id));
        StringBuffer zeros = new StringBuffer();
        while (zeros.length()+sb.length()<16) zeros.append("0");
        String aux = zeros.toString() + sb.toString();
        return path + sep + aux.substring(0, 2) + sep + aux.substring(2, 4)
                + sep + aux.substring(4, 6) + sep + aux.substring(6, 8)
                + sep + aux.substring(8, 10) + sep + aux.substring(10, 12)
                + sep + aux.substring(12, 14) + sep + aux.substring(14, 16);
    }

    public List<DiskOperation> getOperations() {
        return operations;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return autocommit;
    }

    private void checkPath(String path) {
        File f = new File(path);
        if (!f.exists()) {
            checkPath(f.getParent());
            f.mkdir();
        }
    }

    MapStoreItem getByIdVersion(int id, int version) throws SQLException{
        try {
            String file = getPath(id) + System.getProperty("file.separator") + version;
            return deserialize(new File(file));
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(DiskConnection.class.getName()).log(Level.SEVERE, null, ex);
            throw new SQLException("Can not deserialize file");
        } catch (FileNotFoundException ex) {
            Logger.getLogger(DiskConnection.class.getName()).log(Level.SEVERE, null, ex);
            throw new SQLException("Item does not exist");
        } catch (IOException ex) {
            Logger.getLogger(DiskConnection.class.getName()).log(Level.SEVERE, null, ex);
            throw new SQLException("Can not read data");
        }
    }

    @Override
    public void close() throws SQLException {
        closed = true;
    }


}
