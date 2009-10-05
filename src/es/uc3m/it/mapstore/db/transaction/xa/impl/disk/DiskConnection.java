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
    private Set<Long> locked;
    private boolean autocommit;
    List<DiskOperation> operations;

    public DiskConnection(String path) {
        this.path = path;
        data = new HashMap<File,ByteArrayOutputStream>();
        dataDelete = new ArrayList<File>();
        autocommit = false;
        locked = new HashSet<Long>();
        operations = new ArrayList<DiskOperation>();
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
        for (Long l: locked) {
            DiskLock.releaseLock(l);
        }
    }

    private void acquireLock(Long id) throws SQLException {
        DiskLock.acquireLock(id);
        locked.add(id);
    }

    public void storeNew(MapStoreItem i) throws SQLException, IOException {
        long id = i.getId();
        acquireLock(id);
        File f = getLastVersion(id);
        if (f != null) throw new SQLException("Item already exist");
        MapStoreItem toRecord = eliminateNonPrimitive(i);
        serialize(getInitialVersion(id), toRecord);
        operations.add(new DiskOperation(DiskOperation.CREATE, new Object[]{i}));
    }

    public void store(MapStoreItem i) throws SQLException, IOException {
        long id = i.getId();
        acquireLock(id);
        File f = getNextVersion(id);
        if (f == null) throw new SQLException("Item can not be updated: Item does not exist");
        MapStoreItem toRecord = eliminateNonPrimitive(i);
        serialize(f, toRecord);
        operations.add(new DiskOperation(DiskOperation.UPDATE, new Object[]{i}));
    }

    public void delete(long id) throws SQLException {
        acquireLock(id);
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
        operations.add(new DiskOperation(DiskOperation.DELETE, new Object[]{id}));
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

    public List<DiskOperation> getOperations() {
        return operations;
    }
}
