/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package es.uc3m.it.mapstore.db.transaction.xa.impl.torrent;

import es.uc3m.it.mapstore.db.transaction.xa.impl.AbstractConnection;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.CRC32;
import javax.transaction.xa.XAResource;
import org.gudy.azureus2.plugins.PluginManager;

/**
 *
 * @author Pablo
 */
public class TorrentConnection extends AbstractConnection {

    private String path;
    private int port;
    private String host;
    private List<TorrentOperation> operations;
    private List<TorrentOperation> operationsCommited;

    public TorrentConnection(String path, String host, int port) {
        this.path = path;
        this.host = host;
        this.port = port;
        operations = new ArrayList<TorrentOperation>();
    }

    public void addTorrent(byte[] data, int id, int version, String property) {
        byte[] old = getLastTorrentOriginal(id, version, property);
        if (old != null && old.length == data.length) {
            CRC32 oldCRC = new CRC32();
            CRC32 newCRC = new CRC32();
            oldCRC.update(old);
            newCRC.update(data);
            if (oldCRC.getValue() == newCRC.getValue()) {
                return;
            }
        }
        //Hemos determinado que el torrent nuevo es distinto del ultimo almacenado
        String filePath = getPath(id, version, property, false);
        String orig = filePath + "_orig.torrent";
        String modified = filePath + ".torrent";
        byte[] dataModified = modifyTorrent(data);
        operations.add(new TorrentOperation(TorrentOperation.ADD_TORRENT_ORIGINAL, new Object[]{orig, data}));
        operations.add(new TorrentOperation(TorrentOperation.ADD_TORRENT_MODIFIED, new Object[]{modified, dataModified}));
    }

    public void addFile(byte[] data, int id, int version, String property) {
        byte[] old = getLastTorrentOriginal(id, version, property);
        if (old != null && old.length == data.length) {
            CRC32 oldCRC = new CRC32();
            CRC32 newCRC = new CRC32();
            oldCRC.update(old);
            newCRC.update(data);
            if (oldCRC.getValue() == newCRC.getValue()) {
                return;
            }
        }
        //Hemos determinado que el archivo nuevo es distinto del ultimo almacenado
        String filePath = getPath(id, version, property, false);
        operations.add(new TorrentOperation(TorrentOperation.ADD_FILE, new Object[]{filePath, data}));
    }

    private byte[] getLastFile(int id, int version, String property) {
        int aux = version;
        while (aux >= 0) {
            try {
                String fileStr = getPath(id, aux, property, true) + ".dat";
                File f = new File(fileStr);
                if (f.exists()) {
                    return readFile(f);
                }
            } catch (FileNotFoundException ex) {
                Logger.getLogger(TorrentConnection.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(TorrentConnection.class.getName()).log(Level.SEVERE, null, ex);
            }
            aux--;
        }
        return null;
    }

    private byte[] getLastTorrentOriginal(int id, int version, String property) {
        int aux = version;
        while (aux >= 0) {
            try {
                String fileStr = getPath(id, aux, property, false) + "_orig.torrent";
                File f = new File(fileStr);
                if (f.exists()) {
                    return readFile(f);
                }
            } catch (FileNotFoundException ex) {
                Logger.getLogger(TorrentConnection.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(TorrentConnection.class.getName()).log(Level.SEVERE, null, ex);
            }
            aux--;
        }
        return null;
    }

    private byte[] getLastTorrentModified(int id, int version, String property) {
        int aux = version;
        while (aux >= 0) {
            try {
                String fileStr = getPath(id, aux, property, false) + ".torrent";
                File f = new File(fileStr);
                if (f.exists()) {
                    return readFile(f);
                }
            } catch (FileNotFoundException ex) {
                Logger.getLogger(TorrentConnection.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(TorrentConnection.class.getName()).log(Level.SEVERE, null, ex);
            }
            aux--;
        }
        return null;
    }

    private String getPath(int id, int version, String property, boolean isForFiles) {
        String sep = System.getProperty("file.separator");
        String value = getPath(id, isForFiles) + sep + property + sep + version;
        return value;
    }

    private String getPath(int id, boolean isForFiles) {
        String sep = System.getProperty("file.separator");
        StringBuffer sb = new StringBuffer(Long.toHexString(id));
        StringBuffer zeros = new StringBuffer();
        while (zeros.length() + sb.length() < 16) {
            zeros.append("0");
        }
        String aux = zeros.toString() + sb.toString();
        return path + sep + aux.substring(0, 2) + sep + aux.substring(2, 4)
                + sep + aux.substring(4, 6) + sep + aux.substring(6, 8)
                + sep + aux.substring(8, 10) + sep + aux.substring(10, 12)
                + sep + aux.substring(12, 14) + sep + aux.substring(14, 16);

    }

    private byte[] readFile(File f) throws FileNotFoundException, IOException {
        BufferedInputStream bis = null;
        try {
            bis = new BufferedInputStream(new FileInputStream(f));
            byte[] data = new byte[(int) f.length()]; //OJO: LImitaciona fichero de 2 gb
            int file_offset = 0;
            int bytes_read = 0;
            while (file_offset < data.length && (bytes_read = bis.read(data, file_offset, data.length - file_offset)) >= 0) {
                file_offset += bytes_read;
            }
            if (file_offset < data.length) {
                throw new IOException("Could not completely read file \"" + f.getName() + "\".");
            }
            return data;
        } finally {
            if (bis != null) {
                bis.close();
            }
        }
    }

    private void writeFile(File f, byte[] data) throws FileNotFoundException, IOException {
        BufferedOutputStream bos = null;
        bos = new BufferedOutputStream(new FileOutputStream(f));
        bos.write(data);
    }

    public List<TorrentOperation> getOperations() {
        return operations;
    }

    public int prepare() throws SQLException {
        for (TorrentOperation op : operations) {
            String filePath = (String) op.getParameters()[0];
            File aux = new File(filePath);
            if (aux.exists()) {
                throw new SQLException("Error while writing to disk");
            }
        }
        return (!operations.isEmpty()) ? XAResource.XA_OK : XAResource.XA_RDONLY;
    }

    public void commit() throws SQLException {
        for (TorrentOperation op : operations) {
            int opCode = op.getOperation();
            String filePath = (String) op.getParameters()[0];
            byte[] data = (byte[]) op.getParameters()[1];
            File aux = new File(filePath);
            checkPath(aux.getParent());
            BufferedOutputStream bos = null;
            try {
                bos = new BufferedOutputStream(new FileOutputStream(aux));
                bos.write(data);
            } catch (FileNotFoundException ex) {
                throw new SQLException(ex);
            } catch (IOException ex) {
                throw new SQLException(ex);
            } finally {
                if (bos != null) {
                    try {
                        bos.close();
                    } catch (IOException ex) {
                        throw new SQLException(ex);
                    }
                }
            }
            if (TorrentOperation.ADD_TORRENT_ORIGINAL == opCode) {
                startDownloadTorrent(data, filePath);
            }
            if (TorrentOperation.ADD_TORRENT_MODIFIED == opCode) {
                startShareTorrent(data, filePath);
            }
            operationsCommited.add(op);
        }
    }

    private void checkPath(String path) {
        File f = new File(path);
        if (!f.exists()) {
            checkPath(f.getParent());
            f.mkdir();
        }
    }

    private void startDownloadTorrent(byte[] data, String filePath) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private void startShareTorrent(byte[] data, String filePath) {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    private byte[] modifyTorrent(byte[] data) {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
