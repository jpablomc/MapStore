/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package es.uc3m.it.mapstore.db.transaction.xa.impl.torrent;

import es.uc3m.it.mapstore.db.transaction.xa.impl.AbstractXAResource;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 *
 * @author Pablo
 */
public class TorrentXAResource extends AbstractXAResource {

    private Map<Xid, Set<TorrentConnection>> connections;
    private String path;
    private String host;
    private int port;

    public TorrentXAResource(String path, String host, int port) {
        connections = new HashMap<Xid, Set<TorrentConnection>>();
        this.host = host;
        this.port = port;
        this.path = path;
        try {
            recoverPrepared();
        } catch (SQLException ex) {
            Logger.getLogger(TorrentXAResource.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (IOException ex) {
            Logger.getLogger(TorrentXAResource.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(TorrentXAResource.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        } catch (XAException ex) {
            Logger.getLogger(TorrentXAResource.class.getName()).log(Level.SEVERE, null, ex);
            throw new MapStoreRunTimeException(ex);
        }
    }

    public synchronized void addConnection(TorrentConnection conn) {
        Xid xid = getXidCurrentThread();
        if (xid != null) {
            Set<TorrentConnection> conns = connections.get(xid);
            if (conns == null) {
                conns = new HashSet<TorrentConnection>();
                connections.put(xid, conns);
            }
            conns.add(conn);
        }
    }

    @Override
    protected synchronized int doPrepare(Xid arg0) throws XAException {
        try {
            String xidFile = getXidFile(arg0);
            List<TorrentOperation> operations = new ArrayList<TorrentOperation>();
            boolean modifies = false;
            Set<TorrentConnection> conns = connections.get(arg0);
            if (conns != null) {
                for (TorrentConnection conn : conns) {
                    try {
                        int i = conn.prepare();
                        operations.addAll(conn.getOperations());
                        switch (i) {
                            case XAResource.XA_OK:
                                modifies = true;
                        }
                    } catch (SQLException ex) {
                        Logger.getLogger(TorrentXAResource.class.getName()).log(Level.SEVERE, null, ex);
                        throw new XAException(XAException.XA_RBROLLBACK);
                    }
                }
            }
            createXidContent(operations, xidFile);
            return (modifies) ? XAResource.XA_OK : XAResource.XA_RDONLY;
        } catch (IOException ex) {
            Logger.getLogger(TorrentXAResource.class.getName()).log(Level.SEVERE, null, ex);
            throw new XAException(XAException.XA_RBROLLBACK);
        }
    }

    @Override
    protected synchronized void doCommit(Xid arg0) throws XAException {
        Set<TorrentConnection> conns = connections.get(arg0);
        for (TorrentConnection conn : conns) {
            try {
                conn.commit();
            } catch (SQLException ex) {
                Logger.getLogger(TorrentXAResource.class.getName()).log(Level.SEVERE, null, ex);
                throw new XAException(XAException.XA_RBROLLBACK);
            }
        }
        try {
            deleteXidContent(getXidFile(arg0));
        } catch (IOException ex) {
            throw new XAException(XAException.XA_RBROLLBACK);
        }
    }

    @Override
    protected synchronized void doRollback(Xid arg0) throws XAException {
        Logger.getLogger(TorrentXAResource.class.getName()).log(Level.INFO, "Rollingback transaction");
        Set<TorrentConnection> conns = connections.get(arg0);
        for (TorrentConnection conn : conns) {
            try {
                conn.rollback();
            } catch (SQLException ex) {
                Logger.getLogger(TorrentXAResource.class.getName()).log(Level.SEVERE, null, ex);
                throw new XAException(XAException.XA_RBROLLBACK);
            }
        }
    }
    public static final String TORRENT_TX = "TorrentTx.log";

    private synchronized void createXidContent(List<TorrentOperation> operations, String xid) throws IOException {
        File f = new File(path + System.getProperty("file.separator") + xid);
        ObjectOutput out = new ObjectOutputStream(new FileOutputStream(f));
        out.writeObject(operations);
        out.close();
        f = new File(path + System.getProperty("file.separator") + TORRENT_TX);
        BufferedWriter bw = new BufferedWriter(new FileWriter(f, true));
        bw.write(xid);
        bw.newLine();
        bw.close();
    }

    private synchronized void deleteXidContent(String xid) throws IOException {
        File f = new File(path + System.getProperty("file.separator") + xid);
        f.delete();
        f = new File(path + System.getProperty("file.separator") + TORRENT_TX);
        BufferedReader br = new BufferedReader(new FileReader(f));
        StringBuffer sb = new StringBuffer();
        while (br.ready()) {
            String aux = br.readLine();
            if (!aux.equals(xid)) {
                sb.append(aux).append(System.getProperty("line.separator"));
            }
        }
        br.close();
        BufferedWriter bw = new BufferedWriter(new FileWriter(f));
        bw.write(sb.toString());
        bw.close();
    }

    private String getXidFile(Xid arg0) throws XAException {
        String sb;
        try {
            StringBuffer aux = new StringBuffer();
            sb = aux.append(arg0.getFormatId()).append("_").append(getHexString(arg0.getGlobalTransactionId())).append("_").append(getHexString(arg0.getBranchQualifier())).toString();
        } catch (UnsupportedEncodingException ex) {
            Logger.getLogger(TorrentXAResource.class.getName()).log(Level.SEVERE, null, ex);
            throw new XAException(XAException.XAER_RMERR);
        }
        return sb;
    }
    static final byte[] HEX_CHAR_TABLE = {
        (byte) '0', (byte) '1', (byte) '2', (byte) '3',
        (byte) '4', (byte) '5', (byte) '6', (byte) '7',
        (byte) '8', (byte) '9', (byte) 'a', (byte) 'b',
        (byte) 'c', (byte) 'd', (byte) 'e', (byte) 'f'
    };

    public static String getHexString(byte[] raw) throws UnsupportedEncodingException {
        byte[] hex = new byte[2 * raw.length];
        int index = 0;

        for (byte b : raw) {
            int v = b & 0xFF;
            hex[index++] = HEX_CHAR_TABLE[v >>> 4];
            hex[index++] = HEX_CHAR_TABLE[v & 0xF];
        }
        return new String(hex, "ASCII");
    }

    private void recoverPrepared() throws IOException, ClassNotFoundException, SQLException, XAException {
        File f = new File(path + System.getProperty("file.separator") + TORRENT_TX);
        if (f.exists()) {
            BufferedReader br = null;
            try {
                List<Xid> xids = new ArrayList<Xid>();
                br = new BufferedReader(new FileReader(f));
                while (br.ready()) {
                    //Cada linea contiene el nombre del fichero a procesar
                    String aux = br.readLine();
                    //Deserializar el contenido
                    File faux = new File(path + System.getProperty("file.separator") + aux);
                    List<TorrentOperation> ops = deserializeOperations(faux);
                    //Ejecutar las operaciones
                    TorrentConnection conn = new TorrentConnection(path, host, port);
                    Xid xid = getXidFromString(aux);
                    Set<TorrentConnection> connSet = new HashSet<TorrentConnection>();
                    connSet.add(conn);
                    connections.put(xid, connSet);
                    for (TorrentOperation op : ops) {
                        Object[] params = op.getParameters();
/*
                            switch (op.getOperation()) {
                            case TorrentOperation.CREATE:
                                conn.storeNew((MapStoreItem) params[0]);
                                break;
                            case TorrentOperation.UPDATE:
                                conn.store((MapStoreItem) params[0]);
                                break;
                            case TorrentOperation.DELETE:
                                conn.delete((MapStoreItem) params[0]);
                                break;
                         }
  */
                    }
                    //Ejecutar el prepare
                    conn.prepare();
                    xids.add(xid);
                }
                setPreparedForRecover(xids);
            } finally {
                if (br != null) {
                    br.close();
                }
            }
        }
    }

    private List<TorrentOperation> deserializeOperations(File f) throws ClassNotFoundException, FileNotFoundException, IOException {
        ObjectInputStream in = null;
        List<TorrentOperation> operations = null;
        try {
            in = new ObjectInputStream(new FileInputStream(f));
            // Deserialize the object
            operations = (List<TorrentOperation>) in.readObject();
            in.close();
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(TorrentXAResource.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        } catch (FileNotFoundException ex) {
            Logger.getLogger(TorrentXAResource.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        } catch (IOException ex) {
            Logger.getLogger(TorrentXAResource.class.getName()).log(Level.SEVERE, null, ex);
            throw ex;
        } finally {
            try {
                in.close();
            } catch (IOException ex) {
                Logger.getLogger(TorrentXAResource.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return operations;
    }
    
    private Xid getXidFromString(String arg0) throws XAException {
        String[] token = arg0.split("_");
        int formatId = Integer.valueOf(token[0]);
        byte[] global = new byte[token[1].length()/2];
        for (int i= 0;i<global.length;i++) {
            String aux = token[1].substring(2*i, 2*i+1);
            global[i] = Byte.parseByte(aux, 16);
        }
        byte[] branch = new byte[token[2].length()/2];
        for (int i= 0;i<branch.length;i++) {
            String aux = token[2].substring(2*i, 2*i+1);
            branch[i] = Byte.parseByte(aux, 16);
        }
        return new XidImpl(formatId, global, branch);
   }

}
