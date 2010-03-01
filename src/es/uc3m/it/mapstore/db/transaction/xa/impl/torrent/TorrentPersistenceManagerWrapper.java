/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.torrent;

import es.uc3m.it.mapstore.bean.MapStoreBasicCondition;
import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.bean.MapStoreResult;
import es.uc3m.it.mapstore.db.transaction.xa.ResourceManagerWrapper;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.XAConnection;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;


/**
 *
 * @author Pablo
 */
public class TorrentPersistenceManagerWrapper implements ResourceManagerWrapper {
    private String name;
    private String type;
    private TorrentXADataSource ds;
    private TrackerInterface tracker;
    private String host;
    private int port;
    private String subDirTorrent;
    private String subDirFiles;

    @Override
    public XAConnection getXAConnection() {
        return ds.getXAConnection();
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
        host = prop.getProperty("host");
        subDirTorrent = prop.getProperty("subDirTorrent");
        if (subDirTorrent == null) subDirTorrent = "torrent";
        subDirFiles = prop.getProperty("subDirFiles");
        if (subDirFiles == null) subDirFiles = "torrent";
        port = Integer.parseInt(prop.getProperty("port"));

        String pathConfig = prop.getProperty("type");
        String directory = prop.getProperty("directory");
        if (directory == null) {
            directory = (new File("")).getAbsolutePath() +
                    System.getProperty("file.separator") + "db" +
                    System.getProperty("file.separator") + "torrent";
        }
        ds = new TorrentXADataSource(directory, subDirTorrent, subDirFiles, host, port);
        //tracker = new TrackerInterface(pathConfig);
    }

    @Override
    public void create(MapStoreItem item, Transaction t) {
        try {
            boolean enlisted = false;
            TorrentXAResource res = null;
            TorrentXAConnection xaconn = ds.getXAConnection();
            res = xaconn.getXAResource();
            enlisted = t.enlistResource(res);
            TorrentConnection conn = xaconn.getConnection();
            for (String property : item.getProperties().keySet()) {
                Object value = item.getProperty(property);
                if (value instanceof String) {
                    String val = (String) value;
                    int id = item.getId();
                    int version = item.getVersion();
                    if (isURL(val)) {
                        boolean substituirURL = true;
/*
                        try {

                            byte[] data = retrieveURL(val);
                            if (!ClientInterface.isTorrentFile(data)) {
                                substituirURL = false;
                            } else {
                                //AQUI TENEMOS EL TORRENT DESCARGADO EN DATA
                            }
 
                        } catch (MalformedURLException ex) {
                            //IMposible se chequea en el if anterior
                        } catch (IOException ex) {
                            substituirURL = false;
                        }
 */
                        String url = val;
                        if (substituirURL) {
                            url = replaceURL(id, version, property);
                        }
                        item.setProperty(MapStoreItem.NONPROCESSABLE_URL + property, url);
                    } else {
/*
                        if (ClientInterface.isTorrentFile(val.getBytes())) {
                            //AQUI TENEMOS UN TORRENT PASADO POR FICHERO
                            String url = replaceURL(id, version, property);
                            item.setProperty(MapStoreItem.NONPROCESSABLE_URL + property, url);
                        }

 */
                    }
                }
            }
            
            enlisted = !(t.delistResource(res, XAResource.TMSUCCESS));
        } catch (RollbackException ex) {
            throw new MapStoreRunTimeException(ex);
        } catch (IllegalStateException ex) {
            throw new MapStoreRunTimeException(ex);
        } catch (SystemException ex) {
            throw new MapStoreRunTimeException(ex);
        } catch (SQLException ex) {
            throw new MapStoreRunTimeException(ex);
        }







    }

    @Override
    public void update(MapStoreItem item, MapStoreItem old, Transaction t) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void delete(MapStoreItem item, MapStoreItem old, Transaction t) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public MapStoreResult findByConditions(List<MapStoreBasicCondition> cond, int flag, Date fecha) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void getAll() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean canFindByNameType() {
        return false;
    }

    @Override
    public Integer findByNameType(String name, String type) {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public Set<Integer> findByType(String type) {
        throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public void shutdown() {
        throw new UnsupportedOperationException("Not supported.");
    }

    private byte[] retrieveURL(String url) throws MalformedURLException, IOException {
		StringBuffer sb = new StringBuffer();
		URL dir = new URL(url);
		BufferedReader in = new BufferedReader(new InputStreamReader(dir.openStream()));
		String inputLine;
		while ((inputLine = in.readLine()) != null) sb.append(inputLine);
		in.close();
		return sb.toString().getBytes();
    }

    private boolean isURL(String url) {
        boolean isURL = true;
        try {
            URL dir = new URL(url);
        } catch (MalformedURLException ex) {
            isURL = false;
        }
        return isURL;
    }

    private String replaceURL(Integer id, int version, String property) {
        return "http://"+host+":"+port+"/"+id+"/"+version+"/"+property;
    }
}
