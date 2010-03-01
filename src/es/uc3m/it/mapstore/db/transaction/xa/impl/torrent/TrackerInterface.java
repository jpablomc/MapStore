/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package es.uc3m.it.mapstore.db.transaction.xa.impl.torrent;

import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.gudy.azureus2.plugins.PluginException;
import org.gudy.azureus2.plugins.PluginManager;
import org.gudy.azureus2.plugins.utils.StaticUtilities;
import org.gudy.azureus2.plugins.utils.resourcedownloader.ResourceDownloader;
import org.gudy.azureus2.plugins.utils.resourcedownloader.ResourceDownloaderFactory;
import org.gudy.azureus2.plugins.utils.resourceuploader.ResourceUploaderFactory;
import org.gudy.azureus2.pluginsimpl.local.PluginInitializer;



/**
 *
 * @author Pablo
 */
public class TrackerInterface {
    private static TrackerInterface instance;
    private List<File> currentDownloads;
    private List<File> currentUploads;
    File downloads;
    File uploads;

    private AzureusPlugin azureus;

    private TrackerInterface(File downloads, File uploads) {
        azureus = new AzureusPlugin();
        AzureusStarter as = new AzureusStarter(azureus);
        this.downloads = downloads;
        this.uploads = uploads;
        currentUploads = new ArrayList<File>();
        currentDownloads = new ArrayList<File>();
    }

    private class AzureusStarter implements Runnable {
        private AzureusPlugin azureus;

        private AzureusStarter(AzureusPlugin azureus) {
            this.azureus = azureus;
        }

        @Override
        public void run() {
            Properties prop = new Properties();
            prop.setProperty(PluginManager.PR_MULTI_INSTANCE, "true");
            PluginManager.registerPlugin(azureus, "MapStore");
            PluginManager.startAzureus(PluginManager.UI_NONE, prop);
        }


    }

    private static TrackerInterface start(File downloads, File uploads) {
        TrackerInterface ti = new TrackerInterface(downloads, uploads);
        List<String> list = ti.deserialize(downloads);
        for (String file : list) {
            ti.downloadTorrent(new File(file));
        }
        list = ti.deserialize(uploads);
        for (String file : list) {
            ti.uploadTorrent(new File(file));
        }
        return ti;
    }
    
    public static TrackerInterface getInstance(File downloads, File uploads) {
        if (instance == null) instance = start(downloads,uploads);
        return instance;
    }
    
    public void shutdown() {
        azureus.shutdown();
        List<String> aux = new ArrayList<String>();
        for (File f: currentDownloads) {
            aux.add(f.getAbsolutePath());
        }
        serialize(downloads,aux);
        aux = new ArrayList<String>();
        for (File f: currentUploads) {
            aux.add(f.getAbsolutePath());
        }
        serialize(uploads,aux);
        instance = null;
    }

    public void downloadTorrent(File f) {

        ResourceDownloaderFactory rdf = StaticUtilities.getResourceDownloaderFactory();
        ResourceDownloader rd = rdf.create(f);
        //Determinar el path donde descargar... file sera de la forma path/version_orig.torrent
        String aux = f.getAbsolutePath();
        if (aux.endsWith("_orig.torrent")) aux = aux.substring(0, aux.lastIndexOf("_orig.torrent"));
        else if (aux.endsWith(".torrent")) aux = aux.substring(0, aux.lastIndexOf("_torrent"));
        aux += System.getProperty("file.separator");
        ResourceDownloader files = rdf.getTorrentDownloader(rd, true, new File(aux));
        files.asyncDownload();
        //Añadirlo a la lista de descargas        
        currentDownloads.add(f);
    }

    public void uploadTorrent(File f) {
        ResourceUploaderFactory ruf = StaticUtilities.getResourceUploaderFactory();

    }

    private List<String> deserialize(File f) {
        if (!f.exists()) return new ArrayList<String>();
        ObjectInputStream in = null;
        List<String> result = null;
        try {
            in = new ObjectInputStream(new FileInputStream(f));
            // Deserialize the object
            result = (List<String>) in.readObject();
            in.close();
        } catch (ClassNotFoundException ex) {
            throw new MapStoreRunTimeException(ex);
        } catch (FileNotFoundException ex) {
            throw new MapStoreRunTimeException(ex);
        } catch (IOException ex) {
            throw new MapStoreRunTimeException(ex);
        } finally {
            try {
                in.close();
            } catch (IOException ex) {
                throw new MapStoreRunTimeException(ex);
            }
        }
        return result;

    }

    private void serialize(File f, List<String> aux) {
        ObjectOutput out= null;
        try {
            out = new ObjectOutputStream(new FileOutputStream(f));
            out.writeObject(aux);
            out.close();
        } catch (FileNotFoundException ex) {
            
        } catch (IOException ex) {
            throw new MapStoreRunTimeException(ex);
        } finally {
            if (out != null) try {
                out.close();
            } catch (IOException ex) {
                throw new MapStoreRunTimeException(ex);
            }
        }
    }

}
