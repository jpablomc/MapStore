/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package es.uc3m.it.mapstore.db.transaction.xa.impl.torrent;

import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import org.gudy.azureus2.plugins.Plugin;
import org.gudy.azureus2.plugins.PluginException;
import org.gudy.azureus2.plugins.PluginInterface;

/**
 *
 * @author Pablo
 */
public class AzureusPlugin implements Plugin {
    private PluginInterface pi;

    @Override
    public void initialize(PluginInterface pi) throws PluginException {
        this.pi = pi;
    }

    public void shutdown() {
        try {
            pi.getPluginManager().stopAzureus();
        } catch (PluginException ex) {
            throw new MapStoreRunTimeException(ex);
        }
    }
}

