/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.torrent;

import java.util.Properties;
import org.gudy.azureus2.plugins.PluginException;
import org.gudy.azureus2.plugins.PluginManager;
import org.junit.Test;

/**
 *
 * @author Pablo
 */

public class AzureusTest {
    private PluginManager pm;

    @Test
    public void startAzureus() throws InterruptedException, PluginException {
        Properties prop = new Properties();
        prop.setProperty(PluginManager.PR_MULTI_INSTANCE, "false");
        pm = PluginManager.startAzureus(PluginManager.UI_NONE, prop);
        Thread.sleep(20000);
        PluginManager.stopAzureus();
        Thread.sleep(10000);
    }
}
