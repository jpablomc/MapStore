/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.torrent;

import java.io.Serializable;

/**
 *
 * @author Pablo
 */
public class TorrentOperation implements Serializable{
   public static final int ADD_TORRENT_ORIGINAL = 0;
   public static final int ADD_TORRENT_MODIFIED = 1;
   public static final int ADD_FILE = 2;

    int operation;
    Object parameters[];

    public TorrentOperation(int operation, Object[] parameters) {
        this.operation = operation;
        this.parameters = parameters;
    }

    public int getOperation() {
        return operation;
    }

    public Object[] getParameters() {
        return parameters;
    }
}
