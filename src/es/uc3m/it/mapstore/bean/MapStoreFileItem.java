/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.bean;

/**
 *
 * @author Pablo
 */
public class MapStoreFileItem {
    String filename;
    byte[] data;

    public MapStoreFileItem(String filename, byte[] data) {
        this.filename = filename;
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }
    
}
