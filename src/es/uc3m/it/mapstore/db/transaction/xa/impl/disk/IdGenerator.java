/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.disk;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Pablo
 */
public class IdGenerator {
    private String path;
    private int lastID;
    private Map<Integer,Integer> lastVersion;

    public IdGenerator(String path) {
        lastID = initLastId(path);
        lastVersion = new HashMap<Integer,Integer>();
    }

    public synchronized int getNewId() {
        int result = lastID++;
        getNewVersion(result);
        return result;
    }

    public synchronized int getNewVersion(int id) {
        Integer version = lastVersion.get(id);
        if (version == null) {
            version = getLastVersion(id);
        }
        lastVersion.put(id, version++);
        return version;
    }

    public synchronized void freeVersion(long id, long version) {
        long v = lastVersion.get(id);
        if (v == version) lastVersion.remove(id);
    }

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


    private int initLastId(String path) {
        int depth = 8;
        String aux = path;
        int[] id = new int[depth];
        while (depth > 0) {            
            int result = initLastId(aux, 0, 256);
            if (result == -1) result = 256;
            else if (result == 0) result = 1;
            id[depth-1] = result -1;
            String hex = Integer.toString(id[depth-1]);
            while (hex.length()<2) hex = "0" + hex;
            aux = aux + System.getProperty("file.separator") + hex;
            depth--;
        }
        int result = 0;
        for (int i = id.length-1; i>=0;i--) {
            result = result*256 + id[i];
        }
        return result + 1;
    }

    //Returns the first free register
    private int initLastId(String path, int first, int last) {
        String sep = System.getProperty("file.separator");
        int mean = (first + last)/2;
        String hex = Integer.toHexString(mean);
        while (hex.length()<2) hex = "0" + hex;
        File f = new File(path + sep + hex);
        if (f.exists()) {
            if (first >= last - 1 ) return -1;
            return initLastId(path, mean, last);
        } else {
            if (first == last) return mean;
            int l = initLastId(path, first, mean);
            if (l != -1) return l;
            else return mean;
        }
    }
    
    private int getLastVersion(long id) {
        List<File> files = getAllVersions(id);
        File aux = null;
        int vMax = Integer.MIN_VALUE;
        for (File f : files) {
            String version = f.getName();
            int v = Integer.valueOf(version);
            if (aux == null || v>vMax) {
                aux = f;
                vMax = v;
            }
        }
        if (vMax < 0) vMax = 0;
        return vMax;
    }

    private List<File> getAllVersions(long id) {
        File f = new File(getPath(id));
        List<File> versions = new ArrayList<File>();
        if (f.exists())Arrays.asList(f.listFiles());
        return versions;
    }
}
