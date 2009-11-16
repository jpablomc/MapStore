/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.neo;

import org.neo4j.impl.core.PropertyIndex;

/**
 *
 * @author Pablo
 */
public class NeoPropertyIndex extends PropertyIndex {
    public static final int RELATION_DATE_START_KEY = 0;
    public static final int RELATION_DATE_END_KEY = 1;
    public static final int RELATION_NAME_KEY = 2;
    public static final int RELATION_VERSION_START_KEY = 3;
    public static final int RELATION_VERSION_END_KEY = 4;
    public static final int NODE_VERSION_BASE = 100;


    public static final NeoPropertyIndex RELATION_DATE_START = new NeoPropertyIndex("RELATION_START", RELATION_DATE_START_KEY);
    public static final NeoPropertyIndex RELATION_DATE_END = new NeoPropertyIndex("RELATION_END", RELATION_DATE_END_KEY);
    public static final NeoPropertyIndex RELATION_VERSION_START = new NeoPropertyIndex("RELATION_START", RELATION_VERSION_START_KEY);
    public static final NeoPropertyIndex RELATION_VERSION_END = new NeoPropertyIndex("RELATION_END", RELATION_VERSION_END_KEY);
    public static final NeoPropertyIndex RELATION_NAME = new NeoPropertyIndex("RELATION_NAME", RELATION_NAME_KEY);

    public NeoPropertyIndex(String key, int keyId) {
        super(key, keyId);
    }

    public static NeoPropertyIndex getNeoPropertyIndexForVersion(int version) {
        return new NeoPropertyIndex("NODE_VERSION_"+version, version+NODE_VERSION_BASE);
    }

    public static int getVersionFromNeoPropertyIndex(int relID) {
        return relID - NODE_VERSION_BASE;
    }

}
