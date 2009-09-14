/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.impl;

import es.uc3m.it.mapstore.db.transaction.xa.ResourceManagerWrapper;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Pablo
 */
public class BalanceLoad {
    private static final Map<ResourceManagerWrapper,Long> loadMap = new HashMap<ResourceManagerWrapper,Long>();

    public static ResourceManagerWrapper getLessLoaded(List<ResourceManagerWrapper> resources) {
        ResourceManagerWrapper lessLoaded = null;
        Long minLoad = Long.MAX_VALUE;
        synchronized(loadMap) {
            for (ResourceManagerWrapper r : resources) {
                Long load = loadMap.get(r);
                if (load == null) {
                    load = new Long(0);
                    loadMap.put(r, load);
                }
                if (lessLoaded == null) {
                    minLoad = load;
                    lessLoaded = r;
                } else {
                    if (minLoad > load) {
                        minLoad = load;
                        lessLoaded = r;
                    }
                }
            }
        }
        return lessLoaded;
    }

    public static void increaseLoad(ResourceManagerWrapper res, long load) {
        synchronized (loadMap) {
            Long l = loadMap.get(res);
            if (l == null) l = load;
            else l = l + load;
            loadMap.put(res, l);
        }
    }

    public static void decreaseLoad(ResourceManagerWrapper res, long load) {
        synchronized (loadMap) {
            Long l = loadMap.get(res);
            if (l == null) l = -1* load;
            else l = l - load;
            loadMap.put(res, l);
        }
    }

    public static ResourceManagerWrapper getLessLoadedAndIncrease(List<ResourceManagerWrapper> resources,long extraLoad) {
        ResourceManagerWrapper lessLoaded = null;
        Long minLoad = Long.MAX_VALUE;
        synchronized(loadMap) {
            for (ResourceManagerWrapper r : resources) {
                Long load = loadMap.get(r);
                if (load == null) {
                    load = new Long(0);
                    loadMap.put(r, load);
                }
                if (lessLoaded == null) {
                    minLoad = load;
                    lessLoaded = r;
                } else {
                    if (minLoad > load) {
                        minLoad = load;
                        lessLoaded = r;
                    }
                }
            }
            Long l = loadMap.get(lessLoaded);
            l = l + extraLoad;
            loadMap.put(lessLoaded, l);
        }
        return lessLoaded;
    }


}
