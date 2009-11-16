/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.neo;

import es.uc3m.it.mapstore.bean.MapStoreTraverserDescriptor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Pablo
 */
public class NeoResultsManager{
    private int search_type;
    private List<Integer> toProcess;
    private Map<Integer,Integer> profundidad;
    private List<Integer> processed;

    public NeoResultsManager(int search_type) {
        this.search_type = search_type;
        toProcess = new ArrayList<Integer>();
        processed = new ArrayList<Integer>();
    }

    @SuppressWarnings("empty-statement")
    public Integer getNext() {
        Integer toReturn = null;
        if (!toProcess.isEmpty()) {
            switch (search_type) {
                case MapStoreTraverserDescriptor.DEPTH_TRAVERSER:
                    toReturn = toProcess.get(toProcess.size());
                    break;
                case MapStoreTraverserDescriptor.BREADTH_TRAVERSER:
                    toReturn = toProcess.get(0);
                    break;
                default:
                    throw new IllegalArgumentException("Type of search is not supported");
            }
            while(toProcess.remove(toReturn));
            processed.add(toReturn);
        }
        return toReturn;
    }

    public void add(int id, int depth) {
        if (!processed.contains(id)) {
            toProcess.add(id);
            switch (search_type) {
                case MapStoreTraverserDescriptor.DEPTH_TRAVERSER:
                    profundidad.put(id, depth);
                    break;
                case MapStoreTraverserDescriptor.BREADTH_TRAVERSER:
                    if (!profundidad.containsKey(id)) profundidad.put(id, depth);
                    break;
                default:
                    throw new IllegalArgumentException("Type of search is not supported");
            }
        }

    }

    public Integer getDepth(int id) {
        return profundidad.get(id);
    }
}
