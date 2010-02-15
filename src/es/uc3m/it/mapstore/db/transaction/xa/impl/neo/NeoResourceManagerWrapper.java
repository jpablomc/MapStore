/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.neo;

import es.uc3m.it.mapstore.bean.MapStoreBasicCondition;
import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.bean.MapStoreResult;
import es.uc3m.it.mapstore.bean.MapStoreTraverserDescriptor;
import es.uc3m.it.mapstore.bean.NeoRelationship;
import es.uc3m.it.mapstore.db.impl.MapStoreSession;
import es.uc3m.it.mapstore.db.transaction.xa.impl.ResourceManagerlImpl;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.sql.XAConnection;
import javax.transaction.Transaction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.impl.nioneo.store.PropertyData;
import org.neo4j.impl.nioneo.store.RelationshipData;
import org.neo4j.impl.persistence.PersistenceManager;
import org.neo4j.impl.persistence.PersistenceModule;
import org.neo4j.impl.util.ArrayMap;


/**
 *
 * @author Pablo
 */
public class NeoResourceManagerWrapper extends ResourceManagerlImpl {
    private String type;
    private String name;
    private PersistenceModule pm;

    @Override
    public XAConnection getXAConnection() {
        return new NeoXAConnection(getPersistanceManager().getPersistenceSource().getXaDataSource().getXaConnection());
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
        //TODO:Inicializar
        name = prop.getProperty("name");
        type = prop.getProperty("type");
        pm = new PersistenceModule();
    }

    private synchronized PersistenceManager getPersistanceManager() {
        PersistenceManager manager = pm.getPersistenceManager();       
        if (manager == null) {
            init();
            manager = pm.getPersistenceManager();
        }
        return manager;
    }

    @Override
    public void create(MapStoreItem item, Transaction t) {
        //TODO: VER COMO ARREGLAMOS CUANDO HAYA MAS OBJETOS DE LOS ADMITIDOS POR INTEGER        
        int id = Long.valueOf(item.getId()).intValue();
        PersistenceManager manager = getPersistanceManager();
        int nextId = pm.getPersistenceManager().getPersistenceSource().nextId(Node.class);
        if (nextId != id) throw new MapStoreRunTimeException("Neo is not sinchronized with id generator");
        manager.nodeCreate(id);
        List<String> props = getPropertiesToProcess(item);
        for (String property : props) {
            Object value = item.getProperty(property);
            MapStoreSession session = MapStoreSession.getSession();
            MapStoreItem newItem = session.findAnonymous(value,id+"|"+property);
            int[] idRef;
            if (session.needsUpdate(value, id+"|"+property)) {
                if (newItem == null) idRef = session.saveAnonymous(value,id+"|"+property);
                else idRef = session.updateAnonymous(value, name);
            } else {
                idRef = new int[]{newItem.getId(),newItem.getVersion()};
            }
            int relType;
            if (value instanceof Collection) {
                relType = NeoRelationship.CONTAINS;
            } else if (value instanceof Map) {
                relType = NeoRelationship.MAPS;
            } else relType = NeoRelationship.RELATES_TO;
            int idRel = pm.getPersistenceManager().getPersistenceSource().nextId(Relationship.class);
            manager.relationshipCreate(idRel, relType, id, idRef[0]);
            manager.relAddProperty(idRel, NeoPropertyIndex.RELATION_DATE_START, item.getRecordDate().getTime());
            manager.relAddProperty(idRel, NeoPropertyIndex.RELATION_VERSION_START, item.getVersion());
            manager.relAddProperty(idRel, NeoPropertyIndex.RELATION_NAME, property);
            manager.nodeAddProperty(nextId, NeoPropertyIndex.getNeoPropertyIndexForVersion(item.getVersion()), item.getRecordDate().getTime());
            item.setProperty(MapStoreItem.NONPROCESSABLE + property, idRef[0]+ "_" + idRef[1]);
        }
    }

    @Override
    public void delete(MapStoreItem item, MapStoreItem old, Transaction t) {
        update(item, old, t);
    }

    @Override
    public MapStoreResult findByConditions(List<MapStoreBasicCondition> cond, int flag, Date fecha) {
        MapStoreResult items = null;
        for (MapStoreBasicCondition c : cond) {
            if (c.getOperator() != MapStoreBasicCondition.OP_RELATED) throw new IllegalArgumentException("Unsupported operator. Neo can only resolve relations");
            if (!(c.getValue() instanceof MapStoreTraverserDescriptor)) throw new IllegalArgumentException("Unsupported value. Value must be a MapStoreTraverserDescriptor instance");
            MapStoreTraverserDescriptor traverser = (MapStoreTraverserDescriptor) c.getValue();
            MapStoreResult aux = findByCondition(traverser, fecha);
            if (items == null) items = aux;
            else {
                switch (flag) {
                    case MapStoreSession.CONJUNCTIVE_SEARCH:
                        items.and(aux);
                        break;
                    case MapStoreSession.DISJUNCTIVE_SEARCH:
                        items.or(aux);
                        break;
                }
            }
        }
        return items;
    }

    private MapStoreResult findByCondition(MapStoreTraverserDescriptor traverser, Date fecha) {
        PersistenceManager manager = getPersistanceManager();
        MapStoreResult items = new MapStoreResult();
        MapStoreResult validVersions = new MapStoreResult();
        for (Integer initialNode : traverser.getInitialNodes()) {
            //Inicialización
            NeoResultsManager results = new NeoResultsManager(traverser.getSearchAlgorithm());
            results.add(initialNode, 0);
            if (validVersions.getVersionsForId(initialNode) == null) {
                int version = getValidVersionForNode(initialNode, fecha);
                validVersions.addIdVersion(initialNode, version);
            }
            //Procesado
            Integer next = results.getNext();

            while (next != null) {
                int depth = results.getDepth(next);
                if (depth >= traverser.getDistanceMin() && depth <= traverser.getDistanceMax()) {
                    items.addIdVersion(next,validVersions.getVersionsForId(next));
                }
                if (depth < traverser.getDistanceMax()) {
                    String relation = traverser.getRouteForDistance(depth+1);
                    int direction = traverser.getDirectionForDistance(depth+1);
                    for (RelationshipData rd : manager.loadRelationships(depth)) {
                        Integer newNode = mustBeTraversed(next, rd, relation, direction, fecha);
                        if (newNode != null) {
                            if (validVersions.getVersionsForId(newNode) == null) {
                                int version = getValidVersionForNode(initialNode, fecha);
                                validVersions.addIdVersion(initialNode, version);
                            }
                            int newDepth = depth+1;
                            switch(rd.relationshipType()) {
                                case NeoRelationship.CONTAINS:
                                case NeoRelationship.MAPS:
                                    newDepth = depth;
                            }
                            results.add(newNode, newDepth);
                        }
                    }
                }
                next = results.getNext();
            }
        }
        return items;
    }


    private Integer mustBeTraversed(int node, RelationshipData rd, String relation, int direction, Date fecha) {
        Integer result = null;
        boolean isValidRelation = true;
        if (!(direction == MapStoreTraverserDescriptor.DIRECTION_ANY) &&
                !(direction == MapStoreTraverserDescriptor.DIRECTION_FROM_FIRST_TO_SECOND && rd.firstNode() == node) &&
                !(direction == MapStoreTraverserDescriptor.DIRECTION_FROM_SECOND_TO_FIRST && rd.secondNode() == node)) isValidRelation = false;
        Long initTime = null;
        Long endTime = null;
        Integer minVersion = null;
        Integer maxVersion = null;
        String propertyName = null;
        PersistenceManager manager = getPersistanceManager();
        if (isValidRelation) {
            ArrayMap<Integer, PropertyData> loadRelProperties = pm.getPersistenceManager().loadRelProperties(rd.getId());
            for (Integer key : loadRelProperties.keySet()) {
                PropertyData pd = loadRelProperties.get(key);
                switch(key) {
                    case NeoPropertyIndex.RELATION_DATE_END_KEY:
                        endTime = (Long) pd.getValue();
                        break;
                    case NeoPropertyIndex.RELATION_DATE_START_KEY:
                        initTime = (Long) pd.getValue();
                        break;
                    case NeoPropertyIndex.RELATION_NAME_KEY:
                        propertyName = (String)manager.loadPropertyValue(pd.getId());
                        break;
                    case NeoPropertyIndex.RELATION_VERSION_END_KEY:
                        minVersion = (Integer) pd.getValue();
                        break;
                    case NeoPropertyIndex.RELATION_VERSION_START_KEY:
                        maxVersion = (Integer) pd.getValue();
                        break;
                }
            }
            if (!(relation.equals(MapStoreTraverserDescriptor.ROUTE_ANY)) && !((relation.equals(propertyName)))) isValidRelation = false;
        }
        if (isValidRelation) {
            if (fecha == null) {
                //Debe estar activa
                if (endTime == null) {
                    result = (node == rd.firstNode())?(Integer)rd.secondNode():(Integer)rd.firstNode();

                }
            } else {
                long time = fecha.getTime();
                if (initTime <= time && (endTime == null || endTime>time)) result = (node == rd.firstNode())?(Integer)rd.secondNode():(Integer)rd.firstNode();
            }
        }
        return result;
    }

    private int getValidVersionForNode(int node, Date fecha) {
        int value;
        if (fecha == null) value = getLastVersion(node);
        else value = getActiveVersionAtDate(node, fecha);
        return value;
    }

    private int getLastVersion(int node) {
        PersistenceManager manager = getPersistanceManager();
        ArrayMap<Integer, PropertyData> nodeProperties = manager.loadNodeProperties(node);
        int lastVersion = Integer.MIN_VALUE;
        for (Integer key : nodeProperties.keySet()) {
            if (key > NeoPropertyIndex.NODE_VERSION_BASE) {
                int version = NeoPropertyIndex.getVersionFromNeoPropertyIndex(key);
                if (version > lastVersion) lastVersion = version;
            }
        }
        return lastVersion;
    }

    private int getActiveVersionAtDate(int node, Date fecha) {
        long time = fecha.getTime();
        PersistenceManager manager = getPersistanceManager();
        ArrayMap<Integer, PropertyData> nodeProperties = manager.loadNodeProperties(node);
        int lastVersion = Integer.MIN_VALUE;
        for (Integer key : nodeProperties.keySet()) {
            if (key > NeoPropertyIndex.NODE_VERSION_BASE) {
                int version = NeoPropertyIndex.getVersionFromNeoPropertyIndex(key);
                PropertyData pd = nodeProperties.get(key);
                long auxTime = (Long)pd.getValue();
                if (lastVersion<version && auxTime<=time) lastVersion = version;
            }
        }
        return lastVersion;

    }


    @Override
    public void update(MapStoreItem item, MapStoreItem old, Transaction t) {
        int id = Long.valueOf(item.getId()).intValue();
        PersistenceManager manager = getPersistanceManager();
        manager.nodeAddProperty(id, NeoPropertyIndex.getNeoPropertyIndexForVersion(item.getVersion()), item.getRecordDate().getTime());
        //Recuperar las antiguas relaciones del objeto:
        Iterable<RelationshipData> relationship = manager.loadRelationships(id);
        Map<String, RelationshipData> relations = new HashMap<String, RelationshipData>();
        //Creamos un mapa con las relaciones activas. La clave sera el nombre de la propiedad
        for (RelationshipData rd : manager.loadRelationships(id)) {
            if (rd.firstNode() == id) {
                ArrayMap<Integer, PropertyData> loadRelProperties = pm.getPersistenceManager().loadRelProperties(rd.getId());
                Long time_end = null;
                String nameProperty = null;
                for (Integer key : loadRelProperties.keySet()) {
                    PropertyData pd = loadRelProperties.get(key);
                    switch (key) {
                        case NeoPropertyIndex.RELATION_DATE_END_KEY:
                            time_end = (Long) pd.getValue();
                            break;
                        case NeoPropertyIndex.RELATION_NAME_KEY:
                            nameProperty = (String) manager.loadPropertyValue(pd.getId());
                            break;
                    }
                }
                //Si la relación esta activa la añadimos a la lista
                if (time_end == null) {
                    relations.put(nameProperty, rd);
                }
            }
        }

        List<String> props = getPropertiesToProcess(item);
        //Recorremos las propiedades de relación
        for (String property : props) {
            Object value = item.getProperty(property);
            MapStoreSession session = MapStoreSession.getSession();
            MapStoreItem relatedItem = session.findAnonymous(value,id+"|"+property);
            int[] id2;
            if (session.needsUpdate(value, id+"|"+property)) {
                if (relatedItem == null) id2 = session.saveAnonymous(value,id+"|"+property);
                else id2 = session.updateAnonymous(value,id+"|"+property);
            } else {
                id2 = new int[]{relatedItem.getId(),relatedItem.getVersion()};
            }
            int relType;
            if (value instanceof Collection) {
                relType = NeoRelationship.CONTAINS;
            } else if (value instanceof Map) {
                relType = NeoRelationship.MAPS;
            } else relType = NeoRelationship.RELATES_TO;

            RelationshipData rd = relations.get(property);
            if (rd != null) {
                //Existia la relación
                if (rd.secondNode() != id2[0]) {
                    //Ha cambiado el destino... finalizar la antigua y crear la nueva
                    manager.relAddProperty(rd.getId(), NeoPropertyIndex.RELATION_DATE_END, item.getRecordDate().getTime());
                    manager.relAddProperty(rd.getId(), NeoPropertyIndex.RELATION_VERSION_END, item.getVersion()-1);
                    int idRel = pm.getPersistenceManager().getPersistenceSource().nextId(Relationship.class);
                    manager.relationshipCreate(idRel, relType, id, id2[0]);
                    manager.relAddProperty(idRel, NeoPropertyIndex.RELATION_DATE_START, item.getRecordDate().getTime());
                    manager.relAddProperty(rd.getId(), NeoPropertyIndex.RELATION_VERSION_START, item.getVersion());
                    manager.relAddProperty(idRel, NeoPropertyIndex.RELATION_NAME, property);
                }
                //Si no ha cambiado se deja igual
            } else {
                //No existia la relacion... se crea
                    int idRel = pm.getPersistenceManager().getPersistenceSource().nextId(Relationship.class);
                    manager.relationshipCreate(idRel, relType, id, id2[0]);
                    manager.relAddProperty(idRel, NeoPropertyIndex.RELATION_DATE_START, item.getRecordDate().getTime());
                    manager.relAddProperty(idRel, NeoPropertyIndex.RELATION_VERSION_START, item.getVersion());
                    manager.relAddProperty(idRel, NeoPropertyIndex.RELATION_NAME, property);
            }
            item.setProperty(MapStoreItem.NONPROCESSABLE + property, id2[0]+ "_" + id2[1]);
            relations.remove(property);
        }
        //Ahora queda anular las relaciones que antes estaban activas y ahora no existen
        for (RelationshipData rd :relations.values()) {
               manager.relAddProperty(rd.getId(), NeoPropertyIndex.RELATION_DATE_END, item.getRecordDate().getTime());
               manager.relAddProperty(rd.getId(), NeoPropertyIndex.RELATION_VERSION_END, item.getVersion()-1);
        }
    }

    private void init() throws MapStoreRunTimeException {
        NeoPersistenceManager.init(pm);
    }

    @Override
    public void getAll() {
        int i = 0;
        int max = (new Long(getPersistanceManager().getPersistenceSource().getHighestPossibleIdInUse(Node.class))).intValue();
        while (i <= max) {

            if (getPersistanceManager().loadLightNode(i)) {
                System.out.println("Node: " + i);
                ArrayMap<Integer, PropertyData> props = getPersistanceManager().loadNodeProperties(i);
                System.out.println("\tNode Properties: ");
                for (Integer key : props.keySet()) {
                    PropertyData pd = props.get(key);
                    System.out.println("\t\t" + key + "\t" + pd.getId() + "\t" + pd.getValue());
                }
                System.out.println("\tNode Relationship : ");
                for (RelationshipData rd : getPersistanceManager().loadRelationships(i)) {
                    if (rd.firstNode() == i) {
                        System.out.println("\t\tFrom: " + rd.firstNode() + "\t to: " + rd.secondNode() + "\t type" + rd.relationshipType());
                        ArrayMap<Integer, PropertyData> loadRelProperties = pm.getPersistenceManager().loadRelProperties(rd.getId());
                        for (Integer key : loadRelProperties.keySet()) {
                            PropertyData pd = loadRelProperties.get(key);
                            Object propValue = pd.getValue();
                            if (propValue == null) {
                                propValue = getPersistanceManager().loadPropertyValue(pd.getId());
                            }
                            //String propValue = getPersistanceManager().loadIndex(pd.getId());
                            System.out.println("\t\t\t" + key + "\t" + pd.getId() + "\t" + propValue);
                        }
                    }
                }
            }
            i++;
        }

    }

    @Override
    public boolean canFindByNameType() {
        return false;
    }

    @Override
    public Integer findByNameType(String name, String type) {
        throw new UnsupportedOperationException("This resource does not support find by type/name");
    }

    @Override
    public void shutdown() {
        pm.stop();
        pm = null;
    }

    @Override
    public Set<Integer> findByType(String type) {
        throw new UnsupportedOperationException("Operation is not supported");
    }
}
