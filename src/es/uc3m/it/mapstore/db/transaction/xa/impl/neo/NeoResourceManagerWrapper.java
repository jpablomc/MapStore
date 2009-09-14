/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.neo;

import es.uc3m.it.mapstore.bean.MapStoreCondition;
import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.bean.NeoRelationship;
import es.uc3m.it.mapstore.config.MapStoreConfig;
import es.uc3m.it.mapstore.db.impl.MapStoreSession;
import es.uc3m.it.mapstore.db.transaction.xa.*;
import es.uc3m.it.mapstore.exception.MapStoreRunTimeException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.XAConnection;
import javax.transaction.Transaction;
import org.neo4j.api.core.Node;
import org.neo4j.impl.nioneo.store.PropertyData;
import org.neo4j.impl.nioneo.store.RelationshipData;
import org.neo4j.impl.persistence.PersistenceManager;
import org.neo4j.impl.persistence.PersistenceModule;
import org.neo4j.impl.util.ArrayMap;


/**
 *
 * @author Pablo
 */
public class NeoResourceManagerWrapper implements ResourceManagerWrapper  {
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

    private PersistenceManager getPersistanceManager() {
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
        if (!manager.loadLightNode(id)) {
            manager.nodeCreate(id);
        }
        List<String> props = getPropertiesToProcess(item);
        for (String property : props) {
            Object value = item.getProperty(property);
            MapStoreSession session = MapStoreSession.getSession();
            MapStoreItem newItem = session.find(value);
            long idRef;
            if (newItem == null) idRef = session.save(newItem);
            else idRef = newItem.getId();
            int id2 = Long.valueOf(idRef).intValue();
            int relId = getNextId();
            Class clazz;
            List<Class> interfaces = new ArrayList<Class>();
            try {
                clazz = Class.forName(item.getType());
                interfaces = Arrays.asList(clazz.getInterfaces());
            } catch (ClassNotFoundException ex) {
                Logger.getLogger(NeoResourceManagerWrapper.class.getName()).log(Level.SEVERE, null, ex);
            }
            int relType;
            if (interfaces.contains(Collection.class)) {
                relType = NeoRelationship.CONTAINS;
            } else if (interfaces.contains(Map.class)) {
                relType = NeoRelationship.MAPS;
            } else relType = NeoRelationship.RELATES_TO;
            manager.relationshipCreate(relId, relType, id, id2);
        }
    }

    private static Integer relationship_id;

    private synchronized int getNextId() {
        int aux;
        aux = ++relationship_id;
        return aux;
    }

    @Override
    public void delete(long id, Transaction t) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<Long> findByConditions(List<MapStoreCondition> cond) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void update(MapStoreItem item, Transaction t) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    private List<String> getPropertiesToProcess(MapStoreItem item) {
        List<String> props = new ArrayList<String>();
        Map<Class,List<ResourceManagerWrapper>> mapa = new HashMap<Class,List<ResourceManagerWrapper>>();
        for (String prop :item.getProperties().keySet()) {
            if (processable(prop)) {
                Object value = item.getProperty(prop);
                List<ResourceManagerWrapper> lista = mapa.get(value.getClass());
                if (lista == null) {
                    lista = MapStoreConfig.getInstance().getXaResourceLookupForClass(value.getClass());
                    mapa.put(value.getClass(),lista);
                }
                if (lista.contains(this)) props.add(prop);
            }
        }
        return props;
    }

    private void init() throws MapStoreRunTimeException {
        NeoPersistenceManager.init(pm);
    }

    private boolean processable(String prop) {
        return true;
    }

    @Override
    public void getAll() {
        int i = 0;
        int max = (new Long(getPersistanceManager().getPersistenceSource().getHighestPossibleIdInUse(Node.class))).intValue();
        while (i<max) {

            if (getPersistanceManager().loadLightNode(i)) {
                System.out.println("Node: " + i);
                ArrayMap<Integer,PropertyData> props = getPersistanceManager().loadNodeProperties(i);
                System.out.println("\tNode Properties: " );
                for (Integer key :props.keySet()) {
                    PropertyData pd = props.get(key);
                    System.out.println("\t\t"+ key + "\t" + pd.getId() + "\t"+ pd.getValue());
                }
                System.out.println("\tNode Relationship : " );
                for (RelationshipData rd : getPersistanceManager().loadRelationships(i)) {
                    System.out.println("\t\tFrom: " + rd.firstNode() + "\t to: "+ rd.secondNode() + "\t type" + rd.relationshipType());
                }
            }
            i++;
        }

    }

}
