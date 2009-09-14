/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.neo4j.impl.nioneo.xa;

import javax.transaction.xa.XAResource;
import org.neo4j.impl.core.PropertyIndex;
import org.neo4j.impl.nioneo.store.PropertyData;
import org.neo4j.impl.nioneo.store.PropertyIndexData;
import org.neo4j.impl.nioneo.store.PropertyStore;
import org.neo4j.impl.nioneo.store.RelationshipData;
import org.neo4j.impl.nioneo.store.RelationshipTypeData;
import org.neo4j.impl.persistence.ResourceConnection;
import org.neo4j.impl.util.ArrayMap;

/**
 *
 * @author Pablo
 */
    public class ReadWriteResourceConnection implements ResourceConnection {
        private NeoStoreXaConnection xaCon;
        private NodeEventConsumer nodeConsumer;
        private RelationshipEventConsumer relConsumer;
        private RelationshipTypeEventConsumer relTypeConsumer;
        private PropertyIndexEventConsumer propIndexConsumer;
        private PropertyStore propStore;

        ReadWriteResourceConnection( NeoStoreXaDataSource xaDs )
        {
            this.xaCon = (NeoStoreXaConnection) xaDs.getXaConnection();
            nodeConsumer = xaCon.getNodeConsumer();
            relConsumer = xaCon.getRelationshipConsumer();
            relTypeConsumer = xaCon.getRelationshipTypeConsumer();
            propIndexConsumer = xaCon.getPropertyIndexConsumer();
            propStore = xaCon.getPropertyStore();
        }

    @Override
        public XAResource getXAResource()
        {
            return this.xaCon.getXaResource();
        }

    @Override
        public void destroy()
        {
            xaCon.destroy();
            xaCon = null;
            nodeConsumer = null;
            relConsumer = null;
            relTypeConsumer = null;
            propIndexConsumer = null;
        }

    @Override
        public void nodeDelete( int nodeId )
        {
            nodeConsumer.deleteNode( nodeId );
        }

    @Override
        public int nodeAddProperty( int nodeId, PropertyIndex index,
            Object value )
        {
            int propertyId = propStore.nextId();
            nodeConsumer.addProperty( nodeId, propertyId, index, value );
            return propertyId;
        }

    @Override
        public void nodeChangeProperty( int nodeId, int propertyId, Object value )
        {
            nodeConsumer.changeProperty( nodeId, propertyId, value );
        }

    @Override
        public void nodeRemoveProperty( int nodeId, int propertyId )
        {
            nodeConsumer.removeProperty( nodeId, propertyId );
        }

    @Override
        public void nodeCreate( int nodeId )
        {
            nodeConsumer.createNode( nodeId );
        }

    @Override
        public void relationshipCreate( int id, int typeId, int startNodeId,
            int endNodeId )
        {
            relConsumer.createRelationship( id, startNodeId, endNodeId, typeId );
        }

    @Override
        public void relDelete( int relId )
        {
            relConsumer.deleteRelationship( relId );
        }

    @Override
        public int relAddProperty( int relId, PropertyIndex index, Object value )
        {
            int propertyId = propStore.nextId();
            relConsumer.addProperty( relId, propertyId, index, value );
            return propertyId;
        }

    @Override
        public void relChangeProperty( int relId, int propertyId, Object value )
        {
            relConsumer.changeProperty( relId, propertyId, value );
        }

    @Override
        public void relRemoveProperty( int relId, int propertyId )
        {
            relConsumer.removeProperty( relId, propertyId );
        }

    @Override
        public String loadIndex( int id )
        {
            return propIndexConsumer.getKeyFor( id );
        }

    @Override
        public PropertyIndexData[] loadPropertyIndexes( int maxCount )
        {
            return propIndexConsumer.getPropertyIndexes( maxCount );
        }

    @Override
        public Object loadPropertyValue( int id )
        {
            return xaCon.getNeoTransaction().propertyGetValue( id );
        }

    @Override
        public RelationshipTypeData[] loadRelationshipTypes()
        {
            RelationshipTypeData relTypeData[] =
                relTypeConsumer.getRelationshipTypes();
            RelationshipTypeData rawRelTypeData[] =
                new RelationshipTypeData[relTypeData.length];
            for ( int i = 0; i < relTypeData.length; i++ )
            {
                rawRelTypeData[i] = new RelationshipTypeData(
                    relTypeData[i].getId(), relTypeData[i].getName() );
            }
            return rawRelTypeData;
        }

    @Override
        public boolean nodeLoadLight( int id )
        {
            return nodeConsumer.loadLightNode( id );
        }

    @Override
        public ArrayMap<Integer,PropertyData> nodeLoadProperties( int nodeId )
        {
            return nodeConsumer.getProperties( nodeId );
        }

    @Override
        public Iterable<RelationshipData> nodeLoadRelationships( int nodeId )
        {
            return nodeConsumer.getRelationships( nodeId );
        }

    @Override
        public RelationshipData relLoadLight( int id )
        {
            return relConsumer.getRelationship( id );
        }

    @Override
        public ArrayMap<Integer,PropertyData> relLoadProperties( int relId )
        {
            return relConsumer.getProperties( relId );
        }

    @Override
        public void createPropertyIndex( String key, int id )
        {
            propIndexConsumer.createPropertyIndex( id, key );
        }

    @Override
        public void createRelationshipType( int id, String name )
        {
            relTypeConsumer.addRelationshipType( id, name );
        }
    }
