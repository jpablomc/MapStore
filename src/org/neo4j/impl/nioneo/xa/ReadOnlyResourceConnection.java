/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.neo4j.impl.nioneo.xa;

import javax.transaction.xa.XAResource;
import org.neo4j.impl.core.PropertyIndex;
import org.neo4j.impl.nioneo.store.PropertyData;
import org.neo4j.impl.nioneo.store.PropertyIndexData;
import org.neo4j.impl.nioneo.store.RelationshipData;
import org.neo4j.impl.nioneo.store.RelationshipTypeData;
import org.neo4j.impl.nioneo.store.RelationshipTypeStore;
import org.neo4j.impl.persistence.ResourceConnection;
import org.neo4j.impl.util.ArrayMap;

public class ReadOnlyResourceConnection implements ResourceConnection {
    private final NeoReadTransaction neoTransaction;
        private final RelationshipTypeStore relTypeStore;

        ReadOnlyResourceConnection( NeoStoreXaDataSource xaDs )
        {
            this.neoTransaction = xaDs.getReadOnlyTransaction();
            this.relTypeStore = xaDs.getNeoStore().getRelationshipTypeStore();
        }

        @Override
        public XAResource getXAResource()
        {
            throw new IllegalStateException(
                "This is a read only transaction, " +
                "this method should never be invoked" );
        }

        @Override
        public void destroy()
        {
            throw new IllegalStateException(
                "This is a read only transaction, " +
                "this method should never be invoked" );
        }

        @Override
        public void nodeDelete( int nodeId )
        {
            throw new IllegalStateException(
                "This is a read only transaction, " +
                "this method should never be invoked" );
        }

        @Override
        public int nodeAddProperty( int nodeId, PropertyIndex index,
            Object value )
        {
            throw new IllegalStateException(
                "This is a read only transaction, " +
                "this method should never be invoked" );
        }

        @Override
        public void nodeChangeProperty( int nodeId, int propertyId, Object value )
        {
            throw new IllegalStateException(
                "This is a read only transaction, " +
                "this method should never be invoked" );
        }

        @Override
        public void nodeRemoveProperty( int nodeId, int propertyId )
        {
            throw new IllegalStateException(
                "This is a read only transaction, " +
                "this method should never be invoked" );
        }

        @Override
        public void nodeCreate( int nodeId )
        {
            throw new IllegalStateException(
                "This is a read only transaction, " +
                "this method should never be invoked" );
        }

        @Override
        public void relationshipCreate( int id, int typeId, int startNodeId,
            int endNodeId )
        {
            throw new IllegalStateException(
                "This is a read only transaction, " +
                "this method should never be invoked" );
        }

        @Override
        public void relDelete( int relId )
        {
            throw new IllegalStateException(
                "This is a read only transaction, " +
                "this method should never be invoked" );
        }

        @Override
        public int relAddProperty( int relId, PropertyIndex index, Object value )
        {
            throw new IllegalStateException(
                "This is a read only transaction, " +
                "this method should never be invoked" );
        }

        @Override
        public void relChangeProperty( int relId, int propertyId, Object value )
        {
            throw new IllegalStateException(
                "This is a read only transaction, " +
                "this method should never be invoked" );
        }

        @Override
        public void relRemoveProperty( int relId, int propertyId )
        {
            throw new IllegalStateException(
                "This is a read only transaction, " +
                "this method should never be invoked" );
        }

        @Override
        public String loadIndex( int id )
        {
            return neoTransaction.getPropertyIndex( id );
        }

        @Override
        public PropertyIndexData[] loadPropertyIndexes( int maxCount )
        {
            return neoTransaction.getPropertyIndexes( maxCount );
        }

        @Override
        public Object loadPropertyValue( int id )
        {
            return neoTransaction.propertyGetValue( id );
        }

        @Override
        public RelationshipTypeData[] loadRelationshipTypes()
        {
            RelationshipTypeData relTypeData[] =
                relTypeStore.getRelationshipTypes();
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
            return neoTransaction.nodeLoadLight( id );
        }

        @Override
        public ArrayMap<Integer,PropertyData> nodeLoadProperties( int nodeId )
        {
            return neoTransaction.nodeGetProperties( nodeId );
        }

        @Override
        public Iterable<RelationshipData> nodeLoadRelationships( int nodeId )
        {
            return neoTransaction.nodeGetRelationships( nodeId );
        }

        public RelationshipData relLoadLight( int id )
        {
            return neoTransaction.relationshipLoad( id );
        }

        public ArrayMap<Integer,PropertyData> relLoadProperties( int relId )
        {
            return neoTransaction.relGetProperties( relId );
        }

        public void createPropertyIndex( String key, int id )
        {
            throw new IllegalStateException(
                "This is a read only transaction, " +
                "this method should never be invoked" );
        }

        public void createRelationshipType( int id, String name )
        {
            throw new IllegalStateException(
                "This is a read only transaction, " +
                "this method should never be invoked" );
        }
    }

