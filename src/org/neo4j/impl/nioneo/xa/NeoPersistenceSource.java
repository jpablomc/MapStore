/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.nioneo.xa;

import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.neo4j.impl.persistence.ResourceConnection;
import org.neo4j.impl.transaction.xaframework.XaDataSource;

/**
 * The NioNeo persistence source implementation. If this class is registered as
 * persistence source for Neo operations that are performed on the node space
 * will be forwarded to this class {@link ResourceConnection} implementation.
 */
public class NeoPersistenceSource extends NioNeoDbPersistenceSource
{
    private static final String MODULE_NAME = "NioNeoDbPersistenceSource";

    private NeoStoreXaDataSource xaDs = null;
    private String dataSourceName = null;
    private ResourceConnection readOnlyResourceConnection;

    public NeoPersistenceSource(TransactionManager t, NeoStoreXaDataSource ds, String dir) {
        xaDs = ds;
        readOnlyResourceConnection = new ReadOnlyResourceConnection(xaDs);
    }

    @Override
    public synchronized void init()
    {
        // Do nothing
    }

    @Override
    public synchronized void reload()
    {
        // Do nothing
    }

    @Override
    public synchronized void stop()
    {
        if ( xaDs != null )
        {
            xaDs.close();
        }
    }

    @Override
    public synchronized void destroy()
    {
        // Do nothing
    }

    @Override
    public String getModuleName()
    {
        return MODULE_NAME;
    }

    @Override
    public ResourceConnection createResourceConnection()
    {
        return new ReadWriteResourceConnection( this.xaDs );
    }

    @Override
    public ResourceConnection createReadOnlyResourceConnection()
    {
        return readOnlyResourceConnection;
    }




    @Override
    public String toString()
    {
        return "A Nio Neo Db persistence source to [" + dataSourceName + "]";
    }

    @Override
    public int nextId( Class<?> clazz )
    {
        return xaDs.nextId( clazz );
    }

    // for recovery, returns a xa
    @Override
    public XAResource getXaResource()
    {
        return this.xaDs.getXaConnection().getXaResource();
    }

    @Override
    public void setDataSourceName( String dataSourceName )
    {
        this.dataSourceName = dataSourceName;
    }

    @Override
    public String getDataSourceName()
    {
        return this.dataSourceName;
    }

    @Override
    public long getHighestPossibleIdInUse( Class<?> clazz )
    {
        return xaDs.getHighestPossibleIdInUse( clazz );
    }

    @Override
    public long getNumberOfIdsInUse( Class<?> clazz )
    {
        return xaDs.getNumberOfIdsInUse( clazz );
    }

    @Override
    public XaDataSource getXaDataSource()
    {
        return xaDs;
    }

}