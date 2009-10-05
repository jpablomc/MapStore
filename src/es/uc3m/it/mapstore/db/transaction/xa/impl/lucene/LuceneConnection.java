/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.lucene;

import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.db.transaction.xa.impl.AbstractConnection;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.transaction.xa.XAResource;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.LockObtainFailedException;

/**
 *
 * @author Pablo
 */
public class LuceneConnection extends AbstractConnection {
    IndexWriter w;
    Map<Long,Document> data;
    Set<Long> dataDelete;
    boolean prepared;
    String path;
    Analyzer analyzer;
    List<LuceneOperation> operations;
      
    public LuceneConnection(String path) throws CorruptIndexException, LockObtainFailedException, IOException {
        analyzer = new StandardAnalyzer();
        data = new HashMap<Long,Document>();
        dataDelete = new HashSet<Long>();
        this.path = path;
        operations = new ArrayList<LuceneOperation>();
    }
    
    @Override
    public void commit() throws SQLException {
        if (!prepared) prepare();
        try {
            w.commit();
            w.close();
        } catch (CorruptIndexException ex) {
            Logger.getLogger(LuceneConnection.class.getName()).log(Level.SEVERE, null, ex);
            throw new SQLException(ex);
        } catch (IOException ex) {
            Logger.getLogger(LuceneConnection.class.getName()).log(Level.SEVERE, null, ex);
            throw new SQLException(ex);
        }
    }
    public void indexNew(long id, String property, Object value) throws SQLException {        
        doNew(id, property, value);
        LuceneOperation op = new LuceneOperation(LuceneOperation.CREATE, new Object[]{id,property,value});
        operations.add(op);
    }

    public void index(long id, String property, Object value) throws SQLException {
        doUpdate(id,property,value);
        LuceneOperation op = new LuceneOperation(LuceneOperation.UPDATE, new Object[]{id,property,value});
        operations.add(op);
    }

    public void delete(long id) throws SQLException {
        doDelete(id);
        LuceneOperation op = new LuceneOperation(LuceneOperation.DELETE, new Object[]{id});
        operations.add(op);
    }

    public int prepare() throws SQLException {        
        try {
            w = new IndexWriter(path,analyzer, IndexWriter.MaxFieldLength.UNLIMITED);
            for (Long id: dataDelete) {
                Term t = new Term(MapStoreItem.ID, id.toString());
                w.deleteDocuments(t);
            }
            for (Document d: data.values()) {
                w.addDocument(d);
            }
        } catch (CorruptIndexException ex) {
            Logger.getLogger(LuceneConnection.class.getName()).log(Level.SEVERE, null, ex);
            throw new SQLException(ex);
        } catch (IOException ex) {
            Logger.getLogger(LuceneConnection.class.getName()).log(Level.SEVERE, null, ex);
            throw new SQLException(ex);
        }
        try {
            w.prepareCommit();
        } catch (CorruptIndexException ex) {
            Logger.getLogger(LuceneConnection.class.getName()).log(Level.SEVERE, null, ex);
            throw new SQLException(ex);
        } catch (IOException ex) {
            Logger.getLogger(LuceneConnection.class.getName()).log(Level.SEVERE, null, ex);
            throw new SQLException(ex);
        }
        prepared = true;
        return (dataDelete.size() + data.size()>0)?XAResource.XA_OK:XAResource.XA_RDONLY;
    }

    public List<Document> getAll() {
        List<Document> docs = new ArrayList<Document>();
        try {            
            IndexReader r = IndexReader.open(path);
            int max = r.maxDoc();
            for (int i = 0; i < max; i++) {
                docs.add(r.document(i));
            }
            r.close();
        } catch (CorruptIndexException ex) {
            Logger.getLogger(LuceneConnection.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(LuceneConnection.class.getName()).log(Level.SEVERE, null, ex);
        }
        return docs;
    }

    private void doDelete(long id) {
        dataDelete.add(id);
    }

    private void doNew(long id, String property, Object value) {
        Document d = data.get(id);
        if (d == null) {
            d = new Document();
            data.put(id, d);
            Field f = new Field(MapStoreItem.ID, Long.toString(id), Field.Store.YES, Field.Index.NOT_ANALYZED);
            d.add(f);
        }
        //Comprobar que no sea el id...
        if (!MapStoreItem.ID.equals(property)) {
            Field f = new Field(property, value.toString(), Field.Store.NO, Field.Index.ANALYZED);
            d.add(f);
        }
    }

    private void doUpdate(long id, String property, Object value) throws SQLException {
        doDelete(id); //TODO: Tal vez versionar
        doNew(id, property, value);
    }

    public List<LuceneOperation> getOperations() {
        return operations;
    }
}
