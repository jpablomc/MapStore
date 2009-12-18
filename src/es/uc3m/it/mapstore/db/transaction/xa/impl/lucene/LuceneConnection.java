/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package es.uc3m.it.mapstore.db.transaction.xa.impl.lucene;

import es.uc3m.it.mapstore.bean.MapStoreBasicCondition;
import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.bean.MapStoreResult;
import es.uc3m.it.mapstore.db.transaction.xa.impl.AbstractConnection;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
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
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RangeQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.LockObtainFailedException;

/**
 *
 * @author Pablo
 */
public class LuceneConnection extends AbstractConnection {

    private IndexWriter w;
    private Map<Long, Document> data;
    private Set<Long> dataDelete;
    private boolean prepared;
    private String path;
    private Analyzer analyzer;
    private List<LuceneOperation> operations;
    private SimpleDateFormat df;

    public LuceneConnection(String path) throws CorruptIndexException, LockObtainFailedException, IOException {
        analyzer = new StandardAnalyzer();
        data = new HashMap<Long, Document>();
        dataDelete = new HashSet<Long>();
        this.path = path;
        operations = new ArrayList<LuceneOperation>();
        df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void commit() throws SQLException {
        if (!prepared) {
            prepare();
        }
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

    public void indexNew(long id, long version, String property, Object value) throws SQLException {
        doNew(id, version, property, value);
        LuceneOperation op = new LuceneOperation(LuceneOperation.CREATE, new Object[]{id, version, property, value});
        operations.add(op);
    }

    public void index(long id, long version, String property, Object value) throws SQLException {
        doUpdate(id, version, property, value);
        LuceneOperation op = new LuceneOperation(LuceneOperation.UPDATE, new Object[]{id, version, property, value});
        operations.add(op);
    }

    public void delete(long id) throws SQLException {
        doDelete(id);
        LuceneOperation op = new LuceneOperation(LuceneOperation.DELETE, new Object[]{id});
        operations.add(op);
    }

    public int prepare() throws SQLException {
        try {
            w = new IndexWriter(path, analyzer, IndexWriter.MaxFieldLength.UNLIMITED);
            for (Long id : dataDelete) {
                Term t = new Term(MapStoreItem.ID, id.toString());
                w.deleteDocuments(t);
            }
            for (Document d : data.values()) {
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
        return (dataDelete.size() + data.size() > 0) ? XAResource.XA_OK : XAResource.XA_RDONLY;
    }

    public int prepare(IndexWriter w) throws SQLException {
        try {
            for (Long id : dataDelete) {
                Term t = new Term(MapStoreItem.ID, id.toString());
                w.deleteDocuments(t);
            }
            for (Document d : data.values()) {
                w.addDocument(d);
            }
        } catch (CorruptIndexException ex) {
            Logger.getLogger(LuceneConnection.class.getName()).log(Level.SEVERE, null, ex);
            throw new SQLException(ex);
        } catch (IOException ex) {
            Logger.getLogger(LuceneConnection.class.getName()).log(Level.SEVERE, null, ex);
            throw new SQLException(ex);
        }
        prepared = true;
        return (dataDelete.size() + data.size() > 0) ? XAResource.XA_OK : XAResource.XA_RDONLY;
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

    private void doNew(long id, long version, String property, Object value) {
        Document d = data.get(id);
        if (d == null) {
            d = new Document();
            data.put(id, d);
            Field f = new Field(MapStoreItem.ID, Long.toString(id), Field.Store.YES, Field.Index.NOT_ANALYZED);
            d.add(f);
            f = new Field(MapStoreItem.VERSION, Long.toString(version), Field.Store.YES, Field.Index.NOT_ANALYZED);
            d.add(f);
        }
        //Comprobar que no sea el id...
        if (!MapStoreItem.ID.equals(property)) {
            if (value instanceof Collection) {
                Collection col = (Collection) value;
                for (Object obj : col) {
                    Field f = new Field(property, value.toString(), Field.Store.NO, Field.Index.ANALYZED);
                    d.add(f);
                }
            } else {
                Field f = new Field(property, value.toString(), Field.Store.NO, Field.Index.ANALYZED);
                d.add(f);
            }
        }
    }

    private void doUpdate(long id, long version, String property, Object value) throws SQLException {
        doDelete(id); //TODO: Tal vez versionar
        doNew(id, version, property, value);
    }

    public List<LuceneOperation> getOperations() {
        return operations;
    }
    public final static int CONJUNCTIVE_SEARCH = 0;
    public final static int DISJUNCTIVE_SEARCH = 1;

    public MapStoreResult findByConditions(List<MapStoreBasicCondition> cond, int flag, Date fecha) throws SQLException {
        BooleanQuery q = new BooleanQuery();
        BooleanClause.Occur bc = null;
        switch (flag) {
            case CONJUNCTIVE_SEARCH:
                bc = BooleanClause.Occur.MUST;
                break;
            case DISJUNCTIVE_SEARCH:
                bc = BooleanClause.Occur.SHOULD;
                break;
        }
        for (MapStoreBasicCondition c : cond) {
            Query qAux = null;
            switch (c.getOperator()) {
                case MapStoreBasicCondition.OP_EQUALS:
                    Term t = new Term(c.getProperty(), c.getValue().toString());
                    qAux = new TermQuery(t);
                    break;
                case MapStoreBasicCondition.OP_NOTEQUALS:
                    t = new Term(c.getProperty(), c.getValue().toString());
                    qAux = new TermQuery(t);
                    BooleanQuery baux = new BooleanQuery();
                    baux.add(qAux, BooleanClause.Occur.MUST_NOT);
                    qAux = baux;
                    break;
                case MapStoreBasicCondition.OP_PHRASE:
                    PhraseQuery pq = new PhraseQuery();
                    String auxStr = c.getValue().toString();
                    String[] terms = auxStr.split(" ");
                    for (String tStr : terms) {
                        t = new Term(c.getProperty(), tStr);
                        pq.add(t);
                    }
                    qAux = pq;
                    break;
                case MapStoreBasicCondition.OP_SIMILARITY:
                    t = new Term(c.getProperty(), c.getValue().toString());
                    qAux = new FuzzyQuery(t);
                    break;
            }
            if (qAux != null) {
                q.add(qAux, bc);
            }
        }
        if (fecha != null) {
            Term lowerTerm = new Term(MapStoreItem.RECORDDATE, "1900-01-01 00:00:00");
            Term upperTerm = new Term(MapStoreItem.RECORDDATE, df.format(fecha));
            RangeQuery rq = new RangeQuery(lowerTerm, upperTerm, true);
            q.add(rq, BooleanClause.Occur.MUST);
        }
        IndexSearcher is = null;
        try {
            is = new IndexSearcher(path);
            int maxDocs = is.maxDoc();
            MapStoreResult results = new MapStoreResult();
            if (maxDocs > 0) {
                TopDocs td = is.search(q, maxDocs);
                ScoreDoc[] sd = td.scoreDocs;
                IndexReader r = is.getIndexReader();
                for (int i = 0; i < sd.length; i++) {
                    Document d = r.document(sd[0].doc);
                    Integer id = new Integer(d.getFieldable(MapStoreItem.ID).stringValue());
                    Integer ver = new Integer(d.getFieldable(MapStoreItem.VERSION).stringValue());
                    results.addIdVersion(i, ver);
                }
            }
            return results;
        } catch (FileNotFoundException ex) {
            return null;
        } catch (CorruptIndexException ex) {
            Logger.getLogger(LuceneConnection.class.getName()).log(Level.SEVERE, null, ex);
            throw new SQLException(ex);
        } catch (IOException ex) {
            Logger.getLogger(LuceneConnection.class.getName()).log(Level.SEVERE, null, ex);
            throw new SQLException(ex);
        } finally {
            if (is != null) try {
                    is.close();
                } catch (IOException ex) {
                    Logger.getLogger(LuceneConnection.class.getName()).log(Level.SEVERE, null, ex);
                    throw new SQLException(ex);
                }
            }

    }
}
