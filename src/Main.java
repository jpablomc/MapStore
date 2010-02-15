
import es.uc3m.it.mapstore.bean.MapStoreBasicCondition;
import es.uc3m.it.mapstore.bean.MapStoreItem;
import es.uc3m.it.mapstore.bean.MapStoreListCondition;
import es.uc3m.it.mapstore.bean.annotations.Name;
import es.uc3m.it.mapstore.bean.annotations.Type;
import es.uc3m.it.mapstore.db.impl.MapStoreSession;
import es.uc3m.it.mapstore.transformers.exception.UnTransformableException;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.LockObtainFailedException;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author Pablo
 */
public class Main {

        private long prop1;
        private int prop2;
        private Main relatedTo;
        @Name(order = 0)
        private String name;        
        private String texto;
        private List<Integer> numeros;
        private List<String> caracteres;


        private Main(String name,String texto) {
            prop1 = 10;
            prop2 = 10;
            this.name = name;
            this.texto = texto;
            numeros = Arrays.asList(new Integer[]{1,2,3,5,7,11});
            caracteres = Arrays.asList(new String[]{"1","2a","a3","b",""});
        }

        public String getName() {
            return name;
        }

        public long getProp1() {
            return prop1;
        }

        public int getProp2() {
            return prop2;
        }

        public Main getRelatedTo() {
            return relatedTo;
        }

         public String getTexto() {
            return texto;
        }

        public void setTexto(String texto) {
            this.texto = texto;
        }

    public List<Integer> getNumeros() {
        return numeros;
    }

    public void setNumeros(List<Integer> numeros) {
        this.numeros = numeros;
    }

    public List<String> getCaracteres() {
        return caracteres;
    }

    public void setCaracteres(List<String> caracteres) {
        this.caracteres = caracteres;
    }

    public static void main(String args[]) throws Exception {
        testBeginTransaction();
        recoverAll();
        //testSearchByName();

        test();
    }

    public static void testBeginTransaction() throws UnTransformableException {
        MapStoreSession instance = MapStoreSession.getSession();
        if (instance.findByNameType("Fulano", Main.class.getName()) != null) return;
        Main p1 = new Main("Fulano","En un lugar de la Mancha de cuyo nombre no quiero acordarme");
        Main p2 = new Main("Mengano","En el principio creó Dios los cielos y la tierra.");
        Main p3 = new Main("Zutano","EL espacio la última frontera");
        p1.relatedTo = p2;
        instance.beginTransaction();
        instance.save(p1);
        instance.commit();

        instance.close();
        instance = MapStoreSession.getSession();
        instance.beginTransaction();
        p1.relatedTo = p3;
        instance.update(p1);
        instance.commit();
        instance.close();
        instance = MapStoreSession.getSession();
        instance.beginTransaction();
        MapStoreItem toDelete = instance.findByNameType(p2);
        instance.delete(toDelete.getId());
        instance.commit();
        instance.close();
    }

    public static void recoverAll() {
        MapStoreSession instance = MapStoreSession.getSession();
        instance.beginTransaction();
        instance.getAll();
        instance.close();
    }

    public static void testSearchByName() throws SQLException, CorruptIndexException, LockObtainFailedException, IOException {

        MapStoreSession instance = MapStoreSession.getSession();
        MapStoreItem i = instance.findByNameType("Mengano", Main.class.getName());
        for (String key : i.getProperties().keySet()) {
            Object value = i.getProperty(key);
            System.out.println("Key: "+ key + " value: " + value.toString());
        }
        instance.close();
    }

    private static void test() {
        MapStoreSession instance = MapStoreSession.getSession();

        MapStoreBasicCondition c1 = new MapStoreBasicCondition("_ID", Arrays.asList(new Double[]{1.0,3.0}), MapStoreBasicCondition.OP_BETWEEN);
        MapStoreBasicCondition c2 = new MapStoreBasicCondition("_ID", new Double(4), MapStoreBasicCondition.OP_BIGGEROREQUALSTHAN);
        MapStoreBasicCondition c3 = new MapStoreBasicCondition("_ID", instance, MapStoreBasicCondition.OP_BIGGERTHAN);
        MapStoreBasicCondition c4 = new MapStoreBasicCondition("texto", "la", MapStoreBasicCondition.OP_EQUALS);
        MapStoreBasicCondition c5 = new MapStoreBasicCondition("_ID", instance, MapStoreBasicCondition.OP_IN);
        MapStoreBasicCondition c6 = new MapStoreBasicCondition("_ID", instance, MapStoreBasicCondition.OP_LESSOREQUALSTHAN);
        MapStoreBasicCondition c7 = new MapStoreBasicCondition("_ID", instance, MapStoreBasicCondition.OP_LESSTHAN);
        MapStoreBasicCondition c8 = new MapStoreBasicCondition("texto", "lugar", MapStoreBasicCondition.OP_NOTEQUALS);
        MapStoreBasicCondition c9 = new MapStoreBasicCondition("texto", "un lugar", MapStoreBasicCondition.OP_PHRASE);
        MapStoreBasicCondition c10 = new MapStoreBasicCondition(null, instance, MapStoreBasicCondition.OP_RELATED);
        MapStoreBasicCondition c11 = new MapStoreBasicCondition("texto", "lugar", MapStoreBasicCondition.OP_SIMILARITY);
        MapStoreListCondition conditions = new MapStoreListCondition(true);
        conditions.addCondition(c1);
        conditions.addCondition(c2);
        List<MapStoreItem> items = instance.query(conditions, null);
        for (MapStoreItem i : items) {
            for (String key : i.getProperties().keySet()) {
                Object value = i.getProperty(key);
                System.out.println("Key: "+ key + " value: " + value.toString());
            }
        }
        instance.close();

    }

    private static void NumberToText() {
/*
 long l = -999999999999999L;
        while (true) {
            String aux = TextToNumber.toText(l);
            System.out.println(l + " - " + aux);
            l++;
        }
 */
    }

}
