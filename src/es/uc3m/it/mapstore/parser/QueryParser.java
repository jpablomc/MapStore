/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.parser;

import es.uc3m.it.mapstore.bean.MapStoreBasicCondition;
import es.uc3m.it.mapstore.bean.MapStoreComplexCondition;
import es.uc3m.it.mapstore.bean.MapStoreCondition;
import es.uc3m.it.mapstore.bean.MapStoreListCondition;
import es.uc3m.it.mapstore.bean.MapStoreTraverserDescriptor;
import es.uc3m.it.mapstore.parser.exception.ParserException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Pablo
 */
public class QueryParser {

    private static int LEFT_OPERATOR_IS_NULL = 0;
    private static  int LEFT_OPERATOR_IS_OR = 1;
    private static  int LEFT_OPERATOR_IS_AND = 2;

    private static String PATTERN_DATE = "\\d\\d/\\d\\d/\\d\\d\\d\\d|\\d\\d/\\d\\d/\\d\\d";
    private static String PATTERN_NUMBER_INTEGER = "[\\d]+";
    private static String PATTERN_NUMBER_DECIMAL = "[\\d]+\\.[\\d]+";
    private static String PATTERN_NUMBER = PATTERN_NUMBER_DECIMAL
                                           + "|" +
                                           PATTERN_NUMBER_INTEGER
                                           ;
    private static String PATTERN_WORD = "[\\d\\w\\.]+";
    private static String PATTERN_PHRASE = "\""+"["+PATTERN_WORD+" ]*"+PATTERN_WORD+"\"";

    private static String PATTERN_RANGE_DATE = "\\[" + PATTERN_DATE + " ?, ?"+ PATTERN_DATE +"\\]";
    private static String PATTERN_RANGE_NUMBER = "\\[" + PATTERN_NUMBER + " ?, ?"+ PATTERN_NUMBER +"\\]";
    private static String PATTERN_RANGE_WORD = "\\[" + PATTERN_WORD + " ?, ?"+ PATTERN_WORD +"\\]";
    private static String PATTERN_RANGE_PHRASE = "\\[" + PATTERN_PHRASE + " ?, ?"+ PATTERN_PHRASE +"\\]";
    private static String PATTERN_RANGE = 
                                          PATTERN_RANGE_DATE+"|"+
                                          PATTERN_RANGE_NUMBER+"|"+
                                          PATTERN_RANGE_WORD+"|"+
                                          PATTERN_RANGE_PHRASE
                                          ;

    private static String PATTERN_LIST_DATE = "\\[(" + PATTERN_DATE + " ?, ?){2,}"+ PATTERN_DATE +"\\]";
    private static String PATTERN_LIST_NUMBER = "\\[(" + PATTERN_NUMBER + " ?, ?){2,}"+ PATTERN_NUMBER +"\\]";
    private static String PATTERN_LIST_WORD = "\\[(" + PATTERN_WORD + " ?, ?){2,}"+ PATTERN_WORD +"\\]";
    private static String PATTERN_LIST_PHRASE = "\\[(" + PATTERN_PHRASE + " ?, ?){2,}"+ PATTERN_PHRASE +"\\]";
    private static String PATTERN_LIST =
                                          PATTERN_LIST_DATE+"|"+
                                          PATTERN_LIST_NUMBER+"|"+
                                          PATTERN_LIST_WORD+"|"+
                                          PATTERN_LIST_PHRASE
                                          ;

    private static String PATTERN_ARRAY_DATE = "\\[(" + PATTERN_DATE + " ?, ?)+"+ PATTERN_DATE +"\\]";
    private static String PATTERN_ARRAY_NUMBER = "\\[(" + PATTERN_NUMBER + " ?, ?)+"+ PATTERN_NUMBER +"\\]";
    private static String PATTERN_ARRAY_WORD = "\\[(" + PATTERN_WORD + " ?, ?)+"+ PATTERN_WORD +"\\]";
    private static String PATTERN_ARRAY_PHRASE = "\\[(" + PATTERN_PHRASE + " ?, ?)+"+ PATTERN_PHRASE +"\\]";
    private static String PATTERN_ARRAY =
                                          PATTERN_ARRAY_DATE+"|"+
                                          PATTERN_ARRAY_NUMBER+"|"+
                                          PATTERN_ARRAY_WORD+"|"+
                                          PATTERN_ARRAY_PHRASE
                                          ;



    private static String PATTERN_PROPERTY = "[\\w_][\\d\\w]+";
    private static String PATTERN_ARRAY_PROPERTY = "\\[(" + PATTERN_PROPERTY + " ?, ?)+"+ PATTERN_PROPERTY +"\\]";

    private static String PATTERN_DIRECTION_OUTGOING = "\\-\\->";
    private static String PATTERN_DIRECTION_INCOMING = "<\\-\\-";
    private static String PATTERN_DIRECTION_ANY = "<\\->";

    private static String PATTERN_DIRECTION = PATTERN_DIRECTION_ANY +"|" +
                                              PATTERN_DIRECTION_INCOMING +"|" +
                                              PATTERN_DIRECTION_OUTGOING;

    private static String PATTERN_ARRAY_DIRECTION = "\\[(" + PATTERN_DIRECTION + " ?, ?)+"+ PATTERN_DIRECTION +"\\]";

    private static String PATTERN_DEPTH_ALGORITHM = "DEPTH";
    private static String PATTERN_BREADTH_ALGORITHM = "BREADTH";

    private static String PATTERN_TRAVERSER_ALGORITHM = PATTERN_DEPTH_ALGORITHM +"|" +
                                              PATTERN_BREADTH_ALGORITHM;



    private static String PATTERN_TRAVERSER_1 = "\\{"+
                                                PATTERN_PROPERTY + " ?, ?" +
                                                PATTERN_NUMBER_INTEGER +
                                                "\\}";

    private static String PATTERN_TRAVERSER_2 = "\\{"+
                                                PATTERN_ARRAY_PROPERTY + " ?, ?" +
                                                PATTERN_NUMBER_INTEGER +
                                                "\\}";

    private static String PATTERN_TRAVERSER_3 = "\\{"+
                                                PATTERN_ARRAY_PROPERTY + " ?, ?" +
                                                PATTERN_ARRAY_DIRECTION + " ?, ?" +
                                                PATTERN_NUMBER_INTEGER + " ?" +
                                                "\\}";

    private static String PATTERN_TRAVERSER_4 = "\\{"+
                                                PATTERN_ARRAY_PROPERTY + " ?, ?" +
                                                PATTERN_ARRAY_DIRECTION + " ?, ?" +
                                                PATTERN_NUMBER_INTEGER + " ?, ?" +
                                                PATTERN_NUMBER_INTEGER + " ?" +
                                                "\\}";

    private static String PATTERN_TRAVERSER_5 = "\\{"+
                                                PATTERN_ARRAY_PROPERTY + " ?, ?" +
                                                PATTERN_ARRAY_DIRECTION + " ?, ?" +
                                                PATTERN_NUMBER_INTEGER + " ?, ?" +
                                                PATTERN_NUMBER_INTEGER + " ?, ?" +
                                                PATTERN_TRAVERSER_ALGORITHM + " ?" +
                                                "\\}";


    private static String PATTERN_TRAVERSER = PATTERN_TRAVERSER_1 +"|" +
                                             PATTERN_TRAVERSER_2 +"|" +
                                             PATTERN_TRAVERSER_3 +"|" +
                                             PATTERN_TRAVERSER_4 +"|" +
                                             PATTERN_TRAVERSER_5
            ;


    private static String PATTERN_VALUE =
                                          PATTERN_TRAVERSER+"|"+
                                          PATTERN_LIST+"|"+
                                          PATTERN_RANGE+"|"+
                                          PATTERN_PHRASE+"|"+
                                          PATTERN_DATE+"|"+
                                          PATTERN_NUMBER+"|"+
                                          PATTERN_WORD
                                          ;
    private static String PATTERN_OP_EQUALS = "=";
    private static String PATTERN_OP_SIMILARITY = "~";    
    private static String PATTERN_OP_BIGGERTHAN = ">";
    private static String PATTERN_OP_BIGGEROREQUALSTHAN = ">=";
    private static String PATTERN_OP_LESSTHAN = "<";
    private static String PATTERN_OP_LESSOREQUALSTHAN = "<=";
    private static String PATTERN_OP_NOTEQUALS = "!=";
    private static String PATTERN_OP_IN = "IN ";
    private static String PATTERN_OP_BETWEEN = "BETWEEN ";
    private static String PATTERN_OP_RELATED = "\\->";
          
    private static String PATTERN_OPERATOR = PATTERN_OP_SIMILARITY+ "|" +
                                             PATTERN_OP_BIGGEROREQUALSTHAN+ "|" + PATTERN_OP_BIGGERTHAN+ "|" +
                                             PATTERN_OP_LESSOREQUALSTHAN+ "|" + PATTERN_OP_LESSTHAN+ "|" +
                                             PATTERN_OP_NOTEQUALS+ "|" + PATTERN_OP_EQUALS+ "|" +
                                             PATTERN_OP_BETWEEN+ "|" + PATTERN_OP_RELATED;

    // A condition will be :
    // C = PROP OP VALUE | (COND) OP VALUE

    private static boolean isMMDDYYYY = false;

    public static void setLocale(boolean isMMDDYYYY) {
        QueryParser.isMMDDYYYY = isMMDDYYYY;
    }



    public static MapStoreCondition queryToConditions(String query) {
        MapStoreCondition cond = queryToConditions(query, null);
        simplify(cond);
        return cond;
    }


    private static MapStoreCondition queryToConditions(String query,MapStoreListCondition leftCondition) {
        //Obtain the subject
        //The Subject could be a property ot a condition
        int end = -1;
        boolean subjectIsCondition = query.startsWith("(");
        if (subjectIsCondition) end = processBrackets(query);
        else end = getEndPattern(query, PATTERN_PROPERTY);        
        //Determine operator
        String subject = query.substring(0,end);
        System.out.println("S: "+ subject);
        String query2 = query.substring(end).trim();
        end = getEndPattern(query2,PATTERN_OPERATOR);
        String operator = query2.substring(0, end);
        System.out.println("O: "+ operator);
        String query3 = query2.substring(end).trim();
        if (query3.startsWith("{")) end = getEndPattern(query3, PATTERN_TRAVERSER);
        else end = getEndPattern(query3, PATTERN_VALUE);
        String predicate = query3.substring(0, end);
        System.out.println("P: "+ predicate);
        MapStoreCondition subjectCondition = null;
        if (subjectIsCondition) subjectCondition = queryToConditions(subject.substring(1,subject.length()-1),null);
        MapStoreCondition newCondition;
        int operatorInt = getOperator(operator);
        Object value = getObjectForValue(predicate);
        if (value.getClass().isArray()) {
            MapStoreListCondition cndtmp = new MapStoreListCondition(false);
            newCondition = cndtmp;
            Object[] aux = (Object[])value;
            for (Object a : aux) {
                if (subjectCondition == null) cndtmp.addCondition(new MapStoreBasicCondition(subject, a, operatorInt));
                else cndtmp.addCondition(new MapStoreComplexCondition(subjectCondition, (MapStoreTraverserDescriptor)a,operatorInt));
            }
        } else {
            if (subjectCondition == null) newCondition = new MapStoreBasicCondition(subject, value, operatorInt);
            else newCondition = new MapStoreComplexCondition(subjectCondition, (MapStoreTraverserDescriptor)value, operatorInt);
        }
        //Hemos creado la condición ahora toca tratar los elementos a izquierda (recibidos por parametro) y a derecha
        String query4= query3.substring(end).trim();
        MapStoreCondition returnedValue;
        if (query4.length()>0) {
            if (query4.toLowerCase().startsWith("and ")) {
                if (leftCondition == null) {
                    //Es la primera condición
                    MapStoreListCondition list = new MapStoreListCondition(true);
                    list.addCondition(newCondition);
                    returnedValue = queryToConditions(query4.substring("and ".length()), list);
                } else if (leftCondition.isAndList()) {
                    //Venia de un AND y va a un AND--> TODOS estan al mismo nivel de prioridad
                    leftCondition.addCondition(newCondition);
                    returnedValue = queryToConditions(query4.substring("and ".length()), leftCondition);
                } else {
                    //Venia de un or y pasa a un AND -> Cambia de nivel hacia abajo
                    MapStoreListCondition list = new MapStoreListCondition(true);
                    list.addCondition(newCondition);
                    returnedValue = queryToConditions(query4.substring("and ".length()), list);
                    leftCondition.addCondition(returnedValue);
                    returnedValue = leftCondition;
                }
            } else {
                if (query4.toLowerCase().startsWith("or ")) query4 = query4.substring("or ".length());
                if (leftCondition == null) {
                    //Es la primera condición
                    MapStoreListCondition list = new MapStoreListCondition(false);
                    list.addCondition(newCondition);
                    returnedValue = queryToConditions(query4,list);
                } else if (leftCondition.isAndList()) {
                    //Venia de un AND y va a un OR--> Cambio de nivel hacia arriba
                    leftCondition.addCondition(newCondition);
                    MapStoreListCondition list = new MapStoreListCondition(false);
                    list.addCondition(leftCondition);
                    returnedValue = queryToConditions(query4,list);
                } else {
                    //Venia de un or y pasa a un or -> Mantiene el nivel
                    leftCondition.addCondition(newCondition);
                    returnedValue = queryToConditions(query4,leftCondition);
                }
            }
        } else {
            //No hay mas que procesar
                if (leftCondition == null) {
                    returnedValue = newCondition;
                } else {
                    //Siempre se añadira a la lista que venga
                    leftCondition.addCondition(newCondition);
                    returnedValue = leftCondition;
                }
        }
        return returnedValue;
    }

    private static int getEndPattern(String query, String pattern) {
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(query);
        if (!m.find()) throw new ParserException("Pattern ("+ pattern +") not found on " +query);
        if (m.start() != 0) throw new ParserException("Pattern ("+ pattern +") not found at the beggining of the query " +query +
                "Pattern found was : " + query.substring(m.start(), m.end()));
        return m.end();
    }

    private static int processBrackets(String query) throws ParserException {
        int openBrackets = 1;
        int index = 1;
        int end = -1;
        while (end < 0) {
            //A condition... it ends with a bracket
            //TODO: Support to brackets inside quotation marks
            if (index >= query.length()) {
                throw new ParserException("Query is missing ')' character: " + query);
            }
            char aux = query.charAt(index);
            if (aux == ')') {
                openBrackets--;
            } else if (aux == '(') {
                openBrackets++;
            }
            index++;
            if (openBrackets == 0) {
                end = index;
            }
        }
        return end;
    }


    private static String OP_EQUALS = "=";
    private static String OP_SIMILARITY = "~";
    private static String OP_BIGGERTHAN = ">";
    private static String OP_BIGGEROREQUALSTHAN = ">=";
    private static String OP_LESSTHAN = "<";
    private static String OP_LESSOREQUALSTHAN = "<=";
    private static String OP_NOTEQUALS = "!=";
    private static String OP_IN = "IN ";
    private static String OP_BETWEEN = "BETWEEN ";
    private static String OP_RELATED = "->";
    

    private static int getOperator(String operator) {
        int result;
        if (OP_EQUALS.equals(operator)) result = MapStoreCondition.OP_EQUALS;
        else if (OP_SIMILARITY.equals(operator)) result = MapStoreCondition.OP_SIMILARITY;
        else if (OP_BIGGERTHAN.equals(operator)) result = MapStoreCondition.OP_BIGGERTHAN;
        else if (OP_BIGGEROREQUALSTHAN.equals(operator)) result = MapStoreCondition.OP_BIGGEROREQUALSTHAN;
        else if (OP_LESSTHAN.equals(operator)) result = MapStoreCondition.OP_LESSTHAN;
        else if (OP_LESSOREQUALSTHAN.equals(operator)) result = MapStoreCondition.OP_LESSOREQUALSTHAN;
        else if (OP_NOTEQUALS.equals(operator)) result = MapStoreCondition.OP_NOTEQUALS;
        else if (OP_IN.equals(operator)) result = MapStoreCondition.OP_IN;
        else if (OP_BETWEEN.equals(operator)) result = MapStoreCondition.OP_BETWEEN;
        else result = MapStoreCondition.OP_RELATED;
        return result;
    }

    private static Object getObjectForValue(String predicate) {
        return getObjectForValuePattern(predicate);
    }

    private static Object getObjectForValuePattern(String predicate) {
        Object toReturn;
        Pattern p = Pattern.compile(PATTERN_TRAVERSER);
        Matcher m = p.matcher(predicate);
        if (m.find() && m.start() == 0) {
            toReturn = getObjectForValuePattern_1(predicate);
        } else toReturn = getObjectForValueList(predicate);
        return toReturn;        
    }

    private static Object getObjectForValueList(String predicate) {
        Object toReturn;
        Pattern p = Pattern.compile(PATTERN_LIST);
        Matcher m = p.matcher(predicate);
        if (m.find() && m.start() == 0) {
            Map<Class, List<Object>> mapa = new HashMap<Class,List<Object>>();
            String[] splitted = predicate.substring(1, predicate.length()-1).split(",");
            for (String aux: splitted) {
                Object result = getObjectForValuePhrase(aux.trim());
                if (!result.getClass().isArray()) {
                    result = new Object[]{result};
                }
                //Los resultados estan en forma de array... dentro de cada array solo hay u objeto por clase
                for (Object a: (Object[])result) {
                    List<Object> lista= mapa.get(a.getClass());
                    if (lista == null) {
                        lista = new ArrayList<Object>();
                        mapa.put(a.getClass(), lista);
                    }
                    lista.add(a);
                }
            }
            Object[] tmp = new Object[mapa.size()];
            int index = 0;
            for (Class c : mapa.keySet()) {
                tmp[index] = mapa.get(c);
                index++;
            }
            if (tmp.length == 1) toReturn = tmp[0];
            else toReturn = tmp;
        } else toReturn = getObjectForValueRange(predicate);
        return toReturn;
    }

    private static Object getObjectForValueRange(String predicate) {
        Object toReturn;
        Pattern p = Pattern.compile(PATTERN_RANGE);
        Matcher m = p.matcher(predicate);
        if (m.find() && m.start() == 0) {
            Map<Class, List<Object>> mapa = new HashMap<Class,List<Object>>();
            String[] splitted = predicate.substring(1, predicate.length()-1).split(",");
            for (String aux: splitted) {
                Object result = getObjectForValuePhrase(aux.trim());
                if (!result.getClass().isArray()) {
                    result = new Object[]{result};
                }
                //Los resultados estan en forma de array... dentro de cada array solo hay u objeto por clase
                for (Object a: (Object[])result) {
                    List<Object> lista= mapa.get(a.getClass());
                    if (lista == null) {
                        lista = new ArrayList<Object>();
                        mapa.put(a.getClass(), lista);
                    }
                    lista.add(a);
                }
            }
            Object[] tmp = new Object[mapa.size()];
            int index = 0;
            for (Class c : mapa.keySet()) {
                tmp[index] = mapa.get(c);
                index++;
            }
            if (tmp.length == 1) toReturn = tmp[0];
            else toReturn = tmp;
        } else toReturn = getObjectForValuePhrase(predicate);
        return toReturn;
    }

    private static Object getObjectForValuePhrase(String predicate) {
        Object toReturn;
        Pattern p = Pattern.compile(PATTERN_PHRASE);
        Matcher m = p.matcher(predicate);
        if (m.find() && m.start() == 0) {
            //Debemos comprobar si es un numero o una fecha
            Number n = null;
            try {
                n =TextToNumber.toNumber(predicate);
            } catch (NumberFormatException e) {/* No es número*/ }
            Date d = null;
            try {
            d = TextToDate.toDate(predicate);
            } catch (ParserException e) {/*No es fecha*/}
            if (n == null && d == null) toReturn = predicate;
            else if (n== null && d != null) toReturn = new Object[]{predicate, d};
            else if (n!= null && d== null) toReturn = new Object[]{predicate, n};
            else toReturn = new Object[]{predicate, d, n}; //Caso imposible
        } else toReturn = getObjectForValueDate(predicate);
        return toReturn;
    }

    private static Object getObjectForValueDate(String predicate) {
        Object toReturn;
        Pattern p = Pattern.compile(PATTERN_DATE);
        Matcher m = p.matcher(predicate);
        if (m.find() && m.start() == 0) {
            String aux = (isMMDDYYYY)?"MM/dd/yy":"dd/MM/yy";
            SimpleDateFormat df;
            if (predicate.length() == "dd/MM/yyyy".length()) df = new SimpleDateFormat(aux+"yy");
            else df = new SimpleDateFormat(aux);
            Date d;
            try {
                d = df.parse(predicate);
            } catch (ParseException ex) {
                throw new ParserException("Error parsing date: " + predicate);
            }
            toReturn = new Object[]{predicate,d};
        } else toReturn = getObjectForValueNumber(predicate);
        return toReturn;
    }

    private static Object getObjectForValueNumber(String predicate) {
        Object toReturn;
        Pattern p = Pattern.compile(PATTERN_NUMBER);
        Matcher m = p.matcher(predicate);
        if (m.find() && m.start() == 0) {
            List<Number> convertedValues = getConvertedNumberForString(predicate);
            Object[] tmp = new Object[convertedValues.size() +1 ];
            Object[] aux = convertedValues.toArray();
            System.arraycopy(aux, 0, tmp, 0, aux.length);
            tmp[tmp.length-1] = predicate;
            toReturn = tmp;
        } else toReturn = getObjectForValueWord(predicate);
        return toReturn;
    }

    private static List<Number> getConvertedNumberForString(String predicate) {
        Double d = Double.parseDouble(predicate);
        float f = d.floatValue();
        int i = d.intValue();
        long l = d.longValue();
        List<Number> result = new ArrayList<Number>();
        result.add(d);
        if (d.equals(new Double(f))) result.add(Float.valueOf(f));
        if (d.equals(new Double(i))) result.add(Integer.valueOf(i));
        if (d.equals(new Double(l))) result.add(Long.valueOf(l));
        return result;
    }


    private static Object getObjectForValueWord(String predicate) {
        Object result;
        Double n = null;
        try {
            n = TextToNumber.toNumber(predicate);
        } catch (NumberFormatException e) {/*No es transformable lo ignoramos}*/}
        if (n == null) result = predicate;
        else result = new Object[]{predicate,n};
        return result;
    }

    private static void simplify(MapStoreCondition cond) {
        if (cond instanceof MapStoreListCondition) {
            MapStoreListCondition aux = (MapStoreListCondition) cond;
            Set<MapStoreCondition> dependencies = aux.getRequieredConditions();
            for (MapStoreCondition cond2 : dependencies) {
                simplify(cond2);
                if (cond2 instanceof MapStoreListCondition) {
                    MapStoreListCondition aux2 = (MapStoreListCondition) cond2;
                    if (aux.isAndList() == aux2.isAndList()) {
                        aux.removeCondition(cond2);
                        for (MapStoreCondition cond3 : aux2.getRequieredConditions()) {
                            aux.addCondition(cond3);
                        }
                    }
                }
            }
        }
    }

    private static Object getObjectForValuePattern_1(String predicate) {
        Object toReturn;
        Pattern p = Pattern.compile(PATTERN_TRAVERSER_1);
        Matcher m = p.matcher(predicate);
        if (m.find() && m.start() == 0) {
            String[] splitted = predicate.substring(1, predicate.length()-1).split(",");
            //Must be an String and a Integer
            String property = splitted[0].trim();
            Integer distance = Integer.valueOf(splitted[1].trim());
            toReturn = new MapStoreTraverserDescriptor(property, distance);
        } else toReturn = getObjectForValuePattern_2(predicate);
        return toReturn;
    }

    private static Object getObjectForValuePattern_2(String predicate) {
        Object toReturn;
        Pattern p = Pattern.compile(PATTERN_TRAVERSER_2);
        Matcher m = p.matcher(predicate);
        if (m.find() && m.start() == 0) {
            //Obtain property list [prop1,prop2,...]
            String str = predicate;
            p = Pattern.compile(PATTERN_ARRAY_PROPERTY);
            m = p.matcher(str);
            if (!m.find()) throw new ParserException("Imposible case");
            int begin = m.start();
            int end = m.end();
            String propertyList = predicate.substring(begin, end);
            str = str.substring(end);
            // Obtain distance
            p = Pattern.compile(PATTERN_NUMBER_INTEGER);
            m = p.matcher(str);
            if (!m.find()) throw new ParserException("Imposible case");
            begin = m.start();
            end = m.end();
            String distanceStr = str.substring(begin, end);
            List<String> property = getPropertyList(propertyList);
            Integer distance = Integer.valueOf(distanceStr);
            toReturn = new MapStoreTraverserDescriptor(property, distance);
        } else toReturn = getObjectForValuePattern_3(predicate);
        return toReturn;
    }

    private static Object getObjectForValuePattern_3(String predicate) {
        Object toReturn;
        Pattern p = Pattern.compile(PATTERN_TRAVERSER_3);
        Matcher m = p.matcher(predicate);
        if (m.find() && m.start() == 0) {
            //Obtain property list [prop1,prop2,...]
            String str = predicate;
            p = Pattern.compile(PATTERN_ARRAY_PROPERTY);
            m = p.matcher(str);
            if (!m.find()) throw new ParserException("Imposible case");
            int begin = m.start();
            int end = m.end();
            String propertyList = str.substring(begin, end);
            str = str.substring(end);
            //Obtain firection list [-->,<->,...]
            p = Pattern.compile(PATTERN_ARRAY_DIRECTION);
            m = p.matcher(str);
            if (!m.find()) throw new ParserException("Imposible case");
            m.start();
            m.end();
            String directionList = str.substring(begin, end);
            str = str.substring(end);
            // Obtain distance
            p = Pattern.compile(PATTERN_NUMBER_INTEGER);
            m = p.matcher(str);
            if (!m.find()) throw new ParserException("Imposible case");
            begin = m.start();
            end = m.end();
            String distanceStr = str.substring(begin, end);
            List<String> property = getPropertyList(propertyList);
            List<Integer> directions = getDirectionList(directionList);
            Integer distance = Integer.valueOf(distanceStr);
            toReturn = new MapStoreTraverserDescriptor(property, directions, distance);
        } else toReturn = getObjectForValuePattern_4(predicate);
        return toReturn;
    }

    private static Object getObjectForValuePattern_4(String predicate) {
        Object toReturn;
        Pattern p = Pattern.compile(PATTERN_TRAVERSER_3);
        Matcher m = p.matcher(predicate);
        if (m.find() && m.start() == 0) {
            //Obtain property list [prop1,prop2,...]
            String str = predicate;
            p = Pattern.compile(PATTERN_ARRAY_PROPERTY);
            m = p.matcher(str);
            if (!m.find()) throw new ParserException("Imposible case");
            int begin = m.start();
            int end = m.end();
            String propertyList = str.substring(begin, end);
            str = str.substring(end);
            //Obtain firection list [-->,<->,...]
            p = Pattern.compile(PATTERN_ARRAY_DIRECTION);
            m = p.matcher(str);
            if (!m.find()) throw new ParserException("Imposible case");
            m.start();
            m.end();
            String directionList = str.substring(begin, end);
            str = str.substring(end);
            // Obtain distance min
            p = Pattern.compile(PATTERN_NUMBER_INTEGER);
            m = p.matcher(str);
            if (!m.find()) throw new ParserException("Imposible case");
            begin = m.start();
            end = m.end();
            String distanceMinStr = str.substring(begin, end);
            str = str.substring(end);
            // Obtain distance max
            p = Pattern.compile(PATTERN_NUMBER_INTEGER);
            m = p.matcher(str);
            if (!m.find()) throw new ParserException("Imposible case");
            begin = m.start();
            end = m.end();
            String distanceMaxStr = str.substring(begin, end);
            List<String> property = getPropertyList(propertyList);
            List<Integer> directions = getDirectionList(directionList);
            Integer distanceMin = Integer.valueOf(distanceMinStr);
            Integer distanceMax = Integer.valueOf(distanceMaxStr);
            toReturn = new MapStoreTraverserDescriptor(property, directions, distanceMin, distanceMax);
        } else toReturn = getObjectForValuePattern_5(predicate);
        return toReturn;
    }

    private static Object getObjectForValuePattern_5(String predicate) {
        Object toReturn;
        Pattern p = Pattern.compile(PATTERN_TRAVERSER_3);
        Matcher m = p.matcher(predicate);
        if (m.find() && m.start() == 0) {
            //Obtain property list [prop1,prop2,...]
            String str = predicate;
            p = Pattern.compile(PATTERN_ARRAY_PROPERTY);
            m = p.matcher(str);
            if (!m.find()) throw new ParserException("Imposible case");
            int begin = m.start();
            int end = m.end();
            String propertyList = str.substring(begin, end);
            str = str.substring(end);
            //Obtain firection list [-->,<->,...]
            p = Pattern.compile(PATTERN_ARRAY_DIRECTION);
            m = p.matcher(str);
            if (!m.find()) throw new ParserException("Imposible case");
            m.start();
            m.end();
            String directionList = str.substring(begin, end);
            str = str.substring(end);
            // Obtain distance min
            p = Pattern.compile(PATTERN_NUMBER_INTEGER);
            m = p.matcher(str);
            if (!m.find()) throw new ParserException("Imposible case");
            begin = m.start();
            end = m.end();
            String distanceMinStr = str.substring(begin, end);
            str = str.substring(end);
            // Obtain distance max
            p = Pattern.compile(PATTERN_NUMBER_INTEGER);
            m = p.matcher(str);
            if (!m.find()) throw new ParserException("Imposible case");
            begin = m.start();
            end = m.end();
            String distanceMaxStr = str.substring(begin, end);
            str = str.substring(end);
            // Obtain algorithm
            p = Pattern.compile(PATTERN_TRAVERSER_ALGORITHM);
            m = p.matcher(str);
            if (!m.find()) throw new ParserException("Imposible case");
            begin = m.start();
            end = m.end();
            String algorithm = str.substring(begin, end);
            List<String> property = getPropertyList(propertyList);
            List<Integer> directions = getDirectionList(directionList);
            Integer distanceMin = Integer.valueOf(distanceMinStr);
            Integer distanceMax = Integer.valueOf(distanceMaxStr);
            int search = getTraverserAlgorithm(algorithm);
            toReturn = new MapStoreTraverserDescriptor(property, directions, distanceMin, distanceMax, search);
        } else throw new ParserException("Imposible");
        return toReturn;
    }


    private static List<String> getPropertyList(String propertyList) {
        List<String> results = new ArrayList<String>();
        Pattern p = Pattern.compile(PATTERN_PROPERTY);
        Matcher m = p.matcher(propertyList);
        while (m.find()) {
            int begin = m.start();
            int end = m.end();
            results.add(propertyList.substring(begin, end));
        }
        return results;
    }

    private static List<Integer> getDirectionList(String directionList) {
        List<Integer> results = new ArrayList<Integer>();
        Pattern p = Pattern.compile(PATTERN_DIRECTION);
        Matcher m = p.matcher(directionList);
        while (m.find()) {
            int begin = m.start();
            int end = m.end();
            String aux = directionList.substring(begin, end);
            if (aux.matches(PATTERN_DIRECTION_ANY)) results.add(MapStoreTraverserDescriptor.DIRECTION_ANY);
            else if (aux.matches(PATTERN_DIRECTION_INCOMING)) results.add(MapStoreTraverserDescriptor.DIRECTION_FROM_SECOND_TO_FIRST);
            else if (aux.matches(PATTERN_DIRECTION_OUTGOING)) results.add(MapStoreTraverserDescriptor.DIRECTION_FROM_FIRST_TO_SECOND);
            else throw new ParserException("Imposible case");
        }
        return results;
    }

    private static int getTraverserAlgorithm(String algorithm) {
        int result;
        if (algorithm.matches(PATTERN_BREADTH_ALGORITHM)) result = MapStoreTraverserDescriptor.BREADTH_TRAVERSER;
        else if (algorithm.matches(PATTERN_DEPTH_ALGORITHM)) result = MapStoreTraverserDescriptor.DEPTH_TRAVERSER;
        else throw new ParserException("Imposible case");
        return result;
    }





}

