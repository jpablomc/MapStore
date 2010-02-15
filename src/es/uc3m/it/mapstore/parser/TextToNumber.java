/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author Pablo
 */
public class TextToNumber {
    private static final String[] ZERO = {"zero"};
    private static final String[] UNIDADES = {"one","two","three","four","five","six","seven","eight","nine"};
    private static final String[] DECENA = {"ten"};
    private static final String[] DECENAS = {"twenty","thirty","forty","fifty","sixty","seventy","eighty","ninety"};
    private static final String[] CENTENA = {"hundred"};
    private static final String[] SEPARATOR = {"thousand","million","billion","trillion"};
    private static final String[] SPECIAL = {"eleven","twelve","thirteen","fourteen","fifteen","sixteen","seventeen","eighteen","nineteen"};

//    private static List<String> special_tens;

//    static {
//        special_tens = Arrays.asList(UNIDADES);
//        special_tens.addAll(Arrays.asList(DECENA));
//        special_tens.addAll(Arrays.asList(SPECIAL));
//    }

/*
    private void toNumber(String text) {
        String[] terms = text.split(" ");
        StringBuffer sb = new StringBuffer();
        long MAX_SEPARATOR = Long.MIN_VALUE;
        int separator_position = Integer.MIN_VALUE;
        for (int i = 0;i<terms.length;i++) {

        }

    }

    private String[] getStringWithNumbers(String texto) {
        Set<String> separators = new HashSet<String>(Arrays.asList(SEPARATOR));
        Set<String> validWords = new HashSet<String>(Arrays.asList(UNIDADES));
        validWords.addAll(new HashSet<String>(Arrays.asList(ZERO)));
        validWords.addAll(new HashSet<String>(Arrays.asList(DECENA)));
        validWords.addAll(new HashSet<String>(Arrays.asList(DECENAS)));
        validWords.addAll(separators);
        validWords.addAll(new HashSet<String>(Arrays.asList(SPECIAL)));
        validWords.add("a");
        validWords.add("and");
        String[] words = texto.split(" ");
        int init_pos = Integer.MIN_VALUE;
        int end_pos = Integer.MIN_VALUE;
        for (int i = 0;i<DECENAS.length;i++) {
            String w = words[i];
            if (validWords.contains(w)) {
                //No esta iniciado el número
                if (init_pos == Integer.MIN_VALUE) {
                    //Si es la particula "a" comprobar que la siga un separador
                    if ("a".equals(w)) {
                        if (separators)
                    }
                }
            }
        }
        return null;
    }

    private String getRegularExpression() {
    }

    private void test() {
        List<String> 
    }
*/
/*
    public static String toText(double number) {
        List<String> auxStr = new ArrayList<String>();
        StringBuffer sb = new StringBuffer();
        //Tratar el signo
        if (number<0) {
            sb.append("minus ");
            number = number*-1;
        }
        int index = 0;
        do {
            int resto = (int)number%1000;
            StringBuffer aux = toTextLessThanAThousandNumber(resto);
            if (index>0 && aux.length()>0) {
                aux.append(" ").append(SEPARATOR[index-1]);
            }
            auxStr.add(aux.toString());
            number = number / 1000;
            index++;
        } while (number>0);
        for (int i = auxStr.size()-1;i>=0;i--) {
            sb.append(auxStr.get(i));
            if (i>0) sb.append(" ");
        }
        if (sb.length() == 0 ) sb.append(ZERO[0]);
        return sb.toString();
    }

    private static StringBuffer toTextLessThanAThousandNumber(int number) {
        StringBuffer sb = new StringBuffer();
        if (number>= 100) {
            int cociente = number/100;
            sb.append(UNIDADES[cociente-1]).append(" hundred");
            number = number%100;
            if (number > 0 ) sb.append(" and ");
        }
        if (number == 0) {
        } else if (number < 20) {
            sb.append(special_tens.get(number-1));
        } else {
            int cociente = number/10;
            sb.append(DECENAS[cociente-2]);
            number = number%10;
            if (number > 0) sb.append(" ").append(UNIDADES[number - 1]);
        }
        return sb;
    }
*/
    public static Double toNumber(String predicate) throws NumberFormatException {
        //TODO:To implement
        throw new NumberFormatException("Not implemented yet");
    }

    public static String toText(long l) {
        return Long.toString(l);
    }

}
