/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package es.uc3m.it.mapstore.db.transaction.xa.impl.torrent;

/**
 *
 * @author Pablo
 */
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class HTTPServer {
    private HttpServer server;

    public HTTPServer(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", new Handler());
        server.start();
    }

    public void shutdown() {
        server.stop(5);
        server = null;
    }

}

class Handler implements HttpHandler {
  public void handle(HttpExchange xchg) throws IOException {
    Headers headers = xchg.getRequestHeaders();
    Set<Map.Entry<String, List<String>>> entries = headers.entrySet();

    StringBuffer response = new StringBuffer();
    for (Map.Entry<String, List<String>> entry : entries)
      response.append(entry.toString() + "\n");

    xchg.sendResponseHeaders(200, response.length());
    OutputStream os = xchg.getResponseBody();
    os.write(response.toString().getBytes());
    os.close();
  }
}
