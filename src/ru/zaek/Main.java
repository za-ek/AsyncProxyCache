package ru.zaek;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;

/**
 * This program is an async cache proxy for "Data" data receiving.
 * Cache proxy work is based on standard blocking queue.
 * It makes a http server on SERVER_ADDR:SERVERP_PORT and accept connections on ENDPOINT.
 * Returns 200 if data accepted correctly.
 *
 * Class Data stores data received from client.
 *
 * Class Server is using for handle requests and fill queue.
 *  It uses CachedThreadPool as executor service so it able to take all
 *  available resources for threads.
 *
 * Class Consumer get data from queue and send to destination if
 *  destination is unreachable it wait TIMEOUT ms and retry.
 *
 */
public class Main {
    private static final String TARGET_ADDR = "http://127.0.0.1:1888/someEndpoint";

    private static final int TIMEOUT = 1000;
    private static final String SERVER_ADDR = "192.168.1.15";
    private static final String ENDPOINT = "/";
    private static final int SERVER_PORT = 4444;
    private static final int CONSUMER_THREADS = 1;


    public static void main(String[] args) throws IOException {
        LinkedBlockingQueue<Data> queue = new LinkedBlockingQueue<>();

        newFixedThreadPool(CONSUMER_THREADS).submit(new Consumer(queue));
        new Server(queue);
    }

    private static class Server {
        Server(Queue<Data> queue) throws IOException {
            HttpServer httpServer = HttpServer.create();

            // Careful! It can make infinite number of threads
            httpServer.setExecutor(newCachedThreadPool());
            // httpServer.setExecutor(newFixedThreadPool(10));

            httpServer.bind(new InetSocketAddress(SERVER_ADDR, SERVER_PORT), 0);
            httpServer.createContext(ENDPOINT, httpExchange -> {
                try {
                    if(queue.offer(new Data(httpExchange))) {
                        httpExchange.sendResponseHeaders(200, 0);
                    } else {
                        // @ToDo
                        // Something to do if queue is full
                        httpExchange.sendResponseHeaders(503, 0);
                    }
                } catch (IOException e) {
                    // Parsing error
                    httpExchange.sendResponseHeaders(400, 0);
                }

                httpExchange.close();
            });
            httpServer.start();
        }
    }

    private static class Consumer implements Runnable {
        BlockingQueue<Data> queue;
        Consumer(BlockingQueue<Data> queue) {
            this.queue = queue;
        }

        private boolean sendData(Data data) {
            HttpRequest httpRequest = HttpRequest
                    .newBuilder(URI.create(TARGET_ADDR))
                    .POST(data.bodyPublisher())
                    .build();
            try {
                HttpClient httpClient = HttpClient.newHttpClient();
                HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
                if(response.statusCode() != 200) {
                    throw new IOException();
                }
            } catch (IOException | InterruptedException e) {
                return false;
            }

            return true;
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Data data = queue.take();
                    if(!sendData(data)) {
                        queue.add(data);
                        Thread.sleep(TIMEOUT);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Data structure
     */
    private static class Data {
        byte[] n;
        Data(HttpExchange httpExchange) throws IOException {
            n = httpExchange.getRequestBody().readAllBytes();
        }

        public HttpRequest.BodyPublisher bodyPublisher() {
            return HttpRequest.BodyPublishers.ofByteArray(n);
        }
    }
}
