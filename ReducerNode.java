package org.example;

import org.zeromq.ZMQ;
import org.zeromq.SocketType;
import java.util.HashMap;
import java.util.Map;

public class ReducerNode {
    public static void main(String[] args) {
        // Creating a new ZeroMQ context with one I/O thread
        ZMQ.Context context = ZMQ.context(1);

        // Creating a PULL socket to receive data from the MapperNode
        ZMQ.Socket receiver = context.socket(SocketType.PULL);
        receiver.connect("tcp://localhost:6778");  // Connect to the MapperNode at port 6778
        System.out.println("ReducerNode: Connected to MapperNode on port 6778.");

        // Creating a PUSH socket to send data to the MasterNode
        ZMQ.Socket sender = context.socket(SocketType.PUSH);
        sender.connect("tcp://localhost:6779");  // Connect to the MasterNode at port 6779
        System.out.println("ReducerNode: Connected to MasterNode on port 6779.");

        try {
            System.out.println("ReducerNode: Sockets successfully bound to ports.");
            Map<String, Integer> wordCounts = new HashMap<>();

            while (!Thread.currentThread().isInterrupted()) {
                String data = receiver.recvStr().trim();  // Blocking call to receive a string message
                System.out.println("ReducerNode: Received data: '" + data + "'");

                // Checking if the "END" signal is received to break out of the loop
                if ("END".equals(data)) {
                    System.out.println("ReducerNode: Received END signal. Exiting loop.");
                    break;
                }

                // Splitting the received data into parts using ":" as a delimiter
                String[] parts = data.split(":");
                if (parts.length == 3 && "Word".equals(parts[0].trim())) {
                    try {
                        String word = parts[1].trim();
                        int count = Integer.parseInt(parts[2].trim());
                        wordCounts.put(word, wordCounts.getOrDefault(word, 0) + count);
                        System.out.println("ReducerNode: Updated count for word '" + word + "': " + wordCounts.get(word));
                    } catch (NumberFormatException e) {
                        System.err.println("ReducerNode: Error parsing count: " + e.getMessage());
                    }
                } else {
                    System.out.println("ReducerNode: Invalid data format received: '" + data + "'");
                }
            }

            // Sending the final aggregated word counts to the MasterNode
            if (!wordCounts.isEmpty()) {
                for (Map.Entry<String, Integer> entry : wordCounts.entrySet()) {
                    String result = entry.getKey() + ": " + entry.getValue();
                    sender.send(result);
                    System.out.println("ReducerNode: Sent word count to master: " + result);
                }

                int totalWordCount = wordCounts.values().stream().mapToInt(Integer::intValue).sum();
                String totalCountMessage = "Total Word Count: " + totalWordCount;
                sender.send(totalCountMessage);
                System.out.println("ReducerNode: Sent total word count to master: " + totalCountMessage);
            } else {
                System.out.println("ReducerNode: No words to send to master.");
            }

            // Sending the "END" signal to the MasterNode
            sender.send("END");
            System.out.println("ReducerNode: Sent END signal to master.");
        } catch (Exception e) {
            System.err.println("Error in ReducerNode: " + e.getMessage());
            e.printStackTrace();
        } finally {
            receiver.close();
            sender.close();
            context.term();
            System.out.println("ReducerNode: Closed sockets and terminated context.");
        }
    }
}
