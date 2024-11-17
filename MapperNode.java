package org.example;

import org.zeromq.ZMQ;
import org.zeromq.SocketType;
import java.util.StringTokenizer;

public class MapperNode {
    public static void main(String[] args) {
        ZMQ.Context context = ZMQ.context(1);

        // Creating a PULL socket to receive data from the MasterNode
        ZMQ.Socket receiver = context.socket(SocketType.PULL);
        receiver.connect("tcp://localhost:6777"); // Connect to the MasterNode at port 6777
        System.out.println("MapperNode: Connected to MasterNode on port 6777.");

        // Creating a PUSH socket to send data to the ReducerNode
        ZMQ.Socket sender = context.socket(SocketType.PUSH);
        sender.connect("tcp://localhost:6778"); // Connect to the ReducerNode at port 6778
        System.out.println("MapperNode: Connected to ReducerNode on port 6778.");

        try {
            while (!Thread.currentThread().isInterrupted()) {
                String line = receiver.recvStr().trim();  // Blocking call to receive data from MasterNode
                System.out.println("MapperNode: Received line from master: " + line);

                // Check for the "END" signal from the MasterNode
                if ("END".equals(line)) {
                    System.out.println("MapperNode: Received END signal. Exiting.");
                    break;
                }

                // Tokenizing the line into words and send each word to the ReducerNode
                StringTokenizer tokenizer = new StringTokenizer(line);
                while (tokenizer.hasMoreTokens()) {
                    String word = tokenizer.nextToken().toLowerCase();
                    String message = "Word:" + word + ":1";  // Send in "Word:<word>:1" format
                    sender.send(message);
                    System.out.println("MapperNode: Sent word to reducer: " + message);
                }

                // Adding a small delay to avoid overwhelming the reducer
                Thread.sleep(50);
            }

            // Sending the "END" signal to the ReducerNode
            sender.send("END");
            System.out.println("MapperNode: Sent END signal to reducer.");
            Thread.sleep(100);  // Small delay to ensure the reducer processes the end signal
        } catch (Exception e) {
            System.err.println("Error in MapperNode: " + e.getMessage());
            e.printStackTrace();
        } finally {
            receiver.close();
            sender.close();
            context.term();
            System.out.println("MapperNode: Closed sockets and terminated context.");
        }
    }
}
