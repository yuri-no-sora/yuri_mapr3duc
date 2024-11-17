package org.example;

import org.zeromq.ZMQ;
import org.zeromq.SocketType;
import java.util.List;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class MasterNode {
    public static void main(String[] args) {
        // Creating a ZeroMQ context
        ZMQ.Context context = ZMQ.context(1);

        // Creating and binding a PUSH socket to send data to Mappers
        ZMQ.Socket mapperSender = context.socket(SocketType.PUSH);
        mapperSender.bind("tcp://*:6777"); // Port to send data to Mappers
        System.out.println("MasterNode: Bound to port 6777 for sending data to Mappers.");

        // Creating and binding a PULL socket to receive data from Reducers
        ZMQ.Socket reducerReceiver = context.socket(SocketType.PULL);
        reducerReceiver.bind("tcp://*:6779"); // Port to receive data from Reducers
        System.out.println("MasterNode: Bound to port 6779 for receiving data from Reducers.");

        try {
            // Reading the input file to send data to Mappers
            String inputFilePath = "input.txt"; // Path to the input file
            List<String> lines = Files.readAllLines(Paths.get(inputFilePath));
            for (String line : lines) {
                mapperSender.send(line); // Send each line to Mappers
                System.out.println("MasterNode: Sent to mapper: " + line);
            }

            // Sending the "END" signal to Mappers
            mapperSender.send("END");
            System.out.println("MasterNode: Sent END signal to Mappers.");

            // Receiving results from Reducers
            int totalWordCount = 0;
            while (!Thread.currentThread().isInterrupted()) {
                String result = reducerReceiver.recvStr().trim(); // Blocking call to receive data from Reducers
                if ("END".equals(result)) { // If "END" signal is received, break the loop
                    System.out.println("MasterNode: Received END signal from Reducer. Exiting.");
                    break;
                }

                System.out.println("MasterNode: Received from reducer: " + result);
                String[] parts = result.split(":");
                if (parts.length == 2 && "Total Word Count".equals(parts[0])) {
                    totalWordCount += Integer.parseInt(parts[1].trim());
                }
            }

            System.out.println("MasterNode: Final Total Word Count: " + totalWordCount);
        } catch (IOException e) {
            System.err.println("I/O error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up sockets and terminate the context
            mapperSender.close();
            reducerReceiver.close();
            context.term();
            System.out.println("MasterNode: Closed sockets and terminated context.");
        }
    }
}
