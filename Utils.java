package org.example;

import org.zeromq.ZMQ;
import java.util.Set;
import java.util.HashSet;

public class Utils {
    private static final Set<String> TRANSPORTS = new HashSet<>();

    static {
        TRANSPORTS.add("udp");
        TRANSPORTS.add("tcp");
        TRANSPORTS.add("ipc");
        TRANSPORTS.add("inproc");
    }

    /* Method for creating a ZeroMQ address automatically and centrally
       to avoid manual string creation every time a socket is created or connected. */
    public static String zmqAddr(int port, String transport, String host) {
        if (host == null) {
            host = "127.0.0.1";
        }

        if (transport == null) {
            transport = "tcp";
        }

        if (!TRANSPORTS.contains(transport)) {
            throw new IllegalArgumentException("Invalid transport type");
        }

        if (port <= 1000 || port >= 10000) {
            throw new IllegalArgumentException("Port must be between 1000 and 10000");
        }

        return String.format("%s://%s:%d", transport, host, port);
    }

    // Method to create a ZeroMQ socket in bind mode using PUSH (for the master node)
    public static ZMQ.Socket createBoundSocket(ZMQ.Context context, int port, String transport, String host) {
        String address = zmqAddr(port, transport, host);
        ZMQ.Socket socket = context.socket(ZMQ.PUSH);  // Using PUSH socket for sending messages
        socket.bind(address); // The socket listens for incoming connections on the specified port
        return socket;
    }

    // Method to create a ZeroMQ socket in connect mode using PULL (for worker nodes)
    public static ZMQ.Socket createConnectedSocket(ZMQ.Context context, int port, String transport, String host) {
        String address = zmqAddr(port, transport, host);
        ZMQ.Socket socket = context.socket(ZMQ.PULL);  // Using PULL socket for receiving messages
        socket.connect(address); // Connects to a socket that has been bound to an address
        return socket;
    }
}

