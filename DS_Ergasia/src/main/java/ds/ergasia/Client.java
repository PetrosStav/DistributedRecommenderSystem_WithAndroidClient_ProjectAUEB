package ds.ergasia;
/* Authors:
Petros Stavropoulos - 3150230
Kwstas Savvidis     - 3150229
Erasmia Kornelatou  - 3120076
 */

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Scanner;

// Class for Client
// The Client connects to the Master and asks for the K-best POI's for a specific user
// used for testing purposes -- the real client will be for Android
public class Client {
    // The Client's main method
    public static void main(String[] args) {

        // Initialize the Socket for the connection to null
        Socket requestSocket = null;
        // Initialize a ObjectInputStream object to null
        ObjectInputStream in = null;
        // Initialize a ObjectOutputStream object to null
        ObjectOutputStream out = null;

        // The host to connect
        String host;
        // The port number to connect
        int portNum;

        // If there are not console arguments
        if(args.length ==0) {
            // Set default host
            host = "127.0.0.1";
            // Set default port number
            portNum = 4200;
        }else{
            // Else first argument is the host
            host = args[0];
            // Second argument is the port number
            portNum = Integer.parseInt(args[1]);
        }

        // Create a Scanner
        Scanner sc = new Scanner(System.in);
        // Declare ints u for user and k for the number of results
        int u,k;
        // Initialize a list of pois to null
        List<Poi> kBestPois = null;

        // Print message to console
        System.out.print("Enter user id >");
        // Read u from scanner
        u = sc.nextInt();
        // Print message to console
        System.out.print("Enter K >");
        // Read k from scanner
        k = sc.nextInt();

        // Try to create connection with master/server
        try {

            // Print message to console
            System.out.println("Connecting to: " + host + "@" + portNum);
            // Connect to host and port number given
            requestSocket = new Socket(host, portNum);

            // Create an ObjectOutputStream from the connection
            out = new ObjectOutputStream(requestSocket.getOutputStream());
            // Create an ObjectInputStream from the connection
            in = new ObjectInputStream(requestSocket.getInputStream());

            // Print message to console
            System.out.println("Connected to Master.");

            // Send user id
            out.writeInt(u);
            out.flush();

            // Send K
            out.writeInt(k);
            out.flush();

            // Wait for Master's answer and assign it to the POIs list
            kBestPois = (List<Poi>)in.readObject();

            // For each POI in the list
            for(Poi p : kBestPois){
                // Print the POI to console
                System.out.println(p);
            }


        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally{
            // Try to close the input and output object streams
            // as well as the Socket connection
            try {
                in.close();
                out.close();
                requestSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


    }
}
