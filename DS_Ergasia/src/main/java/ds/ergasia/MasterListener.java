package ds.ergasia;
/* Authors:
Petros Stavropoulos - 3150230
Kwstas Savvidis     - 3150229
Erasmia Kornelatou  - 3120076
 */

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;

// The listener of the Master, which listens for incoming connections from Workers
// who connect after the starting Worker connections
public class MasterListener extends Thread{

    // The threads which wait to be added to the master
    ArrayList<Thread> awaitingConnections;

    // The server socket
    ServerSocket providerSocket;

    // The number of workers of the master
    int numOfWorkers;

    // The timeout of each worker
    int workerTimeout;

    // The alpha constant used in the calculations
    int alpha;
    // The lambda constant used in the calculations
    double lambda;

    // Boolean to terminate the listener
    boolean quit = false;

    // Boolean to indicate if there are new workers added
    boolean newWorkers;

    // MasterListener's Parametrized Constructor
    public MasterListener(ServerSocket providerSocket, int numOfWorkers, int workerTimeout, int alpha, double lambda) {
        this.providerSocket = providerSocket;
        this.numOfWorkers = numOfWorkers;
        this.newWorkers = false;
        this.workerTimeout = workerTimeout;
        this.alpha = alpha;
        this.lambda = lambda;

        awaitingConnections = new ArrayList<>();
    }

    // The MasterListener thread run method
    public void run(){

            try {

                // While quit is false
                while(!quit) {

                    // Accept an incoming connection
                    Socket connection = providerSocket.accept();

                    // Indicate that there are new Workers waiting
                    newWorkers = true;

                    // Print message to console
                    System.out.println("Listener: Accepted connection from: " + connection.getInetAddress() + "@" + connection.getPort());

                    // Create a new MasterWorkerThread with the connection's and worker's info
                    Thread worker = new MasterWorkerThread(numOfWorkers,"Worker_"+numOfWorkers, connection,workerTimeout,alpha,lambda);

                    // Add it to the waiting connections List
                    awaitingConnections.add(worker);

                    // Start the MasterWorkerThread
                    worker.start();

                    // Increase the number of Workers that the Listener knows
                    numOfWorkers++;

                }

            } catch (SocketException e){
                // There was a Socket Exception

                // If quit is true
                if(quit){
                    // Print message to console
                    System.out.println("Listener terminated.");
                }else{
                    // Print the stack trace for the exception
                    e.printStackTrace();
                }
            } catch (IOException e) {
                // There was an other IOException

                // Print the stack trace for the exception
                e.printStackTrace();
            }

    }

    // Method that returns the arraylist of the Worker threads waiting to be added to Master
    public ArrayList<Thread> getConnectionsWaiting(){

        // Get the first Worker's id for renaming and to indicate what the new number of workers will be
        int first_id = ((MasterWorkerThread)awaitingConnections.get(0)).id;

        // Check which of the connections are still alive
        // Initialize the boolean workerDC to false
        boolean workerDC = false;
        // Create an arraylist for the 'to-be-removed' Worker threads
        ArrayList<Thread> removeList = new ArrayList<>();
        // For every thread in the awaitingConnections
        for(Thread t : awaitingConnections){
            // Cast thread to MasterWorkerThread
            MasterWorkerThread worker = (MasterWorkerThread)t;
            // Ping worker
            worker.sendPing();
            // If worker is not alive
            if(!worker.alive){
                // Print message to console
                System.out.println("Listener: " + worker.name + " not responding to PING.");
                // Indicate that we have a disconnected worker
                workerDC = true;
                // Add the thread to the 'to-be-removed' list
                removeList.add(t);
                // Terminate the worker
                worker.terminate();
            }
        }
        // If there was a disconnected Worker
        if(workerDC) {
            // Remove all the Workers with lost connections
            awaitingConnections.removeAll(removeList);

            // Rename all workers in the listener
            // For every thread in the awaitingConnections
            for (int i = 0; i < awaitingConnections.size(); i++) {
                // Cast the thread to a MasterWorkerThread
                MasterWorkerThread worker = (MasterWorkerThread) awaitingConnections.get(i);
                // Change the worker's id using the first Worker's id from above
                worker.id = first_id + i;
                // Print message to console
                System.out.print("Listener: Renaming " + worker.name + " to ");
                // Change the worker's name using it's new id
                worker.name = "Worker_" + worker.id;
                // Print message to console
                System.out.println(worker.name);
            }
        }

        // Find the correct numOfWorkers
        numOfWorkers = first_id + awaitingConnections.size();

        // Copy the awaitingConnections to a new list
        ArrayList<Thread> connectionsWaiting = new ArrayList<>(awaitingConnections);

        // Clear the awaitingConnections
        awaitingConnections.clear();

        // Reset newWorkers to false
        newWorkers = false;

        // Return the connectionsWaiting (copy of awaitingConnections) list
        return connectionsWaiting;
    }

    // Method to terminate the Listener -- set quit to true
    public void terminateListener(){
        quit = true;
    }

}
