package ds.ergasia;
/* Authors:
Petros Stavropoulos - 3150230
Kwstas Savvidis     - 3150229
Erasmia Kornelatou  - 3120076
 */

import org.apache.commons.math3.linear.RealMatrix;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.CountDownLatch;

// Class for the Master's thread that connects the Master to a Worker
public class MasterWorkerThread extends Thread {

    // Worker's id
    int id;

    // Worker's name
    String name;

    // The Socket of the Master-Worker connection
    Socket connection;

    // Initialize the ObjectOutputStream to null
    ObjectOutputStream out = null;
    // Initialize the ObjectInputStream to null
    ObjectInputStream in = null;

    // The X Matrix from the Worker
    RealMatrix X;
    // The Y Matrix from the Worker
    RealMatrix Y;

    // A CountDownLatch object that synchronizes the Master with the MasterWorkerThreads
    // Initialize it to null
    CountDownLatch latch = null;

    // The Worker's free memory
    long freemem;
    // The Worker's CPU cores
    int cores;
    // The duration that took the Worker to calculate X
    long workerDurationX;
    // The duration that took the Worker to calculate Y
    long workerDurationY;
    // The timeout for the Worker -- used for ObjectInputStream read methods
    int workerTimeout;

    // The alpha constant for calculations
    int alpha;
    // The lambda constant for calculations
    double lambda;

    // Boolean to indicate if the MasterWorkerThread must terminate
    boolean terminate = false;
    // Boolean to indicate to wait for results for the X matrix
    boolean resultsX = false;
    // Boolean to indicate to wait for results for the Y matrix
    boolean resultsY = false;

     // Boolean to indicate if there was a connection error
    boolean pingErr = false;

    // Boolean to indicate if the connection is alive
    boolean alive = true;

    // Parametrized Constructor
    public MasterWorkerThread(int id,String name,Socket connection,int workerTimeout,int alpha,double lambda) {
        this.id = id;
        this.name = name;
        this.connection = connection;
        this.workerTimeout = workerTimeout;
        this.alpha = alpha;
        this.lambda = lambda;
        // Try to create the input and output object streams from the connection
        try {
            out = new ObjectOutputStream(connection.getOutputStream());
            in = new ObjectInputStream(connection.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    // Thread's run method
    public void run() {
        try {
            // Set timeout for the answer to 5 seconds
            connection.setSoTimeout(5*1000);

            // Read the free memory from the Worker
            freemem = in.readLong();
            // Print it to console
            System.out.println(name + ": " + "Free Memory: " + freemem);

            // Read the Worker's cores
            cores = in.readInt();
            // Print it to console
            System.out.println(name + ": " + "Available Cores: " + cores);

            // Disable timeout for the connection
            connection.setSoTimeout(0);

            // Synchronized block for wait() and notify()
            synchronized (this) {
                // This will be the command centre

                // While terminate boolean is false
                while (!terminate) {
                    // Wait for notify() -- from Master using object methods
                    wait();
                    // Thread is awake
                    // If the resultsX boolean is true we wait for the X matrix from the Worker
                    if (resultsX) {
                        // Initialize an object to null
                        Object obj = null;
                        try {
                            // Set the timeout for the answer
                            connection.setSoTimeout(workerTimeout);
                            // Read the answer and set it to the object obj
                            obj = in.readObject();
                            // Skip all "PONG" answers , which happens if the Worker was sent "PING" messages more than 1 time
                            while(obj instanceof String && ((String)obj).equals("PONG")) obj = in.readObject();
                            // Cast the object to a RealMatrix and assign it to X
                            X = (RealMatrix)obj;
                            // Read the duration for X from Worker
                            workerDurationX = in.readLong();
                            // Disable timeout for the connection
                            connection.setSoTimeout(0);
                            // Print message to console
                            System.out.println("Result X from " + name);
                            // Reset resultsX to false
                            resultsX = false;

                            // Wait for master to reach the latch -- needed only if the R matrix is very small
                            sleep(20);

                            // Notify master that the job is done
                            // by counting down the latch
                            latch.countDown();
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    } else if (resultsY) {
                        // If the resultsY boolean is true we wait for the Y matrix from the Worker

                        // Initialize an object to null
                        Object obj = null;
                        try {

                            // Set the timeout for the answer
                            connection.setSoTimeout(workerTimeout);
                            // Read the answer and set it to the object obj
                            obj = in.readObject();
                            // Skip all "PONG" answers , which happens if the Worker was sent "PING" messages more than 1 time
                            while(obj instanceof String && ((String)obj).equals("PONG")) obj = in.readObject();
                            // Cast the object to a RealMatrix and assign it to Y
                            Y = (RealMatrix)obj;
                            // Read the duration for Y from Worker
                            workerDurationY = in.readLong();
                            // Disable timeout for the connection
                            connection.setSoTimeout(0);
                            // Print message to console
                            System.out.println("Result Y from " + name);
                            // Reset resultsY to false
                            resultsY = false;

                            // Wait for master to reach the latch -- needed only if the R matrix is very small
                            sleep(20);

                            // Notify master that the job is done
                            // by counting down the latch
                            latch.countDown();
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                        }
                    } else if (pingErr){
                        // If there was an error with the connection -- ping error
                        // throw a SocketTimeoutException
                        throw new SocketTimeoutException();
                    }
                }
            }

        } catch (SocketException e) {
            // If there is a socket exception
            // Print message to console
            System.out.println(name +" ended unexpectedly.");
        } catch (SocketTimeoutException e){
            // If there is a socket timeout exception
            // Print message to console
            System.out.println(name + " timeout.");
        }catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally{
            try {
                // If there isn't a problem with the connection
                // try to close input and output object streams
                if(!pingErr) {
                    in.close();
                    out.close();
                }
                // If terminate is false
                if(!terminate) {
                    // If the connection is alive
                    if(alive) {
                        // Socket connection ended or timeout
                        // Print message to console
                        System.out.println(name + " not responding.");

                        // Set alive to false
                        alive = false;
                    }

                    // Wait 3 seconds for other dc's and Master
                    sleep(3000);

                    // Count down the Master's latch as if it has finished
                    latch.countDown();

                }

            } catch (IOException ioe) {
                ioe.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    // Method to terminate the MasterWorkerThread
    public void terminate(){
        // Synchronized block for wait() and notify()
        synchronized (this) {
            try {
                // Set terminate to true
                terminate = true;
                // If the connection is alive
                if(alive) {
                    // Send QUIT command to Worker
                    out.writeObject(new String("QUIT"));
                    out.flush();
                }
                // Notify the thread
                notify();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // Method to send the Data for the X Matrix Calculation to Worker
    public void sendCalculateXuData(RealMatrix Y,RealMatrix Ik, RealMatrix R){
        try {
            // If the connection is alive
            if(alive) {

                // Set X to null
                X = null;

                // Send "CALC_XU" command to Worker
                out.writeObject(new String("CALC_XU"));
                out.flush();

                // Write a copy of Y and send it to the Worker because ObjectOutputStream
                // uses reference's hash value and sends the same object otherwise, without
                // checking if the data of the matrix have been changed
                out.writeObject(Y.copy());
                out.flush();

                // Send I KxK Matrix to Worker
                out.writeObject(Ik);
                out.flush();

                // Send R Matrix to Worker (the specific rows from R that the Worker will calculate,
                // not the whole R matrix)
                out.writeObject(R);
                out.flush();

                // Send alpha constant
                out.writeInt(alpha);
                out.flush();

                // Send lambda constant
                out.writeDouble(lambda);
                out.flush();

                // Reset the output stream
                out.reset();

                // Set resultsX to true to indicate that the thread will be waiting
                // for results for the X matrix from the Worker
                resultsX = true;
            }

        } catch (IOException e) {
            // If there was an IOException

            // Set alive to false
            alive = false;
            // Print message to console
            System.out.println(name + " can't send X data.");
            // Set pingErr to true
            pingErr = true;
        }
    }

    // Method to send the Data for the Y Matrix Calculation to Worker
    public void sendCalculateYiData(RealMatrix X,RealMatrix Ik, RealMatrix R){
        try {
            // If the connection is alive
            if(alive) {
                // Set Y to null
                Y = null;
                // Send "CALC_YI" command to Worker
                out.writeObject(new String("CALC_YI"));
                out.flush();

                // Write a copy of X and send it to the Worker because ObjectOutputStream
                // uses reference's hash value and sends the same object otherwise, without
                // checking if the data of the matrix have been changed
                out.writeObject(X.copy());
                out.flush();

                // Send I KxK Matrix to Worker
                out.writeObject(Ik);
                out.flush();

                // Send R Matrix to Worker (the specific rows from R that the Worker will calculate,
                // not the whole R matrix)
                out.writeObject(R);
                out.flush();

                // Send alpha constant
                out.writeInt(alpha);
                out.flush();

                // Send lambda constant
                out.writeDouble(lambda);
                out.flush();

                // Reset the output stream
                out.reset();

                // Set resultsY to true to indicate that the thread will be waiting
                // for results for the Y matrix from the Worker
                resultsY = true;

            }

        } catch (IOException e) {
            // If there was an IOException

            // Set alive to false
            alive = false;
            // Print message to console
            System.out.println(name + " can't send Y data.");
            // Set pingErr to true
            pingErr = true;
        }
    }

    // Method that sends a command to Worker to start calculating the X or Y Matrix
    // this method is called after sendCalculateXuData/sendCalculateYiData
    public void calculate(){
        // Synchronized block for wait() and notify()
        synchronized (this) {
            // If the connection doesn't have an error
            if(!pingErr) {
                try {
                    // Try to send the START command to the Worker
                    out.writeObject(new String("START"));
                    out.flush();
                } catch (IOException e) {
                    // If there was an IOException
                    // Set alive to false
                    alive = false;
                    // Print message to console
                    System.out.println(name + " can't send START command.");
                    // Set pingErr to true
                    pingErr = true;
                }
            }
            // Notify the thread
            notify();
        }
    }

    // Method that sends pings to the Worker to determine if the connection is alive
    // Returns true if everything was successful ot false otherwise
    public boolean sendPing(){

        // Set the number of pings that the thread tries to send to the Worker
        int count = 3;

        // While the count is greater than zero
        while(count>0) {
            try {
                // Try to send a PING command to the Worker
                out.writeObject(new String("PING"));
                out.flush();

                // Set timeout for the answer to 5 seconds
                connection.setSoTimeout(5 * 1000);

                // Read the answer of the Worker
                Object obj = in.readObject();
                // If the object is not the String "PONG"
                if (!((String) obj).equals("PONG")) {
                    // Print message to console
                    System.out.println(name + " instead of PONG was: " + (String)obj);
                    // Throw a socket exception
                    throw new SocketException();
                }
                // Disable the timeout for the connection
                connection.setSoTimeout(0);

                // Everything was successful -- return true
                return true;

            } catch (IOException e) {
                // If there was an IOException

                // If the count is 1
                if(count==1) {
                    // Set alive to false
                    alive = false;
                    // Print message to console
                    System.out.println(name + " not responding to PING.");
                    // Set pingErr to false
                    pingErr = true;
                }else{
                    // Else
                    // Print message to console
                    System.out.println("Retrying sending PING to "+ name+".");
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                // Something went wrong with the class of object returned
                // Set alive to false
                alive = false;
                // Set pingErr to false
                pingErr = true;
                // return false -- dont retry to send PING
                return false;
            }finally {
                // Try to disable the connection's timeout
                try {
                    connection.setSoTimeout(0);
                } catch (SocketException e) {
                    e.printStackTrace();
                }
            }

            // Decrement the count value
            count--;

            // Try to sleep the thread for 500 millis
            try {
                sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // Return to while -- retry sending PING

        }

        // The Worker is not responding -- return false
        return false;
    }
}
