package ds.ergasia;
/* Authors:
Petros Stavropoulos - 3150230
Kwstas Savvidis     - 3150229
Erasmia Kornelatou  - 3120076
 */

import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.OpenMapRealMatrix;
import org.apache.commons.math3.linear.QRDecomposition;
import org.apache.commons.math3.linear.RealMatrix;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;

@SuppressWarnings("Duplicates")
// Class of the Worker
// The Worker connects to the Master and gets commands in order to calculate the matrices X and Y for the Master.
public class Worker extends Thread{

    // Declarations

    // The Socket for the connection with the Master
    Socket requestSocket;

    // An object output stream to write bytes to the Master
    ObjectOutputStream out;
    // An object input stream to read bytes from the Master
    ObjectInputStream in;

    // The initial sparse matrix, containing M users and N points of interest
    OpenMapRealMatrix R;
    // A binary sparse matrix representing the preference of a user for a POI
    OpenMapRealMatrix P;
    // A matrix of size M(x)K, used to approximate the initial matrix R
    RealMatrix X;
    // The transpose of matrix X
    RealMatrix Xt;
    // The product of X multiplied by its transpose
    RealMatrix XtX;
    // A matrix of size NxK, used to approximate the initial matrix R
    RealMatrix Y;
    // The transpose of matrix Y
    RealMatrix Yt;
    // The product of Y multiplied by its transpose
    RealMatrix YtY;
    // A matrix representing the confidence in observing preference between a user and a POI
    RealMatrix C;
    // A diagonal NxN matrix, where the non-zero values represent
    // the confidence in observing preference from a specific user to all the POIs
    RealMatrix Cu;
    // A diagonal MxM matrix, where the non-zero values represent
    // the confidence in observing preference from all the users to a specific POI
    RealMatrix Ci;
    // A diagonal identity matrix NxN
    RealMatrix Iu;
    // A diagonal identity matrix MxM
    RealMatrix Ii;
    // A diagonal identity matrix KxK
    RealMatrix Ik;
    // The number of users
    int M;
    // The number of POIs
    int N;
    // The K dimension of matrices X and Y
    int K;
    // The duration of the Worker's calculation of the X matrix
    long workerDurationX;
    // The duration of the Worker's calculation of the Y matrix
    long workerDurationY;
    // The host that the Worker is connecting to
    String host;
    // The port number of the host
    int portNum;
    // The alpha constant used in calculations
    int alpha;
    // The lambda constant used in calculations
    double lambda;
    // Boolean that indicates whether the Worker must terminate
    boolean quit = false;
    // Boolean that indicates whether the Worker has lost connection with the MasterWorkerThread
    // it is connected to
    boolean connLost = false;

    // Default Constructor
    public Worker(){
        this.host = "127.0.0.1";
        this.portNum = 4200;
    }

    // Parametrized Constructor
    public Worker(String host,int portNum){
        this.host = host;
        this.portNum = portNum;
    }

    // Thread's run method
    public void run(){

        // Initialize the Worker with the host and portNum of the Worker
        initialize(host,portNum);

        // If the connection is lost
        if(connLost){
            // Print message to console
            System.out.println("Worker terminated.");
            // Return -- end the run method
            return;
        }

        try{

            // Create an object input stream from the connection
            in = new ObjectInputStream((requestSocket.getInputStream()));

            // While quit is false
            while(!quit) {

                // If the connection is lost then throw a socket exception
                if(connLost) throw new SocketException();
                // Print message to console
                System.out.println("Waiting for command.");
                // Wait for a command
                // Read an object sent from the MasterWorkerThread
                Object obj = in.readObject();

                // If the object is a string QUIT
                if (((String) obj).equals("QUIT")) {
                    // Print message to console
                    System.out.println("Notified and stopping...");
                    // Set quit to true to end the while loop
                    quit = true;
                }else if (((String) obj).equals("PING")){
                    // If the object is a string PING

                    // Print message to console
                    System.out.println("Ping requested from Master.");
                    // Send a string PONG to MasterWorkerThread
                    out.writeObject(new String("PONG"));
                    out.flush();
                    // Print message to console
                    System.out.println("Sent response to Master.");
                }else if (((String) obj).equals("CALC_XU")){
                    // If the object is a string CALC_XU

                    // Print message to console
                    System.out.println("Fetching data for X...");

                    // Fetch the data from MasterWorkerThread

                    // Get the Y matrix
                    Y = (RealMatrix)in.readObject();
                    // Get the Ik matrix
                    Ik = (RealMatrix)in.readObject();
                    // Get the R matrix
                    R = (OpenMapRealMatrix)in.readObject();
                    // Get alpha constant
                    alpha = in.readInt();
                    // Get lambda constant
                    lambda = in.readDouble();

                    // Print message to console
                    System.out.println("Fetched data for X.");

                    // Find the dimensions M,N and K from the matrices
                    M = R.getRowDimension();
                    N = R.getColumnDimension();
                    K = Ik.getRowDimension();

                    // Create a identity matrix NxN
                    Iu = MatrixUtils.createRealIdentityMatrix(N);

                    // Calculate the matrices P and C
                    calculatePMatrix();
                    calculateCMatrix();

                    // Assign the transpose of Y to Yt
                    Yt = Y.transpose();
                    // Precalculate YtY which will be used in the calculation of xu
                    YtY = preCalculateYY();

                    // Create an empty X matrix MxK
                    X = MatrixUtils.createRealMatrix(M,K);

                    // Wait for command START to start the calculation

                    // Read an object from MasterWorkerThread
                    Object o = in.readObject();
                    // If the object is a string START
                    if(((String) o).equals("START")){
                        // Print message to console
                        System.out.println("Calculating Xu...");
                    }else{
                        // If it isn't

                        // Print message to console
                        System.out.println("START command not given.");
                        // Throw an IOException to stop the calculation
                        throw new IOException();
                    }

                    // Start measuring calculation time -- get the current time
                    long startTime = System.currentTimeMillis();

                    // Calculate all xu for the X matrix
                    for(int u=0;u<M;u++){
                        // Calculate the progress
                        double progress = u/((double)M);
                        // Print the progress to the console
                        if(u % 10 == 0) System.out.printf("\r %.2f%% Done",progress*100);
                        // Calculate Cu
                        calculateCuMatrix(u);
                        // Calculate the xu for the user u and assign it to the X matrix
                        // as a row for the user u (row u)
                        X.setRowMatrix(u,calculate_x_u(u,lambda));
                    }
                    // Print message to console
                    System.out.println("\r 100% Done  ");
                    // Stop measuring calculation time -- get the current time
                    long endTime = System.currentTimeMillis();

                    // Find the duration using endTime and startTime
                    workerDurationX = endTime - startTime;

                    // Print message to console
                    System.out.println("Calculated Xu.");

                    // Send results and free memory
                    sendResultsToMaster(X,workerDurationX);

                } else if (((String) obj).equals("CALC_YI")){
                    // If the object is a string CALC_YI

                    // Print message to console
                    System.out.println("Fetching data for Y...");

                    // Fetch the data from MasterWorkerThread

                    // Get the X matrix
                    X = (RealMatrix)in.readObject();
                    // Get the Ik matrix
                    Ik = (RealMatrix)in.readObject();
                    // Get the R matrix
                    R = (OpenMapRealMatrix)in.readObject();
                    // Get alpha constant
                    alpha = in.readInt();
                    // Get lambda constant
                    lambda = in.readDouble();

                    // Print message to console
                    System.out.println("Fetched data for Y.");

                    // Find the dimensions M,N and K from the matrices
                    M = R.getRowDimension();
                    N = R.getColumnDimension();
                    K = Ik.getRowDimension();

                    // Create a identity matrix MxM
                    Ii = MatrixUtils.createRealIdentityMatrix(M);

                    // Calculate the matrices P and C
                    calculatePMatrix();
                    calculateCMatrix();

                    // Assign the transpose of X to Xt
                    Xt = X.transpose();
                    // Precalculate XtX which will be used in the calculation of yi
                    XtX = preCalculateXX();

                    // Create an empty Y matrix NxK
                    Y = MatrixUtils.createRealMatrix(N,K);

                    // Wait for command START to start the calculation

                    // Read an object from MasterWorkerThread
                    Object o = in.readObject();
                    // If the object is a string START
                    if(((String) o).equals("START")){
                        // Print message to console
                        System.out.println("Calculating Yi...");
                    }else{
                        // If it isn't

                        // Print message to console
                        System.out.println("START command not given.");
                        // Throw an IOException to stop the calculation
                        throw new IOException();
                    }

                    // Start measuring calculation time -- get the current time
                    long startTime = System.currentTimeMillis();

                    // Calculate all yi for the Y matrix
                    for(int i=0;i<N;i++){
                        // Calculate the progress
                        double progress = i/((double)N);
                        // Print the progress to the console
                        if(i % 10 == 0) System.out.printf("\r %.2f%% Done",progress*100);
                        // Calculate Ci
                        calculateCiMatrix(i);
                        // Calculate the yi for the POI i and assign it to the Y matrix
                        // as a row for the POI i (row i)
                        Y.setRowMatrix(i,calculate_y_i(i,lambda));
                    }
                    // Print message to console
                    System.out.println("\r 100% Done  ");

                    // Stop measuring calculation time -- get the current time
                    long endTime = System.currentTimeMillis();

                    // Find the duration using endTime and startTime
                    workerDurationY = endTime - startTime;

                    // Print message to console
                    System.out.println("Calculated Yi.");

                    // Send results and free memory
                    sendResultsToMaster(Y,workerDurationY);

                }

            }

        } catch (IOException ioException) {
            // If there was an IOException

            // Print message to console
            System.out.println("Connection with Master lost.");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                // If it isn't null try to close ObjectOutputStream
                if(out!=null) {
                    out.close();
                }
                // If it isn't null try to close ObjectInputStream
                if(in!=null){
                    in.close();
                }
                // If it isn't null try to close the Worker's Socket
                if(requestSocket!=null) {
                    requestSocket.close();
                }
                // Print message to console
                System.out.println("Worker terminated.");
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }

    }

    // Method that initializes the Worker
    public void initialize(String host, int portNum){

        // Print message to console
        System.out.println("Worker running");

        // Initialize the Worker's Socket to null
        requestSocket = null;
        // Initialize the object output stream to null
        out = null;
        // Initialize the object input stream to null
        in = null;

        try {

            // Print message to console
            System.out.println("Connecting to: " + host + "@" + portNum);
            // Connect to ip and port given as parameters
            requestSocket = new Socket(host, portNum);
            // Create an object output stream from the connection
            out = new ObjectOutputStream(requestSocket.getOutputStream());

            // Get the System's free memory
            long mem = Runtime.getRuntime().freeMemory();
            // Write the free memory to the connected MasterWorkerThread
            out.writeLong(mem);
            out.flush();

            // Get the System's available processors/cores
            int cores = Runtime.getRuntime().availableProcessors();
            // Write the available cores to the connected MasterWorkerThread
            out.writeInt(cores);
            out.flush();

            // Print message to console
            System.out.println("Connected and sent CPU and RAM info to Master.");

        } catch (ConnectException e){
            // If there was a connection error
            // Print message to console
            System.out.println("Can't connect to Master.");
            // Indicate that the connection is lost
            connLost = true;
        } catch (UnknownHostException e) {
            // If there was an unknon host exception
            // Print message to console
            System.out.println("The host you are trying to connect is unknown.");
            // Indicate that the connection is lost
            connLost = true;
        } catch (IOException e) {
            // If there was another IO error
            // Print stack trace of the exception
            e.printStackTrace();
            // Indicate the connection is lost
            connLost = true;
        }
    }

    // Method that calculates the C matrix from the R matrix
    public void calculateCMatrix(){
        // Create an empty MxN Matrix and assign it to C
        C = MatrixUtils.createRealMatrix(M,N);

        // For i from 0 to C.getRowDimension()-1
        for(int i =0 ; i< C.getRowDimension();i++){
            // For j from 0 to C.getColumnDimension()-1
            for(int j = 0 ; j< C.getColumnDimension();j++){
                // Set the entry i,j of matrix C to: 1 + R(i,j) * alpha
                C.setEntry(i,j,1 + R.getEntry(i,j)*alpha);
            }
        }
    }

    // Method that calculates the P matrix from the R matrix
    public void calculatePMatrix(){
        // Create an empty sparse MxN Matrix and assign it to C
        P = new OpenMapRealMatrix(M,N);

        // For i from 0 to P.getRowDimension()-1
        for(int i =0 ; i< P.getRowDimension();i++){
            // For j from 0 to P.getColumnDimension()-1
            for(int j = 0 ; j< P.getColumnDimension();j++){
                // Set the entry i,j of matrix P to:
                // 1 if R(i,j) > 0
                // 0 otherwise
                P.setEntry(i,j,(R.getEntry(i,j)>0)?1:0);
            }
        }
    }

    // Method that calculates the Cu matrix for the user u
    public void calculateCuMatrix(int u){
        // Create a diagonal matrix NxN where the main diagonal has values
        // from the u row of the C matrix
        Cu = MatrixUtils.createRealDiagonalMatrix(C.getRow(u));
    }

    // Method that calculates the Cu matrix for the POI i
    public void calculateCiMatrix(int i){
        // Create a diagonal matrix MxM where the main diagonal has values
        // from the i column of the C matrix
        Ci = MatrixUtils.createRealDiagonalMatrix(C.getColumn(i));
    }

    // Method that calculates YtY
    RealMatrix preCalculateYY(){
        return Yt.multiply(Y);
    }

    // Method that calculates XtX
    RealMatrix preCalculateXX(){
        return Xt.multiply(X);
    }

    // Method that calculates a xu for the X matrix
    RealMatrix calculate_x_u(int u,double lambda){
        // Calculate YtY + Yt(C^u - I)Y + λI and assign it to temp
        RealMatrix temp = YtY.add(Yt.multiply(Cu.subtract(Iu)).multiply(Y)).add(Ik.scalarMultiply(lambda));
        // Inverse the matrix and assign it to temp_inverse
        RealMatrix temp_inverse = new QRDecomposition(temp).getSolver().getInverse();
        // Multiply temp_inverse with Yt*C^u*p(u)
        RealMatrix xu = temp_inverse.multiply(Yt).multiply(Cu).multiply(P.getRowMatrix(u).transpose());
        // Return xu -- it is transposed because it is a RealMatrix so we want it to have dimensions
        // 1xK to be a row for X
        return xu.transpose();
    }

    // Method that calculates a yi for the Y matrix
    RealMatrix calculate_y_i(int i, double lambda){
        // Calculate XtX + Xt(C^i - I)X + λI and assign it to temp
        RealMatrix temp = XtX.add(Xt.multiply(Ci.subtract(Ii)).multiply(X)).add(Ik.scalarMultiply(lambda));
        // Inverse the matrix and assign it to temp_inverse
        RealMatrix temp_inverse = new QRDecomposition(temp).getSolver().getInverse();
        // Multiply temp_inverse with Xt*C^i*p(i)
        RealMatrix yi = temp_inverse.multiply(Xt).multiply(Ci).multiply(P.getColumnMatrix(i));
        // Return yi -- it is transposed because it is a RealMatrix so we want it to have dimensions
        // 1xK to be a row for Y
        return yi.transpose();
    }

    // Method that send the results (the calculated X or Y matrix) to the MasterWorkerThread
    // the Worker is connected to
    void sendResultsToMaster(RealMatrix M,long workerDuration){

        try {
            // Send the Matrix M passed as a method argument to the MasterWorkerThread
            out.writeObject(M);
            out.flush();

            // Send the duration for the calculation
            out.writeLong(workerDuration);
            out.flush();

        } catch (IOException e) {
            // If there was an error

            // Print message to console
            System.out.println("Worker can't write results to Master.");
            // Indicate that the connection is lost
            connLost = true;
        }

        // Set everything to null, so that the garbage collector can free the memory
        R = null;
        P = null;
        X = null;
        Xt = null;
        XtX = null;
        Y = null;
        Yt = null;
        YtY = null;
        C = null;
        Cu = null;
        Ci = null;
        Iu = null;
        Ii = null;
        Ik = null;

    }

    // Main method of Worker
    public static void main(String[] args) {
        // If there aren't arguments
        if(args.length==0) {
            // Create a Worker with Default Constructor and start it's thread
            new Worker().start();
        }else{
            // If there are arguments
            // Create a Worker with Parametrized Constructor using the arguments and start it's thread
            new Worker(args[0],Integer.parseInt(args[1])).start();
        }
    }

}
