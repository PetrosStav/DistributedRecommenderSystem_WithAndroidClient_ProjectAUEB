package ds.ergasia;
/* Authors:
Petros Stavropoulos - 3150230
Kwstas Savvidis     - 3150229
Erasmia Kornelatou  - 3120076
 */

import org.apache.commons.math3.linear.*;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.CountDownLatch;

@SuppressWarnings("Duplicates")
// Class of the Master-Server
// The Master coordinates all the Workers in order to train the matrices X and Y
// which are then used in order to answer queries from Clients, regarding a user and
// the POIs that are recommended for him
public class Master {

    // The Listener of the Master
    MasterListener listener;

    // The Server Socket of the Master-Server
    ServerSocket providerSocket;

    // The list of Workers which are connected to the Master
    ArrayList<Thread> workersList = new ArrayList<Thread>();

    // The number of rows that each Worker has to calculate from R
    ArrayList<Integer> rowsForWorkers;
    // The number of columns that each Worker has to calculate from R
    ArrayList<Integer> colsForWorkers;

    // A list of the POI Objects of the R matrix column ids
    ArrayList<Poi> poiList;

    // The sparse Matrix R, which has the data of the users regrading the POIs
    OpenMapRealMatrix R;
    // The sparse Matrix P, which has the preferences of the users regarding the POIs
    OpenMapRealMatrix P;
    // The Matrix C, which has the confidence of the users regarding the POIs
    RealMatrix C;
    // The Matrix X with dimensions MxK
    RealMatrix X;
    // The Matrix Y with dimensions NxK
    RealMatrix Y;
    // The Matrix I with dimensions NxN
    RealMatrix Iu;
    // The Matrix I with dimensions MxM
    RealMatrix Ii;
    // The Matrix I with dimensions KxK
    RealMatrix Ik;

    // The number of active-connected Workers to the Master
    int NumOfWorkers;
    // The M dimension (users)
    int M; // users : 765 for Rdata
    // The N dimension (POIs)
    int N; // pois : 1964 for Rdata
    // The K dimension, a number smaller than the min{M,N}
    int K;

    // The lambda constant used for calculations (and regularization)
    double lambda;
    // The alpha constant used for calculations
    int alpha;

    // Max steps for the training
    int maxsteps = 5;

    // The port number of the Server Socket
    int portNum;
    // The max allowed connections of the Master-Server
    int allowedConnections;

    // Default Constructor
    public Master(){
        this.portNum = 4200;
        this.allowedConnections = 25;
    }

    // Parametrized Constructor
    public Master(int portNum, int allowedConnections){
        this.portNum = portNum;
        this.allowedConnections = allowedConnections;
    }

    // Main method of Master-Server
    public static void main(String[] args) {

        // M : 765 for Rdata
        // N : 1964 for Rdata

        // The path/filename of the R matrix
        String filename = "RdataFinal.csv";
        // The users
//        int M = 765;
        // final dataset M = 835
        int M = 835;
        // The POIs
//        int N = 1964;
        // final dataset N = 1692
        int N = 1692;
        // The K dimension for X and Y matrices
        int K = 20;
        // The threshold after which the training will stop (if the diff of errors is less than it)
        double threshold = 0.1;
        // The lambda constant for calculations
        double lambda = 0.1;
        // The alpha contant for calculations
        int alpha = 40;
        // The starting number of Workers that Master will wait to connect
        int numOfWorkers = 6;
        // The timeout of each worker -- the amount of millis that each MasterWorkerThread will wait
        // for an answer after considering that the connection with the Worker is dead
        int workerTimeout = 300*1000;

        // Create a Master Object
        Master m;

        // If there are no arguments call the default constructor -- default values
        if(args.length==0) {
            m = new Master();
        }else{
            // If there are, then call the parametrized constructor -- specific values
            m = new Master(Integer.parseInt(args[0]),Integer.parseInt(args[1]));
        }

        // Create a List of all the POIs of Master
        m.poiList = new ArrayList<>();

        // Add N POIs to the Master using the randomized constructor of the POI Class
//        for(int i=0;i<N;i++) m.poiList.add(new Poi(i));

        // Create the POIs from the JSON file

        // Create a JSON parser
        JSONParser parser = new JSONParser();

        try {
            // Parse the POIs.json file and get the json object
            JSONObject jsonObj = (JSONObject)parser.parse(new FileReader("POIs.json"));

            // For i from 0 to N-1
            for(int i=0;i<N;i++){
                // Get the JSON object from the POI with id = i
                JSONObject jsnObjTemp = (JSONObject) jsonObj.get(""+i);

                // Get the POI's hash
                String hash = (String)jsnObjTemp.get("POI");

                // Get the POI's latitude
                double lat = (Double)jsnObjTemp.get("latidude");

                // Get the POI's longitude
                double lon = (Double)jsnObjTemp.get("longitude");

                // Get the POI's photo link
                String photo = (String)jsnObjTemp.get("photos");

                // Get the POI's category
                String category = (String)jsnObjTemp.get("POI_category_id");

                // Get the POI's name
                String name = (String)jsnObjTemp.get("POI_name");

                // Add a new poi with the above to the list
                m.poiList.add(new Poi(i,name,lat,lon,category,photo,hash));

            }

        } catch (ParseException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


        // Boolean to indicate if we want to load X,Y matrices
        // or to train them
        // false -> train
        // true -> load
        boolean fromDisk = true;

        // Boolean to save X and Y every epoch as Xtemp and Ytemp
        // false -> don't save
        // true -> save
        boolean epochSave = false;

        // Boolean to loadFromEpochSave to continue training from the Xtemp and Ytemp files
        // false -> don't load
        // true -> load
        boolean loadTemp = false;

        // Print message to console
        System.out.println("Master running");

        // Initialize the Master using the above values
        m.initialize(filename,M,N,K,alpha,lambda,numOfWorkers,workerTimeout,fromDisk);

        // Calculate P matrix from R
        m.calculatePMatrix();

        // Calculate C matrix from R
        m.calculateCMatrix();

        // If fromDisk is false then start the training
        if(!fromDisk) {

            // Print message to console
            System.out.println("Starting training.");

            // Set diff to a very big number (max double)
            double diff = Double.MAX_VALUE;

            // Set error (cost function) to a very big number (max double)
            double error = Double.MAX_VALUE;

            // Calculate distribution for the workers
            m.calculateDistribution();

            // If loadTemp is true, then load Xtemp and Ytemp matrices from disk to continue the training
            // from these matrices instead of random values for X and Y
            if(loadTemp) m.loadMatricesXY(filename.substring(0,filename.length()-4) + "_X_temp.csv",filename.substring(0,filename.length()-4) + "_Y_temp.csv");

            // Create the Master's Listener for Workers
            m.listener = new MasterListener(m.providerSocket, m.NumOfWorkers,workerTimeout,alpha,lambda);
            // Start the Master's Listener
            m.listener.start();

            // Boolean to indicate if a disconnection occured at Y calculation, in order to jump to Y
            boolean jumptoY = false;
            // Boolean that indicates if the epoch has been restarted
            boolean restart = false;

            // Main loop
            // While the difference of errors is greater than the threshold or the Master hasn't reached the max steps for the training
            while (diff > threshold && m.maxsteps-- > 0) {

                // Check if new workers have connected to the Master's Listener
                if(m.listener.newWorkers){
                    // Print message to console
                    System.out.println("Adding new workers.");
                    // Call the getConnectionsWaiting() to the Listener and add all the new connections to the workersList
                    m.workersList.addAll(m.listener.getConnectionsWaiting());
                    // Print message to console
                    System.out.println("Added " + (m.listener.numOfWorkers-m.NumOfWorkers) + " Workers.");
                    // Update the Master's number of workers from the listener
                    m.NumOfWorkers = m.listener.numOfWorkers;
                    // Print message to console
                    System.out.println("Number Of Workers: " + m.NumOfWorkers);
                    // If the aren't any workers left
                    if (m.NumOfWorkers == 0) {
                        // If we have new Workers from the listener
                        if(m.listener.newWorkers){
                            // Goto start of while loop
                            continue;
                        }
                        // Print message to console
                        System.out.println("Oops, no workers!");
                        // Print message to console
                        System.out.println("Shutting Master down.");
                        // Terminate the server
                        m.terminateServer();
                        // Exit main
                        System.exit(0);
                    }
                    // Print message to console
                    System.out.println("Redistributing work to workers.");
                    // Call the calculateDistribution() as the workersList has been updated
                    m.calculateDistribution();
                }

                // If the epoch has been restarting don't print this message
                if(!restart) System.out.println("Starting new epoch.");
                // Set reastart to false
                restart = false;

                // If jumptoX is false
                if(!jumptoY) {
                    // Print message to console
                    System.out.println("Distributing X to Workers.");
                    // Distribute calculation for X matrix to Workers
                    if (!m.distributeXMatrixToWorkers()) {
                        // If it failed
                        // Print message to console
                        System.out.println("Number Of Workers: " + m.NumOfWorkers);
                        // Update the Listener's number of workers from the Master
                        m.listener.numOfWorkers = m.NumOfWorkers;
                        // If the aren't any workers left
                        if (m.NumOfWorkers == 0) {
                            // If we have new Workers from the listener
                            if(m.listener.newWorkers){
                                // Print message to console
                                System.out.println("Listener has Workers.");
                                // Print message to console
                                System.out.println("Restarting epoch.");
                                // Set restart to true
                                restart = true;
                                // Goto the top of the while loop
                                continue;
                            }
                            // Print message to console
                            System.out.println("Oops, no workers!");
                            // Print message to console
                            System.out.println("Shutting Master down.");
                            // Terminate the server
                            m.terminateServer();
                            // Exit main
                            System.exit(0);
                        }
                        // Print message to console
                        System.out.println("Redistributing work to workers.");
                        // Call the calculateDistribution() as the workersList has been updated
                        m.calculateDistribution();
                        // Print message to console
                        System.out.println("Restarting epoch.");
                        // Set restart to true
                        restart = true;
                        // Goto the top of the while loop
                        continue;
                    }
                }
                // Set jumptoY to false
                jumptoY = false;
                // Print message to console
                System.out.println("Distributing Y to Workers.");

                // Distribute calculation for Y matrix to Workers
                if(!m.distributeYMatrixToWorkers()){
                    // If it failed
                    // Print message to console
                    System.out.println("Number Of Workers: " + m.NumOfWorkers);
                    // Update the Listener's number of workers from the Master
                    m.listener.numOfWorkers = m.NumOfWorkers;
                    // If the aren't any workers left
                    if (m.NumOfWorkers == 0) {
                        // If we have new Workers from the listener
                        if(m.listener.newWorkers){
                            // Print message to console
                            System.out.println("Listener has Workers.");
                            // Print message to console
                            System.out.println("Restarting epoch.");
                            // Set jumptoY to true
                            jumptoY = true;
                            // Set restart to true
                            restart = true;
                            // Goto the top of the while loop
                            continue;
                        }
                        // Print message to console
                        System.out.println("Oops, no workers!");
                        // Print message to console
                        System.out.println("Shutting Master down.");
                        // Terminate the server
                        m.terminateServer();
                        // Exit main
                        System.exit(0);
                    }
                    // Print message to console
                    System.out.println("Redistributing work to workers.");
                    // Call the calculateDistribution() as the workersList has been updated
                    m.calculateDistribution();
                    // Print message to console
                    System.out.println("Restarting epoch.");
                    // Set jumptoY to true
                    jumptoY = true;
                    // Set restart to true
                    restart = true;
                    // Goto the top of the while loop
                    continue;
                }

                // Calculate the error (cost function) and assign it to error_new
                double error_new = m.calculateError();

                // Print message to console
                System.out.println("Error: " + error_new);

                // Calculate the absolute difference between the error_new and error (the previous epoch error)
                diff = Math.abs(error - error_new);

                // Print message to console
                System.out.println("Diff: " + diff);

                // Update error variable to error_new
                error = error_new;

                // If epochSave is true
                if(epochSave){
                    // Save matrices X and Y as Xtemp and Ytemp
                    m.saveMatricesXY(filename.substring(0,filename.length()-4) + "_X_temp.csv",filename.substring(0,filename.length()-4) + "_Y_temp.csv");
                    // Print message to console
                    System.out.println("Saved Temp Matrices X,Y to disk.");
                }

                // Redistribute work to workers according to the duration of their work -- factor : delays
                m.redistributeByDelay(0.9);

            }

            // Training has ended

            // Save the matrices X,Y
            m.saveMatricesXY(filename.substring(0,filename.length()-4) + "_X.csv", filename.substring(0,filename.length()-4) + "_Y.csv");
            // Print message to console
            System.out.println("Saved Matrices X,Y to disk.");

            // Terminate server
            m.terminateServer();

        }else{
            // Load matrices x,y from disk
            m.loadMatricesXY(filename.substring(0,filename.length()-4) + "_X.csv",filename.substring(0,filename.length()-4) + "_Y.csv");
        }

        // Do queries -- accept clients

        // Open a new ServerSocket for the clients
        try {
            m.providerSocket = new ServerSocket(m.portNum,m.allowedConnections);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // Print message to console
        System.out.println("Waiting for clients.");
        // Initialize socket for incoming connections to null
        Socket sock = null;
        // Initialize an ObjectInputStream to null
        ObjectInputStream in = null;
        // Initialize an ObjectOutputStream to null
        ObjectOutputStream out = null;
        try {
            // While true -- main loop for accepting clients
            while(true) {
                // Accept a connection
                sock = m.providerSocket.accept();
                // Create an ObjectInputStream using the connection
                in = new ObjectInputStream(sock.getInputStream());
                // Create an ObjectOutputStream using the connection
                out = new ObjectOutputStream(sock.getOutputStream());
                // Print message to console
                System.out.println("Accepted connection from: " + sock.getInetAddress() + "@" + sock.getPort());

                // Declare u for users and k or k-best pois
                int u, k;

                // Read user from Client
                u = in.readInt();

                // Read k from Client
                k = in.readInt();

                // Read the location latitude and longitude
                double lat,lon;
                lat = in.readDouble();
                lon = in.readDouble();

                // Read the maximum distance from the current location
                double maxD;
                maxD = in.readDouble();

                // Make a criterion that the user u hasn't checked in POI p
//                Criterion crit = p -> {return m.P.getEntry(u,p.id)==0 ;} ;

                // Make a criterion that the distance is no more than x kilometers
//                Criterion crit = p -> {return distance(lat,p.latitude,lon,p.longitude,0,0)<=(maxD*1000);};

                // Combine the two above
                Criterion crit = p -> {return distance(lat,p.latitude,lon,p.longitude)<=(maxD*1000) &&  m.P.getEntry(u,p.id)==0 ;};

                // Calculate K best Poi's for user u using criterion crit
                List<Poi> kBestPoi = m.calculateBestKPoisForUser(u, k, crit);

                // Send list of K best pois for user u to Client
                out.writeObject(kBestPoi);
                out.flush();

                // Print message to console
                System.out.println("Sent answer to : " + sock.getInetAddress() + "@" + sock.getPort() + " ( u: " + u + ", k: " + k + ", lat: " + lat + ", lon: " + lon + ", maxD: " + maxD +  " )");

            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally{
            try {
                // If in isn't null close it
                if(in!=null) {
                    in.close();
                }
                // If out isn't null close it
                if(out!=null) {
                    out.close();
                }
                // If sock isn't null close it
                if(sock!=null) {
                    sock.close();
                }
                // Terminate server
                m.terminateServer();

                // Sleep for 1 second
                Thread.sleep(1000);

                // Terminate main
                System.exit(0);

            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    // Method that initializes the Master-Server
    public void initialize(String filename,int M, int N, int K,int alpha,double lambda,int numOfWorkers, int workerTimeout, boolean fromDisk){

        // If fromDisk is false -- we don't have to recruit Workers, there won't be any training
        if(!fromDisk) {
            // Open the server
            try {
                // Create a new ServerSocket connection using portNum and allowedConnections
                providerSocket = new ServerSocket(portNum, allowedConnections);

                // Set the number of workers from the method's parameters
                NumOfWorkers = numOfWorkers;

                // Print message to console
                System.out.println("Waiting for " + NumOfWorkers + " Workers to connect...");

                // Set count to 0
                int count = 0;

                // While count is less than NumOfWorkers -- while there must connect more workers
                while (count < NumOfWorkers) {

                    // Accept a connection and assign it to sock
                    Socket sock = providerSocket.accept();
                    // Print message to console
                    System.out.println("Accepted connection from: " + sock.getInetAddress() + "@" + sock.getPort());
                    // Create a MasterWorkerThread and assign it to a thread reference
                    Thread worker = new MasterWorkerThread(count, "Worker_" + count, sock, workerTimeout, alpha, lambda);
                    // Start the worker thread
                    worker.start();
                    // Add the worker thread to the workerList
                    workersList.add(worker);
                    // Increment the count
                    count++;
                }
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }

        }

        // Set M from the method's parameters
        this.M = M;
        // Set N from the method's parameters
        this.N = N;
        // Set alpha from the method's parameters
        this.alpha = alpha;
        // Set lambda from the method's parameters
        this.lambda = lambda;
        // Create an empty sparse MxN Matrix and assign it to R
        R = new OpenMapRealMatrix(M,N);

        // Initialize a BufferedReader to null
        BufferedReader bf = null;
        try {
            // Create a BufferedReader with the filename
            bf = new BufferedReader(new FileReader(new File(filename)));
            // Declare a String for the file's lines
            String line;
            // While there are more lines in the file, read the line
            while((line=bf.readLine())!= null){
                // Split the line using comma as a separator
                String[] line_parts = line.split(",");
                // Set u to the first line_part as an Integer
                int u = Integer.parseInt(line_parts[0].trim());
                // Set i to the second line_part as an Integer
                int i = Integer.parseInt(line_parts[1].trim());
                // Set data to the third line_part as an Integer
                int data = Integer.parseInt(line_parts[2].trim());
                // Set entry u,i of R matrix to data
                R.setEntry(u,i,data);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            // If bf isn't null close it
            if(bf!=null){
                try {
                    bf.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }

        // Set K value from the method's parameters
        this.K = K;

        // Create a Random object
        Random rand = new Random();
        // TODO: REMOVE
//        JDKRandomGenerator rand = new JDKRandomGenerator();
//        rand.setSeed(1);

        // If fromDisk is false -- we don't need to put random Values to X,Y and calculate Ik,Iu,Ii
        if(!fromDisk) {

            // Initialize X matrix randomly

            // Create a MxK Matrix and assign it to X
            X = MatrixUtils.createRealMatrix(M, K);
            // For i from 0 to X.getRowDimension()-1
            for (int i = 0; i < X.getRowDimension(); i++) {
                // For j from 0 to X.getColumnDimension()-1
                for (int j = 0; j < X.getColumnDimension(); j++) {
                    // Set entry i,j of X matrix to a random double value between 0 and 1
                    X.setEntry(i, j, rand.nextDouble());
                }
            }

            // Initialize Y matrix randomly

            // Create a NxK Matrix and assign it to Y
            Y = MatrixUtils.createRealMatrix(N, K);
            // For i from 0 to Y.getRowDimension()-1
            for (int i = 0; i < Y.getRowDimension(); i++) {
                // For j from 0 to Y.getColumnDimension()-1
                for (int j = 0; j < Y.getColumnDimension(); j++) {
                    // Set entry i,j of Y matrix to a random double value between 0 and 1
                    Y.setEntry(i, j, rand.nextDouble());
                }
            }

            // Create I NxN matrix and assign it to Iu
            Iu = MatrixUtils.createRealIdentityMatrix(N);

            // Create I MxM matrix and assign it to Ii
            Ii = MatrixUtils.createRealIdentityMatrix(M);

            // Create I KxK matrix and assign it to Ik
            Ik = MatrixUtils.createRealIdentityMatrix(K);

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

    // Method that calculates/updates the rowsForWorkers and colsForWorkers by the factor of their Free Memory and Available Cores
    // meaning how to distribute the calculations of X and Y matrix to the Master's Workers
    public void calculateDistribution(){

        // Create a list of the Worker's Free Memory Values
        ArrayList<Long> memlist = new ArrayList<Long>();
        // Create a list of the Worker's Available Cores Values
        ArrayList<Integer> corelist = new ArrayList<Integer>();

        // Set sumMem to 0
        long sumMem = 0;
        // Set sumCores to 0
        int sumCores = 0;

        // For every thread in workersList
        for(Thread t : workersList){
            // Cast the thread to a MasterWorkerThread
            MasterWorkerThread worker = (MasterWorkerThread)t;

            // Add the worker's Free Memory to the memList
            memlist.add(worker.freemem);
            // Increase sumMem with the worker's Free Memory
            sumMem += worker.freemem;
            // Add the worker's Available Cores to the coreList
            corelist.add(worker.cores);
            // Increase sumCores with the worker's Awailable Cores
            sumCores += worker.cores;
        }

        // Create a list for the percentages for the Free Memory Values
        ArrayList<Double> pmemList = new ArrayList<Double>();
        // Create a list for the percentages for the Available Core Values
        ArrayList<Double> pcoreList = new ArrayList<Double>();

        // For i from 0 to memlist.size()-1 (the same as workerList.size()-1)
        for(int i=0;i<memlist.size();i++){
            // Add ot the pmemList the value of the worker's free memory divided by the sum of the workers' free memory
            // which gives us the percentage that the Worker contributes to the sum
            pmemList.add(memlist.get(i)/((double)sumMem));
            // Add ot the pcoreList the value of the worker's available cores divided by the sum of the workers' available
            // cores which gives us the percentage that the Worker contributes to the sum
            pcoreList.add(corelist.get(i)/((double)sumCores));
        }

        // For X

        // We already have the percentages
        // Multiply with M users (the number of xu rows)

        // Create a new arrayList for the rowsForWorkers
        rowsForWorkers = new ArrayList<>();

        // Set sumForWorkers to 0
        int sumForWorkers = 0;

        // Set maxRows to 0 -- the value of the Worker with the max rows
        int maxRows = 0;
        // Set maxRowsIdx to -1 -- the index of the Worker with the max rows
        int maxRowsIdx = -1;
        // Set minRows to the max Integer value -- the value of the Worker with the max rows
        int minRows = Integer.MAX_VALUE;
        // Set minRowsIdx to -1 -- the index of the Worker with the min rows
        int minRowsIdx = -1;

        // For i from 0 to pmemList.size()-1
        for (int i = 0; i < pmemList.size(); i++) {

            // Set rowsFromCores to the percentage from the pcoreList multiplied by M and rounded to an Integer
            // meaning how many rows should the Worker calculate by using the Worker's Available Cores as a factor
            int rowsFromCores = (int) Math.round(pcoreList.get(i) * M);
            // Set rowsFromMem to the percentage from the pmemList multiplied by M and rounded to an Integer
            // meaning how many rows should the Worker calculate by using the Worker's Free Memory as a factor
            int rowsFromMem = (int) Math.round(pmemList.get(i) * M);

            // Set rows to the sum of the rowsFromCores and rowsFromMem
            int rows = rowsFromCores + rowsFromMem;

            // If the rows for the Worker is bigger than the maxRows
            if (rows > maxRows) {
                // Set maxRows to the rows of the Worker
                maxRows = rows;
                // Set maxRowsIdx to i
                maxRowsIdx = i;
            }

            // If the rows for the Worker is less than the minRows
            if(rows < minRows){
                // Set minRows to the rows of the Worker
                minRows = rows;
                // Set minRowsIdx to i
                minRowsIdx = i;
            }

            // Add the rows to the rowsForWorkers list
            rowsForWorkers.add(rows);

            // Increase sumForWorkers by the Worker's rows
            sumForWorkers += rows;
        }

        // Find the final rows for each worker

        // Set sumFinalRows to 0
        int sumFinalRows = 0;

        // For i from 0 to rowsForWorkers.size()-1
        for (int i = 0; i < rowsForWorkers.size(); i++) {
            // Set finalRows to the rowsForWorker's value for the Worker divided by the SumForWorkers, multiplied by M and rounded to an Integer
            // which is the finalRows that the Worker will calculate
            int finalRows = (int) Math.round((rowsForWorkers.get(i) / ((double) sumForWorkers)) * M);

            // Check if a Worker has zero rows to calculate and give it 1
            // which will be taken from the one that calculates the maxRows
            if(finalRows==0) finalRows=1;

            // Add to the finalRows of the Worker to the sumFinalRows
            sumFinalRows += finalRows;

            // Update the rowsForWorkers value with the finalRows of the Worker
            rowsForWorkers.set(i, finalRows);
        }



        // Check if the sum of final rows exceeds M
        // eg. 10 users and 4, 3, 4 as results will become 4, 3, 3
        if (sumFinalRows > M) {
            // Update the Worker with the maxRows rows subtracting the difference to reach M
            rowsForWorkers.set(maxRowsIdx, rowsForWorkers.get(maxRowsIdx) - (sumFinalRows-M));
        }else if(sumFinalRows < M){
            // Check if the sum of final rows does not reach up to M

            // Update the Worker with the minRows rows adding the difference to reach M
            rowsForWorkers.set(minRowsIdx, rowsForWorkers.get(minRowsIdx) + (M-sumFinalRows));
        }

        // For Y

        // We already have the percentages
        // Multiply with N users (the number of yi cols)

        // Create a new arrayList for the colsForWorkers
        colsForWorkers = new ArrayList<>();

        // Set sumForWorkers to 0
        sumForWorkers = 0;

        // Set maxCols to 0 -- the value of the Worker with the max cols
        int maxCols = 0;
        // Set maxColsIdx to -1 -- the index of the Worker with the max cols
        int maxColsIdx = -1;
        // Set minCols to the max Integer value -- the value of the Worker with the max cols
        int minCols = Integer.MAX_VALUE;
        // Set minColsIdx to -1 -- the index of the Worker with the min cols
        int minColsIdx = -1;

        // For i from 0 to pmemList.size()-1
        for(int i=0;i<pmemList.size();i++){

            // Set colsFromCores to the percentage from the pcoreList multiplied by N and rounded to an Integer
            // meaning how many cols should the Worker calculate by using the Worker's Available Cores as a factor
            int colsFromCores = (int)Math.round(pcoreList.get(i) * N);
            // Set colsFromMem to the percentage from the pmemList multiplied by N and rounded to an Integer
            // meaning how many cols should the Worker calculate by using the Worker's Free Memory as a factor
            int colsFromMem = (int)Math.round(pmemList.get(i) * N);

            // Set the cols to the sum of colsFromCores and colsFromMem
            int cols = colsFromCores+colsFromMem;

            // If the cols for the Worker is bigger than the maxCols
            if(cols > maxCols){
                // Set maxCols to the cols of the Worker
                maxCols = cols;
                // Set maxColsIdx to i
                maxColsIdx = i;
            }

            // If the cols for the Worker is less than the minCols
            if(cols < minCols){
                // Set minCols to the cols of the Worker
                minCols = cols;
                // Set minColsIdx to i
                minColsIdx = i;
            }
            // Add the cols to the colsForWorkers list
            colsForWorkers.add(cols);

            // Increase sumForWorkers by the Worker's cols
            sumForWorkers += cols;
        }

        // Find the final cols for each worker

        // Set sumFinalCols to 0
        int sumFinalCols = 0;

        // For i from 0 to colsForWorkers.size()-1
        for(int i=0;i<colsForWorkers.size();i++){
            // Set finalCols to the colsForWorker's value for the Worker divided by the SumForWorkers, multiplied by N and rounded to an Integer
            // which is the finalCols that the Worker will calculate
            int finalCols = (int)Math.round((colsForWorkers.get(i)/((double)sumForWorkers))*N);

            // Check if a Worker has zero cols to calculate and give it 1
            // which will be taken from the one that calculates the maxCols
            if(finalCols==0) finalCols=1;

            // Add to the finalCols of the Worker to the sumFinalCols
            sumFinalCols += finalCols;

            // Update the colsForWorkers value with the finalCols of the Worker
            colsForWorkers.set(i, finalCols);
        }

        // Check if the sum of final cols exceeds N
        // eg. 10 pois and 4, 3, 4 as results will become 4, 3, 3
        if(sumFinalCols > N){
            // Update the Worker with the maxCols cols subtracting the difference to reach N
            colsForWorkers.set(maxColsIdx,colsForWorkers.get(maxColsIdx) - (sumFinalCols-N));
        }else if(sumFinalCols < N){
            // Check if the sum of final cols does not reach up to N

            // Update the Worker with the minCols cols adding the difference to reach N
            colsForWorkers.set(minColsIdx,colsForWorkers.get(minColsIdx) + (N-sumFinalCols));
        }

    }

    // Method that calculates/updates the rowsForWorkers and colsForWorkers by the factor of their calculation durations
    // meaning that if a worker delays the whole calculation, then distribute again the rows and cols for X and Y
    public void redistributeByDelay(double threshold){

        // Create a list for the delays
        ArrayList<Long> delayList = new ArrayList<>();

        // Initialize average to 0
        double avgDelay = 0;

        // For every thread in workersList
        for(Thread t : workersList){

            // Cast the thread to MasterWorkerThread
            MasterWorkerThread worker = (MasterWorkerThread)t;

            // Increase the avgDelay with the sum of the worker's delays for X and Y calculations
            avgDelay += worker.workerDurationX + worker.workerDurationY;

            // Add the sum of the worker's delays to the list
            delayList.add(worker.workerDurationX + worker.workerDurationY);

        }

        // Divide with number of workers to get the average delay
        avgDelay /= NumOfWorkers;

        // Check if it is worth to redistribute
        // Boolean to indicate the above
        boolean worthIt = false;

        // For i from 0 to delayList.size()-1
        for(int i=0;i<delayList.size();i++){
            // If the Worker's delay multiplied by the threshold is greater than the avgDelay
            if(delayList.get(i) * threshold > avgDelay){
                // Cast the thread to a MasterWorkerThread
                MasterWorkerThread worker = (MasterWorkerThread)workersList.get(i);
                // Print message to console
                System.out.println(worker.name + " delaying.");
                // Indicate that it is worth redistributing the rows and cols
                worthIt = true;
            }
        }

        // If it is worth doing
        if(worthIt){
            // Print message to console
            System.out.println("Redistributing work to workers.");
            // Set sumForWorkersR to 0
            int sumForWorkersR = 0;
            // Set maxRows to 0
            int maxRows = 0;
            // Set maxRowsIdx to -1
            int maxRowsIdx = -1;
            // Set minRows to max Integer value
            int minRows = Integer.MAX_VALUE;
            // Set minRowsIdx to -1
            int minRowsIdx = -1;
            // Set sumForWorkersC to 0
            int sumForWorkersC = 0;
            // Set maxCols to 0
            int maxCols = 0;
            // Set maxColsIdx to -1
            int maxColsIdx = -1;
            // Set minCols to max Integer value
            int minCols = Integer.MAX_VALUE;
            // Set minColsIdx to -1
            int minColsIdx = -1;

            // For i from 0 to NumOfWorkers-1
            for(int i=0;i<NumOfWorkers;i++){

                // Get the factor that we will multiply the rows and columns of this worker
                // by dividing the avgDelay with the Worker's delay
                double factor = avgDelay/delayList.get(i);

                // Set the rows to the previous rowsForWorkers multiplied by the factor above
                int rows = (int)Math.round(rowsForWorkers.get(i)*factor);

                // If the rows are greater than the maxRows
                if (rows > maxRows) {
                    // Set the Worker's rows as the maxRows
                    maxRows = rows;
                    // Set the Worker's index i as the maxRowsIdx
                    maxRowsIdx = i;
                }

                // If the rows are less than the minRows
                if(rows < minRows){
                    // Set the Worker's rows as the minRows
                    minRows = rows;
                    // Set the Worker's index i as the maxRowsIdx
                    minRowsIdx = i;
                }

                // Check if a Worker has zero rows to calculate and give it 1
                // which will be taken from the one that calculates the maxRows
                if(rows==0) rows=1;

                // Add the Worker's rows to the sumForWorkersR
                sumForWorkersR += rows;

                // Update the rowsForWorkers with the rows
                rowsForWorkers.set(i,rows);

                // Set the cols to the previous colsForWorkers multiplied by the factor above
                int cols = (int)Math.round(colsForWorkers.get(i)*factor);

                // If the cols are greater than the maxCols
                if(cols > maxCols){
                    // Set the Worker's cols as the maxCols
                    maxCols = cols;
                    // Set the Worker's index i as the maxColsIdx
                    maxColsIdx = i;
                }

                // If the cols are less than the minCols
                if(cols < minCols){
                    // Set the Worker's cols as the minCols
                    minCols = cols;
                    // Set the Worker's index i as the maxColsIdx
                    minColsIdx = i;
                }

                // Check if a Worker has zero cols to calculate and give it 1
                // which will be taken from the one that calculates the maxCols
                if(cols==0) cols=1;

                // Add the Worker's cols to the sumForWorkersC
                sumForWorkersC += cols;

                // Update the colsForWorkers with the cols
                colsForWorkers.set(i, cols);

            }

            // Check if the sum of rows (sumForWorkersR) exceeds M
            if (sumForWorkersR > M) {
                // Update the Worker's with the maxRows rows subtracting the difference to reach M
                rowsForWorkers.set(maxRowsIdx, rowsForWorkers.get(maxRowsIdx) - (sumForWorkersR-M));
            }else if(sumForWorkersR < M){
                // Check if the sum of rows (sumForWorkersR) does not reach up to M

                // Update the Worker's with the minRows rows adding the difference to reach M
                rowsForWorkers.set(minRowsIdx, rowsForWorkers.get(minRowsIdx) + (M-sumForWorkersR));
            }

            // Check if the sum of cols (sumForWorkersC) exceeds N
            if(sumForWorkersC > N){
                // Update the Worker's with the maxCols cols subtracting the difference to reach N
                colsForWorkers.set(maxColsIdx,colsForWorkers.get(maxColsIdx) - (sumForWorkersC-N));
            }else if(sumForWorkersC < N){
                // Check if the sum of cols (sumForWorkersC) does not reach up to N

                // Update the Worker's with the minCols cols adding the difference to reach N
                colsForWorkers.set(minColsIdx,colsForWorkers.get(minColsIdx) + (N-sumForWorkersC));
            }

        }

    }

    // Method that distributes the calculation of X matrix to the Workers
    // returns true if successful, false otherwise
    public boolean distributeXMatrixToWorkers() {

        // We need to send to each worker:
        // Y, R (specific rows), Ik

        // Initialize idx to 0
        int idx = 0;
        // Initialize start to 0
        int start = 0;
        // Initialize end to 0
        int end = 0;

        // Boolean that indicates that an error with the PING command has occured
        boolean pingErr = false;

        // Print message to console
        System.out.println("Sending PING to all Workers.");
        // For every thread in the workersList
        for(Thread t : workersList) {
            // Cast the thread to a MasterWorkerThread
            MasterWorkerThread worker = (MasterWorkerThread) t;
            // Call the sendPing() method to the worker and if it has failed set pingErr to true
            if(!worker.sendPing()) pingErr = true;
        }

        // If there is a worker that didn't answer to PING then skip this part -- if pingErr is false
        if(!pingErr) {
            // Print message to console
            System.out.println("Sending data to all Workers.");
            // For every thread in workersList
            for (Thread t : workersList) {
                // Cast thread to MasterWorkerThread
                MasterWorkerThread worker = (MasterWorkerThread) t;
                // Increase end variable with the rows of the Worker with index idx
                end += rowsForWorkers.get(idx);
                // Create a sparse matrix Rtemp that has end-start users and N POIs
                OpenMapRealMatrix Rtemp = new OpenMapRealMatrix(end - start, N);
                // For i from 0 to end-start-1
                for (int i = 0; i < end - start; i++) {
                    // Set the row i of Rtemp matrix to the row start+i of the R matrix
                    Rtemp.setRowVector(i, R.getRowVector(start + i));
                }
                // Assign the end value to start
                start = end;
                // Call sendCalculateXuData method to the worker in order to send the data
                // for the X Matrix calculation
                worker.sendCalculateXuData(Y, Ik, Rtemp);
                // Increment idx
                idx++;
            }

            // Print message to console
            System.out.println("Sending START command to all Workers.");

                       // Create a CountDownLatch that requires NumOfWorkers countdowns to unlock
            CountDownLatch latch = new CountDownLatch(NumOfWorkers);

            // For every thread in the workersList
            for (Thread t : workersList) {
                // Cast the thread to MasterWorkerThread
                MasterWorkerThread worker = (MasterWorkerThread) t;
                // Call the calculate method to the worker in order
                // to send the START command, so that the workers will
                // start calculating the X matrix
                worker.calculate();
                // Set the Worker's latch to the latch created above
                worker.latch = latch;
            }

            try {
                // Print message to console
                System.out.println("Waiting for Workers to finish.");
                // Wait for the latch to unlock -- after the Workers' countdowns
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        // Check that all workers are running

        // Boolean to indicate if there was a worker disconnection
        boolean workerDc = false;
        // Create a list of all the 'to-be-removed' Workers
        ArrayList<Thread> removeList = new ArrayList<>();
        // For every thread in the workersList
        for(Thread t : workersList){
            // Cast the thread to MasterWorkerThread
            MasterWorkerThread worker = (MasterWorkerThread)t;
            // If the worker's connection isn't alive
            if(!worker.alive){
                // Set workerDc to true
                workerDc = true;
                // Add the thread to the removeList
                removeList.add(t);
                // Set the worker's terminate boolean to true
                worker.terminate = true;
            }
        }
        // If there was a disconnection -- if workerDc is true
        if(workerDc){
            // Print message to console
            System.out.println("Master found disconnected Workers.");
            // Print message to console
            System.out.println("Removing disconnected Workers.");
            // Remove all the disconnected workers from the workersList
            workersList.removeAll(removeList);
            // Print message to console
            System.out.println("Removed " + removeList.size() + " Workers.");
            // Change worker names and ids of remaining workers and numOfWorkers
            // Set NumOfWorkers to the workersList size
            NumOfWorkers = workersList.size();
            // For i from 0 to numOfWorkers-1
            for(int i=0;i<NumOfWorkers;i++){
                // Get the thread from the workerList and cast it to a MasterWorkerThread
                MasterWorkerThread worker = (MasterWorkerThread)workersList.get(i);
                // Set the worker id to i
                worker.id = i;
                // Print message to console
                System.out.print("Renaming " + worker.name + " to ");
                // Update the worker's name using the worker's id
                worker.name = "Worker_"+worker.id;
                // Print the new name of the worker
                System.out.println(worker.name);
            }
            // Rename Workers in MasterListener
            // FOr i from 0 to listener.awaitingConnections.size()-1
            for(int i=0;i<listener.awaitingConnections.size();i++){
                // Get the thread from the listener.awaitingConnections and cast it to a MasterWorkerThread
                MasterWorkerThread worker = (MasterWorkerThread)listener.awaitingConnections.get(i);
                // Set the worker id to i + NumOfWorkers
                worker.id = i+NumOfWorkers;
                // Print message to console
                System.out.print("Renaming " + worker.name + " to ");
                // Update the worker's name using the worker's id
                worker.name = "Worker_"+worker.id;
                // Print the new name of the worker
                System.out.println(worker.name + " @ Listener.");
            }
            // Return false because there was a disconnection
            return false;
        }

        // Construct X from each worker's job

        // Set start to 0
        start = 0;
        // Set end to 0
        end = 0;
        // Set idx to 0
        idx = 0;

        // For every thread in the workersList
        for(Thread t : workersList){
            // Cast the thread to a MasterWorkerThread
            MasterWorkerThread worker = (MasterWorkerThread)t;
            // Increase end variable with the rows of the Worker with index idx
            end += rowsForWorkers.get(idx);
            // For i from 0 to end-start-1
            for(int i=0;i<end-start;i++){
                // Set the Master's X matrix start+i row to the worker's X i row
                X.setRowVector(start+i,worker.X.getRowVector(i));
            }
            // Assign end value to start
            start = end;
            // Increment idx
            idx++;
        }
        // Return true because everything was successful
        return true;

    }

    // Method that distributes the calculation of Y matrix to the Workers
    // returns true if successful, false otherwise
    public boolean distributeYMatrixToWorkers(){

        // We need to send to each worker:
        // X, R (specific cols), Ik

        // Initialize idx to 0
        int idx = 0;
        // Initialize start to 0
        int start = 0;
        // Initialize end to 0
        int end = 0;

        // Boolean that indicates that an error with the PING command has occured
        boolean pingErr = false;

        // Print message to console
        System.out.println("Sending PING to all Workers.");
        // For every thread in the workersList
        for(Thread t : workersList) {
            // Cast the thread to a MasterWorkerThread
            MasterWorkerThread worker = (MasterWorkerThread) t;
            // Call the sendPing() method to the worker and if it has failed set pingErr to true
            if(!worker.sendPing()) pingErr = true;
        }

        // If there is a worker that didn't answer to PING then skip this part -- if pingErr is false
        if(!pingErr) {
            // Print message to console
            System.out.println("Sending data to all Workers.");
            // For every thread in the workersList
            for (Thread t : workersList) {
                // Cast the thread to a MasterWorkerThread
                MasterWorkerThread worker = (MasterWorkerThread) t;
                // Increase end variable with the cols of the Worker with index idx
                end += colsForWorkers.get(idx);
                // Create a sparse matrix Rtemp that has M users and end-start POIs
                OpenMapRealMatrix Rtemp = new OpenMapRealMatrix(M, end - start);
                // For i from 0 to end-start-1
                for (int i = 0; i < end - start; i++) {
                    // Set the col i of Rtemp matrix to the col start+i of the R matrix
                    Rtemp.setColumnVector(i, R.getColumnVector(start + i));
                }
                // Assign the end value to start
                start = end;
                // Call sendCalculateYiData method to the worker in order to send the data
                // for the Y Matrix calculation
                worker.sendCalculateYiData(X, Ik, Rtemp);
                // Increment idx
                idx++;

            }
            // Print message to console
            System.out.println("Sending START command to all Workers.");

            // Create a CountDownLatch that requires NumOfWorkers countdowns to unlock
            CountDownLatch latch = new CountDownLatch(NumOfWorkers);

            // For every thread in the workersList
            for (Thread t : workersList) {
                // Cast the thread to a MasterWorkerThread
                MasterWorkerThread worker = (MasterWorkerThread) t;
                // Call the calculate method to the worker in order
                // to send the START command, so that the workers will
                // start calculating the Y matrix
                worker.calculate();
                // Set the Worker's latch to the latch created above
                worker.latch = latch;
            }

            try {
                // Print message to console
                System.out.println("Waiting for Workers to finish.");
                // Wait for the latch to unlock -- after the Workers' countdowns
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        // Check that all workers are running

        // Boolean to indicate if there was a worker disconnection
        boolean workerDc = false;
        // Create a list of all the 'to-be-removed' Workers
        ArrayList<Thread> removeList = new ArrayList<>();
        // For every thread in the workersList
        for(Thread t : workersList){
            // Cast the thread to a MasterWorkerThread
            MasterWorkerThread worker = (MasterWorkerThread)t;
            // If the worker's connection isn't alive
            if(!worker.alive){
                // Set workerDc to true
                workerDc = true;
                // Add the thread to the removeList
                removeList.add(t);
                // Set the worker's terminate boolean to true
                worker.terminate = true;
            }
        }
        // If there was a disconnection -- if workerDc is true
        if(workerDc){
            // Print message to console
            System.out.println("Master found disconnected Workers.");
            // Print message to console
            System.out.println("Removing disconnected Workers.");
            // Remove all the disconnected workers from the workersList
            workersList.removeAll(removeList);
            // Print message to console
            System.out.println("Removed " + removeList.size() + " Workers.");
            // Change worker names and ids of remaining workers and numOfWorkers
            // Set NumOfWorkers to the workersList size
            NumOfWorkers = workersList.size();
            // For i from 0 to numOfWorkers-1
            for(int i=0;i<NumOfWorkers;i++){
                // Get the thread from the workerList and cast it to a MasterWorkerThread
                MasterWorkerThread worker = (MasterWorkerThread)workersList.get(i);
                // Set the worker id to i
                worker.id = i;
                // Print message to console
                System.out.print("Renaming " + worker.name + " to ");
                // Update the worker's name using the worker's id
                worker.name = "Worker_"+worker.id;
                // Print the new name of the worker
                System.out.println(worker.name);
            }
            // Rename Workers in MasterListener
            // For i from 0 to listener.awaitingConnections.size()-1
            for(int i=0;i<listener.awaitingConnections.size();i++){
                // Get the thread from the listener.awaitingConnections and cast it to a MasterWorkerThread
                MasterWorkerThread worker = (MasterWorkerThread)listener.awaitingConnections.get(i);
                // Set the worker id to i + NumOfWorkers
                worker.id = i+NumOfWorkers;
                // Print message to console
                System.out.print("Renaming " + worker.name + " to ");
                // Update the worker's name using the worker's id
                worker.name = "Worker_"+worker.id;
                // Print the new name of the worker
                System.out.println(worker.name + " @ Listener.");
            }
            // Return false because there was a disconnection
            return false;
        }

        // Construct X from each worker's job

        // Set start to 0
        start = 0;
        // Set end to 0
        end = 0;
        // Set idx to 0
        idx = 0;

        // For every thread in the workersList
        for(Thread t : workersList){
            // Cast the thread to a MasterWorkerThread
            MasterWorkerThread worker = (MasterWorkerThread)t;
            // Increase end variable with the cols of the Worker with index idx
            end += colsForWorkers.get(idx);
            // For i from 0 to end-start-1
            for(int i=0;i<end-start;i++){
                // Set the Master's Y matrix start+i row to the worker's Y i row
                Y.setRowVector(start+i,worker.Y.getRowVector(i));
            }
            // Assign end value to start
            start = end;
            // Increment idx
            idx++;
        }
        // Return true because everything was successful
        return true;

    }

    // Method that calculates the L2 norm of a RealVector to the power of 2
    // (we created this method, to avoid using the RealVector's method getNorm(),
    // as it would do 2 more operations, a sqrt and a power of 2, in order to produce
    // the required result)
    public double normPow2(RealVector vec){
        // Initialize res to 0.0
        double res = 0.0;
        // For every entry of the vector
        for(int i=0;i<vec.getDimension();i++){
            // Increase res by the vector's entry to the power of 2
            res += Math.pow(vec.getEntry(i),2);
        }
        // Return the res variable
        return res;
    }

    // Method that calculates the error - aka the cost function
    // which the Master tries to minimize in the training
    public double calculateError(){
        // Initialize sum to 0.0
        double sum = 0.0;

        // For every user
        for(int u=0;u<M;u++){
            // For evert POI
            for(int i=0;i<N;i++){
                // Add to sum the cui*(pui - xu^t*yi)^2
                // xu^T * yi here is the u row of X * i row of Y transposed instead which is the same thing
                sum += C.getEntry(u,i) * Math.pow(( P.getEntry(u,i) - X.getRowMatrix(u).multiply(Y.getRowMatrix(i).transpose()).getEntry(0,0)),2);
            }
        }

        // Initialize sum of xu norms to 0.0
        double sumNormXu = 0.0;
        // Initialize sum of yi norms to 0.0
        double sumNormYi = 0.0;

        // For every user u
        for(int u=0;u<M;u++){
            // Add to sumNormXu the norm of xu to the power of 2
            sumNormXu += normPow2(X.getRowVector(u));
        }

        // For every POI i
        for(int i=0;i<N;i++){
            // Add to sumNormYi the norm of yi to the power of 2
            sumNormYi += normPow2(Y.getRowVector(i));
        }

        // return the sum added by the regularization term (lambda * (sumNormXu + sumNormYi))
        return sum + lambda*( sumNormXu + sumNormYi );
    }

    // Method that calculates the recommendation for user u and POI i
    public double calculateScore(int u, int i){
        // r^ui = xu^T * yi
        // Here we return the u row of X * i row of Y transposed instead which is the same thing
        return X.getRowMatrix(u).multiply(Y.getRowMatrix(i).transpose()).getEntry(0,0);

    }

    // Method that calculates the K best POIs for the user u using criterion crit
    public List<Poi> calculateBestKPoisForUser(int u, int k, Criterion crit){

        // Create a TreeMap for the POI Scores
        // The map has as a key the score and as a value the index of the POI,
        // in the R matrix (or in the POI list)
        // The TreeMap class sorts it's entries according the key, here we use the
        // reverse order sorting
        // TODO: FIX CHANGE TREEMAP BECAUSE SAME VALUES WILL BE IGNORED
        Map<Double,Integer> poiScores = new TreeMap<>(Collections.reverseOrder());

        // For every POI
        for(int i=0;i<N;i++){
            // Calculate the score for the user u and POI i
            // and put the score along with the POI index in the Treemap
            poiScores.put(calculateScore(u,i),i);
        }

        // Create a list of the POI indices
        ArrayList<Integer> poiIndices = new ArrayList<>();

        // Initialize stop int to 0
        int stop = 0;
        // For every entry in poiScores
        for(Map.Entry<Double,Integer> e : poiScores.entrySet()){
            // If stop is equal to k then break the loop
            if(stop==k) break;

            // If the criterion isn't null
            if(crit!=null) {
                // If the criterion isn't met then skip this POI
                if (!crit.meetsCriterion(poiList.get(e.getValue()))) continue;
            }

            // Add the index of the POI to the poiIndices list
            poiIndices.add(e.getValue());
            // Increment stop
            stop++;
        }

        // We have the indices of the best POIs

        // Create a list of the K best POIs
        ArrayList<Poi> bestKPois = new ArrayList<>();

        // For every index in poiIndices
        for(Integer i : poiIndices){
            // Add the POI with the specific index to the bestKPois list
            bestKPois.add(poiList.get(i));
        }

        // Return the list of K best POIs
        return bestKPois;
    }

    // Method that saves matrices X and Y from Master to filenameX and filenameY
    public void saveMatricesXY(String filenameX, String filenameY){

        // Initialize buffered writers to null
        BufferedWriter bwX = null;
        BufferedWriter bwY = null;

        try {
            // Create the buffered writers from the corresponding filenames
            bwX = new BufferedWriter(new FileWriter(new File(filenameX)));
            bwY = new BufferedWriter(new FileWriter(new File(filenameY)));

            // Write to filenameX the X dimensions
            bwX.write(X.getRowDimension() + " x " + X.getColumnDimension());
            // Write to filenameX a new line
            bwX.newLine();

            // For i from 0 to X Row Dimension - 1
            for(int i=0;i<X.getRowDimension();i++){
                // For j from 0 to X Column Dimension - 2
                for(int j=0;j<X.getColumnDimension()-1;j++){
                    // Write X entry value at i,j to file with comma separator
                    bwX.write(X.getEntry(i,j)+", ");
                }
                // Write the last i,j entry to file without a comma separator
                bwX.write(X.getEntry(i,X.getColumnDimension()-1)+"");
                // Write a new line
                bwX.newLine();
            }

            // Write to filenameY the Y dimensions
            bwY.write(Y.getRowDimension() + " x " + Y.getColumnDimension());
            // Write to filenameX a new line
            bwY.newLine();

            // For i from 0 to Y Row Dimension - 1
            for(int i=0;i<Y.getRowDimension();i++){
                // For j from 0 to Y Column Dimension - 2
                for(int j=0;j<Y.getColumnDimension()-1;j++){
                    // Write Y entry value at i,j to file with comma separator
                    bwY.write(Y.getEntry(i,j)+", ");
                }
                // Write the last i,j entry to file without a comma separator
                bwY.write(Y.getEntry(i,Y.getColumnDimension()-1)+"");
                // Write a new line
                bwY.newLine();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                // If bwX isn't null, close it
                if(bwX!=null) {
                    bwX.close();
                }
                // If bwY isn't null, close it
                if(bwY!=null) {
                    bwY.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    // Method that loads matrices X and Y to the Master from two corresponding filenames
    public void loadMatricesXY(String filenameX, String filenameY){
        // Initialize buffered readers to null
        BufferedReader bfX = null;
        BufferedReader bfY = null;
        try {
            // Create the buffered readers from the corresponding filenames
            bfX = new BufferedReader(new FileReader(new File(filenameX)));
            bfY = new BufferedReader(new FileReader(new File(filenameY)));

            // Declare a String which will hold the lines of the files
            String line;

            // Read a line from bfX
            line = bfX.readLine();
            // Get the dimensions of X from the first line
            String[] lineParts = line.split("x");
            // Create an empty matrix X using the dimensions above
            X = MatrixUtils.createRealMatrix(Integer.parseInt(lineParts[0].trim()),Integer.parseInt(lineParts[1].trim()));
            // Initialize int i to 0
            int i=0;
            // While there are more lines in bfX, read the line
            while((line=bfX.readLine())!= null){
                // Split the line using comma as a separator
                String[] line_parts = line.split(",");
                // Check the K dimension and throw an IOException if incorrect
                if(line_parts.length != K) throw new IOException();
                // For every value in the line_parts
                for(int j=0;j<line_parts.length;j++) {
                    // Set entry of matrix X using i,j and the value from line_parts[j]
                    X.setEntry(i, j, Double.parseDouble(line_parts[j].trim()));
                }
                // Increment i
                i++;
            }

            // Read a line from bfY
            line = bfY.readLine();
            // Get the dimensions of Y from the first line
            lineParts = line.split("x");
            // Create an empty matrix Y using the dimensions above
            Y = MatrixUtils.createRealMatrix(Integer.parseInt(lineParts[0].trim()),Integer.parseInt(lineParts[1].trim()));
            // Initialize int i to 0
            i=0;
            // While there are more lines in bfY, read the line
            while((line=bfY.readLine())!= null){
                // Split the line using comma as a separator
                String[] line_parts = line.split(",");
                // Check the K dimension and throw an IOException if incorrect
                if(line_parts.length != K) throw new IOException();
                // For every value in the line_parts
                for(int j=0;j<line_parts.length;j++) {
                    // Set entry of matrix Y using i,j and the value from line_parts[j]
                    Y.setEntry(i, j, Double.parseDouble(line_parts[j].trim()));
                }
                // Increment i
                i++;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            // If bfX isn't null then close it
            if(bfX!=null){
                try {
                    bfX.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
            // If bfY isn't null then close it
            if(bfY!=null){
                try {
                    bfY.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }

        }
    }

    // Method that terminates all Master's Workers
    public void terminateWorkers(){
        // For every thread in workersList
        for (Thread t : workersList) {
            // Cast the thread to a MasterWorkerThread
            MasterWorkerThread worker = (MasterWorkerThread) t;
            // Call the terminate method to the worker
            worker.terminate();
        }

    }

    // Method that terminates the Master-Server
    public void terminateServer(){
        try {
            // Terminate the Master's Workers first
            terminateWorkers();
            // Print message to console
            System.out.println("Workers terminated.");
            // If the listener isn't null, terminate the listener
            if(listener!=null) listener.terminateListener();
            // Wait for a second for the listener to terminate
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // Close the Server Socket
            providerSocket.close();
            // Print message to console
            System.out.println("Master terminated.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*
    Method that finds the Geographical distance between two coordinates (lat,lon) in the Map
    taking in account the curvature of the Earth, but not the height of the two coordinates.
    (Haversine formula)
     */
    public static double distance(double lat1, double lat2, double lon1, double lon2) {
        // Radius of the earth
        final int R = 6371;
        // Find the distance between the two latitudes
        double latD = Math.toRadians(lat2 - lat1);
        // Find the distance between the two longitudes
        double lonD = Math.toRadians(lon2 - lon1);
        // Calculate sin^2(latD/2) + sin^2(lonD/2) + cos(latD/2)*cos(lonD/2)
        double a = Math.sin(latD / 2) * Math.sin(latD / 2) + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lonD / 2) * Math.sin(lonD / 2);
        // Calculate 2* atan2(sqrt(a), sqrt(1-a))
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        // Convert to meters
        double distance = R * c * 1000;
        // Return distance
        return distance;
    }

}
