package ds.ergasia;

import android.annotation.SuppressLint;
import android.app.ProgressDialog;
import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.Color;
import android.graphics.drawable.Drawable;
import android.location.Address;
import android.location.Geocoder;
import android.location.Location;
import android.location.LocationListener;
import android.location.LocationManager;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.os.AsyncTask;
import android.support.v4.app.FragmentActivity;
import android.os.Bundle;
import android.util.Log;
import android.view.MotionEvent;
import android.view.View;
import android.view.inputmethod.InputMethodManager;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ImageView;
import android.widget.SeekBar;
import android.widget.TextView;
import android.widget.Toast;

import com.google.android.gms.maps.CameraUpdateFactory;
import com.google.android.gms.maps.GoogleMap;
import com.google.android.gms.maps.OnMapReadyCallback;
import com.google.android.gms.maps.SupportMapFragment;
import com.google.android.gms.maps.model.BitmapDescriptorFactory;
import com.google.android.gms.maps.model.Circle;
import com.google.android.gms.maps.model.CircleOptions;
import com.google.android.gms.maps.model.LatLng;
import com.google.android.gms.maps.model.Marker;
import com.google.android.gms.maps.model.MarkerOptions;
import com.squareup.picasso.Picasso;
import com.squareup.picasso.Target;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import static android.location.LocationManager.GPS_PROVIDER;
import static android.location.LocationManager.NETWORK_PROVIDER;
import static android.location.LocationManager.PASSIVE_PROVIDER;

public class MapsActivity extends FragmentActivity implements OnMapReadyCallback {

    // The GoogleMap
    private GoogleMap mMap;

    // A LocationListener Object used by the Location Manager to get the user's GPS Location
    private final LocationListener mLocationListener = new LocationListener() {
        @Override
        public void onLocationChanged(Location location) {

        }

        @Override
        public void onStatusChanged(String s, int i, Bundle bundle) {

        }

        @Override
        public void onProviderEnabled(String s) {

        }

        @Override
        public void onProviderDisabled(String s) {

        }
    };

    // The Location Manager that gets the user's GPS Location
    private LocationManager mLocationManager;

    // The user's actual GPS Location
    private Location myLocation;

    // Reference to the user's current Location ( the blue marker )
    private Location currLocation;

    // An ArrayList of all the markers in the google map
    private ArrayList<Marker> markers;

    // A marker of a Location that the user's selected using long tap
    // on the google map ( the magenta marker )
    private Marker selectedLocationMarker;

    // The Address Bar
    private EditText etAddress;

    // The Go Button
    private Button btnGo;

    // The SeekBar for the Max Distance
    private SeekBar seekBar;

    // The Text View for the Max Distance
    private TextView tvSeek;

    // The Text View for the minus symbol, which decreases the Max Results
    private TextView tvMinus;

    // The Text View for the Max Results
    private TextView tvK;

    // The Text View for the plus symbol, which increases the Max Results
    private TextView tvPlus;

    // The host -- the master's ip
    private String host;

    // The port number of the master
    private int portNum;

    // The UID to send to master
    private int uid;

    // The Max Results - K to send to master
    private int k;

    // The Max Distance to send to master to filter the POIs
    private double maxD;

    // The client that will connect to the master
    private Client client;

    /*
    Method that executes code when the Android Activity is created
     */
    @SuppressLint("ClickableViewAccessibility")
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        // Set the content view to the maps activity layout
        setContentView(R.layout.activity_maps);
        // Obtain the SupportMapFragment and get notified when the map is ready to be used
        SupportMapFragment mapFragment = (SupportMapFragment) getSupportFragmentManager()
                .findFragmentById(R.id.map);
        // Get the Google Map using async call -- callback is onMapReady() on this activity
        mapFragment.getMapAsync(this);

        // Find the Address Bar Edit Text using R
        etAddress = (EditText) findViewById(R.id.etAddress);

        // Find the Go Button using R
        btnGo = (Button) findViewById(R.id.btnGo);

        // Set an onClickListener to the go button
        btnGo.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                // When go button is clicked

                // Hide the keyboard
                InputMethodManager imm = (InputMethodManager)getSystemService(Context.INPUT_METHOD_SERVICE);
                if(imm!=null) imm.hideSoftInputFromWindow(view.getWindowToken(),0);
                // Call updateLocation() to update the current location
                updateLocation();
            }
        });

        // Find the Seek Bar for the Max Distance using R
        seekBar = (SeekBar) findViewById(R.id.seekBar);

        // Find the Text View for the Max Distance using R
        tvSeek = (TextView) findViewById(R.id.tvSeek);

        // Create an onSeekBarChangeListener to handle the seek bar's value changes
        seekBar.setOnSeekBarChangeListener(new SeekBar.OnSeekBarChangeListener() {

            // Initialize a variable progress to 1 which will be the min
            // value for the seek bar (the progress value is maxD * 10)
            int progress = 1;

            /*
            If the progress of the seek bar has been changed this method is called, as a callback
             */
            @Override
            public void onProgressChanged(SeekBar seekBar, int i, boolean b) {
                // i is the new progress

                // If i is smaller that 1 then set progress to 1
                if(i<5) progress = 5;
                // Else set the progress to i
                else progress = i;
                // Make sure it is a multiple of 5
                progress = (int)(Math.round((progress*1.0)/5)*5);
                // Change the maxD variable according to the progress variable
                maxD = ((progress*1.0)/10);
                // Change the text of the Text View using the progress variable
                tvSeek.setText(""+((progress*1.0)/10));
            }

            @Override
            public void onStartTrackingTouch(SeekBar seekBar) {

            }

            @Override
            public void onStopTrackingTouch(SeekBar seekBar) {

            }
        });

        // Change the progress in the seekBar using the maxD variable
        seekBar.post(new Runnable() {
            @Override
            public void run() {
                seekBar.setProgress((int)(maxD*10));
            }
        });

        // Set the step value that the seek bar will increment on manual changes from the user
        seekBar.incrementProgressBy(1);

        // Set the max value for the seek bar (max progress value is the max value of maxD * 10)
        seekBar.setMax(5000);

        // Find the Text View for the minus symbol for the Max Results
        tvMinus = (TextView) findViewById(R.id.tvMinus);

        // Find the Text View for the Max Results
        tvK = (TextView) findViewById(R.id.tvK);

        // Change the text of the max results Text View using max results variable (k)
        tvK.post(new Runnable() {
            @Override
            public void run() {
                tvK.setText(""+k);
            }
        });

        // Find the Text View for the plus symbol for the Max Results
        tvPlus = (TextView) findViewById(R.id.tvPlus);

        // Set an onTouchListener for the minus Text View
        tvMinus.setOnTouchListener(new View.OnTouchListener() {

            // Threshold variable that handles how fast the max results variable (k) is decreased
            int threshold = 5;

            /*
            Method is a called when the TextView is touched
             */
            @Override
            public boolean onTouch(View view, MotionEvent motionEvent) {
                // Decrease the threshold
                threshold--;
                // If the threshold is less or equal to 0
                if(threshold<=0) {
                    // Decrease the max results variable
                    k--;
                    // If the max results variable is less than 0 then set it to 0 (as a min value)
                    if (k < 0) k = 0;
                    // Change the text of the max results Text View
                    tvK.setText("" + k);
                    // Reset threshold to 5
                    threshold = 5;
                }
                return true;
            }
        });

        // Set an onTouchListener for the plus Text View
        tvPlus.setOnTouchListener(new View.OnTouchListener() {

            // Threshold variable that handles how fast the max results variable (k) is increased
            int threshold = 5;

            /*
            Method is a called when the TextView is touched
             */
            @Override
            public boolean onTouch(View view, MotionEvent motionEvent) {
                // Decrease the threshold
                threshold--;
                // If the threshold is less or equal to 0
                if(threshold<=0) {
                    // Increase the max results variable
                    k++;
                    // Change the text of the max results Text View
                    tvK.setText("" + k);
                    // Reset threshold to 5
                    threshold = 5;
                }
                return true;
            }
        });

        // Get the extras from the MainActivity as a Bundle Object
        Bundle extras = getIntent().getExtras();

        // Initialize values for the Client
        host = null;
        portNum = -1;
        uid = -1;
        k = -1;
        maxD = -1;

        // Get the values from extras if extras is not null
        if(extras!=null){
            host = extras.getString("host");
            portNum = extras.getInt("port");
            uid = extras.getInt("uid");
            k = extras.getInt("k");
            maxD = extras.getDouble("maxD");
            // truncate the maxD to one decimal and make it a multiple of 0.5
            maxD = maxD * 10;
            maxD = (int)(Math.round((maxD*1.0)/5)*5);
            maxD = maxD / 10;
        }else{
            // If extras is null log error
            Log.e("ERROR","Extras are null.");
        }

        /*
        In this section the app will try to get the device's location
         */

        // Get the location manager as a system service
        mLocationManager = (LocationManager)getSystemService(LOCATION_SERVICE);

        // Initialize myLocation to null
        myLocation = null;

        // Try to get location
        try {
            // Request a location update from the GPS provider
            mLocationManager.requestLocationUpdates(GPS_PROVIDER, 1000L, 500.0f, mLocationListener);
            // Get the last known location from the GPS provider
            myLocation = mLocationManager.getLastKnownLocation(GPS_PROVIDER);
            // If myLocation is null then GPS provider did not return a location
            if (myLocation == null) {
                // Request a location update from the NETWORK provider
                mLocationManager.requestLocationUpdates(NETWORK_PROVIDER, 1000L, 500.0f, mLocationListener);
                // Get the last known location from the NETWORK provider
                myLocation = mLocationManager.getLastKnownLocation(NETWORK_PROVIDER);
            }
            // If myLocation is null then NETWORK provider did not return a location
            if (myLocation == null) {
                // Request a location update from the PASSIVE provider
                mLocationManager.requestLocationUpdates(PASSIVE_PROVIDER, 1000L, 500.0f, mLocationListener);
                // Get the last known location from the PASSIVE provider
                myLocation = mLocationManager.getLastKnownLocation(PASSIVE_PROVIDER);
            }
        }catch(SecurityException e){
            // If was a security exception print the stack trace
            e.printStackTrace();
        }

        // If myLocation is not null
        if(myLocation!=null) {

            // Change the current location the device's location
            currLocation = myLocation;

            // Create a new Client Object
            client = new Client();

            // Run Client using parameters and the device's location
            client.execute(host, portNum, uid, k, maxD, myLocation);

        }else{
            // If myLocation is null then none provider could get the device's location

            // Send a Toast to the user to inform him that he should enable the GPS Location or instead enter an address at the address bar
            Toast.makeText(this, "The app can't find your location. Check if your location services are disabled. Alternatively, you can input the location" +
                    " using the address bar on the top.", Toast.LENGTH_LONG).show();

        }


    }

    /**
     * Manipulates the map once available.
     * This callback is triggered when the map is ready to be used.
     * This is where we can add markers or lines, add listeners or move the camera. In this case,
     * we just add a marker near Sydney, Australia.
     * If Google Play services is not installed on the device, the user will be prompted to install
     * it inside the SupportMapFragment. This method will only be triggered once the user has
     * installed Google Play services and returned to the app.
     */
    @Override
    public void onMapReady(GoogleMap googleMap) {
        // Set mMap variable to googleMap
        mMap = googleMap;
        // Create the markers list
        markers = new ArrayList<>();

        // Set a custom InfoWindowAdapter for the mMap
        mMap.setInfoWindowAdapter(new GoogleMap.InfoWindowAdapter() {

            // List of all targets -- used as a strong reference for the garbage collector error of Picasso
            final List<Target> targets = new ArrayList<>();

            /*
            Method that is called when an InfoWindow is called to open
             */
            @Override
            public View getInfoWindow(final Marker marker) {

                // Inflate the view provided in the layout XML
                View v = getLayoutInflater().inflate(R.layout.custom_info_window, null);

                // Assign the title and snippet text from the marker
                // to the TextViews in the layout
                String markerTitle = marker.getTitle();
                String markerSnippet = marker.getSnippet();

                // Get the marker's photo link from the marker's tag
                String markerPhoto = (String)marker.getTag();

                // If the marker's title isn't null
                if (markerTitle != null) {
                    // Find the TextView title from R and set it to viewTitle
                    TextView viewTitle = (TextView) v.findViewById(R.id.title);
                    // Set viewTitle text to the marker's title
                    viewTitle.setText(markerTitle);

                    // If the marker's snippet isn't null
                    if (markerSnippet != null){
                        // Find the TextView info from R and set it to viewInfo
                        TextView viewInfo = (TextView) v.findViewById(R.id.info);
                        // Set the viewInfo's text to the marker's snippet
                        viewInfo.setText(markerSnippet);
                    }else{
                        // If the marker's snippet is null
                        // Find the TextView info from R and set it to viewInfo
                        TextView viewInfo = (TextView) v.findViewById(R.id.info);
                        // Set visibility to GONE for viewInfo, so that it doesn't
                        // take space in the info window
                        viewInfo.setVisibility(View.GONE);
                    }

                    // If the marker's photo link is null or it equals "Not exists"
                    if(markerPhoto!=null && !markerPhoto.equals("Not exists")){
                        // Find the ImageView imageView from R and set it to imageView
                        final ImageView imageView = (ImageView) v.findViewById(R.id.imageView);
                        // Create a new Target Object and set it as final
                        final Target target = new Target() {
                            /*
                            Method that is called when the bitmap is loaded from Picasso
                             */
                            @Override
                            public void onBitmapLoaded(Bitmap bitmap, Picasso.LoadedFrom from) {
                                // Log to debug
                                Log.d("DEBUG","onBitmapLoaded");
                                // Set imageView's image to the bitmap
                                imageView.setImageBitmap(bitmap);
                                // If the marker isn't null and the marker's info window is shown
                                if(marker!=null && marker.isInfoWindowShown()){
                                    // Hide the info window
                                    marker.hideInfoWindow();
                                    // Show the info window ( with the loaded image )
                                    marker.showInfoWindow();
                                }
                                // If this target object is in the targets list, remove it
                                if(targets.contains(this)) targets.remove(this);
                            }

                            /*
                            Method that is called if the bitmap failed to load
                             */
                            @Override
                            public void onBitmapFailed(Exception e, Drawable errorDrawable) {
                                // Log to ERROR
                                Log.e("ERROR","onBitmapFailed");
                            }

                            /*
                            Method that is called before the bitmap is loaded
                             */
                            @Override
                            public void onPrepareLoad(Drawable placeHolderDrawable) {
                                // Logto DEBUG
                                Log.d("DEBUG","onPrepareLoad");
                            }
                        };
                        // Call Picasso to load the image from the marker's photo link
                        // resize it to 500x500 pixels
                        // center it using crop
                        // and load it into the target
                        Picasso.get().load(markerPhoto).resize(500,500).centerCrop().into(target);
                        // Add the target to the targets list, in order to create a strong reference and not let the garbage collector
                        // remove the target from the memory
                        targets.add(target);
                        // Set the visibility of the imageView to VISIBLE
                        imageView.setVisibility(View.VISIBLE);
                    }else{
                        // Find the ImageView imageView from R and set it to imageView
                        ImageView imageView = (ImageView) v.findViewById(R.id.imageView);
                        // Set visibility to GONE for imageView, so that it doesn't
                        // take space in the info window
                        imageView.setVisibility(View.GONE);
                    }

                    // Return the custom view
                    return v;
                } else {
                    // The marker's title is null which means it doesn't have one

                    // Return null -- meaning normal marker click behaviour
                    return null;
                }
            }

            @Override
            public View getInfoContents(Marker marker) {
                return null;
            }

        });

        // Set an OnMapLongClickListener to the mMap
        mMap.setOnMapLongClickListener(new GoogleMap.OnMapLongClickListener() {
            /*
            Method that is called when the user long taps on the map
             */
            @Override
            public void onMapLongClick(LatLng point) {
                // Set the address bar's text to the point's coordinates that
                // the user long clicked
                etAddress.setText(""+point.latitude + ", " + point.longitude);

                // If the selectedLocationMarker isn't null, remove it from the map
                if(selectedLocationMarker!=null) selectedLocationMarker.remove();

                // Add a magenta marker to the point the user long clicked with the title "Selected Location" on the map
                // and set the returned Marker object to selectedLocationMarker
                selectedLocationMarker = mMap.addMarker(new MarkerOptions().position(point).title("Selected Location").icon(BitmapDescriptorFactory.defaultMarker(BitmapDescriptorFactory.HUE_MAGENTA)));

                // Send a Toast to the user informing him about the selected location coordinates
                Toast.makeText(MapsActivity.this, "Selected location with coordinates: "+point+".", Toast.LENGTH_SHORT).show();
            }
        });

        // Set an OnMapClickListener to the mMap
        mMap.setOnMapClickListener(new GoogleMap.OnMapClickListener() {
            /*
            Method that is called when the user taps on the map
             */
            @Override
            public void onMapClick(LatLng latLng) {
                // If the selectedLocationMarker isn't null
                if(selectedLocationMarker!=null) {
                    // Remove the selectedLocationMarker from the Map
                    selectedLocationMarker.remove();
                    // Set it to null
                    selectedLocationMarker = null;
                }
                // Clear the text of the address bar
                etAddress.setText("");
                // Hide the keyboard
                InputMethodManager imm = (InputMethodManager)getSystemService(Context.INPUT_METHOD_SERVICE);
                if(imm!=null && getCurrentFocus()!=null) imm.hideSoftInputFromWindow(getCurrentFocus().getWindowToken(),0);
            }
        });

        // Set an OnMarkerClickListener to the mMap
        mMap.setOnMarkerClickListener(new GoogleMap.OnMarkerClickListener() {
            /*
            Method that is called when a marker is tapped from the user
             */
            @Override
            public boolean onMarkerClick(Marker marker) {
                // Set the text of the address bar to the marker's lat, lon coordinates
                etAddress.setText(""+marker.getPosition().latitude+", "+marker.getPosition().longitude);
                // Return false
                return false;
            }
        });

        try {
            // Try to enable MyLocation for the mMap
            mMap.setMyLocationEnabled(true);
        }catch(SecurityException se){
            // If there was a security exception then print the stack trace
            se.printStackTrace();
        }

        // If the device location isn't null
        if(myLocation!=null){
            // Create a LatLng Object from the device's location
            LatLng currLoc = new LatLng(myLocation.getLatitude(),myLocation.getLongitude());

            // Create a MarkerOptions object
            MarkerOptions options = new MarkerOptions();
            // Set the position to the currLoc
            options.position(currLoc);
            // Set the title to "Current Location"
            options.title("Current Location");
            // Change the icon of the marker to blue
            options.icon(BitmapDescriptorFactory.defaultMarker(BitmapDescriptorFactory.HUE_BLUE));

            // Create a marker for the google map and add it to the markers list
            markers.add(mMap.addMarker(options));

            // Animate camera to current Location and zoom level 8.0
            mMap.animateCamera(CameraUpdateFactory.newLatLngZoom(currLoc,8.0f),2000,null);
        }

    }

    /*
    Method that updates the Google Map with the markers for the POIs and the current location of the user
     */
    public void updateMap(){
        // Get the list of the best Pois from the client
        List<Poi> bestPois = client.kBestPois;

        // Initialize a variable which holds the distance of the farthest POI from the current Location
        double maxDist = 0.0;

        // Initialize i to 1
        int i = 1;

        // For each POI in the bestPois list
        for(Poi p : bestPois){

            // Get the LatLng coordinates from the Poi
            LatLng poiCoordinates = new LatLng(p.latitude,p.longitude);

            // Find the distance between the Poi and the current Location
            double dist = distance(p.latitude,currLocation.getLatitude(),p.longitude,currLocation.getLongitude());

            // If the distance is bigger than the max distance
            if(dist > maxDist){
                // Set the max distance to the distance
                maxDist = dist;
            }

            // Create a MarkerOptions object
            MarkerOptions options = new MarkerOptions();
            // Set the position to the Poi's coordinates
            options.position(poiCoordinates);
            // Set the title to the Poi's name
            options.title(p.name);
            // Set the snippet to inform about the ranking and the category of the Poi
            options.snippet("Ranking: "+(i++) + "\nCategory: " + p.category);

            // Add the marker to the Google Map and get the marker's reference
            Marker marker = mMap.addMarker(options);

            // Set a tag to the marker of the Poi's photo link
            marker.setTag(p.photo);

            // Add the marker to the markers list
            markers.add(marker);

        }

        // Get the LatLng coordinates of the current Location
        LatLng currLoc = new LatLng(currLocation.getLatitude(),currLocation.getLongitude());

        // Set a radius variable to the maxDist increasing it by 100 meters ( for better visibility )
        double radius = maxDist+100;

        // Create a circle on the Google Map with the current location as a center and using the above radius
        Circle circle = mMap.addCircle(new CircleOptions().center(currLoc).radius(radius));

        // Set the circle to be invisible
        circle.setVisible(false);

        // Create another circle on the Google Map with the current location as a center, using maxD*1000 as a radius
        // using a red stroke color for the outline
        Circle circle2 = mMap.addCircle(new CircleOptions().center(currLoc).radius(maxD*1000).strokeColor(Color.RED).strokeWidth(5));

        // Set the fill color to a transparent blue -- to show the area that the user requested to search for the best Pois
        circle2.setFillColor(0x220000FF);

        // Set the scale variable to the radius / 500
        double scale = radius / 500;

        // Set the zoom level variable to (15.3 - log(scale) / log(2))
        float zoomLvl = (float) (15.3 - Math.log(scale) / Math.log(2));

        // Animate the google map camera to the current location, using the zoomLvl above
        mMap.animateCamera(CameraUpdateFactory.newLatLngZoom(currLoc,zoomLvl),2000,null);

    }

    /*
    Method that updates the current Location, according to the Address Bar's Text
     */
    public void updateLocation(){

        // Create a connectivity manager and get active network information
        ConnectivityManager cm = (ConnectivityManager)getSystemService(Context.CONNECTIVITY_SERVICE);
        NetworkInfo ni = cm.getActiveNetworkInfo();

        // Check connectivity to internet
        if(ni != null && ni.isConnectedOrConnecting()) {
            // If there is an Internet Connection

            // Check if the Address Bar doesn't have any text
            if (etAddress.getText().toString().equals("")) {
                // If the Address Bar's text is empty

                // If the device location is not null
                if(myLocation!=null) {
                    // Clear the google map from the markers
                    mMap.clear();
                    // Clear the markers list
                    markers.clear();

                    /*
                    Go to myLocation if there is an empty address bar
                    */

                    // Set currLocation to myLocation
                    currLocation = myLocation;

                    // Get the LatLng coordinates of the device's Location
                    LatLng currLoc = new LatLng(myLocation.getLatitude(), myLocation.getLongitude());

                    // Create a MarkerOptions object
                    MarkerOptions options = new MarkerOptions();
                    // Set the position to the currLoc
                    options.position(currLoc);
                    // Set the title to "Current Location"
                    options.title("Current Location");
                    // Change the icon of the marker to blue
                    options.icon(BitmapDescriptorFactory.defaultMarker(BitmapDescriptorFactory.HUE_BLUE));

                    // Create a marker for the google map and add it to the markers list
                    markers.add(mMap.addMarker(options));

                    // Animate the camera to the currLoc using zoom level 8.0
                    mMap.animateCamera(CameraUpdateFactory.newLatLngZoom(currLoc, 8.0f), 2000, null);

                    // Create a Client object
                    client = new Client();
                    // Run Client using parameters and the device's location
                    client.execute(host, portNum, uid, k, maxD, myLocation);

                }else{
                    // Device location is null

                    // Try to get location
                    try {
                        // Request a location update from the GPS provider
                        mLocationManager.requestLocationUpdates(GPS_PROVIDER, 1000L, 500.0f, mLocationListener);
                        // Get the last known location from the GPS provider
                        myLocation = mLocationManager.getLastKnownLocation(GPS_PROVIDER);
                        // If myLocation is null then GPS provider did not return a location
                        if (myLocation == null) {
                            // Request a location update from the NETWORK provider
                            mLocationManager.requestLocationUpdates(NETWORK_PROVIDER, 1000L, 500.0f, mLocationListener);
                            // Get the last known location from the NETWORK provider
                            myLocation = mLocationManager.getLastKnownLocation(NETWORK_PROVIDER);
                        }
                        // If myLocation is null then NETWORK provider did not return a location
                        if (myLocation == null) {
                            // Request a location update from the PASSIVE provider
                            mLocationManager.requestLocationUpdates(PASSIVE_PROVIDER, 1000L, 500.0f, mLocationListener);
                            // Get the last known location from the PASSIVE provider
                            myLocation = mLocationManager.getLastKnownLocation(PASSIVE_PROVIDER);
                        }
                    }catch(SecurityException e){
                        // If was a security exception print the stack trace
                        e.printStackTrace();
                    }

                    // Check if device location is still null
                    if(myLocation!=null) {
                        // If it isn't null, then we got the device locaiton

                        // Set current location to device location
                        currLocation = myLocation;

                        // Call this method again
                        updateLocation();

                    }else{
                        // If it is still null, then we can't get the device location -- maybe location services are deactivated

                        // Send a Toast to the user to inform him that he should enable the GPS Location or instead enter an address at the address bar
                        Toast.makeText(this, "The app can't find your location. Check if your location services are disabled. Alternatively, you can input the location" +
                                " using the address bar on the top.", Toast.LENGTH_LONG).show();

                    }

                }
                // End the method here
                return;
            }
            // If the Address Bar's text is not empty

            /*
            Check if the text is lat,lon coordinates or a destination's name

            lat,long coordinates have the format "%d, %d" where %d is a decimal number (with or without a real part)

            Accepted format examples:
            eg. "23, 41"
                "23.12321, 41"
                "23, 41.98761"
                "23.12321, 41.98761"

            */

            // Split the address bar's text using "," as a delimiter
            String[] parts1 = etAddress.getText().toString().split(",");
            // If the parts of the text are 2
            if(parts1.length==2){
                // Split each text part using "\\." as a delimiter
                String[] parts2a = parts1[0].split("\\.");
                String[] parts2b = parts1[1].split("\\.");
                // Check that each part has 1 part (decimal without real part) or 2 parts (decimal with real part)
                if(parts2a.length > 0 && parts2b.length > 0 && parts2a.length<=2 && parts2b.length<=2){
                    try{
                        // Try to cast them to doubles in order to get the lat,lon values
                        double lat = Double.parseDouble(parts1[0]);
                        double lon = Double.parseDouble(parts1[1]);

                        // Check that the coordinates are correct
                        if(!(lat >= -90.0 && lat <= 90.0 && lon >= -180.0 && lon <= 180.0)){
                            // If they are not correct send Toast to inform the user
                            Toast.makeText(this, "The latitude, longitude coordinates you entered are not correct.", Toast.LENGTH_LONG).show();
                            // End the method
                            return;
                        }


                        /*
                         The lat, lon coordinates are correct, so update the location
                        */

                        // Clear the google map from markers
                        mMap.clear();
                        // Clear the markers list
                        markers.clear();

                        // Create a new location without provider and set it to current location
                        currLocation = new Location("");
                        // Set current location's latitude to lat
                        currLocation.setLatitude(lat);
                        // Set current location's longitude to lon
                        currLocation.setLongitude(lon);

                        // Create a LatLng object from lat and lon
                        LatLng currLoc = new LatLng(lat, lon);

                        // Create a MarkerOptions object
                        MarkerOptions options = new MarkerOptions();
                        // Set the position to the currLoc
                        options.position(currLoc);
                        // Set the title to "Current Location"
                        options.title("Current Location");
                        // Change the icon of the marker to blue
                        options.icon(BitmapDescriptorFactory.defaultMarker(BitmapDescriptorFactory.HUE_BLUE));

                        // Create a marker for the google map and add it to the markers list
                        markers.add(mMap.addMarker(options));

                        // Animate the camera to the currLoc using zoom level 8.0
                        mMap.animateCamera(CameraUpdateFactory.newLatLngZoom(currLoc, 8.0f), 2000, null);

                        // Create a Client object
                        client = new Client();
                        // Run Client using parameters and the current location
                        client.execute(host, portNum, uid, k, maxD, currLocation);

                        // End the method
                        return;
                    }catch(NumberFormatException nfe){
                        // If there was a number format exception it is not lat, lon coordinates
                        // Log to debug
                        Log.d("DEBUG","Not lat,lon coordinates");
                    }
                }
            }

            /*
             If parts of text are not 2 then, the text is not lat,lon coordinates
             So we search using the address bar's text as a destination
            */

            // Create a Geocoder Object
            Geocoder gc = new Geocoder(this);
            // Check if the geocoder is present
            if (gc.isPresent()) {
                // If the geocoder is present
                // Initialize a list of Address objects to null
                List<Address> list = null;
                try {
                    // Try to get the location from the address bar's text
                    list = gc.getFromLocationName(etAddress.getText().toString(), 1);
                } catch (IOException e) {
                    // If there was an io exception print the stack trace
                    e.printStackTrace();
                }
                // If the list of addresses is empty
                if(list.isEmpty()){
                    // Send Toast to user to inform him that no results have been found
                    Toast.makeText(this, "No results found for: " + etAddress.getText().toString(), Toast.LENGTH_SHORT).show();
                }else{
                    // If the list of addresses is not empty

                    // Clear the google map from markers
                    mMap.clear();

                    // Clear the markers list
                    markers.clear();

                    // Get the first address from the addresses list
                    Address address = list.get(0);

                    // Get the latitude of the address
                    double lat = address.getLatitude();

                    // Get the longitude of the address
                    double lon = address.getLongitude();

                    // Create a new location with no provider and set it to the current location
                    currLocation = new Location("");

                    // Set the latitude of the current location to lat
                    currLocation.setLatitude(lat);

                    // Set the longitude of the current location to lon
                    currLocation.setLongitude(lon);

                    // Create a LatLng object from lat and lon
                    LatLng currLoc = new LatLng(lat, lon);

                    // Create a MarkerOptions object
                    MarkerOptions options = new MarkerOptions();
                    // Set the position to the currLoc
                    options.position(currLoc);
                    // Set the title to "Current Location"
                    options.title("Current Location");
                    // Change the icon of the marker to blue
                    options.icon(BitmapDescriptorFactory.defaultMarker(BitmapDescriptorFactory.HUE_BLUE));

                    // Create a marker for the google map and add it to the markers list
                    markers.add(mMap.addMarker(options));

                    // Animate the camera to the currLoc using zoom level 8.0
                    mMap.animateCamera(CameraUpdateFactory.newLatLngZoom(currLoc, 8.0f), 2000, null);

                    // Create a Client object
                    client = new Client();
                    // Run Client using parameters and the current location
                    client.execute(host, portNum, uid, k, maxD, currLocation);
                }

            }

        }else{
            // If there is no internet connetion
            // Send Toast to user to inform him
            Toast.makeText(this, "Please make sure you have internet connection.", Toast.LENGTH_LONG).show();

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

    /*
    A class for the Client that connects to the Master and gets the K Best POIs for the
    specific user, at the location the user asked and for a specific distance from him.
    Client is an Async task, which is a Thread and it runs asynchronously from the Activity
     */
    public class Client extends AsyncTask<Object,String,Void> {

        // The list of the K best pois for the user
        List<Poi> kBestPois;

        // The host of the master
        String host;

        // The port of the master
        int portNum;

        // The user id
        int u;

        // The max results -- k
        int k;

        // The max distance from the user
        double maxD;

        // The user's location
        Location myLocation;

        // The progress dialog to show while fetching the list of pois
        ProgressDialog progressDialog;

        /*
        Method that executes before the doInBackground()
         */
        @Override
        protected void onPreExecute() {
            // Show the progress dialog
            progressDialog = ProgressDialog.show(MapsActivity.this,
                    "Progress",
                    "Getting list of best " + k + "POIs...");
        }

        /*
        Method that executes the main code for the Async task
         */
        @Override
        protected Void doInBackground(Object... strings) {

            // Get the information needed from the parameters (strings variable)
            // and cast them appropriately
            host = (String)strings[0];
            portNum = (Integer)strings[1];
            u = (Integer)strings[2];
            k = (Integer)strings[3];
            maxD = (Double)strings[4];
            myLocation = (Location)strings[5];

            // Initialize the Socket for the connection to null
            Socket requestSocket = null;
            // Initialize a ObjectInputStream object to null
            ObjectInputStream in = null;
            // Initialize a ObjectOutputStream object to null
            ObjectOutputStream out = null;

            // Try to create connection with master/server
            try {

                // Log message to debug
                Log.d("DEBUG","Connecting to: " + host + "@" + portNum);
                // Publish the progress to the progress dialog
                publishProgress("Connection to " + host + "@" + portNum);
                // Connect to host and port number given
                requestSocket = new Socket();
                requestSocket.connect(new InetSocketAddress(host,portNum),5000);

                // Create an ObjectOutputStream from the connection
                out = new ObjectOutputStream(requestSocket.getOutputStream());
                // Create an ObjectInputStream from the connection
                in = new ObjectInputStream(requestSocket.getInputStream());

                // Log message to debug
                Log.d("DEBUG","Connected to Master.");
                // Publish the progress to the progress dialog
                publishProgress("Connected to Master.");

                // Send user id
                out.writeInt(u);
                out.flush();
                // Publish the progress to the progress dialog
                publishProgress("Sent UID.");

                // Send K
                out.writeInt(k);
                out.flush();
                // Publish the progress to the progress dialog
                publishProgress("Sent K.");

                // Send Latitude
                out.writeDouble(myLocation.getLatitude());
                out.flush();
                // Publish the progress to the progress dialog
                publishProgress("Sent Latitude.");

                // Send Longitude
                out.writeDouble(myLocation.getLongitude());
                out.flush();
                // Publish the progress to the progress dialog
                publishProgress("Sent Longitude.");

                // Send MaxD
                out.writeDouble(maxD);
                out.flush();
                // Publish the progress to the progress dialog
                publishProgress("Sent maxD.");

                // Wait for Master's answer and assign it to the POIs list
                kBestPois = (List<Poi>)in.readObject();

                // Publish the progress to the progress dialog
                publishProgress("Got list of best "+k+" POIs.");

                // Initialize variable debug
                boolean debug = false;
                // If debug is true
                if(debug) {
                    // For each POI in the list
                    for (Poi p : kBestPois) {
                        // Log the POI to DEBUG
                        Log.d("DEBUG",""+p);
                    }
                }


            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } finally{
                // Try to close the input and output object streams
                // as well as the Socket connection
                try {
                    if(in!=null) {
                        in.close();
                    }
                    if(out!=null){
                        out.close();
                    }
                    if(requestSocket!=null) {
                        requestSocket.close();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                // Dismiss the progress dialog
                progressDialog.dismiss();
            }

            return null;
        }

        /*
        Method that executes when publish progress is called
         */
        @Override
        protected void onProgressUpdate(String... values) {
            // Change the progress dialog message to the parameter
            progressDialog.setMessage(values[0]);
        }

        /*
        Method that executes after doInBackground()
         */
        @Override
        protected void onPostExecute(Void s) {
            // If we have a result from the master the kBestPois won't be null

            // If kBestPois isn't null
            if(kBestPois!=null) {
                // Call updateMap() method
                updateMap();
            }else{
                // Else there was a connection problem with the master

                // Log error
                Log.e("ERROR","Could not connect to "+host+":"+portNum);
                // Send a Toast to the user to inform him
                Toast.makeText(MapsActivity.this, "Could not connect to "+host+":"+portNum, Toast.LENGTH_SHORT).show();
            }
        }
    }

}
