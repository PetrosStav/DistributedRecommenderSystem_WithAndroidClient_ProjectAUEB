package ds.ergasia;

import android.Manifest;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.support.annotation.NonNull;
import android.support.v4.app.ActivityCompat;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

public class MainActivity extends AppCompatActivity {

    // Reference for the Search button
    private Button btn_search;
    // Reference for the Host Edit Text
    private EditText etHost;
    // Reference for the Port Edit Text
    private EditText etPort;
    // Reference for the UID Edit Text
    private EditText etUid;
    // Reference for the Max Results Edit Text
    private EditText etK;
    // Reference for the Max Distance Edit Text
    private EditText etMaxD;

    /*
    Method that executes code when the Android Activity is created
     */
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        // Set the content view to the main activity layout
        setContentView(R.layout.activity_main);

        // Find the Search Button using R
        btn_search = (Button) findViewById(R.id.btn_search);
        // Find the Host Edit Text using R
        etHost = (EditText) findViewById(R.id.etHost);
        // Find the Port Edit Text using R
        etPort = (EditText) findViewById(R.id.etPort);
        // Find the UID Edit Text using R
        etUid = (EditText) findViewById(R.id.etUID);
        // Find the Max Results Edit Text using R
        etK = (EditText) findViewById(R.id.etK);
        // Find the Max Distance Edit Text using R
        etMaxD = (EditText) findViewById(R.id.etD);

        // Set an on click listener to the Search Button that calls the method
        // buttonSearch()
        btn_search.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                buttonSearch();
            }
        });

        // Check for Location Permissions
        if(checkCallingOrSelfPermission("android.permission.ACCESS_FINE_LOCATION") != PackageManager.PERMISSION_GRANTED
                || checkCallingOrSelfPermission("android.permission.ACCESS_COARSE_LOCATION") != PackageManager.PERMISSION_GRANTED){
            // Request the permissions if they are not granted using the requestCode 1 -- for the callback
            ActivityCompat.requestPermissions(this,new String[]{Manifest.permission.ACCESS_FINE_LOCATION,Manifest.permission.ACCESS_COARSE_LOCATION},1);
        }


    }

    /*
    Method that overrides the callback on the Request Permissions Result which we use to set what the app
    should do if the permissions are not given
     */
    @Override
    public void onRequestPermissionsResult(int requestCode, @NonNull String[] permissions, @NonNull int[] grantResults) {
        // We asked for requestCode == 1, so check that
        if(requestCode == 1){
            // Initialize given to false, which checks that all the permissions are given
            boolean given = false;
            // Check that all the permissions asked are granted
            for(int i=0;i<grantResults.length;i++){
                given = grantResults[i] == PackageManager.PERMISSION_GRANTED;
                // If there was a permission not granted then break the loop
                if(!given) break;
            }
            // If any permission is not granted
            if(!given){
                // Disable the Search Button
                btn_search.setEnabled(false);
                // Send a Toast to the user informing him to enable Location Permissions
                Toast.makeText(this, "In order to use this app, you have to enable Location Permissions in the device's Settings.", Toast.LENGTH_LONG).show();
            }
        }

    }

    /*
    Method that is called upon clicking on the Search Button and starts the Map Activity
    using the information from the Edit Texts
     */
    protected void buttonSearch(){

        // Check that all edit texts have information
        if(!etHost.getText().toString().isEmpty() && !etPort.getText().toString().isEmpty()
                && !etUid.getText().toString().isEmpty() && !etK.getText().toString().isEmpty()
                && !etMaxD.getText().toString().isEmpty()) {

            // Create a connectivity manager and get active network information
            ConnectivityManager cm = (ConnectivityManager)getSystemService(Context.CONNECTIVITY_SERVICE);
            NetworkInfo ni = cm.getActiveNetworkInfo();

            // Check connectivity to internet
            if(ni != null && ni.isConnectedOrConnecting()) {
                // If there is internet connection

                // Create an intent to send to the MapsActivity
                Intent intent = new Intent(this, MapsActivity.class);

                // Put all the information needed from the Maps Activity as extras to the intent
                intent.putExtra("host", etHost.getText().toString());
                intent.putExtra("port", Integer.parseInt(etPort.getText().toString()));
                intent.putExtra("uid", Integer.parseInt(etUid.getText().toString()));
                intent.putExtra("k", Integer.parseInt(etK.getText().toString()));
                intent.putExtra("maxD", Double.parseDouble(etMaxD.getText().toString()));

                // Start the Maps Activity using the intent
                startActivity(intent);

            }else{
                // If there is no internet connection

                // Send a Toast to the user informing him that he must have internet connection
                Toast.makeText(this, "Please make sure you have internet connection.", Toast.LENGTH_SHORT).show();
                
            }

        }else{
            // If there is an empty Edit Text send a Toast to the user informing him that
            // all the information must be filled
            Toast.makeText(this, "Please fill all the above information.", Toast.LENGTH_SHORT).show();
            
        }
    }


}
