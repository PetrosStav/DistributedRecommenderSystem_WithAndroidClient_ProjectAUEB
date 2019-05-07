package ds.ergasia;
/* Authors:
Petros Stavropoulos - 3150230
Kwstas Savvidis     - 3150229
Erasmia Kornelatou  - 3120076
 */

// Valid longitudes are from -180 to 180 degrees.
// Valid latitudes are from -85.05112878 to 85.05112878 degrees.

import java.io.Serializable;
        import java.util.Random;

// Class that represents a Place Of Interest (POI)
public class Poi implements Serializable {

    // The POI's id
    int id;

    // The POI's name
    String name;

    // The POI's latitude coordinates
    double latitude;

    // The POI's longitude coordinates
    double longitude;

    // The POI's category
    String category;

    // The POI's photo link or "not exists" if there is no photo
    String photo;

    // The POIs hash value
    String hash;

    // Parametrized Constructor
    public Poi(int id, String name, double latitude, double longitude, String category, String photo, String hash) {
        this.id = id;
        this.name = name;
        this.latitude = latitude;
        this.longitude = longitude;
        this.category = category;
        this.photo = photo;
        this.hash = hash;
    }

    // Randomized Constructor for dummy pois
    public Poi(int id){
        this.id = id;
        name = "Poi_"+id;
        Random rand = new Random();
        latitude = (rand.nextDouble() * 2 - 1) * 85.05112878;
        longitude = (rand.nextDouble() * 2 - 1) * 180;
        category = "Dummy";
        photo = "Not exists";
        hash = ""+id;
    }

    @Override
    // Overridden toString method
    public String toString() {
        return "Poi{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", latitude=" + latitude +
                ", longitude=" + longitude +
                ", category='" + category + '\'' +
                '}';
    }
}
