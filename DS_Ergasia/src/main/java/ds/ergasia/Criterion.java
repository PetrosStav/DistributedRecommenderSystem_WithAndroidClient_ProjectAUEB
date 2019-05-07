package ds.ergasia;

// Functional Interface Criterion
// is used in calculateBestKPoisForUser as a criterion for the POIs
// in order to exclude some POIs from the answer
public interface Criterion{
    // Boolean method that is true if POI p meets the criterion
    // or false otherwise
    boolean meetsCriterion(Poi p);
}