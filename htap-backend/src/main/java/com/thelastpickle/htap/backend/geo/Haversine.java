package com.thelastpickle.htap.backend.geo;

/** Great-circle distance between two positions. */
public final class Haversine {

    private static final double EARTH_RADIUS_M = 6_371_000.0;

    private Haversine() {}

    /** Distance in metres. */
    public static double metres(double lat1, double lon1, double lat2, double lon2) {
        double rlat1 = Math.toRadians(lat1);
        double rlat2 = Math.toRadians(lat2);
        double halfDlat = (rlat2 - rlat1) / 2;
        double halfDlon = Math.toRadians(lon2 - lon1) / 2;
        double a = Math.sin(halfDlat) * Math.sin(halfDlat)
                + Math.cos(rlat1) * Math.cos(rlat2) * Math.sin(halfDlon) * Math.sin(halfDlon);
        // Rounding can put `a` a hair above 1 for antipodal points, where asin is undefined.
        return 2 * EARTH_RADIUS_M * Math.asin(Math.min(1.0, Math.sqrt(a)));
    }
}
