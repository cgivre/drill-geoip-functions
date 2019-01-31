# GeoIP Functions for Apache Drill
This is a collection of GeoIP functions for Apache Drill. These functions are a wrapper for the MaxMind GeoIP Database.

IP Geo-Location is inherently imprecise and should never be relied on to get anything more than a general sense of where the traffic is coming from. 

* **`getCountryName( <ip> )`**:  This function returns the country name of the IP address, "Unknown" if the IP is unknown or invalid.
* **`getCountryConfidence( <ip> )`**:  This function returns the confidence score of the country ISO code of the IP address.
* **`getCountryISOCode( <ip> )`**:  This function returns the country ISO code of the IP address, "Unknown" if the IP is unknown or invalid.
* **`getCityName( <ip> )`**:  This function returns the city name of the IP address, "Unknown" if the IP is unknown or invalid.
* **`getCityConfidence( <ip> )`**:  This function returns confidence score of the city name of the IP address.
* **`getLatitude( <ip> )`**:  This function returns the latitude associated with the IP address.
* **`getLongitude( <ip> )`**:  This function returns the longitude associated with the IP address.
* **`getTimezone( <ip> )`**:  This function returns the timezone associated with the IP address.
* **`getAccuracyRadius( <ip> )`**:  This function returns the accuracy radius associated with the IP address, 0 if unknown.
* **`getAverageIncome( <ip> )`**:  This function returns the average income of the region associated with the IP address, 0 if unknown.
* **`getMetroCode( <ip> )`**:  This function returns the metro code of the region associated with the IP address, 0 if unknown.
* **`getPostalCode( <ip> )`**:  This function returns the postal code associated with the IP address.
* **`getCoordPoint( <ip> )`**:  This function returns a point for use in GIS functions of the lat/long of associated with the IP address.
* **`getASN( <ip> )`**:  This function returns the autonomous system of the IP address, "Unknown" if the IP is unknown or invalid.
* **`getASNOrganization( <ip> )`**:  This function returns the autonomous system organization of the IP address, "Unknown" if the IP is unknown or invalid.

This product includes GeoLite2 data created by MaxMind, available from <a href="https://www.maxmind.com">https://www.maxmind.com</a>.
