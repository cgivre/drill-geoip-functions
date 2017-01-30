# GeoIP Functions for Apache Drill
This is a collection of GeoIP functions for Apache Drill. These functions are a wrapper for the MaxMind GeoIP Database.

* **`getCountryName( <ip> )`**:  This function returns the country name of the IP address, "Unknown" if the IP is unknown or invalid.
* **`getCountryISOCode( <ip> )`**:  This function returns the country ISO code of the IP address, "Unknown" if the IP is unknown or invalid.
* **`getCityName( <ip> )`**:  This function returns the city name of the IP address, "Unknown" if the IP is unknown or invalid.
