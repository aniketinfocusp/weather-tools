### DWD WN data

This is an open data.

I have downloaded it from [https://opendata.dwd.de/weather/radar/composite/wn/](https://opendata.dwd.de/weather/radar/composite/wn/).

[Here](https://www.dwd.de/DE/leistungen/radarprodukte/radarkomposit_wn.pdf?__blob=publicationFile&v=3) is one
documentation on the data it contains.


### Hints

The data is in RADOLAN format.

First, try to figure out a best library to use to parse the source data.

Figuring out the projection and CRS would be challenging. In fact, it was
the most difficult one so far. Let's see if you can figure it out without
my help. Do come to me when you are worn out.

Notes:
possible crs: wgs84-ellipsoid
projection: Polar Stereographic Projection
