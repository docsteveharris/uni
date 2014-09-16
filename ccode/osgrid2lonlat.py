#!/usr/local/bin/python
# Convert from national grid to lat long
# via http://osedok.wordpress.com/2012/01/17/conversion-of-british-national-grid-wkid27700-to-wgs84wkid4326-and-then-to-webmercator-wkid102100/

import pyproj
# Define two projections, one for the British National Grid and one for WGS84 (Lat/Lon)
# You can use the full PROJ4 definition or the EPSG identifier (PROJ4 uses a file that matches the two)
bng = pyproj.Proj(init='epsg:27700')
wgs84 = pyproj.Proj(init='epsg:4326')

out_xy = []
with open('../data/osgrid.txt', 'r') as infile:
    for line in infile:
        in_xy = line.split('\t')
        id, ea, no = in_xy[0], in_xy[1], in_xy[2].strip('\n')
        if len(ea) == 0 or len(no) == 0:
            continue
        ea, no = float(ea), float(no)
        lon, lat = pyproj.transform(bng,wgs84, ea, no)
        xy = "%s\t%d\t%d\t%f\t%f\n" % (id, ea, no, lon, lat)
        out_xy.append(xy)

outfile = open('../data/lonlat.txt', 'w')
outfile.writelines(out_xy)

