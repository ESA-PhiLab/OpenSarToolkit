from ost.helpers import vector

# test aoi_to_wkt
aoi = vector.aoi_to_wkt("IRL")
assert type(aoi) == str and aoi.startswith("POLYGON")

aoi = vector.aoi_to_wkt("POLYGON((1 2,1 4,3 4,3 2,1 2))")
assert type(aoi) == str and aoi.startswith("POLYGON")

aoi = vector.aoi_to_wkt("tests/testdata/test_polygon.geojson")
assert type(aoi) == str and aoi.startswith("POLYGON")

# test latlon_to_wkt
lat, lon = "-67", "-61"
aoi = vector.latlon_to_wkt(lat, lon)
assert type(aoi) == str and aoi.startswith("POINT")

aoi = vector.latlon_to_wkt(lat, lon, buffer_degree=1)
assert type(aoi) == str and aoi.startswith("POLYGON")

aoi = vector.latlon_to_wkt(lat, lon, buffer_degree=1, envelope=False)
assert type(aoi) == str and aoi.startswith("POLYGON")

aoi = vector.latlon_to_wkt(lat, lon, buffer_degree=1, envelope=True)
assert type(aoi) == str and aoi.startswith("POLYGON")

aoi = vector.latlon_to_wkt(lat, lon, buffer_meter=1000, envelope=True)
assert type(aoi) == str and aoi.startswith("POLYGON")
