from functions import *
#UNCOMENT TO SEE AVAILABLE SOURCES

#sources.available.keys()
#sources.south_america.available
#sources.subregions.available.keys()
#sources.subregions.brazil.available

#PYROSM GET FILES
#fp=get_data('south-america',directory='./osm')

#PARQUETIZE PBF FILES TO NODE, WAY, RELATION
#parquetize(r'/home/marko/osm-parquetizer/target/osm-parquetizer-1.0.1-SNAPSHOT.jar','./osm/south-america-latest.osm.pbf')

#LOAD PARQUET FILES:
# provide pbf name: ex: south_america (always use _ instead of -)
pbfname='south_america'

node=spark.read.parquet('./osm/south-america-latest.osm.pbf.node.parquet')
way=spark.read.parquet('./osm/south-america-latest.osm.pbf.way.parquet')
relation=spark.read.parquet('./osm/south-america-latest.osm.pbf.relation.parquet')

#RUN SCRIPTS:
#NODES:

filterr={
#    'amenity':[
#        'college','driving_school','kindergarten',
#        'language_school','library','toy_library',
#        'music_school','school','university',
#        'atm','bank','baby_hatch',
#       'clinic','dentist','doctors',
#       'hospital','nursing_home','pharmacy',
#       'social_facility','veterinary'],
    'aeroway':[
            'aerodrome','apron','control_tower','control_center','gate',
       'hangar','helipad','heliport','holding_position','navigationaid',
       'beacon','parking_position','runway','taxilane','taxiway',
       'terminal','windsock','highway_strip','User defined'],
    'company':'',
    'diplomatic':'',
    'education':'',
    'embassy':'',
    'emergency':'',
    'engineer':'',
    'factory':'',
    'history':'',
    'military':'',
    #'power':''

        }

poi_extractor(file=node,filterr=filterr,save_geojson=True,pbfname='south_america',directory='./osm/geojsons')


#WAYS:

kwargs={'engine':pg_connection('marko','rumarec18','34.91.102.177','5432','crowdpulse')[0],
       #'conn':pg_connection(username,password,host,port,dbname)[1],
        'geoserver_url':'http://34.91.102.177:8080/geoserver',
        'geoserver_username':'admin',
        'geoserver_pass':'Rumarec18*',
        'geoserver_wspace':'crowdpulse',
        'geoserver_store':'crowdpulse_db_polygons'
       }

pdfs=pull_buildings(node,way)
fname='./osm/geojson_polygons/south-america-latest-building'
dump_buildings_to_geojson(fname,pdfs)


#RELATIONS:

pdf=pull_buildings_relations(node,way,relation)
fname_rl='./osm/geojson_polygons/south-america-latest-building-relations'
dump_buildings_to_geojson_relation(fname_rl,pdf)


#READ AND PUSH POLYGONS TO DB AND PUBLISH TO GEOSERVER

read_gpd(path='./osm/geojson_polygons/',table_name=pbfname,scheema='polygons',**kwargs)

























#
