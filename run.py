from functions import *
#CHANGE directory where you want to extract the files
#CHANGE pbfname to match the available pbf source name (see available sources by calling sources.(arguments) see below)
directory='../osm'
pbfnames=['chile','argentina','ecuador']
filterr={
    'amenity':[
        'college','driving_school','kindergarten',
        'language_school','library','toy_library',
        'music_school','school','university',
        'atm','bank','baby_hatch',
       'clinic','dentist','doctors',
       'hospital','nursing_home','pharmacy',
       'social_facility','veterinary'],
    'aeroway':[
            'aerodrome','apron','control_tower','control_center','gate',
       'hangar','helipad','heliport','holding_position','navigationaid',
       'beacon','parking_position','runway','taxilane','taxiway',
       'terminal','windsock','highway_strip','User defined'],
    'consulate':'',
    'police':'',
    'company':'',
    'diplomatic':'',
    'education':'',
    'embassy':'',
    'emergency':'',
    'engineer':'',
    'factory':'',
    'history':'',
    'military':'',
    'power':''
        }

kwargs={'engine':pg_connection('marko','rumarec18','34.147.9.88','5432','crowdpulse')[0],
       #'conn':pg_connection(username,password,host,port,dbname)[1],
        'geoserver_url':'http://34.147.9.88:8080/geoserver',
        'geoserver_username':'admin',
        'geoserver_pass':'Rumarec18*',
        'geoserver_wspace':'crowdpulse',
        'geoserver_store':'crowdpulse_db_polygons'
       }


for pbfname in pbfnames:
    print('PBF NAME: ',pbfname,'\n','###################################')

    try:
        os.mkdir(directory+'/embassies')
        os.mkdir(directory+'/'+pbfname)
        os.mkdir(directory+'/'+pbfname+'/nodes')
        os.mkdir(directory+'/'+pbfname+'/ways')
        os.mkdir(directory+'/'+pbfname+'/relations')
        os.mkdir(directory+'/'+pbfname+'/combination')
    except:
        print('some of the directories already exist')


    #UNCOMENT TO SEE AVAILABLE SOURCES
    #sources.available.keys()
    #sources.south_america.available
    #sources.subregions.available.keys()
    #sources.subregions.brazil.available

    #PYROSM GET FILES
    print('getting data from osm')
    fp=get_data(pbfname,directory=directory+'/'+pbfname,update=True)
    print('data downloaded')

    #PARQUETIZE PBF FILES TO NODE, WAY, RELATION
    driver_location='/home/marko/osm-parquetizer/target/osm-parquetizer-1.0.1-SNAPSHOT.jar'
    print('start parquetizing')
    parquetize(driver_location,directory+'/'+pbfname+'/'+pbfname+'-latest.osm.pbf')
    print('parquetize ended')

    #READ PARQUET FILES
    node=spark.read.parquet(directory+'/'+pbfname+'/'+pbfname+'-latest.osm.pbf.node.parquet')
    way=spark.read.parquet(directory+'/'+pbfname+'/'+pbfname+'-latest.osm.pbf.way.parquet')
    relation=spark.read.parquet(directory+'/'+pbfname+'/'+pbfname+'-latest.osm.pbf.relation.parquet')

    #RUN SCRIPTS:
    
    
    #WAYS:
    pdfs=pull_buildings(node,way)
    fname=directory+'/'+pbfname+'/'+'ways/'+pbfname+'-latest-building'
    dump_buildings_to_geojson(fname,pdfs)

    #RELATIONS:
    pdf=pull_buildings_relations(node,way,relation)
    fname_rl=directory+'/'+pbfname+'/'+'relations/'+pbfname+'-latest-building-relations'
    dump_buildings_to_geojson_relation(fname_rl,pdf)


    #READ AND PUSH POLYGONS TO DB AND PUBLISH TO GEOSERVER
    read_gpd(path=directory+'/'+pbfname,table_name=pbfname,scheema='polygons',**kwargs)


    #NODES:
    poi_extractor(node,filterr,pbfname,publish=False,**kwargs)


    #PULL FINAL GEOJSONS - /combination
    combine_polygon(filterr,pbfname,directory,**kwargs)
    
    #Exctact embassies:
    extract_embassies(directory,pbfname,**kwargs)
    
    #Extract selected:
    extract_selected('amenity','restaurant','name','mamut',pbfname,directory,**kwargs)











#
