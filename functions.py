import findspark
findspark.init()
import pyarrow
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
import pandas as pd
import numpy as np
import pyspark.pandas as ps
from pyspark.sql.functions import col,struct,when
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import array_contains
from pyspark.sql.functions import lit,row_number,col
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from pyspark.sql.functions import expr
import glob

#import geopandas as gpd
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import LongType
from shapely.geometry import Point
from shapely.geometry import Polygon

import traceback
import sys
from pyspark import StorageLevel
import geopandas as gpd
from geopandas import GeoDataFrame as gdf
import pandas as pd
import os
from geo.Geoserver import Geoserver
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine
import geoalchemy2
import webbrowser
import time
import json
import pyrosm
from pyrosm import get_data
import subprocess
from pyrosm.data import sources
import threading as mt
import concurrent.futures

#level = StorageLevel.MEMORY_ONLY

spark = SparkSession.\
    builder.\
    master("local[*]").\
    appName("Pyspark App").\
    getOrCreate()


#SedonaRegistrator.registerAll(spark)
sc = spark.sparkContext
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)

#CONNECT TO THE POSTGIS DATABASE
def pg_connection(username,password,host,port,dbname):
    sconpar='postgresql+psycopg2://'+username+':'+password+'@'+host+':'+port+'/'+dbname+''
    engine = create_engine(sconpar,echo=False)

    param_dic={'user':username,'password':password,'port':port,'host':host,'database':dbname}
    conn = psycopg2.connect(**param_dic)
    return [engine, conn]

# PUSH GEOPANDAS DATAFRAME TO THE DATABASE.
# gdf - geodataframe variable
# engine - sqlalchemy engine (ex: engine=pg_connection(args)[0])
# schemaname - name of the schema in postgis
# tablename - table name
# ifexissts - what to do if the table already exists in the schema options: fail, replace, append
def push_to_postgis(gdf,engine,schemaname,tablename,ifexists):
    gdf.to_postgis(
        con=engine,
        name=tablename,
        if_exists=ifexists,
        schema=schemaname
    )

# PUBLISH GEODATAFRAME ON GEOSERVER.
# path - link to the geoserver instance ex: http://0.0.0.0:8080/geoserver
# username - user name
# password ..
# layername - assign layer name on geoserver
# workspace - geoserver's workspace
# storename - postgis store name
# pgtable - name of the table from the database
def publish_on_geoserver(path,username,password,layername,workspace,storename,pgtable):
    #'http://34.91.102.177:8080/geoserver'
    geo = Geoserver(path, username=username, password=password)
    try:
        geo.delete_layer(layer_name=layername, workspace=workspace)
    except:
        pass
    try:
        geo.publish_featurestore(workspace=workspace, store_name=storename, pg_table=pgtable)
    except:
        print('Layer ',layername,' not published!')

# PARQUETIZE PBF FILES (convert osm.pbf file to node.parquet, way.parquet, relation.parquet - big data friendly format)
# driver - java driver location ex: /home/remote/osm-parquetizer/target/osm-parquetizer-1.0.1-SNAPSHOT.jar
# file - osm.pbf file path
def parquetize(driver,file):
    #bashCommand = "java -jar /home/marko/osm-parquetizer/target/osm-parquetizer-1.0.1-SNAPSHOT.jar /home/marko/osm_pyrosm/chile-latest.osm.pbf"
    #driver path: /home/marko/osm-parquetizer/target/osm-parquetizer-1.0.1-SNAPSHOT.jar
    bashCommand = "java -jar "+driver+" "+file
    process = subprocess.run(bashCommand.split(), stdout=subprocess.PIPE)
    
# PULL ALL BUILDINGS FROM way.parquet FILE
# node - path to node.parquet file
# way - path to way.parquet file
def pull_buildings(node,way):
    st=node.withColumn('tags',node.tags.cast(StringType()))
    st=st.withColumn('nodeId',st.id)

    #FILTER BY BOUNDING BOX
    #n=st.filter((st.latitude > -25) & (st.latitude < -24) &(st.longitude > -47) &(st.longitude < -46) )
    # No filter
    n=st
    
    #
    res=way.select('id','tags','nodes')
    rest=n.select('nodeId','tags','latitude','longitude')
    res=res.withColumn("indexNode",expr(" explode(nodes)" ))
    rest=rest.withColumn('point', struct(['latitude','longitude']))
    rest=rest.select('nodeId','point')
    #select building
    res=res.filter(array_contains(res.tags['key'],bytearray(b'building')))
    way=way.withColumn('tags',res.tags.cast(StringType()))
    
    #prepare file for polygon creation
    wayGeometryDF = res.join(rest, res.indexNode.nodeId==rest.nodeId, how="inner").groupBy('id').agg(f.collect_list(struct('indexNode.index','point',)).alias('colection'))
    #to catch tags
    wgdf=wayGeometryDF.join(way.select('id','tags'),on='id')
    #transfer to pandas on spark to create index column and be able to slice dataframe
    pdfs=wgdf.to_pandas_on_spark()
    pdfs['lindex']=pdfs.index
    return pdfs
    
#DUMP ALL BUILDINGS TO GEOJSON FILES ON 
# fname=directory+'/'+pbfname+'/'+'ways/'+pbfname+'-latest-building' 
# pdfs=pull_buildings(node,way)
def dump_buildings_to_geojson(fname,pdfs):
    c=len(pdfs.index)
    print('count of features: ',c)
    cc=c/500000
    cc=int(cc)+1
    before=0
    after=500000
    br=0
    for i in range(cc):
        print('from:',before,'\nto: ',after,'\nof: ',c)
        pdf=pdfs[pdfs.lindex.between(before,after)].to_pandas()
        before+=500000
        after+=500000
        sorter=lambda df:[i.sort() for i in df.colection]
        #pdf = df.to_pandas()
        print('sorting')
        sorter(pdf)
        
        print('creating geojson')
        d={}
        d['type']="FeatureCollection"
        d['features']=[]
        #features={'type':'Feature','properties':{},'geometry':{'type':'Polygon','coordinates':[]}}
        #def packer(pdf):
        for i in pdf.index:
            lov=[]
            key=pdf.loc[i,'id']
            val=pdf.loc[i,'colection']
            if len(val)<3:
                print('key:',key,'bad_geometry')
            else:
                features={"type":"Feature","properties":{'id':str(key),'tags':str(pdf.loc[i,'tags'])},"geometry":{"type":"Polygon","coordinates":[]}}
                for j in val:
                    lov.append([j.point.longitude,j.point.latitude])
                    #print(lov[0])
                    #break
                features['geometry']['coordinates'].append(lov)
                d['features'].append(features)
                #d[key]=lov
                #print(key,val)
                #if br>2:
                #break
        
        with open(fname+'_'+str(br)+'.geojson','w') as f:
            json.dump(d, f)
        print('iter done'+fname+'_'+str(br)+'.geojson')
        br+=1
        
# READ ALL GEOJSON POLYGONS (WAYS AND RELATIONS) AND PUSH THEM TO THE DATABASE IN TABLES: pbfname_ways, pbfname_relations
# read_gpd(path=directory+'/'+pbfname,table_name=pbfname,scheema='polygons',**kwargs)
def read_gpd(path,table_name,scheema,publish=False,**kwargs):
    try:
        filenames = glob.glob(path + '/ways/' + "/*.geojson")

        dfs = []
        br=0
        for filename in filenames:
            print('reading: ',filename)
            try:
                temp_file=gpd.read_file(filename)
                temp_file.crs='EPSG:4326'
                try:
                    if br==0:
                        print('push file: ',filename+'_ways ')
                        push_to_postgis(temp_file,
                                    kwargs['engine'],
                                    scheema,table_name+'_ways',
                                    'replace')
                    else:
                        
                        push_to_postgis(temp_file,
                                        kwargs['engine'],
                                        scheema,table_name+'_ways',
                                        'append')
                    br+=1
                    print('push file: ',filename+'_ways ', 'successful')
                    if publish==True:
                        try:
                            publish_on_geoserver(kwargs['geoserver_url'],
                                                 kwargs['geoserver_username'],
                                                 kwargs['geoserver_pass'],
                                                 table_name+'_ways',
                                                 kwargs['geoserver_wspace'],
                                                 kwargs['geoserver_store'],
                                                 table_name+'_ways')
                            print('publish file: ',filename+'_ways ', 'successful')
                        except:
                            print('failed publish for: ',filename+'_ways')
                except:
                    print('falied push to db for: ',filename+'_ways')
            except:
                print('failed reading of file: ',filename)
    except:
        print('failed while reading ways')
    #######################################################################    
    try:
        filenames = glob.glob(path + '/relations/' + "/*.geojson")

        dfs = []
        br=0
        for filename in filenames:
            print('reading: ',filename)
            try:
                temp_file=gpd.read_file(filename)
                temp_file.crs='EPSG:4326'
                try:
                    if br==0:
                        print('push file: ',filename+'_relations ')
                        push_to_postgis(temp_file,
                                    kwargs['engine'],
                                    scheema,table_name+'_relations',
                                    'replace')
                    else:
                        print('push file: ',filename+'_relations ')
                        push_to_postgis(temp_file,
                                        kwargs['engine'],
                                        scheema,table_name+'_relations',
                                        'append')
                    br+=1
                    print('push file: ',filename+'_relations ', 'successful')
                    if publish == True:
                        try:
                            publish_on_geoserver(kwargs['geoserver_url'],
                                                 kwargs['geoserver_username'],
                                                 kwargs['geoserver_pass'],
                                                 table_name+'_relations',
                                                 kwargs['geoserver_wspace'],
                                                 kwargs['geoserver_store'],
                                                 table_name+'_relations')
                            print('publish file: ',filename+'_relations ', 'successful')
                        except:
                            print('failed publish for: ',filename+'_relations')
                except:
                    print('falied push to db for: ',filename+'_relations')
            except:
                print('failed reading of file: ',filename)
    except:
        print('failed while reading relations')
        
# PULL ALL BUILDINGS FROM relation.parquet FILE
# node - path to node.parquet file
# way - path to way.parquet file 
# relation - path to relation.parquet file
def pull_buildings_relations(node,way,relation):
    rel=relation.select('id','tags','members')
    #st=node.withColumn('tags',node.tags.cast(StringType()))
    rel=rel.filter(array_contains(rel.tags['key'],bytearray(b'building')))
    rel=rel.withColumn('tags',rel.tags.cast(StringType()))
    #rel=rel.withColumn('member',rel.members.cast(StringType()))
    rel=rel.withColumn("member",expr(" explode(members)" ))
    #rel=rel.withColumn('member',rel.member.cast(StringType()))
    grel = rel.join(way, rel.member.id==way.id, how="inner").groupBy(rel.id).agg(f.collect_list(struct('member.id','member.role','member.type')).alias('colection'))
    grele=grel.withColumn("exp",expr(" explode(colection)" ))

    st=node.withColumn('nodeId',node.id)

    #FILTER BY BOUNDING BOX
    #n=st.filter((st.latitude > -25) & (st.latitude < -24) &(st.longitude > -47) &(st.longitude < -46) )
    # No filter
    n=st

    #
    res=way.select('id','tags','nodes')
    rest=n.select('nodeId','tags','latitude','longitude')
    res=res.withColumn("indexNode",expr(" explode(nodes)" ))
    rest=rest.withColumn('point', struct(['latitude','longitude']))
    rest=rest.select('nodeId','point')
    #select building
    #res=res.filter(array_contains(res.tags['key'],bytearray(b'building')))
    rel=rel.withColumn('tags',rel.tags.cast(StringType()))

    #prepare file for polygon creation
    wayGeometryDF = res.join(rest, res.indexNode.nodeId==rest.nodeId,
                             how="inner").groupBy('id').agg(f.collect_list(struct('indexNode.index',
                                                                                  'point',)).alias('colection'))
    #to catch tags
    #wgdf=wayGeometryDF.join(way.select('id','tags'),on='id')
    #transfer to pandas on spark to create index column and be able to slice dataframe
    #pdfs=wayGeometryDF.to_pandas_on_spark()
    #pdfs['lindex']=pdfs.index

    grelt = grele.join(wayGeometryDF, grele.exp.id==wayGeometryDF.id, how="inner").groupBy(grele.id).agg(f.collect_list(struct(grele.exp,wayGeometryDF.colection)).alias('geometry'))
    grelt=grelt.join(rel.select('id','tags'),on='id',how='inner')

    grelt=grelt.dropDuplicates(['id'])
    pdf=grelt.toPandas()
    def sorter_rel(pdf):
        for i in pdf.geometry:
            for j in i:
                #print(j[1])
                j[1].sort()
            #break
    sorter_rel(pdf)
    return pdf

# DUMP ALL BUILDINGS TO GEOJSON FILES ON 
# fname=directory+'/'+pbfname+'/'+'relations/'+pbfname+'-latest-building' 
# pdf=pull_buildings_relations(node,way,relation)
def dump_buildings_to_geojson_relation(fname,pdf):
    print('creating geojson')
    d={}
    d['type']="FeatureCollection"
    d['features']=[]
    #features={'type':'Feature','properties':{},'geometry':{'type':'Polygon','coordinates':[]}}
    #def packer(pdf):
    for i in pdf.index:
        lov=[]
        key=pdf.loc[i,'id']
        val=pdf.loc[i,'geometry']
        #print(type(val))
        for jj in val:
            #print('val:',jj)
            lo=[]
            #print(jj[0].role.decode())
            #print(jj[1][0])
            if jj[0].role.decode() == 'outer':
                #print(jj[0].role.decode())
                #lo.insert(0,jj[0].role.decode())
                for ii in jj[1]:
                    #print(ii.point.latitude)
                    #lo.insert(0,[ii.point.longitude,ii.point.latitude])
                    lo.append([ii.point.longitude,ii.point.latitude])
                if len(lo)>3:   
                    lov.insert(0,lo)
                    #print(lo)
                elif (len(lo) > 0) and (len(lo)<= 3):
                    for k in lo:
                        lov.append(k)
            else:
                #lo.append(jj[0].role.decode())
                #print(jj[0].role.decode())
                for ii in jj[1]:
                    #print(ii.point.latitude)
                    lo.append([ii.point.longitude,ii.point.latitude])

                if len(lo)>3:   
                    lov.append(lo)
                    #print(lo)
                elif (len(lo) > 0) and (len(lo)<= 3):
                    for k in lo:
                        lov.append(k)

                #print('les than 3 points',key)
            #print(lo)
        #print(lov)
        features={"type":"Feature","properties":{'id':str(key),'tags':str(pdf.loc[i,'tags'])},"geometry":{"type":"MultiPolygon","coordinates":[]}}
        features['geometry']['coordinates'].append(lov)
        #if features not in d['features']:
        d['features'].append(features)
        #break
    with open(fname+'.geojson','w') as f:
        json.dump(d, f)
    print('dumping finished, file name: ',fname+'.geojson')
    
    
# GET SPECIFIC RELATIONS FROM SELECTED TAGS
# node, way, relation - paths to the files
# key, value - string input ex: 'aeroway','aerodrome'
def get_specific_relations(node,way,relation,key,value):
    rel=relation.select('id','tags','members')
    #st=node.withColumn('tags',node.tags.cast(StringType()))
    rel=rel.filter(array_contains(rel.tags['key'],bytearray(bytes(key,'UTF-8'))) & array_contains(rel.tags['value'],bytearray(bytes(value,'UTF-8'))))
    rel=rel.withColumn('tags',rel.tags.cast(StringType()))
    #rel=rel.withColumn('member',rel.members.cast(StringType()))
    rel=rel.withColumn("member",expr(" explode(members)" ))
    #rel=rel.withColumn('member',rel.member.cast(StringType()))
    grel = rel.join(way, rel.member.id==way.id, how="inner").groupBy(rel.id).agg(f.collect_list(struct('member.id','member.role','member.type')).alias('colection'))
    grele=grel.withColumn("exp",expr(" explode(colection)" ))

    st=node.withColumn('nodeId',node.id)

    #FILTER BY BOUNDING BOX
    #n=st.filter((st.latitude > -25) & (st.latitude < -24) &(st.longitude > -47) &(st.longitude < -46) )
    # No filter
    n=st

    #
    res=way.select('id','tags','nodes')
    rest=n.select('nodeId','tags','latitude','longitude')
    res=res.withColumn("indexNode",expr(" explode(nodes)" ))
    rest=rest.withColumn('point', struct(['latitude','longitude']))
    rest=rest.select('nodeId','point')
    #select building
    #res=res.filter(array_contains(res.tags['key'],bytearray(b'building')))
    rel=rel.withColumn('tags',rel.tags.cast(StringType()))

    #prepare file for polygon creation
    wayGeometryDF = res.join(rest, res.indexNode.nodeId==rest.nodeId,
                             how="inner").groupBy('id').agg(f.collect_list(struct('indexNode.index',
                                                                                  'point',)).alias('colection'))
    #to catch tags
    #wgdf=wayGeometryDF.join(way.select('id','tags'),on='id')
    #transfer to pandas on spark to create index column and be able to slice dataframe
    #pdfs=wayGeometryDF.to_pandas_on_spark()
    #pdfs['lindex']=pdfs.index

    grelt = grele.join(wayGeometryDF, grele.exp.id==wayGeometryDF.id, how="inner").groupBy(grele.id).agg(f.collect_list(struct(grele.exp,wayGeometryDF.colection)).alias('geometry'))
    grelt=grelt.join(rel.select('id','tags'),on='id',how='inner')

    grelt=grelt.dropDuplicates(['id'])
    pdf=grelt.toPandas()
    def sorter_rel(pdf):
        for i in pdf.geometry:
            for j in i:
                #print(j[1])
                j[1].sort()
            #break
    sorter_rel(pdf)
    return pdf

# DUMP TO GEOJSON POLYGONS FROM pdf= get_specific_relations(node,way,relation,key,value)
# fname=directory+'/'+pbfname+'/'+'relations/'+pbfname+'-latest-building' 
# pdf=get_specific_relations(node,way,relation,key,value)
def dump_spec_rel(fname,pdf):
    print('creating geojson')
    d={}
    d['type']="FeatureCollection"
    d['features']=[]
    #features={'type':'Feature','properties':{},'geometry':{'type':'Polygon','coordinates':[]}}
    #def packer(pdf):
    br=0
    for i in pdf.index:
        br+=1
        lov=[]
        lot=[]
        key=pdf.loc[i,'id']
        val=pdf.loc[i,'geometry']
        #print(type(val))
        for jj in val:
            #print('val:',jj)
            lo=[]
            #print(jj[0].role.decode())
            if jj[0].role.decode() == 'outer':
                for ii in jj[1]:
                    #print(ii.point.latitude)
                    lo.append([ii.point.longitude,ii.point.latitude])
            else:
                for ii in jj[1]:
                    #print(ii.point.latitude)
                    lo.append([ii.point.longitude,ii.point.latitude])
            if len(lo)>3:   
                #print(len(lo))
                for t in lo:
                    lot.append(t)
                #lov.append(lo)
            #else:
            #    for t in lo:
              #      lot.append(t)

        #break

        #print(lov)
        lov.append(lot)
        features={"type":"Feature","properties":{'id':str(key),'tags':str(pdf.loc[i,'tags'])},"geometry":{"type":"MultiPolygon","coordinates":[]}}
        features['geometry']['coordinates'].append(lov)
        #if features not in d['features']:
        d['features'].append(features)
        #if br == 3:
           # break
    with open(fname+'.geojson','w') as f:
        json.dump(d, f)
    print('dumping finished, file name: ',fname+'.geojson')
    #gpdf=gpd.read_file('test.geojson')
    #return gpdf

# EXTRACT POIs FROM PBF FILE BASED ON TAGS IN filterr dictionary
# file - node.parquet file path
# filterr - python dictionary with osm tags ex: {'amenity':'','power':''}
# pbfname - pbf file name ex: 'chile'
# publish - bool - to publish the table to the geoserver or not
# **kwargs - database and geoserver parameters ex: kwargs={'engine':pg_connection('marko','rumarec18','34.147.9.88','5432','crowdpulse')[0],
       #'conn':pg_connection(username,password,host,port,dbname)[1],
        #'geoserver_url':'http://34.147.9.88:8080/geoserver',
        #'geoserver_username':'admin',
        #'geoserver_pass':'Rumarec18*',
        #'geoserver_wspace':'crowdpulse',
        #'geoserver_store':'crowdpulse_db_polygons'
       #}
def poi_extractor(file,filterr,pbfname,publish=False,**kwargs):
    s=file
    #engine=pg_connection('marko','rumarec18','34.91.102.177','5432','crowdpulse')[0]
    #conn=pg_connection()[1]
    #cur=conn.cursor()
    sf=s.withColumn("key_value",expr(" explode(tags)" ))
    sf=sf.withColumn('key',expr(" key_value.key" ))
    sf=sf.withColumn('value',expr(" key_value.value" ))
    sf=sf.withColumn('key',sf.key.cast(StringType()))
    sf=sf.withColumn('value',sf.value.cast(StringType()))
    sf=sf.withColumn('tags',sf.tags.cast(StringType()))
    sf.createOrReplaceTempView("df")
    for key in filterr:
        print(key,'  ##################################')

        n=sf.filter(sf.key==key)

        res=n.select('id','tags','latitude','longitude','key','value')
        res=res.withColumn('point',f.concat_ws(',',res.longitude,res.latitude))

        df=res.createOrReplaceTempView("df")
        df=spark.sql("select * from df")
        print('Converting to pandas')
        pdf = df.toPandas()
        print("COUNT: ",len(pdf.index))
        gpdf = gpd.GeoDataFrame(pdf, geometry=gpd.points_from_xy(pdf.longitude, pdf.latitude),crs='EPSG:4326')
        try:
            print('Push to database: ',pbfname+'_'+key)
            push_to_postgis(gpdf,kwargs['engine'],'pois',pbfname+'_'+key,'replace')
            
            #publish_on_geoserver('http://34.91.102.177:8080/geoserver','admin','Rumarec18*',pbfname+'_'+key,'crowdpulse','crowdpulse_db',pbfname+'_'+key)
            if publish == True:
                print('Publish on geoserver: ',pbfname+'_'+key)
                publish_on_geoserver(kwargs['geoserver_url'],
                                                 kwargs['geoserver_username'],
                                                 kwargs['geoserver_pass'],
                                                 pbfname+'_'+key,
                                                 kwargs['geoserver_wspace'],
                                                 kwargs['geoserver_store'],
                                                 pbfname+'_'+key)
        except:
            print(traceback.format_exc())
            print('Push to db failed for:',pbfname+'_'+key)

# SPATIAL JOIN POI AND POLYGON DATA FROM THE DATABASE WITH THE SAME KEY, VALUE - result - pbfname_key_value.geojson 
# filterr - python dict key value pairs
# pbfname - region name - ex: 'chile'
# directory - directory of the main pbf file
# **kwargs - same as in poi_extractor
def combine_polygon(filterr,pbfname,directory,**kwargs):
    for key in filterr:
        statement="select distinct on (value) value from pois."+pbfname+"_"+str(key)
        gd_data=pd.read_sql(statement, con=kwargs['engine'])
        gd_data.value=gd_data.value.str.lower()
        gd_data.value=gd_data.value.str.replace(' ','_')
        gd_data.value=gd_data.value.str.replace('-','_')
        values=gd_data.value.values.tolist()
        for val in values:
            print('key:',key,'val:',val)
            statement="""
                select id pol_id, tags pol_tags, geometry geom from 
                polygons."""+pbfname+"""_ways pol where tags ~ '"""+key+', '+val+"""'
                """
            gd_data=gdf.from_postgis(statement, con=kwargs['engine'],geom_col='geom')
            statement="""
                select poi.id poi_id, poi.tags poi_tags, poi.key poi_key, poi.value poi_value, 
                pol.id pol_id, pol.tags pol_tags, pol.geometry geom from 
                pois."""+pbfname+'_'+key+""" as poi, polygons."""+pbfname+"""_ways as pol 
                where
                st_within(poi.geometry, pol.geometry) and poi.value = '"""+val+"""'
                """
            gd_data2=gdf.from_postgis(statement, con=kwargs['engine'],geom_col='geom')
            statement="""
                select id pol_id, tags pol_tags, geometry geom from 
                polygons."""+pbfname+"""_relations pol where tags ~ '"""+key+', '+val+"""'
                """
            gd_data3=gdf.from_postgis(statement, con=kwargs['engine'],geom_col='geom')
            statement="""
                select poi.id poi_id, poi.tags poi_tags, poi.key poi_key, poi.value poi_value, 
                pol.id pol_id, pol.tags pol_tags, pol.geometry geom from 
                pois."""+pbfname+'_'+key+""" as poi, polygons."""+pbfname+"""_relations as pol 
                where
                st_within(poi.geometry, pol.geometry) and poi.value = '"""+val+"""'
                """
            gd_data4=gdf.from_postgis(statement, con=kwargs['engine'],geom_col='geom')
            frames=[gd_data,gd_data2,gd_data3,gd_data4]
            rdf = gdf( pd.concat( frames, ignore_index=True) )
            rdf.drop_duplicates(subset=['pol_id'],keep='first',inplace=True)
            rdf['name:en_pol']=rdf['pol_tags'].str.extract("\{name:en(.*?)\}",expand=True)
            rdf['name_pol']=rdf['pol_tags'].str.extract("\{name(.*?)\}",expand=True)
            rdf['name_pol']=rdf['name_pol'].str.lstrip(', ')
            rdf['name:en_pol']=rdf['name:en_pol'].str.lstrip(', ')
            
            rdf['name:en_poi']=rdf['poi_tags'].str.extract("\{name:en(.*?)\}",expand=True)
            rdf['name_poi']=rdf['poi_tags'].str.extract("\{name(.*?)\}",expand=True)
            rdf['name_poi']=rdf['name_poi'].str.lstrip(', ')
            rdf['name:en_poi']=rdf['name:en_poi'].str.lstrip(', ')
            #break
            try:
                rdf.to_file(directory+'/'+pbfname+'/combination/'+pbfname+'_'+key+'_'+val+'.geojson')
            except:
                print('failed for:',pbfname+'_'+key+'_'+val+'.geojson')
    
# EXTRACT EMBASSIES FRMO THE SOURCE PBF FILE TO THE DESTINATION COUNTRY .geojson file - result - embassy_iso2countrycode.geojson 
# FUNCTION WILL APPEND ENTRIES IN THE .geojson files and dedupe them based on osmid
# directory - directory of the main pbf file
# pbfname - pbf name - ex: 'chile'
# **kwargs - same as above
def extract_embassies(directory,pbfname,**kwargs):
    #key='embassy'
    try:
        os.mkdir(directory+'/embassies/')
    except:
        pass
    statement="""
        select id pol_id, tags pol_tags, geometry geom from 
        polygons."""+pbfname+"""_ways pol where tags ~ 'embassy' or tags ~ 'diplomatic' or tags ~ 'consulate'
    """
    gd_data=gdf.from_postgis(statement, con=kwargs['engine'],geom_col='geom')

    statement="""
        select poi.id poi_id, poi.tags poi_tags, poi.key poi_key, poi.value poi_value, 
        pol.id pol_id, pol.tags pol_tags, pol.geometry geom from 
        (select * from pois."""+pbfname+"""_embassy emb
        union
        select * from pois."""+pbfname+"""_diplomatic dip
        union
        select * from pois."""+pbfname+"""_consulate cons) poi, polygons."""+pbfname+"""_ways as pol 
        where
        st_within(poi.geometry, pol.geometry)
        """
    gd_data2=gdf.from_postgis(statement, con=kwargs['engine'],geom_col='geom')
    statement="""
        select id pol_id, tags pol_tags, geometry geom from 
        polygons."""+pbfname+"""_relations pol where tags ~ 'embassy' or tags ~ 'diplomatic' or tags ~ 'consulate'
        """
    gd_data3=gdf.from_postgis(statement, con=kwargs['engine'],geom_col='geom')
    statement="""
        select poi.id poi_id, poi.tags poi_tags, poi.key poi_key, poi.value poi_value, 
        pol.id pol_id, pol.tags pol_tags, pol.geometry geom from 
        (select * from pois."""+pbfname+"""_embassy emb
        union
        select * from pois."""+pbfname+"""_diplomatic dip
        union
        select * from pois."""+pbfname+"""_consulate cons) poi, polygons."""+pbfname+"""_relations as pol 
        where
        st_within(poi.geometry, pol.geometry)
        """
    gd_data4=gdf.from_postgis(statement, con=kwargs['engine'],geom_col='geom')
    frames=[gd_data,gd_data2,gd_data3,gd_data4]
    rdf = gdf( pd.concat( frames, ignore_index=True) )
    rdf.drop_duplicates(subset=['pol_id'],keep='first',inplace=True)
    rdf['name:en_pol']=rdf['pol_tags'].str.extract("\{name:en(.*?)\}",expand=True)
    rdf['name_pol']=rdf['pol_tags'].str.extract("\{name(.*?)\}",expand=True)
    rdf['name_pol']=rdf['name_pol'].str.lstrip(', ')
    rdf['name:en_pol']=rdf['name:en_pol'].str.lstrip(', ')

    rdf['name:en_poi']=rdf['poi_tags'].str.extract("\{name:en(.*?)\}",expand=True)
    rdf['name_poi']=rdf['poi_tags'].str.extract("\{name(.*?)\}",expand=True)
    rdf['name_poi']=rdf['name_poi'].str.lstrip(', ')
    rdf['name:en_poi']=rdf['name:en_poi'].str.lstrip(', ')
    #break
    #try:
    #    rdf.to_file(directory+'/'+pbfname+'/combination/'+pbfname+'_'+key+'_'+val+'.geojson')
    #except:
    #    print('failed for:',pbfname+'_'+key+'_'+val+'.geojson')
    rdf['country_pol']=rdf['pol_tags'].str.extract("\{country(.*?)\}",expand=True)
    rdf['country_poi']=rdf['poi_tags'].str.extract("\{country(.*?)\}",expand=True)
    rdf['country_pol']=rdf['country_pol'].str.lstrip(', ')
    rdf['country_poi']=rdf['country_poi'].str.lstrip(', ')
    rdf['country']=''
    rdf['country'][rdf['country_pol'].notnull()]=rdf['country_pol'][rdf['country_pol'].notnull()]
    rdf['country'][rdf['country_poi'].notnull()]=rdf['country_poi'][rdf['country_poi'].notnull()]
    rdf['country']=rdf['country'].str.lower()
    rdf.sort_values(['country'], axis=0, ascending=True, inplace=True)
    unique_countries=rdf['country'].unique()
    list_of_countries=os.listdir(directory+'/embassies')
    for i in unique_countries:
        prdf=rdf[rdf['country']==i]
        if str('embassy_'+str(i)+'.geojson') in list_of_countries:
            try:
                #print(i)
                prdf.to_file(directory+'/embassies/embassy_'+str(i)+'.geojson',mode='a')
                print('appending to: ','/embassies/embassy_'+str(i)+'.geojson')
            except:
                print('failed for: ','/embassies/embassy_'+str(i)+'.geojson')
        else:
            try:
            #print(i)
                prdf.to_file(directory+'/embassies/embassy_'+str(i)+'.geojson',mode='w')
                print('writing: ','/embassies/embassy_'+str(i)+'.geojson')
            except:
                print('failed for: ','/embassies/embassy_'+str(i)+'.geojson')
    for i in list_of_countries:
        print('deduping: ',directory+'/embassies/'+i)
        gpdf=gpd.read_file(directory+'/embassies/'+i)
        gpdf.drop_duplicates(subset=['pol_id','poi_id'],inplace=True)
        gpdf.to_file(directory+'/embassies/'+i)
      
    
# EXTRACT SELECTED TAGS BASED ON key, value, sel_key, sel_val ex: extract_selected('amenity','restaurant','name','mamut',pbfname,directory,**kwargs)  
# sel_key and sel_val are not case sensitive
def extract_selected(key,value,sel_key,sel_val,pbfname,directory,**kwargs):
        
    print('key:',key,'value:',value,'sel_key:',sel_key,'sel_val:',sel_val)
    statement="""
        select id pol_id, tags pol_tags, geometry geom from 
        polygons."""+pbfname+"""_ways pol where tags ~ '"""+key+', '+value+"""'
        """
    gd_data=gdf.from_postgis(statement, con=kwargs['engine'],geom_col='geom')
    statement="""
        select poi.id poi_id, poi.tags poi_tags, poi.key poi_key, poi.value poi_value, 
        pol.id pol_id, pol.tags pol_tags, pol.geometry geom from 
        pois."""+pbfname+'_'+key+""" as poi, polygons."""+pbfname+"""_ways as pol 
        where
        st_within(poi.geometry, pol.geometry) and poi.value = '"""+value+"""'
        """
    gd_data2=gdf.from_postgis(statement, con=kwargs['engine'],geom_col='geom')
    statement="""
        select id pol_id, tags pol_tags, geometry geom from 
        polygons."""+pbfname+"""_relations pol where tags ~ '"""+key+', '+value+"""'
        """
    gd_data3=gdf.from_postgis(statement, con=kwargs['engine'],geom_col='geom')
    statement="""
        select poi.id poi_id, poi.tags poi_tags, poi.key poi_key, poi.value poi_value, 
        pol.id pol_id, pol.tags pol_tags, pol.geometry geom from 
        pois."""+pbfname+'_'+key+""" as poi, polygons."""+pbfname+"""_relations as pol 
        where
        st_within(poi.geometry, pol.geometry) and poi.value = '"""+value+"""'
        """
    gd_data4=gdf.from_postgis(statement, con=kwargs['engine'],geom_col='geom')
    frames=[gd_data,gd_data2,gd_data3,gd_data4]
    rdf = gdf( pd.concat( frames, ignore_index=True) )
    rdf.drop_duplicates(subset=['pol_id'],keep='first',inplace=True)
    #rdf['name:en_pol']=rdf['pol_tags'].str.extract("\{name:en(.*?)\}",expand=True)
    #rdf['name_pol']=rdf['pol_tags'].str.extract("\{name(.*?)\}",expand=True)
    #rdf['name_pol']=rdf['name_pol'].str.lstrip(', ')
    #rdf['name:en_pol']=rdf['name:en_pol'].str.lstrip(', ')

    #rdf['name:en_poi']=rdf['poi_tags'].str.extract("\{name:en(.*?)\}",expand=True)
    #rdf['name_poi']=rdf['poi_tags'].str.extract("\{name(.*?)\}",expand=True)
    #rdf['name_poi']=rdf['name_poi'].str.lstrip(', ')
    #rdf['name:en_poi']=rdf['name:en_poi'].str.lstrip(', ')

    rdf[sel_key+'_pol']=rdf['pol_tags'].str.extract("\{"+sel_key+"(.*?)\}",expand=True)
    rdf[sel_key+'_pol']=rdf[sel_key+'_pol'].str.lstrip(', ')

    rdf[sel_key+'_poi']=rdf['poi_tags'].str.extract("\{"+sel_key+"(.*?)\}",expand=True)
    rdf[sel_key+'_poi']=rdf[sel_key+'_poi'].str.lstrip(', ')

    rdf[sel_key]=''
    rdf[sel_key][rdf[sel_key+'_pol'].notna()]=rdf[sel_key+'_pol'][rdf[sel_key+'_pol'].notna()]
    rdf[sel_key][rdf[sel_key+'_poi'].notna()]=rdf[sel_key+'_poi'][rdf[sel_key+'_poi'].notna()]
    rdf[sel_key]=rdf[sel_key].str.lower()
    sel_val=sel_val.lower()
    rdf=rdf[rdf[sel_key]==sel_val]
    #break
    try:
        rdf.to_file(directory+'/'+pbfname+'/combination/'+pbfname+'_'+key+'_'+value+'_'+sel_key+'_'+sel_val+'.geojson')
        print('file saved to: ',directory+'/'+pbfname+'/combination/'+pbfname+'_'+key+'_'+value+'_'+sel_key+'_'+sel_val+'.geojson')
    except:
        print('failed for:',pbfname+'_'+key+'_'+value+'_'+sel_key+'_'+sel_val+'.geojson')
