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

import geopandas as gpd
import pandas as pd
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import LongType
from shapely.geometry import Point
from shapely.geometry import Polygon

from sedona.register import SedonaRegistrator
from sedona.core.SpatialRDD import SpatialRDD
from sedona.core.SpatialRDD import PointRDD
from sedona.core.SpatialRDD import PolygonRDD
from sedona.core.SpatialRDD import LineStringRDD
from sedona.core.enums import FileDataSplitter
from sedona.utils.adapter import Adapter
from sedona.core.spatialOperator import KNNQuery
from sedona.core.spatialOperator import JoinQuery
from sedona.core.spatialOperator import JoinQueryRaw
from sedona.core.spatialOperator import RangeQuery
from sedona.core.spatialOperator import RangeQueryRaw
from sedona.core.formatMapper.shapefileParser import ShapefileReader
from sedona.core.formatMapper import WkbReader
from sedona.core.formatMapper import WktReader
from sedona.core.formatMapper import GeoJsonReader
from sedona.sql.types import GeometryType
from sedona.core.enums import GridType
from sedona.core.SpatialRDD import RectangleRDD
from sedona.core.enums import IndexType
from sedona.core.geom.envelope import Envelope
from sedona.utils import SedonaKryoRegistrator, KryoSerializer


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
    appName("Sedona App").\
    config("spark.serializer", KryoSerializer.getName).\
    config("spark.kryo.registrator", SedonaKryoRegistrator.getName) .\
    config("spark.jars.packages", "org.apache.sedona:sedona-python-adapter-3.0_2.12:1.1.0-incubating,org.datasyslab:geotools-wrapper:1.1.0-25.2") .\
    getOrCreate()


SedonaRegistrator.registerAll(spark)
sc = spark.sparkContext
spark.conf.set('spark.sql.repl.eagerEval.enabled', True)



def pg_connection(username,password,host,port,dbname):
    sconpar='postgresql+psycopg2://'+username+':'+password+'@'+host+':'+port+'/'+dbname+''
    engine = create_engine(sconpar,echo=False)

    param_dic={'user':username,'password':password,'port':port,'host':host,'database':dbname}
    conn = psycopg2.connect(**param_dic)
    return [engine, conn]

def push_to_postgis(gdf,engine,schemaname,tablename,ifexists):
    gdf.to_postgis(
        con=engine,
        name=tablename,
        if_exists=ifexists,
        schema=schemaname
    )
#with engine.begin() as conn:
#    conn.execute("call dedupe_pois()")
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


#GET DATA FROM OSM USING PYROSM
#get_data('sources.available.keys()',directory='./osm/pbf',update=True/False)
#
#LIST DIRECTORY
#os.listdir('./osm_pyrosm')
#
#PARQUETIZE .pbf file to node, way and relation
def parquetize(driver,file):
    #bashCommand = "java -jar /home/marko/osm-parquetizer/target/osm-parquetizer-1.0.1-SNAPSHOT.jar /home/marko/osm_pyrosm/chile-latest.osm.pbf"
    #driver path: /home/marko/osm-parquetizer/target/osm-parquetizer-1.0.1-SNAPSHOT.jar
    bashCommand = "java -jar "+driver+" "+file
    process = subprocess.run(bashCommand.split(), stdout=subprocess.PIPE)
#
#LOAD PARQUET FILES
#
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
        br+=1
        print('iter done'+fname+'_'+str(br)+'.geojson')

def read_gpd(path,table_name,scheema,**kwargs):
    filenames = glob.glob(path + "/*.geojson")
    
    dfs = []
    for filename in filenames:
        print('reading: ',filename)
        try:
            temp_file=gpd.read_file(filename)
            temp_file.crs='EPSG:4326'
            if 'relation' in filename:
                try:
                    push_to_postgis(temp_file,kwargs['engine'],scheema,table_name+'_relation','append')
                except:
                    print('failed for: ',filename+'_relation')
            else:
                try:
                    push_to_postgis(temp_file,kwargs['engine'],scheema,table_name,'append')
                except:
                    print('failed for: ',filename)
        except:
            print('reading failed for: ',filename)
        print('push to db')
        
    print('publish on geoserver')
    if 'relation' in filename:
        try:
            #publish_on_geoserver('http://34.91.102.177:8080/geoserver','admin','Rumarec18*',
            #                     table_name,'crowdpulse','crowdpulse_db_polygons',table_name)
            publish_on_geoserver(kwargs['geoserver_url'],kwargs['geoserver_username'],kwargs['geoserver_pass'],
                                 table_name,kwargs['geoserver_wspace'],kwargs['geoserver_store'],table_name+'_relation')
            print('done: ',filename+'_relation')
        except:
            print('publishing failed ', filename+'_relation')
    else:
        try:
            #publish_on_geoserver('http://34.91.102.177:8080/geoserver','admin','Rumarec18*',
            #                     table_name,'crowdpulse','crowdpulse_db_polygons',table_name)
            publish_on_geoserver(kwargs['geoserver_url'],kwargs['geoserver_username'],kwargs['geoserver_pass'],
                                 table_name,kwargs['geoserver_wspace'],kwargs['geoserver_store'],table_name)
            print('done: ',filename)
        except:
            print('publishing failed ', filename)



    print('publish on geoserver')
    try:
        #publish_on_geoserver('http://34.91.102.177:8080/geoserver','admin','Rumarec18*',
        #                     table_name,'crowdpulse','crowdpulse_db_polygons',table_name)
        publish_on_geoserver(kwargs['geoserver_url'],kwargs['geoserver_username'],kwargs['geoserver_pass'],
                             table_name,kwargs['geoserver_wspace'],kwargs['geoserver_store'],table_name)
        print('done: ',filename)
    except:
        print('publishing failed ', filename)


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
            if jj[0].role.decode() == 'outer':
                for ii in jj[1]:
                    #print(ii.point.latitude)
                    lo.append([ii.point.longitude,ii.point.latitude])
            else:
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
        #print(lov)
        features={"type":"Feature","properties":{'id':str(key),'tags':str(pdf.loc[i,'tags'])},"geometry":{"type":"MultiPolygon","coordinates":[]}}
        features['geometry']['coordinates'].append(lov)
        #if features not in d['features']:
        d['features'].append(features)
    with open(fname+'.geojson','w') as f:
        json.dump(d, f)
    print('dumping finished, file name: ',fname+'.geojson')


def poi_extractor(file,filterr,save_geojson=False,pbfname='',directory=''):
    s=file
    engine=pg_connection('marko','rumarec18','34.91.102.177','5432','crowdpulse')[0]
    #conn=pg_connection()[1]
    #cur=conn.cursor()
    sf=s.withColumn("key_value",expr(" explode(tags)" ))
    sf=sf.withColumn('key',expr(" key_value.key" ))
    sf=sf.withColumn('value',expr(" key_value.value" ))
    sf=sf.withColumn('key',sf.key.cast(StringType()))
    sf=sf.withColumn('value',sf.value.cast(StringType()))
    sf=sf.withColumn('tags',sf.tags.cast(StringType()))
    sf.createOrReplaceTempView("df")

    start_time=time.time()
    for i,j in filterr.items():
        key=i
        values=j
        print(key,'  ##################################')

        if values == '':
            n=sf.filter(sf.key==key)
        else:
            n=sf.filter((sf.key==key) & (sf.value.isin(values)))

        res=n.select('id','tags','latitude','longitude','key','value')
        res=res.withColumn('point',f.concat_ws(',',res.longitude,res.latitude))
        #print('counting rows')
        #print('COUNT:',res.count())

        df=res.createOrReplaceTempView("df")
        df=spark.sql("select * from df")
        grs=spark.sql("SELECT df.id,df.tags,df.key,df.value,ST_PointFromText(df.point,',') AS geom FROM df")
        #print('Converting to pandas')

        print('Converting to pandas')
        df = grs.toPandas()
        print("COUNT: ",len(df.index))
        print('Converting to geopandas')
        gdf = gpd.GeoDataFrame(df, geometry="geom",crs='EPSG:4326')

        #print('writting')

            #gdf.to_file(directory+'/'+key+'.geojson',driver='GeoJSON')
        end_time=time.time()
        print("time elapsed: ",end_time-start_time)
        if save_geojson == True:
            if directory!='':
                try:
                    os.mkdirs(directory)
                except:
                    pass
            print('export file ',pbfname+'_'+key,' to geojson')
            try:
                gdf.to_file(directory+'/'+pbfname+'_'+key+'.geojson')
                print(directory+'/'+pbfname+'_'+key,' file saved')
            except Exception:
                print(directory+'/'+pbfname+'_'+key,' file is not saved')

        print('Push to database')
        try:
            push_to_postgis(gdf,engine,'pois',pbfname+'_'+key,'replace')
            publish_on_geoserver('http://34.91.102.177:8080/geoserver','admin','Rumarec18*',pbfname+'_'+key,'crowdpulse','crowdpulse_db',pbfname+'_'+key)
        except:
            print(traceback.format_exc())
            print('Push to db failed for:',pbfname+'_'+key)
