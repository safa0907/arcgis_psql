import os
import os.path
import arcpy
from arcpy import Command, management
from arcpy import mp
import psycopg2
from datetime import date, datetime
from osgeo import ogr
from shapely.wkb import loads
import gdal
import gdaltools
import time


out_folder_path = arcpy.GetParameterAsText(0)
Instance = arcpy.GetParameterAsText(1)
username = arcpy.GetParameterAsText(2)
password = arcpy.GetParameterAsText(3)
database = arcpy.GetParameterAsText(4)
scene_id = arcpy.GetParameterAsText(5)

params = [out_folder_path,Instance, database,username, password,scene_id]
name_layer="labels_scene%s"%(params[5])

# Establish a connection to the database by creating a cursor object

conn_string = f"host={params[1]}"+ f" dbname={params[2]}" + f" user={params[3]}" + f" password={params[4]}" 

print(conn_string)
conn = psycopg2.connect(conn_string)
# Create a cursor object
cur = conn.cursor()
# Create view
view_name="ml_ops.scene_%s"%(params[5])
query= """create view %s as  
                    SELECT a.id,
                    a.label_id,
                    a.ignore,
                    a.corrected_footprint,
                    a.creation_timestamp ,
                    a.deleted,
                    a.geometry,
                    a.source_osm,
                    a.additional_attribute_id as additional_attribute_id
                    FROM ml_ops.polygon_label a,ml_ops.labeling_roi b    
                    WHERE st_intersects(a.geometry, b.bounding_box) AND b.scene_id = %s;"""%(view_name,params[5])
query_exec=cur.execute(query)
conn.commit()
name_gdb=os.path.join(out_folder_path, "GDB_scene%s.gdb")%(params[5])
name_layer="labels_scene%s"%(params[5])
# Convert query to File GDB
connection=r"host=%s" " dbname=%s" " user=%s" " password=%s" % (params[1],params[2],params[3],params[4])
command=r'start cmd /K ogr2ogr -f "FileGDB" %s  PG:"%s" -sql "select * from ml_ops.scene_%s" -nlt POLYGON -lco LAYER_ALIAS="labels_scene%s" -nln %s ' % (name_gdb,connection,params[5],params[5],name_layer)
os.system(command,)
time.sleep(5)
arcpy.AddMessage("%s is created."%(name_gdb))
cur2 = conn.cursor()
query2="drop view if exists %s"%(view_name)
query_exe=cur2.execute(query2)
conn.commit()
#Close the cursor and database connection
cur.close()
conn.close()

# Add layer to map
aprx = arcpy.mp.ArcGISProject("CURRENT")
m = aprx.listMaps("Map")[0]
file_gdb=os.path.join(name_gdb,name_layer)
Feature=arcpy.MakeFeatureLayer_management(file_gdb, name_layer) 
Layer=Feature.getOutput(0)
m.addLayer(Layer, "TOP")
arcpy.AddMessage("%s is added to Arcmap."%(file_gdb))