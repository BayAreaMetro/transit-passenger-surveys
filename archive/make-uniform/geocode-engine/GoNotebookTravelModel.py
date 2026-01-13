import arcpy
import pandas
	
print("Step 0:  Party started ...")

# overwrite existing files
arcpy.env.overwriteOutput = True

# local variables needed by arc gis
working_directory    = workspace
spatial_ref          = "GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]"

xy_event_layer_name  = "EventLyr"
xy_event_feature     = working_directory + geodatabase + "/" + xy_event_layer_name

spatial_join_name    = "SpatialJoin_Lyr"
spatial_join_feature = working_directory + geodatabase + "/" + spatial_join_name
maz_feature          = working_directory + geodatabase + "/" + geodatabase_name

input_csv_file      = working_directory + input_csv
csv_output_all      = working_directory + "temp.csv"
csv_output_custom   = working_directory + output_csv

# create the xy event layer
arcpy.MakeXYEventLayer_management(input_csv_file, x_field, y_field, xy_event_layer_name, spatial_ref , "")
print("Step 1:  XY event layer created ...")

# copy features
arcpy.CopyFeatures_management(xy_event_layer_name, xy_event_feature, "", "0", "0", "0")
print("Step 2:  Copying features ...")

# spatial join
arcpy.SpatialJoin_analysis(xy_event_feature, maz_feature, spatial_join_feature, "JOIN_ONE_TO_ONE", "KEEP_ALL","" , "INTERSECT", "", "")
print("Step 3:  Geo-coded points to boundaries ...")

# write data out to a csv
field_names = [f.name for f in arcpy.ListFields(spatial_join_feature) if f.type <> 'Geometry']
with open (csv_output_all,'w') as f:
	f.write(','.join(field_names)+'\n')
	with arcpy.da.SearchCursor(spatial_join_feature, field_names) as cursor:
		for row in cursor:
			f.write(','.join([("\"" + str(r) + "\"") for r in row])+'\n')
#            print(','.join([("\"" + str(r) + "\"") for r in row])+'\n')
print("Step 4:  Write to " + csv_output_all + " ...")

# read/write the information I need
data_frame = pandas.read_csv(csv_output_all)
data_frame = data_frame.convert_objects(convert_numeric = True)

for i, column in enumerate(data_frame.columns):
	if column not in keep_fields:
		del data_frame[column]

		data_frame.to_csv(csv_output_custom, header = True, index = False)

print("Step 5:  Write to " + csv_output_custom + " ...")

print ("Finished:  Wrap it up.")