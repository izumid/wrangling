import configparser
import os
import pandas as pd
from unidecode import unidecode
import datetime
#import csv
import warnings
#import time
from pathlib import Path
import md_logfile as lf
import md_genconfig as gcf
import numpy as np
#from pyarrow import feather

def log_time(on,message,time):
	if on == 1: print(f"{message}: {time}")


def debug_code(debug,message,var=None):
	if debug == 1: print(f"{message}: {var};")


def idx_name(var):
	try: return int(var)
	except: return  var


def check_element(list_i,list_j,list_unique):
	for elem in list_i:
		if len(list_j) > 0:
			if elem not in list_j and elem not in list_unique:
				list_unique.append(elem)


def column_first_blank(dataframe,debug=False):
	for i,item in enumerate(dataframe.columns):
		if "Unnamed:" in str(item): idx = i

		if idx is None: idx = (len(dataframe.columns)+1)
	
	debug(debug, "First empty column number", idx)
	return idx


def header_transform(header_current, adjust_model):
	for j in range(len(header_current)):
		for i in range(len(adjust_model[0])):	
			if header_current[j] in adjust_model[1][i]:
				header_current[j] = adjust_model[0][i]

	return(header_current)


def is_num(value):
	try:
		float(value)
		x = True
	except ValueError:
		x = False

	return x


def drop_numbered_col(column_drop_num_name,column_drop,dataframe):
	# Drop all numbered columns
	if '1' in column_drop_num_name and (column_drop == ' ' or column_drop[0] == ''):
		bool_array = [is_num(x) for x in dataframe.columns]
		bool_array = np.array(bool_array)
		dataframe = dataframe.loc[:, ~bool_array]
	
	# Drop just listed numnered columns
	if not(column_drop == ' ' and column_drop[0] =='') and not '1' in column_drop_num_name:
		for column_name in column_drop:
			if column_name in dataframe.columns: 
				dataframe.drop(columns=column_name, inplace=True)


# MARK: Transform Data
def transform(
	path_root,path_destination,name_sheet
	,header_row, header_adjust_model
	,column_skip=False,column_stop_first_blank=False,column_drop_num_name=False
	,column_drop=False,column_not_null=False,column_add_file_control=False
	,row_stop_first_blank=False,row_drop_duplicate=False,drop_thresh_blank=False
	,post_merge=False,logtime=False,debug=False
):
	try:
		count = 0
		for filename in sorted(os.listdir(path_root)):	
			filename = os.path.join(path_root,filename)
			if os.path.isfile(filename):
				debug_code(debug,"01->Filename",filename)
				time_start = datetime.datetime.now()
				excel_dict = pd.read_excel(io=filename,header=header_row,sheet_name=None,na_values=["","-"],dtype=str) #changed
				log_time(logtime,"[Time] Read file",datetime.datetime.now()-time_start)
				
				for name, sheet in excel_dict.items():
					for value in name_sheet:
						if value.lower() in name.lower():
							#print(name,list(sheet.columns))
							debug_code(debug,"02->Sheet name", name)
							
							if column_skip > 0: sheet = sheet.iloc[:,column_skip:]
							
							if column_stop_first_blank:
								time_start = datetime.datetime.now()
								# debug_code(debug,"File columns", sheet.columns)
								idx = column_first_blank(sheet.columns,"Unnamed:")
								debug_code(debug,"03->First blank column index", idx)
								sheet = sheet.iloc[:, :idx]
								log_time(logtime,"[Time] Get dataset until first blank column",datetime.datetime.now()-time_start)
							
							#debug_code(debug, "Datataframe informations", sheet.info())
							debug_code(debug, "04->Datataframe shape", sheet.shape)
							
							drop_numbered_col(column_drop_num_name=column_drop_num_name,column_drop=column_drop, dataframe=sheet)

							#sheet.columns = [(''.join(letter for letter in unidecode(str(elem)) if letter.isalnum())).lower() for elem in sheet.columns]	
							header_current = [(''.join(letter for letter in unidecode(str(elem)) if letter.isalnum())).lower() for elem in sheet.columns]	

							if not header_adjust_model is None: sheet.columns = header_transform(header_current, header_adjust_model)

							if row_stop_first_blank:
								idx_first_empty_row = sheet.isna().all(axis=1).idxmax()
								sheet = sheet[:idx_first_empty_row:]
							
							if not(len(column_not_null) == 1 and column_not_null[0] ==''):
								if row_stop_first_blank:
									idx_first_empty_row = sheet[column_not_null].isna().all(axis=1).idxmax()
									sheet = sheet[:idx_first_empty_row:]
								
								#sheet.columns = [(''.join(letter for letter in unidecode(str(elem)) if letter.isalnum())).lower() for elem in sheet.columns]	
								time_start = datetime.datetime.now()
								debug_code(debug,"05->Columns that must be filled",column_not_null)
								query_filled = "".join("& "+elem+(".notnull() ") if x > 0 else elem+(".notnull() ") for x, elem in enumerate(column_not_null))
								debug_code(debug, "06->Filters", query_filled)
								sheet = sheet.query(query_filled)
								log_time(logtime,"07->[Time] Filter dataset",datetime.datetime.now()-time_start)
							
					
							if drop_thresh_blank > 0: 
								if drop_thresh_blank >= 1: drop_thresh_blank = drop_thresh_blank-1

								debug_code(debug,"08->Remove row if blank column is",drop_thresh_blank)
								time_start = datetime.datetime.now()
								sheet.dropna(thresh=drop_thresh_blank-1, inplace=True)
								log_time(logtime,"[Time] Remove empty row",datetime.datetime.now()-time_start)

							# Deprecated
							"""if not(len(column_drop) == 1 and column_drop[0] ==''):
								for column_name in column_drop:
									print(f"file: {filename}, column: {column_name}-=-=-=-=-=-=-=-=-=")
									if column_name in sheet.columns: sheet.drop(columns=str(column_name), inplace=True)
									print("done")"""

							if row_drop_duplicate:
								time_start = datetime.datetime.now()
								sheet.drop_duplicates(inplace=True,keep="last")
								log_time(logtime,"[Time] Drop duplicates",datetime.datetime.now()-time_start)

							time_start = datetime.datetime.now()
							sheet.dropna(axis=0, how="all", inplace=True)
							log_time(logtime,"[Time] remove empty row",datetime.datetime.now()-time_start)

							if column_add_file_control:
								sheet.insert(0,"filename",Path(filename).stem)
								sheet.insert(1,"sheet",str(name))

							time_start = datetime.datetime.now()

							if post_merge: path_absolute_destination = os.path.join(path_destination,f"{str(count)}_{name}.feather")
							else: path_absolute_destination = os.path.join(path_destination,f"{name}.feather")
							debug_code(debug,"09->Save file in",path_absolute_destination)
							sheet.to_feather(path_absolute_destination)

							log_time(logtime,"[Time] Generate file",datetime.datetime.now()-time_start)
							debug_code(debug,"\n")
							count+=1
				debug_code(debug,"\n\n")
	except:
		lf.log_file(name="log_error",header="Multiple Sheet",datetime=datetime.datetime.today())
	

# MARK: Format Header
def header_format(path_temp,filename,idx_or_name=False,header_row=False,line_number=False,debug=False,logtime=False,add_file_columns=False,column_skip=False):
	try:
		time_start =  datetime.datetime.now()
		previous_header = []
		current_header = []
		unique_columns = []

		for filename in sorted(os.listdir(path_temp)):
			filename = os.path.join(path_temp,filename)
			if os.path.isfile(filename):
				if Path(filename).suffix == ".csv": df = pd.read_csv(filename, sep=";", nrows=line_number, dtype=str,encoding="utf-8-sig")
				else: df = pd.read_excel(io=filename,header=header_row,sheet_name=idx_or_name,na_values=["","-"],dtype=str,nrows=line_number)

				if column_skip > 0: df = df.iloc[column_skip:]

				current_header = [(''.join(letter for letter in unidecode(str(elem)) if letter.isalnum())).lower() for elem in df.columns]

				debug_code(debug,"01->filename", filename)

				if len(unique_columns) > 0:
					check_element(previous_header,current_header,unique_columns)
					check_element(current_header,previous_header,unique_columns)
				else:
					check_element(previous_header,current_header,unique_columns)

			debug_code(debug,"02->previous_header", previous_header)
			debug_code(debug,"03->current_header", current_header)
			debug_code(debug,"04->unique_columns", unique_columns)
	
			previous_header = current_header

		complete_header = [x for x in previous_header if x not in unique_columns] + unique_columns
		
		if add_file_columns == 1:
			complete_header.insert(0,complete_header.pop(complete_header.index('filename')))
			complete_header.insert(1,complete_header.pop(complete_header.index('sheet')))
			if "idx" in complete_header: complete_header.insert(0,complete_header.pop(complete_header.index('idx')))

		log_time(logtime,"Extract/transform headers",datetime.datetime.now()-time_start)
		debug_code(debug,"05->header list",complete_header)
	
		return(complete_header)
	
	except:
		lf.log_file(name="log_error",header="Single Sheet",datetime=datetime.datetime.today())


# MARK: Join Data
def join(path_temp,header_standardized,path_destination,logtime,debug=False):
	time_start =  datetime.datetime.now()
	data = []

	for filename in sorted(os.listdir(path_temp)):
		filename = os.path.join(path_temp,filename)
		if os.path.isfile(filename):
			debug_code(debug,"06->filename",filename)
			df = pd.read_feather(filename)
			df.columns = header_standardized # Duplicated columns error happens here, be carefull with header_standardize parameter!!
			df = df.reindex(df.columns.union(header_standardized, sort=False), axis=1, fill_value=None)
			df = df.reindex(header_standardized,axis=1)
			df = df.dropna(axis=0, how="all")
			data.append(df)

	data = pd.concat(data)
	data.to_feather(path_destination)

	log_time(logtime,"Get data",datetime.datetime.now()-time_start)


# MARK: Delete temple files
def delete_tempfile(path_temp,filename,logtime,debug=False):
	debug_code(debug,"Case: 0, delete temp files")
	ts =  datetime.datetime.now()
	for file in sorted(os.listdir(path_temp)):
		path_absolute_destination = os.path.join(path_temp,file)
		if os.path.isfile(path_absolute_destination) and file != filename: os.remove(path_absolute_destination)
	log_time(logtime,"Delete files",datetime.datetime.now()-ts)


def main():  
	pd.set_option("display.max_columns", None)
	warnings.filterwarnings('ignore') 
  
	# Config File
	config = configparser.RawConfigParser(allow_no_value=True)
	config.read(os.path.join(os.getcwd(),"config/config.ini"), encoding="utf-8")
		
	# To extract data
	windows_username= os.getlogin()
	path_root = config["PATH"]["fileIn"].replace("custom",windows_username)
	path_destination = config["PATH"]["fileOut"].replace("custom",windows_username)
	path_temp = os.path.join(os.getcwd(),"temp")
	if not os.path.exists(path_temp): os.makedirs(path_temp)

	# Transform and load data (T.L)
	post_merge = int(config["MODE"]["post_merge"])
	idx_or_name = idx_name(config["MODE"]["sheet_identification"])

	filename = config["FILE"]["name"] + ".csv"
	name_sheet = config["FILE"]["name_sheet"].lower().split(",")
	
	header_row = int(config["HEADER"]["row"])
	header_adjust_model = config["HEADER"]["adjust_model"]
	if header_adjust_model != [""]:
		v = [x.lower().split(',') for x in header_adjust_model.split(';')]
		k = [x.pop(0) for x in v]
		header_adjust_model = [k,v]
	else: header_adjust_model = None

	column_skip = int(config["COLUMN"]["skip"])
	if column_skip > 2: column_skip = column_skip-1
	column_not_null = [(''.join(letter for letter in unidecode(str(elem)) if letter.isalnum())).lower() for elem in config["COLUMN"]["not_null"].split(",")]
	column_drop = [(''.join(letter for letter in unidecode(str(elem)) if letter.isalnum())).lower() for elem in config["COLUMN"]["drop"].split(",")]
	column_drop_num_name = config["COLUMN"]["drop_num_name"]
	column_stop_first_blank = int(config["COLUMN"]["stop_first_blank"])
	column_add_file_control = int(config["COLUMN"]["add_file_control"])

	row_stop_first_blank = int(config["ROW"]["stop_first_blank"])
	row_drop_duplicate = int(config["ROW"]["drop_duplicate"])
	drop_thresh_blank = int(config["ROW"]["drop_thresh_blank"])

	# Debug/monitoring
	logtime = int(config["DEBUG"]["logtime"])
	debug= int(config["DEBUG"]["on"])
	time_start =  datetime.datetime.now()

	transform(
		path_root,path_destination,name_sheet
		,header_row, header_adjust_model
		,column_skip=column_skip,column_stop_first_blank=column_stop_first_blank,column_drop_num_name=column_drop_num_name
		,column_drop=column_drop,column_not_null=column_not_null,column_add_file_control=column_add_file_control
		,row_stop_first_blank=row_stop_first_blank,row_drop_duplicate=row_drop_duplicate,drop_thresh_blank=drop_thresh_blank
		,post_merge=post_merge,logtime=logtime,debug=debug
	)

	if 1==0:
		if not header_adjust_model is None:
			header_standardized = header_format(path_temp,filename,idx_or_name=False,header_row=False,line_number=False,debug=False,logtime=False,add_file_columns=False,column_skip=column_skip)
			join(path_temp,header_standardized,path_destination,logtime,debug=False)
			delete_tempfile(path_temp,filename,logtime,debug=False)
			
		if int(config["CONFIG"]["save"]) == 1:
			gcf.generate(
				path_in=os.getcwd()
				,path_out=config["CONFIG"]["path_out"].replace("custom",windows_username)
				,name="config"
				,name_new=config["FILE"]["name"]
				,summary=config["CONFIG"]["summary"]
			)
	
	log_time(logtime,"[Time] Start Time",time_start)
	log_time(logtime,"[Time] Total spended",datetime.datetime.now()-time_start)


if __name__ == "__main__":
	print("Start")
	main()
	print("Finished")