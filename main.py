import configparser
import os
import pandas as pd
from unidecode import unidecode
import datetime
import warnings
from pathlib import Path
import md_logfile as lf
import md_genconfig as gc
import numpy as np
import re

def log_time(on,message,time):
	if on == 1: print(f"{message}: {time}")


def debug_code(debug,message,var=None):
	if debug: print(f"{message}: {var};")


def idx_name(var):
	try: return int(var)
	except: return  var

def column_name_adjust(header):

	header = [column_title if not '%' in column_title else column_title.replace('%', "Percentual") for column_title in header]

	column = [
		re.sub(
			r'_+', '_',  # collapse multiple underscores into one
			''.join(letter if letter.isalnum() else "_"  for letter in unidecode(str(elem)))
		).strip('_').lower()  # remove leading/trailing underscores
		for elem in header
	]

	return column

def check_element(list_i,list_j,list_unique):
	for elem in list_i:
		if len(list_j) > 0:
			if elem not in list_j and elem not in list_unique:
				list_unique.append(elem)


def column_first_blank(header,collumn_find,debug=False):
	idx = None
	for i,item in enumerate(header):
		if collumn_find in str(item): 
			idx = i
			break

	if idx is None: idx = (len(header)+1)
	
	debug_code(debug, "First empty column number", idx)
	return idx


def header_transform(header_current, adjust_model):
	for j in range(len(header_current)):
		for i in range(len(adjust_model[0])):	
			if header_current[j] in adjust_model[1][i]:
				header_current[j] = adjust_model[0][i].strip()

	return(header_current)


def is_num_cast(value,decimal=True,boolean=True):
	try:
		if decimal: y = float(value)
		else:  y = int(value)
		
		if boolean: x = True
		else: x = y

	except ValueError:
		x = False
		if boolean: x = False
		else: x = 0

	return x

#drop_numbered_col
def col_drop(column_drop,column_drop_number,dataframe):
	# Drop just listed numnered columns
	
	if column_drop:
		header = dataframe.columns.to_list()
		for column_name in column_drop:
			if column_name in header: 
				dataframe.drop(columns=column_name, inplace=True)
				
	# Drop all numbered columns
	#if (column_drop == ' ' or column_drop[0] == ''):
	if '1' in column_drop_number:
		bool_array = [is_num_cast(x) for x in dataframe.columns]
		bool_array = [str(x).isdigit() for x in dataframe.columns]		
		bool_array = np.array(bool_array)
		dataframe = dataframe.loc[:, ~bool_array]


# MARK: Transform Data
def transform(
	path_root,path_destination,name_sheet
	,header_start_row,header_adjust_model,original_datatype=False
	,column_skip=False,column_lower=False,column_strip=False,column_stop_first_blank=False
	,column_drop=False,column_drop_number=False,column_not_null=False,column_add_file_control=False
	,column_add=False,column_add_sheet_value=False,column_add_sheet_value_name=False,column_limit=False,column_limit_characters=False
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
				
				if '1' in original_datatype:
					excel_dict = pd.read_excel(io=filename,header=header_start_row,sheet_name=None,na_values=["","-"])
				else:  excel_dict = pd.read_excel(io=filename,header=header_start_row,sheet_name=None,na_values=["","-"],dtype=str)

				log_time(logtime,"[Time] Read file",datetime.datetime.now()-time_start)
				
				for name, sheet in excel_dict.items():
					for value in name_sheet:
						if value.lower() in name.lower():
							debug_code(debug,"02->Sheet name", name)
							
							# Those columns changes codes was dislocate to top, avoiding to deal with a large size of columns
							if column_skip: sheet = sheet.iloc[:,column_skip:]
							
							if '1' in column_stop_first_blank:
								time_start = datetime.datetime.now()
								# debug_code(debug,"File columns", sheet.columns)
								idx = column_first_blank(header=sheet.columns,collumn_find="Unnamed:",debug=debug)
								debug_code(debug,"03->First blank column index", idx)
								sheet = sheet.iloc[:, :idx]
								log_time(logtime,"[Time] Get dataset until first blank column",datetime.datetime.now()-time_start)
							
							# Same idea to avoid large number of rows, unnecessary transform in code below
							if '1' in row_stop_first_blank:
								idx_first_empty_row = sheet.isna().all(axis=1).idxmax()
								sheet = sheet[:idx_first_empty_row:]
							debug_code(debug, "04->Datataframe shape", sheet.shape)
							
							# -=-=-= Header -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
							
							sheet.columns = sheet.columns.astype(str)
							header_current =  column_name_adjust(sheet.columns)

							if not header_adjust_model is None: sheet.columns = header_transform(header_current, header_adjust_model)
							else: sheet.columns = header_current

							# -=-=-= Header -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
							
							# -=-=-= Columns -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

							if column_drop[0] != '':
								exclude = []
								for col_name in header_current:
									for drop_col in column_drop:
										if  drop_col in col_name: exclude.append(col_name)

									#ls = [c for c in header if c in column_drop]
								sheet.drop(columns=exclude, inplace=True)
							
							if '1' in column_drop_number:
								sheet.drop(columns=[c for c in header_current if str(c).isdigit()],inplace=True)

							# -=-=-= Columns -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=

							# -=-=-= Rows -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
							
							if not(len(column_not_null) == 1 and column_not_null[0] ==''):
								
								if '1' in row_stop_first_blank:
									idx_first_empty_row = sheet[column_not_null].isna().all(axis=1).idxmax()
									sheet = sheet[:idx_first_empty_row:]
								
								time_start = datetime.datetime.now()
								debug_code(debug,"05->Columns that must be filled",column_not_null)
								query_filled = "".join("& "+elem+(".notnull() ") if x > 0 else elem+(".notnull() ") for x, elem in enumerate(column_not_null))
								debug_code(debug, "06->Filters", query_filled)
								sheet = sheet.query(query_filled)
								log_time(logtime,"07->[Time] Filter dataset",datetime.datetime.now()-time_start)
							
					
							if drop_thresh_blank > 0:
								drop_thresh_blank = int(drop_thresh_blank)
								if drop_thresh_blank >= 1: drop_thresh_blank = drop_thresh_blank-1

								debug_code(debug,"08->Remove row if blank column is",drop_thresh_blank)
								time_start = datetime.datetime.now()
								sheet.dropna(thresh=drop_thresh_blank-1, inplace=True)
								log_time(logtime,"[Time] Remove empty row",datetime.datetime.now()-time_start)

							# -=-=-= Rows -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
							
							time_start = datetime.datetime.now()
							sheet.dropna(axis=0, how="all", inplace=True)
							log_time(logtime,"[Time] remove empty row",datetime.datetime.now()-time_start)
						
							if '1' in column_add_file_control:
								sheet.insert(1,"file_name",Path(filename).stem)
								sheet.insert(2,"sheet",str(name))

							time_start = datetime.datetime.now()

							if '1' in column_lower:
								for col in sheet.columns:
									#print(f"column: {col}, type: {sheet[col].dtypes}")
									check = sheet[col]
									if  isinstance(check, pd.DataFrame):
										print(f"{check.info()}, filename: {filename}, sheet: {name}")
										print(check.head(5))
									if sheet[col].dtype == "object": sheet[col] = sheet[col].str.lower().str.strip()
									

							if '1' in column_strip:
								for col in sheet.columns:
									if sheet[col].dtype == "object": sheet[col] = sheet[col].str.strip()

							if  not '1' in original_datatype:
								for col in sheet.columns:
									if sheet[col].dtype == "object": sheet[col] = sheet[col].str.replace("None",'')

							if len(column_limit) > 0:
								for col in column_limit:
									if col in sheet.columns: sheet[col] = sheet[col].apply(lambda x: x[:column_limit_characters] if isinstance(x, str) else x)

							if '1' in row_drop_duplicate:
								time_start = datetime.datetime.now()
								sheet.drop_duplicates(inplace=True,keep="last")
								log_time(logtime,"[Time] Drop duplicates",datetime.datetime.now()-time_start)
							
							sheet.reset_index(inplace=True)
							sheet.rename(columns={"index": "excel_row"},inplace=True)
							exc_idx = sheet.pop("excel_row")
							sheet.insert(0,"excel_row",exc_idx)


							if '1' in post_merge: path_absolute_destination = os.path.join(path_destination,f"{str(count)}_{name}.feather")
							else: path_absolute_destination = os.path.join(path_destination,f"{name}.feather")
							
							if not os.path.exists(path_destination): os.makedirs(path_destination)

							sheet = sheet.assign(**column_add)
							
							if column_add_sheet_value != '':
								for key in column_add_sheet_value.keys():
									if key in name.lower().replace(' ','_'):
										sheet[column_add_sheet_value_name] = column_add_sheet_value[key]

							#debug_code(debug,f"Sheet information: {sheet.info()}\r\n Sheat header: {sheet.head()}")

							sheet.to_feather(path_absolute_destination)

							debug_code(debug,"09->Saved file in",path_absolute_destination)
							
							log_time(logtime,"[Time] Generate file",datetime.datetime.now()-time_start)
							debug_code(debug,"\n")
							count+=1

				debug_code(debug,"\n\n")
	except:
		lf.log_file(name="log_error",header="Multiple Sheet",datetime=datetime.datetime.today())
	

# MARK: Join Headers
def join_header(path_temp,column_add_file_control,debug=False,logtime=False):
	try:
		time_start =  datetime.datetime.now()
		previous_header = []
		current_header = []
		unique_columns = []

		for filename in sorted(os.listdir(path_temp)):
			abs_path_file = os.path.join(path_temp,filename)
			if os.path.isfile(abs_path_file):

				current_header = pd.read_feather(abs_path_file).columns.to_list()
				debug_code(debug,"01->abs_path_file", abs_path_file)

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
		
		if '1' in column_add_file_control:
			complete_header.insert(0,complete_header.pop(complete_header.index("excel_row")))
			complete_header.insert(1,complete_header.pop(complete_header.index("file_name")))
			complete_header.insert(2,complete_header.pop(complete_header.index("sheet")))
			if "idx" in complete_header: complete_header.insert(0,complete_header.pop(complete_header.index('idx')))

		log_time(logtime,"Extract/transform headers",datetime.datetime.now()-time_start)
		debug_code(debug,"05->header list",complete_header)
	
		return(complete_header)
	except:
		lf.log_file(name="log_error",header="Single Sheet",datetime=datetime.datetime.today())


# MARK: Join Data
def join_dataset(path_temp,header_standardized,path_destination,filename,column_reorder,column_add_missed,logtime,debug=False):
	time_start =  datetime.datetime.now()
	data = []
	
	try: 
		for temp_filename in sorted(os.listdir(path_temp)):
			path_absolute = os.path.join(path_temp,temp_filename)
			if os.path.isfile(path_absolute):
				debug_code(debug,"06->filename",path_absolute)
				df = pd.read_feather(path_absolute)
				df = df.reindex(df.columns.union(header_standardized, sort=False), axis=1, fill_value=None)
				df = df.reindex(header_standardized,axis=1)
				df = df.dropna(axis=0, how="all")
				
				if len(column_reorder) > 1:
					if '1' in column_add_missed: 
						for col in column_reorder:
							if not col in df.columns.to_list(): 
								df[col] = None
								#df[col].astype(str)

					df = df[column_reorder]
				
				data.append(df)
		data = pd.concat(data)

		if not os.path.exists(path_destination): os.makedirs(path_destination)
		data.to_feather(os.path.join(path_destination,filename))

		log_time(logtime,"Get data",datetime.datetime.now()-time_start)
	except:
		lf.log_file(name="log_error",header="Config File",datetime=datetime.datetime.today())

# MARK: Delete temple files
def delete_tempfile(path_temp,filename,logtime,debug=False):
	debug_code(debug,"Case: 0, delete temp files")
	ts =  datetime.datetime.now()
	for file in sorted(os.listdir(path_temp)):
		path_absolute_destination = os.path.join(path_temp,file)
		if os.path.isfile(path_absolute_destination) and file != filename: os.remove(path_absolute_destination)
	log_time(logtime,"Delete files",datetime.datetime.now()-ts)


#MARK: Main
def main():  
	pd.set_option("display.max_columns", None)
	warnings.filterwarnings('ignore') 
  
	try:
		config = configparser.RawConfigParser(allow_no_value=True)
		path_config = os.path.join(os.getcwd(),"config")
		#abs_path_config = os.path.join(path_config,"config.ini")
		abs_path_config = os.path.join(path_config,"config_test.ini")
		config.read(abs_path_config, encoding="utf-8")
	except:
		lf.log_file(name="log_error",header="Config File",datetime=datetime.datetime.today())
		
	# To extract data
	windows_username= os.getlogin()
	path_root = config["FILE"]["path_input"].replace("custom",windows_username)

	post_merge = config["MODE"]["post_merge"]
	root_path = os.getcwd()
	path_destination = config["FILE"]["path_output"].replace("custom",windows_username)
	
	path_temp = os.path.join(root_path,"temp")
	if not os.path.exists(path_temp): os.makedirs(path_temp)

	filename = config["FILE"]["name"] + ".feather"
	name_sheet = config["FILE"]["name_sheet"].lower().split(",")
	original_datatype = config["FILE"]["original_datatype"]
	
	header_start_row = int(config["HEADER"]["start_row"])
	header_adjust_model = config["HEADER"]["adjust_model"]
	
	#values of drop is cansitive? And special characters?
	if header_adjust_model != [""]:
		v = [x.lower().split(',') for x in header_adjust_model.split(';')]
		k = [x.pop(0) for x in v]
		header_adjust_model = [k,v]

	column_skip = int(config["COLUMN"]["skip"])
	if column_skip > 2: column_skip = column_skip-1
	column_lower = config["COLUMN"]["lower"]
	column_strip = config["COLUMN"]["strip"]
	column_not_null = column_name_adjust(config["COLUMN"]["not_null"].split(","))
	column_drop = column_name_adjust(config["COLUMN"]["drop"].split(","))
	column_drop_number = config["COLUMN"]["drop_number"]

	column_stop_first_blank = config["COLUMN"]["stop_first_blank"]
	column_add_file_control = config["COLUMN"]["add_file_control"]
	column_add =  dict(config["ADD_COLUMN"])
	column_add_sheet_value = ''
	column_add_sheet_value_name = ''

	if column_add:
		key_pop = False
		for k,v in column_add.items():
			if ',' in v: key_pop = k

		if key_pop: 
			column_by_sheet_name = column_add.pop(key_pop)
			comma_splitted = [value.split(',') for value in column_by_sheet_name.split(';')]
			column_add_sheet_value = dict([(sub_array[0].lower().replace(' ','_'), sub_array[1].replace(' ','')) for sub_array in comma_splitted])
			column_add_sheet_value_name = k


	column_reorder = config["COLUMN"]["reorder"].split(',')

	column_limit= config["COLUMN"]["limit"].split(',')
	column_limit_characters = is_num_cast(config["COLUMN"]["limit_characters"],decimal=False,boolean=False)

	row_stop_first_blank = config["ROW"]["stop_first_blank"]
	row_drop_duplicate = config["ROW"]["drop_duplicate"]	
	drop_thresh_blank = is_num_cast(config["ROW"]["drop_thresh_blank"],decimal=False,boolean=False)

	logtime = config["DEBUG"]["logtime"]
	debug= config["DEBUG"]["on"]
	export_config_name= config["CONFIG"]["name"]
	time_start =  datetime.datetime.now()

	if '1' in post_merge: aux = path_temp
	else: aux = path_destination

	#delete_tempfile(path_temp,filename,logtime,debug=False)

	transform(
		path_root=path_root,path_destination=aux,name_sheet=name_sheet
		,header_start_row=header_start_row,header_adjust_model=header_adjust_model,original_datatype=original_datatype
		,column_skip=column_skip,column_lower=column_lower,column_strip=column_strip,column_stop_first_blank=column_stop_first_blank
		,column_drop=column_drop,column_drop_number=column_drop_number,column_not_null=column_not_null,column_add_file_control=column_add_file_control
		,column_add=column_add,column_add_sheet_value=column_add_sheet_value,column_add_sheet_value_name=column_add_sheet_value_name,column_limit=column_limit,column_limit_characters=column_limit_characters
		,row_stop_first_blank=row_stop_first_blank,row_drop_duplicate=row_drop_duplicate,drop_thresh_blank=drop_thresh_blank
		,post_merge=post_merge,logtime=logtime,debug=debug
	)

	if '1' in post_merge:
		header_standardized = join_header(path_temp=path_temp,column_add_file_control=column_add_file_control,debug=debug,logtime=logtime)
		join_dataset(
			path_temp=path_temp
			,header_standardized=header_standardized
			,path_destination=path_destination
			,filename=filename
			,column_reorder=column_reorder
			,column_add_missed=config["COLUMN"]["add_missed"]
			,logtime=logtime
			,debug=False
		)
	
	delete_tempfile(path_temp,filename,logtime,debug=False)

	if export_config_name != "" and export_config_name != " ":
		gc.generate_config_file(
			path_file_in=path_config
			,path_file_out=config["CONFIG"]["path_out"].replace("custom",windows_username)
			,name_config=os.path.basename(abs_path_config)
			,name_new_config=f"{export_config_name}_{Path(filename).stem}"
			,summary=config["CONFIG"]["summary"]
			,commentary=config["CONFIG"]["commentary"]
		)
	
	log_time(logtime,"[Time] Start Time",time_start)
	log_time(logtime,"[Time] Total spended",datetime.datetime.now()-time_start)


if __name__ == "__main__": main()