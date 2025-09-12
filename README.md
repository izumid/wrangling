# Parameters Summary 

#MODE
- post_merge (bool)[0,1]:
	0 = for each csv file generate a dataset .feather;
	1 = merge all csv results into a single .feather dataset;


#FILE
- name (string):  desired name to file. Extesions attributed by hardcode;
- sheet(string): its case sensitive, but accepts number or name (case sensitive), default its index 0 (first sheet). Obrigatory to single - sheet;
- name_sheet (list[str]): doesn't work with sheet_single = 1. If a sheet contains the specified terms it will be readed;
- log_name: (string), desired name to file that log errors;
- original_datatype(bool)[0,1]: when 1 python tries to identify the datatype, else all will be treat as string;


#HEADER
- row: (bool)[0,1], default its index 0 (first row);
- adjust_model: (list), separated list of elements to adjust column name based each index 0 name. First element denote the defaultname.
- Comma separate elements of same column and semicolon different collumns. 
	1. Note: by default '%' char is changed to "Percentual", after execute the step below (2);
	2. Important: "adjust model" its executed after adjust header to lower case and change space to desired char, make sure to type values as your setted pattern;


#COLUMN
- skip (int): skip blanks columns in excel to start correctly the dataframe;
- lower (int): change column values to string (also do strip);
- strip (int): remove spaces in the beggning or columns ending;
- not_null (list[str]): list of columns that must be filled, pass the combination of columns that make registry valid, if one of the columns - is null the registry will be dropped;
- drop (list[str]): columns name do drop;
- drop_number (bool)[0,1]: drop columns named just with number, for example duplicate empty column in excel matrixes;
- stop_first_blank (bool)[0,1]: when 1 stop reading columns to dataframe at first blank column ocorrence;
- add_file_control (bool)[0,1]: when 1 add filename and sheet name columns to final databasefile (feather);
- drop_thresh_blank (int): remove row if filled colums match with value specified. drop_thresh_blank_registry = 2 remove row if has just 2  - or less columns filled with data;
- until_first_blank (bool)[0,1]:, stop reading collumns if an empty column is finded;
- reorder (list): list of column titles ordened as disered. Can also be used to return just the desired columns. Be carefull with - - - - duplicate - columns name, by default python will renamed a reapeated column as 1 e.g name become name1
- add_missed (bool)[0,1]:
- limit (bool)[0,1]:
- limit_characters (int):

#ADD_COLUMN
- each key will treat as column name and value the column's default value

#ROW
- stop_first_blank: (bool)[0,1], stop reading rows if an empty column is finded;
- drop_thresh_blank: (bool), if empty rows dont hit that limit row are removed;
- drop_duplicate: (bool)[0,1], 

#PATH
- fileIn (string): folder that file is in;
- fileout (string): file's destination folder;


#DEBUG
- on: (int)[0,1], show scripts steps while running;
- logtime: (int)[0,1], exhibit the expended time to each step;

#CONFIG
- on: (bool): when 1 export used "config.ini" file;
- header (bool): when 1 export this documentation summary in generated "config.ini" file;
- path_out (str): configuration's file destination folder;