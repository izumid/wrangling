import os
import datetime
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)
import md_logfile as lf

def generate_config_file(path_file_in,path_file_out,name_config,name_new_config,summary,commentary):
	content=[]
	ext = "ini"

	try: 
		with open(os.path.join(path_file_in, name_config), 'r', encoding='utf-8') as file:
			matrix = file.read().split("\n")
			for row in matrix:
				if '1' in summary and row.startswith('#'): 
					content.append(str(row))
				elif '1' in commentary and row.startswith(';'):
					content.append(str(row))
				elif not row.startswith('#') and not row.startswith(';'): 
					content.append(str(row))

		
		if not os.path.exists(path_file_out): os.makedirs(path_file_out)
		
		with open(os.path.join(path_file_out, f"mirror_{name_new_config}.{ext}"), 'w', encoding='utf-8') as file: [file.write(f"{str(row)} \n") for row in content]
	except:
		lf.log_file(name="log_error",header="Backup Config File",datetime=datetime.datetime.today())
		