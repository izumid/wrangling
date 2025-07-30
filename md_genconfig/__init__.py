import os
import configparser

def generate_config_file(path_file_in,path_file_out,name_config,name_new_config,summary,commentary):
	content=[]
	ext = "ini"

	with open(os.path.join(path_file_in, name_config), 'r', encoding='utf-8') as file:
		matrix = file.read().split("\n")
		for row in matrix:
			if '1' in summary and row.startswith('#'): 
				content.append(str(row))
			elif '1' in commentary and row.startswith(';'):
				content.append(str(row))
			elif not row.startswith('#') and not row.startswith(';'): 
				content.append(str(row))

	print(path_file_out)

	with open(os.path.join(path_file_out, f"mirror_{name_new_config}.{ext}"), 'w', encoding='utf-8') as file: [file.write(f"{str(row)} \n") for row in content]
		
	if 1 == 0:
		if '1' in summary:
			with open(os.path.join(path_file_in, name_config+ext), 'r', encoding='utf-8') as file:
				matrix = file.read().split("\n")
				for row in matrix:
					if row.endswith(';'): content.append(str(row))
					if not row.startswith(';'): content.append(str(row))

			with open(os.path.join(path_file_out, name_new_config+ext), 'w', encoding='utf-8') as file:
				for row in content:
					file.write(f"{str(row)} \n")
		else:
			config = configparser.RawConfigParser()
			config.read(os.path.join(path_file_in, name_config+ext), encoding="utf-8")

			with open(os.path.join(path_file_out, name_new_config+ext), 'w', encoding='utf-8') as file: 
				for section in config.sections():
					file.write(f"[{section}]\n")
					print(f"[{section}]\n")
					for option in config[section]:
						file.write(f"{option} = {config[section][option]}\n")
						print(f"{option} = {config[section][option]}\n")
					file.write("\n")