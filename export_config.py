import os
import configparser

def generate_config_file(path_root,name_config, name_new_config,header=None):

    ls=[]

    if header is not None:
        with open(os.path.join(path_root, name_config+".ini"), 'r', encoding='utf-8') as file:
            matrix = file.read().split("\n")
            for row in matrix:
                if row.endswith(';'): ls.append(str(row))
                if not row.startswith(';'): ls.append(str(row))

        with open(os.path.join(path_root, name_new_config+".ini"), 'w', encoding='utf-8') as file:
            for row in ls:
                file.write(f"{str(row)} \n")
    else:
        config = configparser.RawConfigParser()
        config.read(path_root, name_config, encoding="utf-8")

        with open(os.path.join(path_root, name_new_config+".ini"), 'w', encoding='utf-8') as file: 
            for section in config.sections():
                file.write(f"[{section}]\n")
                for option in config[section]:
                    file.write(f"{option} = {config[section][option]}\n")
                file.write("\n")