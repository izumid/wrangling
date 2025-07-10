import os
import configparser

def generate(path_in,path_out,name,name_new,summary=""):

    ls=[]
    ext = ".ini"
    suffix = "_config"
    if not os.path.exists(os.path.join(path_out)): os.makedirs(os.path.join(path_out))

    if summary:
        with open(os.path.join(path_in, name+ext), 'r', encoding='utf-8') as file:
            matrix = file.read().split("\n")
            for row in matrix:
                if row.endswith(';'): ls.append(str(row))
                if not row.startswith(';'): ls.append(str(row))

        with open(os.path.join(path_out, name_new+suffix+ext), 'w', encoding='utf-8') as file:
            for row in ls:
                file.write(f"{str(row)} \n")
    else:
        config = configparser.RawConfigParser()
        config.read(os.path.join(path_in, name+ext), encoding="utf-8")

        with open(os.path.join(path_out, name_new+suffix+ext), 'w', encoding='utf-8') as file: 
            for section in config.sections():
                file.write(f"[{section}]\n")
                print(f"[{section}]\n")
                for option in config[section]:
                    file.write(f"{option} = {config[section][option]}\n")
                    print(f"{option} = {config[section][option]}\n")
                file.write("\n")