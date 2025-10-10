import os
import traceback


def log_file(name,header,datetime,message=None):
	name = name + ".txt"
	header=f"=--=-=-= {header}: {datetime};  =-=-=-=-=\n\n"
	if not os.path.exists(os.path.join(os.getcwd(),name)): open(name, "x")
	if message: message = f"\n{message}\r\n"
	if os.stat(name).st_size < 1:
		with open(name, 'a+') as f: 
			f.write(header)
			if not message: traceback.print_exc(file=f)
			else: f.write(message)
			f.write(f"{"="*50}\n\n")
			f.write("\n\n")
	else:
		with open(name, 'w+') as f: 
			f.write(header)
			if not message: traceback.print_exc(file=f)
			else: f.write(message)
			f.write(f"{"="*50}\n\n")
			f.write("\n\n")
