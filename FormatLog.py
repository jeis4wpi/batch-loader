import inspect
import gc
from datetime import datetime
from filelock import FileLock

#decorator functions 
def format_arguments(func):
	""" formate many args into one string, then calls decorated function with string formatted together
	I.E. func("I love ",johnny.fullname,"because he is",100,"years old",**kwargs)
		might look like func("I love Jonnathan because he is 100 years old",**kwargs)
	"""

	from functools import wraps
	@wraps(func)
	def format_args_and_call(self,*args,**kwargs):
		# from pudb import set_trace;set_trace()

		args = list(args)
		if args:
			first = args[0]
			args = args[1:]
			formatter = ""
			for arg in args:
				formatter += ' ' + str(arg)
			first = str(first) + formatter
		else:
			first = ""
		return func(self,first,**kwargs)
	return format_args_and_call

def get_context_wrapper(function_to_call):
	"""
	passes in the context in which the wrapped function was called 
	"""
	from functools import wraps
	@wraps(function_to_call)
	def context_call(self,*args,**kwargs):
		# code from https://stackoverflow.com/a/4506081/4199504 (and edited by myself)
		args = list(args)
		frame = inspect.currentframe().f_back.f_back
		code  = frame.f_code
		globs = frame.f_globals
		functype = type(lambda: 0)
		funcs = []
		for func in gc.get_referrers(code):
			if type(func) is functype:
				if getattr(func, "__code__", None) is code:
					if getattr(func, "__globals__", None) is globs:
						funcs.append(func)
						if len(funcs) > 1:
							break
		
		args.append(str(funcs[0])[10:-16] if funcs and len(funcs) == 1 else "")
		return function_to_call(self,*args,**kwargs)
	return context_call

class FormatLogger():
	"""
	singleton, in order to make everything log to the same files in the right order with out passing in the log class 
	"""
	_instance=None
	def __new__(cls):
		if cls._instance is None:
			cls._instance = object.__new__(cls)
			FormatLogger._instance.logfile	 	 = None 
			FormatLogger._instance.failure_file	 = None
			FormatLogger._instance.time_file	 = None #not part of normal log
			FormatLogger._instance.total_time_file = None #not part of normal log
			FormatLogger._instance.proccess_status = None
			FormatLogger._instance.truncate	 	 = None 
			FormatLogger._instance.files		 = []
			FormatLogger._instance.prints	 	 = 0 
			FormatLogger._instance.num_success	 = 0 
			FormatLogger._instance.num_fail		 = 0
			FormatLogger._instance.current_id	 = None
		return cls._instance
	def __init__(self):
		self.truncate = self._instance.truncate
		self.logfile = self._instance.logfile
		self.failure_file = self._instance.failure_file
		self.proccess_status = self._instance.proccess_status
		self.time_file = self._instance.time_file
		self.total_time_file = self._instance.total_time_file
		self.files = self._instance.files
		self.prints = self._instance.prints
		self.num_success = self._instance.num_success
		self.num_fail = self._instance.num_fail
		self.current_id = self._instance.current_id


	def init(self,logfile = None,failure_file = None,proccess_status = None,time_file = None,truncate = False,prints = 1):
		self.truncate = self._instance.truncatex = truncate
		self.logfile = self._instance.logfile = logfile
		self.failure_file = self._instance.failure_file = failure_file
		self.proccess_status = self._instance.proccess_status = proccess_status
		self.time_file = self._instance.time_file = time_file
		if time_file:
			self.total_time_file = self._instance.total_time_file = f"total_{str(self.time_file)}"
		else:
			self.total_time_file = self._instance.total_time_file
		self.files = self._instance.files = [self.proccess_status,self.logfile,self.failure_file,self.time_file,self.total_time_file]
		self.prints = self._instance.prints = prints
		self.num_success = self._instance.num_success = 0
		self.num_fail = self._instance.num_fail = 0
		self.current_id = self._instance.current_id
		# from pudb import set_trace;set_trace()
		if truncate:
			for file in self.files:
				truncate_file(file)

	def set_print_level(self,n):
		self.prints = self._instance.prints = n
	def set_current_id(self,value):
		self.current_id = self._instance.current_id = value

	@format_arguments
	def output(self,desc,level = 1):
		if level >=3:
			level = 3
		for n,file in enumerate(self.files):
			if n == level:
				break
			write_line_to_file(file,desc)
		if self.prints <= 1:
			print(desc)

	@format_arguments
	def write(self,desc,level = 1):
		if level >=3:
			level = 3
		for n,file in enumerate(self.files):
			if n == level:
				break
			write_line_to_file(file,desc)
	 
	@format_arguments
	@get_context_wrapper
	def status(self,desc,cont):
		if cont:
			string = "Status: in {} - {}".format(cont,desc)
		else:
			string = "Status: {}".format(desc)
		if self.prints <=1:
			print(string)
		for file in [self.proccess_status]:
			write_line_to_file(file,string)
	#alias
	info = status

	@format_arguments
	@get_context_wrapper
	def warning(self,desc,cont,context = False):
		if context and cont:
			string = "Warning: in {} - {}".format(cont,desc)
		else:
			string = "Warning: {} ".format(desc)
		if self.prints<=2:
			print(string)
		for file in [self.logfile,self.proccess_status]:
			write_line_to_file(file,string)
	
	@format_arguments
	def error(self,desc):
		if self.prints <=2:
			print(desc)
		for file in [self.proccess_status]:
			write_line_to_file(file,desc)
	
	@format_arguments
	@get_context_wrapper
	def critical(self,desc,cont):# pylint: disable=E1120
		string = "\n-- CRITIICAL FAILURE in {} --:{}\n".format(cont if cont else "Main Scope?",desc)
		print(string)
		for file in self.files:
			write_line_to_file(file,string)
	@format_arguments
	def success(self,desc):
		self.num_success += 1
		if self.prints <=3:
			print("SUCCESS: " + desc)
		for file in [self.logfile,self.proccess_status]:
			write_line_to_file(file,"SUCCESS: " + desc)
	
	@format_arguments
	def failure(self,desc):
		self.num_fail += 1
		if self.prints <=3:
			print("FAILURE: " + desc)
		for file in self.files:
			write_line_to_file(file,"FAILURE: " + desc)

	def total_time_stamp(self,start,end):
		write_line_to_file(self.total_time_file,str(self.current_id) + '{|-|}' + str(end-start))
	def time_stamp(self,start,end):
		write_line_to_file(self.time_file,str(self.current_id) + '{|-|}' + str(end-start))

	def close(self):
		suc = "Succeded on {} out of {} total".format(self.num_success,self.num_fail+self.num_success)
		if self.prints <=3:
			print(suc)
		write_line_to_file(self.failure_file,"Failed {} out of {} total".format(self.num_fail,self.num_fail+self.num_success))
		write_line_to_file(self.proccess_status,"\n"+suc)
		write_line_to_file(self.logfile,suc)
		for fn in self.files:
			close_up(fn)


def write_line_to_file(file,line=None):
	if not file:
		return
	if line is None:
		line = ''
	# with FileLock(file):
	with open(file,'a') as logfile:
		logfile.write(str(line)+'\n')

def truncate_file(path):
	# with FileLock(path):
	if not path:
		return
	with open(path,'w') as logfile:
		logfile.write('---- logging on {} ----\n'.format(datetime.now()))

def close_up(path):
	# with FileLock(path):
	if not path:
		return
	with open(path,'a') as logfile:
		logfile.write('\n---- end of logging session {} ----\n\n'.format(datetime.now()))
	pass

def get_context():
	stack = inspect.stack()
	if len(stack) <= 2:
		return "main scope - "+str(stack[1].code_context[0])
	if len(stack) == 3:
		return "- Main Scope"
	elif len(stack) > 3:
		return stack[3].code_context[0]



