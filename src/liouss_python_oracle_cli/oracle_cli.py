import json
import os
from liouss_python_toolkit.printer import beautiful_print
from liouss_python_sql_connectors.oracle_connection import OracleConnection
import cmd
import traceback
from concurrent.futures import ThreadPoolExecutor
import datetime
import csv
import sqlparse
from contextlib import nullcontext
from pathlib import Path

def real_path(path_str: str) -> str:
    return str(
        Path(path_str.strip('"').strip("'"))
        .expanduser()
        .resolve(strict=False)
    )



class OracleCmd(cmd.Cmd):
    prompt = "Oracle Prompt> "
    
    def __init__(self, oracle_identifiers, connection, output_folder, pool, completekey = "tab", stdin = None, stdout = None) -> None:
        super().__init__(completekey, stdin, stdout)
        self.oracle_identifiers = oracle_identifiers
        self.connection = connection
        self.output_folder = output_folder
        self.pool = pool
        self.tasks = dict()

    def runscript_oracle(self, identifiers, script, task_id=None):
        queries = [s.strip() for s in sqlparse.split(script) if s.strip()]
        for i, query in enumerate(queries):
            self.query_oracle(identifiers, query.strip(";\n\r "), False, task_id=task_id, sub_task_id=i)
    
    def query_oracle(self, identifiers, query, commit, task_id=None, sub_task_id=0, default_connection=None):
        
        if task_id is None:
            task_id = "NOT_A_TASK"
        try:
            if default_connection is not None:
                connection = default_connection
            else:
                connection = OracleConnection(identifiers["username"],
                                        identifiers["password"],
                                        identifiers["hostname"],
                                        identifiers["service_name"],
                                        logs = False)
            
            with connection if not default_connection else nullcontext():                    
                os.makedirs(os.path.join(self.output_folder, "logs", str(task_id)), exist_ok=True)
                os.makedirs(os.path.join(self.output_folder, "queries", str(task_id)), exist_ok=True)
                output_file = os.path.join(self.output_folder, "queries", str(task_id), f"output_{sub_task_id}.csv")
                log_file = os.path.join(self.output_folder, "logs", str(task_id), f"log_{sub_task_id}.txt")
                
                try:
                    result = connection.query_one(query, print_error=False, ignore_errors=False, include_col_name=True)
                except Exception as e:
                    beautiful_print(f"Error executing query: {query}", log_only=True, log=log_file)
                    stack = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                    beautiful_print(stack, log_only=True, log=log_file)
                    return
                
                if commit:
                    connection.get_db().commit()
                with open(output_file, "w+", newline="") as f:
                    csv.writer(f).writerows(result or [])
                
        except Exception as e:
            beautiful_print(f"Error executing query: {task_id}", log_only=True, log=log_file)
            stack = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            beautiful_print(stack, log_only=True, log=log_file)


    def start_task(self, description, func, *args, **kwargs):
        task_id = f"async_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
        self.tasks[task_id] = {"description": description, "process":self.pool.submit(func, *args, task_id=task_id, **kwargs)}
        beautiful_print(f"Started task {task_id}: {description}")
        return task_id
    
    def do_query(self, arg):
        """Execute a SQL query on the Oracle database.
        Usage: query <SQL_QUERY>"""
        self.start_task(f"query {arg}", self.query_oracle, self.oracle_identifiers, arg, False)
    
    def do_queryc(self, arg):
        """Execute a SQL query on the Oracle database. Commits after execution
        Usage: queryc <SQL_QUERY>"""
        self.start_task(f"queryc {arg}", self.query_oracle, self.oracle_identifiers, arg, True)
        
    def do_tasklst(self, arg):
        """List all running tasks.
        Usage: tasklst [-i]"""
        for task_id, task_info in self.tasks.items():
            status = "R" if not task_info["process"].done() else "C"
            if arg != "-i" or status == "R":
                beautiful_print(f"[{task_id}][{status}] : {task_info['description']}")
    
    def do_stoptsk(self, arg):
        """Stop a running task.
        Usage: stoptsk <TASK_ID>"""
        if arg in self.tasks:
            self.tasks[arg]["process"].cancel()
            beautiful_print(f"[Oracle] Stopped task {arg}: {self.tasks[arg]['description']}")
        else:
            beautiful_print(f"No task found with ID: {arg}")
    
    
    def do_querysync(self, arg):
        """Execute a SQL query on the Oracle database synchronously.
        Usage: querysync <SQL_QUERY>"""
        task_id = f"sync_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
        beautiful_print(f"Computing query {task_id}: {arg}")
        self.query_oracle(self.oracle_identifiers, arg, False, task_id=task_id, default_connection=self.connection)
    
    def do_runscript(self, arg):
        """Run a SQL script from a file.
        Usage: runscript <FILE_PATH>"""
        arg = real_path(arg)
        if not os.path.isfile(arg):
            beautiful_print(f"File not found: {arg}")
            return
        with open(arg, "r") as f:
            script = f.read()
        self.start_task(f"runscript {arg}", self.runscript_oracle, self.oracle_identifiers, script)
        
    def do_exit(self, arg):
        """Exit the Oracle prompt."""
        for task_id in list(self.tasks.keys()):
            if not self.tasks[task_id]["process"].done():
                self.do_stoptsk(task_id)
        return True


def main():
    beautiful_print("~~~----~~~")
    beautiful_print("Oracle CLI V1.0")
    beautiful_print("Author: Liouss")
    beautiful_print("~~~----~~~")
    
    ORACLE_ID_LOCATION = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "ORACLE_IDENTIFIER.json"
    )
    CONFIG = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "config.json"
    )
    with open(ORACLE_ID_LOCATION, "r") as f:
        oracle_identifiers = json.load(f)
    with open(CONFIG, "r") as f:
        output_folder = real_path(json.load(f)["output_directory"])
        os.makedirs(output_folder, exist_ok=True)
    cli = None
    try:
        with ThreadPoolExecutor() as pool:
            connection = OracleConnection(
                        oracle_identifiers["username"],
                        oracle_identifiers["password"],
                        oracle_identifiers["hostname"],
                        oracle_identifiers["service_name"],
                        logs = False)
            cli = OracleCmd(oracle_identifiers, connection, output_folder, pool)
            cli.cmdloop()
            
    finally:
        if cli is not None:
            beautiful_print("Stopping all tasks and exiting")
            cli.do_exit("")
            
        beautiful_print("Oracle prompter stopped")