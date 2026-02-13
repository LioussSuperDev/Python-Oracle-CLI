import json
import os
from typing import Optional
from liouss_python_toolkit.printer import beautiful_print
from liouss_python_sql_connectors.sql_connection import SQLConnection
from liouss_python_sql_connectors import utils
import cmd
import traceback
from concurrent.futures import ThreadPoolExecutor
import datetime
import csv
import sqlparse
from contextlib import nullcontext
from pathlib import Path
import shlex
import subprocess
import tempfile

def real_path(path_str: str) -> str:
    return str(
        Path(path_str.strip('"').strip("'"))
        .expanduser()
        .resolve(strict=False)
    )

def generateConnection(connection_type, identifiers) -> Optional[SQLConnection]:
    return utils.generateConnection(connection_type, identifiers)

def edit_in_editor(initial_text: str = "", suffix: str = ".txt", ignore_lines=0) -> str:
    editor = os.environ.get("EDITOR") or os.environ.get("VISUAL") or "nano"

    with tempfile.NamedTemporaryFile(mode="w+", suffix=suffix, delete=False, encoding="utf-8") as f:
        path = Path(f.name)
        f.write(initial_text)
        f.flush()

    try:
        cmd = shlex.split(editor) + [str(path)]
        subprocess.run(cmd, check=False)
        return "\n".join(path.read_text(encoding="utf-8").splitlines()[ignore_lines:])
    finally:
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass
        
class OracleCmd(cmd.Cmd):
    prompt = "Oracle Prompt> "
    
    def __init__(self, oracle_identifiers, connection:SQLConnection, connection_type, output_folder, pool, completekey = "tab", stdin = None, stdout = None) -> None:
        super().__init__(completekey, stdin, stdout)
        self.oracle_identifiers = oracle_identifiers
        self.connection = connection
        self.connection_type = connection_type
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
                connection = generateConnection(self.connection_type, identifiers)
                if connection is None:
                    return
            
            with connection if not default_connection else nullcontext():                    
                os.makedirs(os.path.join(self.output_folder, "queries", str(task_id)), exist_ok=True)
                output_file = os.path.join(self.output_folder, "queries", str(task_id), f"{sub_task_id}.output.csv")
                log_file = os.path.join(self.output_folder, "queries", str(task_id), f"{sub_task_id}.log.txt")
                
                beautiful_print(f"Executing query:\n{query}", log_only=True, log=log_file)
                
                try:
                    result = connection.query_one(query.strip("; \n\r"), print_error=False, ignore_errors=False, include_col_name=True)
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

    def insert_many(self, identifiers, file_path, table_name, buffer_size, task_id=None, sub_task_id=0, default_connection=None):
        if task_id is None:
            task_id = "NOT_A_TASK"
        try:
            if default_connection is not None:
                connection = default_connection
            else:
                connection = generateConnection(self.connection_type, identifiers)
                if connection is None:
                    return
            
            with connection if not default_connection else nullcontext():                    
                os.makedirs(os.path.join(self.output_folder, "queries", str(task_id)), exist_ok=True)
                log_file = os.path.join(self.output_folder, "queries", str(task_id), f"{sub_task_id}.log.txt")
                
                beautiful_print(f"Inserting file:\n{file_path}", log_only=True, log=log_file)
                
                try:
                    with open(file_path, newline="", encoding="utf-8") as f:
                        reader = csv.reader(f, delimiter=",", quotechar='"', escapechar="\\")
                        header = next(reader)

                        cols = " , ".join(header)
                        binds = " , ".join(f":{i}" for i in range(1, len(header) + 1))
                        sql = f"INSERT INTO {table_name} ({cols}) VALUES ({binds})"

                        batch = []
                        for i,row in enumerate(reader):
                            row = [None if v == "" else v for v in row]

                            batch.append(row)
                            if len(batch) >= buffer_size:
                                connection.query_many(sql, batch, ignore_errors=False, print_error=False)
                                batch.clear()
                                beautiful_print(f"Inserted {i+1} lines", log=log_file, log_only=True)

                        if batch:
                            connection.query_many(sql, batch, ignore_errors=False, print_error=False)
                            beautiful_print(f"Inserted {i+1} lines", log=log_file, log_only=True)
                            
                        connection.query_one("commit")
                        beautiful_print(f"Inserted {i+1} lines", log=log_file, log_only=True)
                        beautiful_print(f"Commit.", log=log_file, log_only=True)
                            
                except Exception as e:
                    beautiful_print(f"Inserting file: {file_path}", log_only=True, log=log_file)
                    stack = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                    beautiful_print(stack, log_only=True, log=log_file)
                    return
                
                
                connection.get_db().commit()
        except Exception as e:
            beautiful_print(f"Error executing query: {task_id}", log_only=True, log=log_file)
            stack = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            beautiful_print(stack, log_only=True, log=log_file)

    def start_task(self, description, func, *args, **kwargs):
        task_id = f"async_{datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')}"
        self.tasks[task_id] = {"description": description, "process":self.pool.submit(func, *args, task_id=task_id, **kwargs)}
        beautiful_print(f"Started task {task_id}: {description}")
        return task_id
    
    def do_query(self, arg):
        """Execute a SQL query on the Oracle database.
        Usage: query <SQL_QUERY>"""
        if arg.strip() == "":
            arg = edit_in_editor("Type your query on the line below", ignore_lines=1)
        self.start_task(f"query {arg}", self.query_oracle, self.oracle_identifiers, arg, False)
    
    def do_queryc(self, arg):
        """Execute a SQL query on the Oracle database. Commits after execution
        Usage: queryc <SQL_QUERY>"""
        if arg.strip() == "":
            arg = edit_in_editor("Type your query on the line below", ignore_lines=1)
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
        if arg.strip() == "":
            arg = edit_in_editor("Type your query on the line below", ignore_lines=1)
        task_id = f"sync_{datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')}"
        beautiful_print(f"Computing query {task_id}: {arg}")
        self.query_oracle(self.oracle_identifiers, arg, False, task_id=task_id, default_connection=self.connection)
    
    def do_insertmany(self, arg):
        """Inserts data in database from a file.
        Usage: insertmany <FILE_PATH> <TABLE>
        """
        arg2 = arg
        arg = shlex.split(arg)
        if len(arg) != 2:
            beautiful_print(f"Expected args: 2. Received: {len(arg)}.")
        self.start_task(f"insertmany {arg2}", self.insert_many, self.oracle_identifiers, real_path(arg[0]), arg[1], 50000)
        
    def do_runscript(self, arg):
        """Run a SQL script from a file.
        Usage: runscript <FILE_PATH>"""
        
        if arg and arg.strip() != "":
            arg = real_path(arg)
            if not os.path.isfile(arg):
                beautiful_print(f"File not found: {arg}")
                return
            with open(arg, "r") as f:
                script = f.read()
        else:
            script = edit_in_editor("Type your script on the line below", ignore_lines=1)
            
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
    
    CONNECTION_TYPES = "oracle"
    
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
            connection = generateConnection(CONNECTION_TYPES, oracle_identifiers)
            if not connection:
                exit(1)
                
            with connection:
                cli = OracleCmd(oracle_identifiers, connection, CONNECTION_TYPES, output_folder, pool)
                cli.cmdloop()
            
    finally:
        if cli is not None:
            beautiful_print("Stopping all tasks and exiting")
            cli.do_exit("")
            
        beautiful_print("Oracle prompter stopped")