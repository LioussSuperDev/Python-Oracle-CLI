import json
import os
from typing import Optional
from liouss_python_toolkit.printer import beautiful_print, GREEN_COLOR, ORANGE_COLOR, RED_COLOR, LIGHT_BLUE_COLOR
from liouss_python_toolkit.utility import edit_in_editor
from liouss_python_toolkit.utility import real_path
from liouss_python_sql_connectors.sql_connection import SQLConnection
from liouss_python_sql_connectors import utils
import cmd
import traceback
from concurrent.futures import ThreadPoolExecutor
import datetime
import csv
import sqlparse
from contextlib import nullcontext
import shlex
from prompt_toolkit import prompt
from prompt_toolkit.history import InMemoryHistory

def generateConnection(connection_type, identifiers) -> Optional[SQLConnection]:
    return utils.generateConnection(connection_type, identifiers)

def get_oracle_connection_identifiers(connection:SQLConnection):
    identifiers = connection.query_one("""
        SELECT s.sid, s.serial#
        FROM v$session s
        WHERE s.audsid = SYS_CONTEXT('USERENV','SESSIONID')
    """)
    return identifiers[0] if identifiers else None
    
    
class OracleCmd(cmd.Cmd):
    prompt = "Oracle Prompt> "
    
    def emptyline(self):
        return
    
    def __init__(self, oracle_identifiers, connection:SQLConnection, connection_type, output_folder, pool, completekey = "tab", stdin = None, stdout = None) -> None:
        super().__init__(completekey, stdin, stdout)
        self.oracle_identifiers = oracle_identifiers
        self.connection = connection
        self.connection_type = connection_type
        self.output_folder = output_folder
        self.pool = pool
        self.tasks = dict()
        self._history = InMemoryHistory()

    def cmdloop(self, intro=None):
        if intro is not None:
            print(intro)
        stop = None
        while not stop:
            try:
                line = prompt(self.prompt, history=self._history)
            except (EOFError, KeyboardInterrupt):
                print()
                break
            line = self.precmd(line)
            stop = self.onecmd(line)
            stop = self.postcmd(stop, line)

    def runscript_oracle(self, identifiers, script, task_id=None):
        queries = [s.strip() for s in sqlparse.split(script) if s.strip()]
        for i, query in enumerate(queries):
            self.query_oracle(identifiers, query.strip(";\n\r "), False, task_id=task_id, sub_task_id=i, default_connection=self.connection)
    
    def query_oracle(self, identifiers, query, commit, task_id=None, sync=False, sub_task_id=0, default_connection=None, placeholders=None):
        
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
                
                self.tasks[task_id]["connection"] = connection
                if connection:
                    self.tasks[task_id]["SID"],self.tasks[task_id]["SERIAL"] = get_oracle_connection_identifiers(connection) or ("NULL","NULL")
                                   
                os.makedirs(os.path.join(self.output_folder, "queries", str(task_id)), exist_ok=True)
                output_file = os.path.join(self.output_folder, "queries", str(task_id), f"{sub_task_id}.output.csv")
                log_file = os.path.join(self.output_folder, "queries", str(task_id), f"{sub_task_id}.log.txt")
                
                beautiful_print(f"Executing query:\n{query}", log_only=True, log=log_file)
                
                try:
                    if not placeholders:
                        result = connection.query_one(query.strip("\n\r"), print_error=False, ignore_errors=False, include_col_name=True)
                    else:
                        result = connection.query_one(query.strip("\n\r"), placeholders, print_error=False, ignore_errors=False, include_col_name=True)
                    beautiful_print(f"Query complete.", log_only=(not sync), log=log_file)
                except Exception as e:
                    beautiful_print(f"Error executing query: {query}", log_only=(not sync), log=log_file, color=RED_COLOR)
                    stack = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                    beautiful_print(stack, log_only=(not sync), log=log_file, color=RED_COLOR)
                    return
                
                if commit:
                    connection.get_db().commit()
                    
                with open(output_file, "w+", newline="") as f:
                    csv.writer(f).writerows(result or [])
                if sync:
                    with open(output_file, "r",) as f:
                        readlines = f.readlines(1000)
                        if len(readlines) > 0:
                            beautiful_print("")
                            for line in readlines:
                                beautiful_print(line.strip(" \n\r"), color=LIGHT_BLUE_COLOR)
                            beautiful_print("")
                
        except Exception as e:
            beautiful_print(f"Error executing query: {task_id}", log_only=(not sync), log=log_file, color=RED_COLOR)
            stack = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            beautiful_print(stack, log_only=(not sync), log=log_file, color=RED_COLOR)

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
                
                self.tasks[task_id]["connection"] = connection
                if connection:
                    self.tasks[task_id]["SID"],self.tasks[task_id]["SERIAL"] = get_oracle_connection_identifiers(connection) or ("NULL","NULL")
                
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
                    beautiful_print(f"Inserting file: {file_path}", log_only=True, log=log_file, color=RED_COLOR)
                    stack = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                    beautiful_print(stack, log_only=True, log=log_file, color=RED_COLOR)
                    return
                
                
                connection.get_db().commit()
        except Exception as e:
            beautiful_print(f"Error executing query: {task_id}", log_only=True, log=log_file, color=RED_COLOR)
            stack = "".join(traceback.format_exception(type(e), e, e.__traceback__))
            beautiful_print(stack, log_only=True, log=log_file, color=RED_COLOR)

    def start_task(self, description, func, sync, *args, **kwargs):
        
        if not sync:
            task_id = f"async_{datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')}"
            self.tasks[task_id] = {"description": description, "process":self.pool.submit(func, *args, task_id=task_id, sync=sync, **kwargs), "connection":None}
            beautiful_print(f"Started task {task_id}: {description}")
        else:
            task_id = f"sync_{datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')}"
            beautiful_print(f"Starting task {task_id}: {description}")
            self.tasks[task_id] = {"description": description, "process":None, "connection":self.connection}
            func(*args, task_id=task_id, sync=sync, **kwargs)
        
        return task_id
    
    def do_query(self, arg):
        """Execute a SQL query on the Oracle database.
        Usage: query <SQL_QUERY>"""
        if arg.strip() == "":
            arg = edit_in_editor("Type your query on the line below", ignore_lines=1)
        self.start_task(f"query {arg}", self.query_oracle, False, self.oracle_identifiers, arg, False)
    
    def do_querys(self, arg):
        """Execute a SQL query on the Oracle database synchronously.
        Usage: querys <SQL_QUERY>"""
        if arg.strip() == "":
            arg = edit_in_editor("Type your query on the line below", ignore_lines=1)
        self.start_task(f"querys {arg}", self.query_oracle, True, self.oracle_identifiers, arg, False)
    
    def do_queryc(self, arg):
        """Execute a SQL query on the Oracle database. Commits after execution
        Usage: queryc <SQL_QUERY>"""
        if arg.strip() == "":
            arg = edit_in_editor("Type your query on the line below", ignore_lines=1)
        self.start_task(f"queryc {arg}", self.query_oracle, False, self.oracle_identifiers, arg, True)
        
    def do_taskls(self, arg):
        """List all running tasks.
        Usage: taskls [-i]"""
        for task_id, task_info in self.tasks.items():
            status = "Running" if task_info["process"] and (not task_info["process"].done()) else "Done"
            color = GREEN_COLOR if status == "Done" else ORANGE_COLOR
            if arg != "-i" or status == "Running":
                sid,serial = self.tasks[task_id]["SID"],self.tasks[task_id]["SERIAL"]
                beautiful_print(f"[{task_id}][{sid},{serial}]: ({status}) {task_info['description']}", color=color)
    
    def do_stoptsk(self, arg):
        """Stop a running task.
        Usage: stoptsk <TASK_ID>"""
        
        if not arg in self.tasks:
            beautiful_print(f"No task found with ID: {arg}", color=RED_COLOR)
            return
        
        if not self.tasks[arg]["process"]:
            beautiful_print(f"Cannot stop sync task", color=RED_COLOR)
            return

        self.tasks[arg]["process"].cancel()
        beautiful_print(f"Stopped task {arg}: {self.tasks[arg]['description']}")

            
    
    def do_insertmany(self, arg):
        """Inserts data in database from a file.
        Usage: insertmany <FILE_PATH> <TABLE>
        """
        arg2 = arg
        arg = shlex.split(arg)
        if len(arg) != 2:
            beautiful_print(f"Expected args: 2. Received: {len(arg)}.", color=RED_COLOR)
        self.start_task(f"insertmany {arg2}", self.insert_many, False, self.oracle_identifiers, real_path(arg[0]), arg[1], 50000)
        
    def do_runscript(self, arg):
        """Run a SQL script from a file.
        Usage: runscript <FILE_PATH>"""
        
        if arg and arg.strip() != "":
            arg = real_path(arg)
            if not os.path.isfile(arg):
                beautiful_print(f"File not found: {arg}", color=RED_COLOR)
                return
            with open(arg, "r") as f:
                script = f.read()
        else:
            script = edit_in_editor("Type your script on the line below", ignore_lines=1)
            
        self.start_task(f"runscript {arg}", self.runscript_oracle, False, self.oracle_identifiers, script)
        
    def do_exit(self, arg):
        """Exit the Oracle prompt."""
        for task_id in list(self.tasks.keys()):
            if self.tasks[task_id]["process"] and (not self.tasks[task_id]["process"].done()):
                self.do_stoptsk(task_id)
        return True
    
    def do_savecmd(self, arg):
        """Save a new user command or overwrites existing
        Usage: savecmd <CMD_NAME>"""
        
        if arg.strip() == "":
            beautiful_print("Please specify the command name", color=RED_COLOR)
        
        os.makedirs(os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "commands"
        ), exist_ok=True)   
        path_to_save_command = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "commands",
            f"{arg}.sql"
        )
        with open(path_to_save_command, "w+") as f:
            f.write(edit_in_editor("Type your query on the line below", ignore_lines=1))
        
    def do_listcmd(self, arg):
        """Lists existing user commands"""
        os.makedirs(os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "commands"
        ), exist_ok=True)   
        path_to_list_command = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "commands"
        )
        beautiful_print("List of existing commands:")
        for s in os.listdir(path_to_list_command):
            beautiful_print(s[:-4])
            
    def do_aruncmd(self, arg):
        """Runs a user command
        USAGE: runcmd <CMD_NAME> [args...]"""
        args = shlex.split(arg)
        if not args:
            beautiful_print("Please specify the command name", color=RED_COLOR)
        
        os.makedirs(os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "commands"
        ), exist_ok=True)   
        path_to_load_command = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "commands",
            f"{args[0]}.sql"
        )
        if not os.path.exists(path_to_load_command):
            beautiful_print("Unknown command. listcmd to get the list of available commands", color=RED_COLOR)
            return
        with open(path_to_load_command,"r") as f:
            command = f.read()
            self.start_task(f"aruncmd {arg}", self.query_oracle, False, self.oracle_identifiers, command, False, placeholders=tuple(args[1:]) if len(args) > 1 else None)
    
    def do_runcmd(self, arg):
        """Runs a user command
        USAGE: runcmd <CMD_NAME> [args...]"""
        args = shlex.split(arg)
        if not args:
            beautiful_print("Please specify the command name", color=RED_COLOR)
        
        os.makedirs(os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "commands"
        ), exist_ok=True)   
        path_to_load_command = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "commands",
            f"{args[0]}.sql"
        )
        if not os.path.exists(path_to_load_command):
            beautiful_print("Unknown command. listcmd to get the list of available commands", color=RED_COLOR)
            return
        with open(path_to_load_command,"r") as f:
            command = f.read()
            self.start_task(f"runcmd {arg}", self.query_oracle, True, self.oracle_identifiers, command, False, placeholders=tuple(args[1:]) if len(args) > 1 else None)
        
        
def main():
    beautiful_print("~~~----~~~")
    beautiful_print("Oracle CLI V0.1.2")
    beautiful_print("Author: Liouss")
    beautiful_print("~~~----~~~")
    
    CONNECTION_TYPES = "oracle"
    
    ORACLE_ID_LOCATION = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "ORACLE_IDENTIFIER.json"
    )
    ORACLE_ID_LOCATION_EXMP = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "ORACLE_IDENTIFIER.example.json"
    )
    CONFIG = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "config.json"
    )
    
    no_identifiers = False
    try:
        with open(ORACLE_ID_LOCATION, "r") as f:
            oracle_identifiers = json.load(f)
    except Exception as e:
        no_identifiers = True
    while no_identifiers:
        with open(ORACLE_ID_LOCATION_EXMP, "r") as f:
            edit_in_editor(f.read(), path=ORACLE_ID_LOCATION)
        try:
            with open(ORACLE_ID_LOCATION, "r") as f:
                oracle_identifiers = json.load(f)
                no_identifiers = False
        except Exception as e:
            no_identifiers = True
    
        
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