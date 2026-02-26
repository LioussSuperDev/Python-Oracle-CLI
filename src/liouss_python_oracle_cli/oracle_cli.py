import json
import os
from typing import Optional
from liouss_python_toolkit.printer import beautiful_print, GREEN_COLOR, ORANGE_COLOR, RED_COLOR, LIGHT_BLUE_COLOR, RESET_COLOR
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
from pathlib import Path
import shutil

CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "config.json"
)
DEFAULT_WORKSPACE = "default"
DEFAULT_WORKSPACE_PATH = "~/oraclecli_default_workspace"

def copy_dir(path_a: str, path_b: str) -> None:
    src = Path(path_a)
    dst = Path(path_b)

    if not src.is_dir():
        raise NotADirectoryError(f"Source introuvable ou pas un dossier: {src}")

    dst.mkdir(parents=True, exist_ok=True)
    shutil.copytree(src, dst, dirs_exist_ok=True)

def generateConnection(connection_type, identifiers) -> Optional[SQLConnection]:
    return utils.generateConnection(connection_type, identifiers)

def get_oracle_connection_identifiers(connection:SQLConnection):
    identifiers = connection.query_one("""
        SELECT s.sid, s.serial#
        FROM v$session s
        WHERE s.audsid = SYS_CONTEXT('USERENV','SESSIONID')
    """)
    return identifiers[0] if identifiers else None

def open_config()->dict:
    with open(CONFIG_PATH, 'r') as f:
        return json.load(f) 

def save_config(config:dict):
    with open(CONFIG_PATH, 'w') as f:
        json.dump(config, f, indent=2)

class OracleCmd(cmd.Cmd):
    prompt = "Oracle Prompt> "
    
    def emptyline(self):
        return
    
    def __init__(self, oracle_identifiers, connection:SQLConnection, connection_type, pool, completekey = "tab", stdin = None, stdout = None) -> None:
        super().__init__(completekey, stdin, stdout)
        self.oracle_identifiers = oracle_identifiers
        self.connection = connection
        self.connection_type = connection_type
        self.pool = pool
        self.tasks = dict()
        self._history = InMemoryHistory()
        self.workspace = DEFAULT_WORKSPACE
        self.workspace_config = {"path":DEFAULT_WORKSPACE_PATH}
        self.workspace_path = real_path(DEFAULT_WORKSPACE_PATH)
        self.prompt = "Ora "
        self.last_query = None
        self.last_query_content = None
        self.query_save_path = os.path.join(DEFAULT_WORKSPACE, "fav")
        self.last_submitted_content = None
        self.last_submitted_content_sync = None
        
    def switch_workspace(self, workspace_name:str):
        workspace_name = workspace_name.lower().strip(" \n\r\t")
        config = open_config()
        workspaces:dict = config["workspaces"]
        
        if not workspace_name in workspaces.keys():
            raise NameError(f"workspace {workspace_name} does not exist")
        
        self.workspace_config = workspaces[workspace_name]
        self.workspace_path = real_path(workspaces[workspace_name]["path"])
        self.workspace = workspace_name
        os.makedirs(self.workspace_path, exist_ok=True)
        self.prompt = f"Ora:{self.workspace}> "
        self.query_save_path = os.path.join(self.workspace_path, "fav")
        os.makedirs(self.query_save_path, exist_ok=True)
        
        config["last_workspace"] = self.workspace
        save_config(config)
        
        beautiful_print(f"switched to {workspace_name}")
    
    def set_workspace(self, workspace_name:str, workspace_path:str):
        workspace_name = workspace_name.lower().strip(" \n\r\t")
        config:dict = open_config()
        config["workspaces"][workspace_name] = {"path":workspace_path}
        save_config(config)
        
        
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
        connection = generateConnection(self.connection_type, identifiers)
        queries = [s.strip() for s in sqlparse.split(script) if s.strip()]
        with connection or nullcontext():
            self.query_oracle(identifiers, [query.strip(";\n\r ") for query in queries], False, task_id=task_id, default_connection=connection)
    
    def get_query_save_folder_path(self, taskid):
        today_str = datetime.date.today().strftime("%Y_%m_%d")
        return os.path.join(self.workspace_path, "queries", today_str, str(taskid))
    
    def query_oracle(self, identifiers, queries:list[str], commit:bool, task_id=None, sync=False, default_connection=None, placeholders=None):
        if default_connection is not None:
            connection = default_connection
        else:
            connection = generateConnection(self.connection_type, identifiers)
            if connection is None:
                return
            
        with connection if not default_connection else nullcontext():
            for sub_task_id, query in enumerate(queries):
                query = query.strip("\n\r")
                self.last_submitted_content = query
                self.last_submitted_content_sync = sync
                if task_id is None:
                    task_id = "NOT_A_TASK"
                try:    
                    self.tasks[task_id]["connection"] = connection
                    if connection:
                        self.tasks[task_id]["SID"],self.tasks[task_id]["SERIAL"] = get_oracle_connection_identifiers(connection) or ("NULL","NULL")
                    
                    save_folder = self.get_query_save_folder_path(task_id)
                    os.makedirs(save_folder, exist_ok=True)
                    output_file = os.path.join(save_folder, f"{sub_task_id}.output.csv")
                    sql_file = os.path.join(save_folder, f"query.sql")
                    log_file = os.path.join(save_folder, f"{sub_task_id}.log.txt")
                    
                    try:
                        if not placeholders:
                            beautiful_print(f"Executing query:\n================\n{query}\n================", log_only=True, log=log_file)
                            result = connection.query_one(query, print_error=False, ignore_errors=False, include_col_name=True)
                        else:
                            beautiful_print(f"Executing query:\n================\n{query}\n{placeholders}\n================", log_only=True, log=log_file)
                            result = connection.query_one(query, placeholders, print_error=False, ignore_errors=False, include_col_name=True)
                        beautiful_print(f"Query {sub_task_id} complete.", log_only=(not sync), log=log_file)
                        self.last_query = save_folder
                        self.last_query_content = query
                    except Exception as e:
                        beautiful_print(f"Error executing query: {query}", log_only=(not sync), log=log_file, color=RED_COLOR)
                        stack = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                        beautiful_print(stack, log_only=(not sync), log=log_file, color=RED_COLOR)
                        return
                        
                    with open(output_file, "w", newline="") as f:
                        csv.writer(f).writerows(result or [])
                    with open(sql_file, "w") as f:
                        f.write(query)
                    if sync:
                        with open(output_file, "r",) as f:
                            readlines = f.readlines(1000)
                            if len(readlines) > 0:
                                beautiful_print("")
                                for line in readlines:
                                    beautiful_print(line.strip(" \n\r\t"), color=LIGHT_BLUE_COLOR)
                                beautiful_print("")
                            if f.read(1):
                                beautiful_print("...")
                                beautiful_print(f"Open file {output_file} to access complete result")
                        
                except Exception as e:
                    beautiful_print(f"Error executing query: {task_id}/{sub_task_id}", log_only=(not sync), log=log_file, color=RED_COLOR)
                    stack = "".join(traceback.format_exception(type(e), e, e.__traceback__))
                    beautiful_print(stack, log_only=(not sync), log=log_file, color=RED_COLOR)
                    return
        if commit:
            connection.get_db().commit()

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
                
                save_folder = self.get_query_save_folder_path(task_id)
                os.makedirs(save_folder, exist_ok=True)
                log_file = os.path.join(save_folder, f"{sub_task_id}.log.txt")
                
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

    def start_task(self, description:str, func, sync, *args, **kwargs):
        description = " ".join([c for c in description.replace("\n"," ").replace("\t"," ").split(" ") if c != ""])
        if len(description) > 100:
            description = f"{description[:100]}..."
        if not sync:
            task_id = f"async_{datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')}"
            self.tasks[task_id] = {"description": description, "process":self.pool.submit(func, *args, task_id=task_id, sync=sync, **kwargs), "connection":None}
            beautiful_print(f"Started task {task_id}: {description}")
        else:
            task_id = f"sync_{datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')}"
            beautiful_print(f"Starting task {task_id}: {description}")
            self.tasks[task_id] = {"description": description, "process":None, "connection":self.connection}
            func(*args, task_id=task_id, sync=sync, default_connection=self.connection, **kwargs)
        
        return task_id
        
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
        Usage: savecmd [-w] <CMD_NAME>"""
        
        args = shlex.split(arg)
        if len(args) == 0:
            beautiful_print("Please specify the command name", color=RED_COLOR)
            return
        
        name = args[0]
        path_to_list_command = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "commands"
        )
        if args[0] == "-w":
            if len(args) < 2:
                beautiful_print("Please specify the command name", color=RED_COLOR)
                return
            name = args[1]
            path_to_list_command = os.path.join(
                self.workspace_path,
                "commands"
            )


        os.makedirs(path_to_list_command, exist_ok=True)
        
        path_to_save_command = os.path.join(
            path_to_list_command,
            f"{name}.sql"
        )
        with open(path_to_save_command, "w+") as f:
            f.write(edit_in_editor("Type your query on the line below", ignore_lines=1))
        
    def do_lscmd(self, arg):
        """Lists existing user commands"""
        
        path_to_list_command = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "commands"
        )
        path_to_load_command_workspace = os.path.join(
            self.workspace_path,
            "commands"
        )
        os.makedirs(path_to_list_command, exist_ok=True)
        os.makedirs(path_to_load_command_workspace, exist_ok=True)
        
        
        beautiful_print("List of existing commands:")
        beautiful_print(" ".join([s[:-4] for s in os.listdir(path_to_list_command)]))
        beautiful_print(" ".join([s[:-4] for s in os.listdir(path_to_load_command_workspace)]))
        
    def do_runcmd(self, arg):
        """Runs a user command
        USAGE: runcmd [-a] <CMD_NAME> [args...]
        -a -> run async"""
        args = shlex.split(arg)
        if not args:
            beautiful_print("Please specify the command name", color=RED_COLOR)
            return
        
        asyn = args[0] == "-a"
        command = args[1] if asyn else args[0]
        
        os.makedirs(os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "commands"
        ), exist_ok=True)
        path_to_load_command = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "commands",
            f"{command}.sql"
        )
        
        os.makedirs(os.path.join(
            self.workspace_path,
            "commands"
        ), exist_ok=True)
        path_to_load_command_workspace = os.path.join(
            self.workspace_path,
            "commands",
            f"{command}.sql"
        )
        
        found_local = True
        if not os.path.exists(path_to_load_command_workspace):
            found_local = False
            if not os.path.exists(path_to_load_command):
                beautiful_print("Unknown command. listcmd to get the list of available commands", color=RED_COLOR)
                return
            
        with open(path_to_load_command_workspace if found_local else path_to_load_command,"r") as f:
            command = f.read()
            self.start_task(f"runcmd {arg}", self.query_oracle, not asyn, self.oracle_identifiers, [command], False, placeholders=tuple(args[1:]) if len(args) > 1 else None)
    
    def do_workspace(self, arg):
        """Switches to specified workspace. No arg = list all workspaces and shows the current one. -c to create new workspace
        USAGE:
        workspace
        workspace [-c] [WORKSPACE_NAME] [PATH]
        workspace [WORKSPACE_NAME]"""
        args = shlex.split(arg)
        
        workspaces = open_config()["workspaces"].keys()
        if len(args) == 0:
            workspaces_str = " ".join([f"{GREEN_COLOR}{w}{RESET_COLOR}" if w == self.workspace else w for w in workspaces])
            beautiful_print(workspaces_str)
            return
        
        if args[0] == "-c":
            if len(args) != 3:
                beautiful_print("please specify <name> and <path>", RED_COLOR)
                return
            self.createworkspace(args[1], args[2])
            return
        
        arg = args[0].lower().strip(" \n\r\t")
        
        if not arg in workspaces:
            beautiful_print(f"no workspace named {arg}", RED_COLOR)
            return
        
        self.switch_workspace(arg)
        
    def createworkspace(self, name, path):
        workspace_name = name.lower().strip(" \n\r\t")
        self.set_workspace(workspace_name, path)
        beautiful_print(f"workspace created {workspace_name} {path}")
        
    def do_saveq(self, arg):
        """
        Saves last query as name
        USAGE: saveq <NAME> [TASK]
        """
        args = shlex.split(arg)
        if len(args) == 0:
            beautiful_print("error: no name specified", RED_COLOR)
            return
        
        if len(args) <= 1 and (self.last_query is None or self.last_query_content is None):
            beautiful_print("error: no query was executed", RED_COLOR)
            return
        
        name = args[0].lower().strip("\n\r ")
        copied = (self.last_query or "") if len(args) == 1 else self.get_query_save_folder_path(args[1])
        
        save_folder_path = os.path.join(self.query_save_path, name)
        
        os.makedirs(save_folder_path, exist_ok=True)
        copy_dir(copied, save_folder_path)

        beautiful_print(f"query {name} saved")
    
    def do_rerun(self, arg):
        """
        USAGE:
        rerun -> lists all available queries to rerun
        rerun <NAME> -> reruns the saved query
        rerun -a <NAME> -> reruns the saved query asyncly
        """
        args = shlex.split(arg)
        existing = os.listdir(self.query_save_path)
        
        if len(args) == 0:
            beautiful_print(" ".join(existing))
            return
        
        saved = args[-1].lower().strip("\n\r ")
        asyn = len(args) >= 2 and args[0] == "-a"
        
        if not saved in existing:
            beautiful_print("error: this query does not exist", RED_COLOR)
            return
        
        with open(os.path.join(self.query_save_path, saved, "query.sql"), 'r') as f:
            query = f.read()
            self.do_q(f"-a {query}" if asyn else query)
    
    def do_q(self, arg):
        """
        USAGE:
        q [QUERY] -> query [QUERY]
        -a -> async task
        -e -> explains plan for query
        -c -> auto commit at the end of the query
        """
        args = shlex.split(arg)

        asyn = False
        explain = False
        commit = False
        i = 0
        while i < len(args) and args[i].startswith("-"):
            flag = args[i]
            if "a" in flag:
                asyn = True
            if "e" in flag:
                explain = True
            if "c" in flag:
                commit = True
            i += 1
            
        query = " ".join(args[i:]).strip(" \n\r\t")
        if not query:
            query = edit_in_editor("Type your query on the line below", ignore_lines=1)
        if not query:
            beautiful_print("No query to run", color=RED_COLOR)
            return
        
        queries = [query]    
        if explain:
            queries[0] = f"EXPLAIN PLAN FOR {queries[0]}"
            queries.append("SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY(NULL, NULL, 'TYPICAL'))")
        
        self.start_task(f"q {arg}", self.query_oracle, not asyn, self.oracle_identifiers, queries, commit)
            
    def do_annotate(self, arg):
        """
        Used to edit the README.md associated to a saveq directory
        USAGE: annotate <NAME>
        """
        args = shlex.split(arg)
        if len(args) == 0:
            beautiful_print("error: please specify a saved query name", color=RED_COLOR)
            return
        
        dir_path = os.path.join(self.query_save_path, args[0])
        if not os.path.isdir(dir_path):
            beautiful_print("error: no saved query with such name", color=RED_COLOR)
            return
        
        save_folder_path = os.path.join(dir_path, "README.md")
        edit_in_editor(path=save_folder_path)
    
    def do_last(self, arg):
        """
        Opens last query in default editor, executes on save (async if last time was async, sync else)
        """
        if not self.last_submitted_content:
            beautiful_print("No last query to run", color=RED_COLOR)
            return
        
        to_exec = edit_in_editor(self.last_submitted_content)
        self.do_q(f"-a {to_exec}" if not self.last_submitted_content_sync else to_exec)
        
def main():
    beautiful_print("~~~----~~~")
    beautiful_print("Oracle CLI V0.2.0")
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
    
    config = open_config()
    last_workspace = config.get("last_workspace", DEFAULT_WORKSPACE)
    
    cli = None
    try:
        with ThreadPoolExecutor() as pool:
            connection = generateConnection(CONNECTION_TYPES, oracle_identifiers)
            if not connection:
                exit(1)
                
            with connection:
                cli = OracleCmd(oracle_identifiers, connection, CONNECTION_TYPES, pool)
                cli.switch_workspace(last_workspace)
                cli.cmdloop()
            
    finally:
        if cli is not None:
            beautiful_print("Stopping all tasks and exiting")
            cli.do_exit("")
            
        beautiful_print("Oracle prompter stopped")