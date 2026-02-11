import json
import os
from liouss_python_toolkit.printer import beautiful_print
from liouss_python_sql_connectors.oracle_connection import OracleConnection
import cmd
import traceback
from concurrent.futures import ThreadPoolExecutor
import datetime
import csv

def query_oracle(identifiers, query, commit, task_id=None):
    try:
        connection = OracleConnection(identifiers["username"],
                                identifiers["password"],
                                identifiers["hostname"],
                                identifiers["service_name"],
                                logs = False)
        
        with connection:
            output_file = os.path.join("oracle_prompt", "queries", f"oracle_output_{task_id}.csv")
            log_file = os.path.join("oracle_prompt", "logs", f"oracle_log_{task_id}.txt")
            
            try:
                result = connection.query_one(query, print_error=False, ignore_errors=False)
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

class OracleCmd(cmd.Cmd):
    prompt = "Oracle Prompt> "
    
    def __init__(self, oracle_identifiers, pool, completekey = "tab", stdin = None, stdout = None) -> None:
        super().__init__(completekey, stdin, stdout)
        self.oracle_identifiers = oracle_identifiers
        self.pool = pool
        self.tasks = dict()
        os.makedirs(os.path.join("oracle_prompt", "logs"), exist_ok=True)
        os.makedirs(os.path.join("oracle_prompt", "queries"), exist_ok=True)
        
    def start_task(self, description, func, *args, **kwargs):
        task_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        self.tasks[task_id] = {"description": description, "process":self.pool.submit(func, *args, task_id=task_id, **kwargs)}
        beautiful_print(f"Started task {task_id}: {description}")
        return task_id
    
    def do_query(self, arg):
        """Execute a SQL query on the Oracle database."""
        """Usage: query <SQL_QUERY>"""
        self.start_task(f"query {arg}", query_oracle, self.oracle_identifiers, arg, False)
    
    def do_queryc(self, arg):
        """Execute a SQL query on the Oracle database."""
        """Commits after execution"""
        """Usage: queryc <SQL_QUERY>"""
        self.start_task(f"queryc {arg}", query_oracle, self.oracle_identifiers, arg, True)
        
    def do_tasklst(self, arg):
        """List all running tasks."""
        """Usage: tasklst [-i]"""
        for task_id, task_info in self.tasks.items():
            status = "R" if not task_info["process"].done() else "C"
            if arg != "-i" or status == "R":
                beautiful_print(f"[{task_id}][{status}] : {task_info['description']}")
    
    def do_stoptsk(self, arg):
        """Stop a running task."""
        """Usage: stoptsk <TASK_ID>"""
        if arg in self.tasks:
            self.tasks[arg]["process"].cancel()
            beautiful_print(f"[Oracle] Stopped task {arg}: {self.tasks[arg]['description']}")
        else:
            beautiful_print(f"No task found with ID: {arg}")
        
    def do_exit(self, arg):
        """Exit the Oracle prompt."""
        for task_id in list(self.tasks.keys()):
            if not self.tasks[task_id]["process"].done():
                self.do_stoptsk(task_id)
        return True


def main():

    ORACLE_ID_LOCATION = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "ORACLE_IDENTIFIER.json"
    )
    with open(ORACLE_ID_LOCATION, "r") as f:
        oracle_identifiers = json.load(f)

    cli = None
    try:
        with ThreadPoolExecutor() as pool:
            cli = OracleCmd(oracle_identifiers, pool)
            cli.cmdloop()
            
    finally:
        if cli is not None:
            beautiful_print("Stopping all tasks and exiting")
            cli.do_exit("")
            
        beautiful_print("[Oracle] Oracle prompter stopped")