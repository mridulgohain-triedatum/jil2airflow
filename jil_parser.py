from typing import Dict, Optional
from autosys_job import AutosysJob
from collections import OrderedDict
import re

class JILParser:
    """Parser for Autosys JIL files"""

    def __init__(self):
        self.jobs: Dict[str, AutosysJob] = {}
        self.current_job: Optional[AutosysJob] = None
        self.autosys_machine_to_airflow_conn_id_map = self.parse_autosys_machine_to_airflow_conn_id_map()

    def parse_autosys_machine_to_airflow_conn_id_map(self) -> Dict[str, str]:
        mapping = {}
        with open("autosys_machine_to_airflow_conn_id_map.cfg", "r") as f:
            for line in f:
                line = line.strip()
                # Skip empty lines or comment lines
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = map(str.strip, line.split("=", 1))
                mapping[key] = value
        return mapping        
    @staticmethod
    def parse_value(v):
        v = str(v).strip().lower()
        if v in ["y", "yes", "1", "true", "f"]: # f is included as because send_notification can have value f/F also
            return 1
        elif v in ["n", "no", "0", "false"]:
            return 0
        return 0
    def parse_file(self, filepath: str) -> Dict[str, AutosysJob]:
        with open(filepath, 'r') as f:
            content = f.read()
        return self.parse_content(content)

    def parse_content(self, content: str) -> Dict[str, AutosysJob]:
        lines = content.split('\n')
        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            self._parse_line(line)
        if self.current_job:
            self.jobs[self.current_job.name] = self.current_job
        # for job in self.jobs.values():
        #     print(f"job={job.name} {job.envvars}")
        return self.jobs

    def _parse_line(self, line: str):
        if line.startswith('insert_job:'):
            if self.current_job:
                self.jobs[self.current_job.name] = self.current_job
            # Split the line into tokens after 'insert_job:'
            tokens = line.split(':', 1)[1].strip().split()
            job_name = tokens[0]
            self.current_job = AutosysJob(name=job_name)
            # Process any additional key-value pairs on the same line
            idx = 1
            while idx < len(tokens):
                if tokens[idx].endswith(':') and idx + 1 < len(tokens):
                    key = tokens[idx][:-1]
                    value = tokens[idx + 1]
                    self._set_job_attribute(key, value)
                    idx += 2
                else:
                    idx += 1
            return
        if not self.current_job:
            return
        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip()
            value = value.strip()
            self._set_job_attribute(key, value)

    def _set_job_attribute(self, key: str, value: str):
        if not self.current_job:
            return
        job = self.current_job
    
        if key == 'job_type':
            job.job_type = value
        elif key == 'command':
            job.command = value.strip().strip('"').strip('\'')
            job.command = re.sub(r"\$\{?AUTO_JOB_NAME\}?", job.name, job.command)
        elif key == 'machine':
            job.machine = self.autosys_machine_to_airflow_conn_id_map.get(value, value)
        elif key == 'owner':
            job.owner = value
        elif key == 'permission':
            job.permission = value
        elif key == 'date_conditions':
            job.date_conditions = self.parse_value(value)
        elif key == 'days_of_week':
            job.days_of_week = value.split(',')
        elif key == 'start_times':
            job.start_times = value
        elif key == 'condition':
            job.condition = value
        elif key == 'description':
            job.description = value
        elif key == 'std_out_file':
            job.std_out_file = value.strip('"').strip('\'').strip()
            replacement = job.name if job.std_out_file.endswith(".log") else job.name + ".log"
            job.std_out_file = re.sub(r"\$\{?AUTO_JOB_NAME\}?", replacement, job.std_out_file)
        elif key == 'std_err_file':
            job.std_err_file = value.strip('"').strip('\'').strip()
            replacement = job.name if job.std_err_file.endswith(".err") else job.name + ".err"
            job.std_err_file = re.sub(r"\$\{?AUTO_JOB_NAME\}?", replacement, job.std_err_file)
        elif key == 'profile':
            job.profile = value.strip().strip('"').strip('\'')
        elif key == 'start_mins':
            job.start_mins = value
        elif key == 'priority':
            job.priority = int(value)
        elif key == 'max_run_alarm':
            job.max_run_alarm = int(value)
        elif key == 'min_run_alarm':
            job.min_run_alarm = int(value)
        elif key == 'watch_file':
            job.watch_file = value.strip().strip('"').strip('\'')
            job.watch_file = re.sub(r"\$\{?AUTO_JOB_NAME\}?", job.name, job.watch_file)
        elif key == 'watch_interval':
            job.watch_interval = int(value)
        elif key == 'watch_file_min_size':
            job.watch_file_min_size = int(value)
        elif key == 'box_name':
            job.box_name = value
        elif key == 'success_codes':
            job.success_codes = [int(x.strip()) for x in value.split(',')]
        elif key == 'failure_codes':
            job.failure_codes = [int(x.strip()) for x in value.split(',')]
        elif key == 'run_calendar':
            job.run_calendar = value.strip('"')
        elif key == 'n_retrys':
            job.n_retrys = int(value)
        elif key == 'term_run_time':
            job.term_run_time = int(value)
        elif key == 'alarm_if_fail':
            job.alarm_if_fail = self.parse_value(value)
        elif key == 'alarm_if_terminated':
            job.alarm_if_terminated = self.parse_value(value)
        elif key == 'timezone':
            job.timezone = value.strip().strip('"').strip('\'')
        elif key == 'send_notification':
            job.send_notification = self.parse_value(value)
        elif key == 'notification_emailaddress':
            job.notification_emailaddress = value.strip().strip('"').strip('\'')
        elif key == 'notification_emailaddress_on_alarm':
            job.notification_emailaddress_on_alarm = value.strip().strip('"').strip('\'')
        elif key == 'notification_emailaddress_on_failure':
            job.notification_emailaddress_on_failure = value.strip().strip('"').strip('\'')
        elif key == 'notification_emailaddress_on_success':
            job.notification_emailaddress_on_success = value.strip().strip('"').strip('\'')
        elif key == 'notification_emailaddress_on_terminated':
            job.notification_emailaddress_on_terminated = value.strip().strip('"').strip('\'')
        elif key == 'db_conn_id':    
            job.db_conn_id = value
        elif key == 'sql_command':
            job.sql_command = value
        elif key == 'sp_name':
            job.sp_name = value
        elif key == 'sp_arg':
            self._resolve_sp_params(value)

    def _resolve_sp_params(self, value: str) -> dict:
        parts = [p.strip() for p in value.split(",")]
        arg_dict = {}
        for p in parts:
            if "=" in p:
                k, v = p.split("=", 1)
                arg_dict[k.strip()] = v.strip()

        #  Only process if it is INPUT argument
        if arg_dict.get("argtype", "").upper() == "OUT":
            return
        name = arg_dict.get("name")
        raw_value = arg_dict.get("value")
        datatype = arg_dict.get("datatype", "VARCHAR").upper()
        # Type casting
        if raw_value is not None:
            if datatype in ("INT", "INTEGER"):
                cast_value = int(raw_value)
            elif datatype in ("FLOAT", "DOUBLE", "DECIMAL", "NUMBER"):
                cast_value = float(raw_value)
            else:
                cast_value = raw_value  # keep string for VARCHAR/CHAR/etc.
        else:
            cast_value = None

        # Populate sp_params
        self.current_job.sp_params[name] = {
            "value": cast_value,
            "datatype": datatype
        }
