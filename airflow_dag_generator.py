from typing import Dict
from autosys_job import AutosysJob
from condition_parser import ConditionParser
from textwrap import dedent
from typing import Dict, List
import json
from collections import defaultdict
import re
from utils.timezone_map import TIMEZONE_MAP
from utils.external_dep_utils import ExternalDepUtils
from utils.converter_utils import Utils

class AirflowDAGGenerator:
    """Generates Airflow DAG from parsed Autosys jobs"""
    
    def __init__(self, jobs: Dict[str, AutosysJob],  ext_dep_list: list, dep_seq_info: str, schedule: str,
                 external_task_to_dependent_task_map: Dict[str, str], external_task_to_dag_id_map: Dict[str, str], dag_id_to_schedule_map: Dict[str, str],
                 downstream_jil_schedule: str, handle_ext_ref=False):
        self.jobs = jobs
        self.box_hierarchy = Utils.build_box_hierarchy(self.jobs)
        self.status_failure = "failure"
        self.status_retry = "retry"
        self.ext_dep_list = ext_dep_list
        self.dep_seq_info = dep_seq_info
        self.handle_ext_ref = handle_ext_ref
        self.schedule = schedule
        self.external_task_to_dependent_task_map = external_task_to_dependent_task_map
        self.external_task_to_dag_id_map = external_task_to_dag_id_map
        self.downstream_jil_schedule = downstream_jil_schedule
        self.dag_id_to_schedule_map = dag_id_to_schedule_map

    def _generate_external_task_sensor_indicator(self) -> bool:
        if self.handle_ext_ref and self.dep_seq_info == "downstream" and self.ext_dep_list and self.downstream_jil_schedule != "None":
            return True
        return False
    

    def generate_dag(self, dag_id: str, schedule_interval: str = None) -> str:
        """Generate complete Airflow DAG code and validate it using Airflow's DagBag."""
        import tempfile
        import os
        import sys
        try:
            from airflow.models import DagBag
        except ImportError:
            return "[ERROR] Airflow is not installed in the current environment. Cannot validate DAG."

        # Extract scheduling info from jobs if not provided
        if not schedule_interval or schedule_interval == '' or schedule_interval.upper() == "DEFAULT":
            schedule_interval = Utils.determine_schedule_interval(self.jobs)
        else:
            # If schedule provided as argument, format it properly
            schedule_interval = repr(schedule_interval)

        if self.handle_ext_ref == True and schedule_interval.strip("'") == "None":
            if self.dep_seq_info == "downstream":
                datasets = [f"Dataset('{job}')" for job in self.ext_dep_list]
                schedule_interval = f"[{', '.join(datasets)}]"
        imports = self._generate_imports()
        dag_definition = self._generate_dag_definition(dag_id, schedule_interval)
        callbacks = self._generate_callbacks()
        tasks = self._generate_tasks()
        dependencies = self._generate_dependencies()
        if self._generate_external_task_sensor_indicator():
            external_dependecies = ExternalDepUtils.generate_external_dependency_tasks(
                self.external_task_to_dependent_task_map,self.external_task_to_dag_id_map, self.downstream_jil_schedule.strip("'"), self.dag_id_to_schedule_map)
            dag_code = f"{imports}\n\n{dag_definition}\n\n{callbacks}\n\n{tasks}\n\n{external_dependecies}\n\n{dependencies}"
        else:
            dag_code = f"{imports}\n\n{dag_definition}\n\n{callbacks}\n\n{tasks}\n\n{dependencies}"
        #return dag_code
        with open("generated_dag.py", "w") as f:
            f.write(dag_code)
        
        # Validate the generated DAG using Airflow's DagBag and a temp file
        # Save current sys.path
        old_sys_path = list(sys.path)
        sys.path.insert(0, os.path.abspath("."))
        with tempfile.NamedTemporaryFile('w+', suffix='.py', delete=False) as tmp_file:
            tmp_file.write(dag_code)
            tmp_file.flush()
            tmp_file_path = tmp_file.name

        dagbag = DagBag(dag_folder=os.path.dirname(tmp_file_path), include_examples=False)
        errors = dagbag.import_errors
        dags = dagbag.dags

        # Clean up temp file
        os.unlink(tmp_file_path)
        sys.path = old_sys_path
        if errors:
            error_msgs = [f"{k}: {v}" for k, v in errors.items()]
            return "[DAG VALIDATION FAILED]\n" + "\n".join(error_msgs) + "\n" + dag_code 
        if dag_id not in dags:
            return f"[DAG VALIDATION FAILED]\nDAG '{dag_id}' not found in parsed DAGs."
        return dag_code

    def _get_required_imports(self) -> set:
        """Determine which operator imports are needed based on job types"""
        imports = {
            "from datetime import datetime, timedelta",
            "from airflow import DAG",
            "from airflow.utils.trigger_rule import TriggerRule",
            "from airflow.utils.task_group import TaskGroup",
            "from airflow.operators.empty import EmptyOperator",
            "from airflow.utils.template import literal",
        }
        
        has_complex_conditions = False
        has_ssh = False
        has_k8s = False
        has_file_sensor = False
        has_calendar = False
        has_timezone = False
        need_email_notification = False
        has_sql = False
        
        for job in self.jobs.values():
            # Check for complex conditions needing branch operator
            if job.condition:
                trigger_rule, _ = ConditionParser.parse_condition(job.condition)
                if trigger_rule == "complex":
                    has_complex_conditions = True
            
            # Check for file watchers
            if job.is_file_watcher():
                has_file_sensor = True
            
            # Check operator types for command jobs
            if job.has_command():
                if getattr(job, "operator_type", "KubernetesPodOperator") == "KubernetesPodOperator":
                    has_k8s = True
                else:
                    has_ssh = True

            if job.run_calendar:
                has_calendar = True
            
            if job.alarm_if_fail or job.alarm_if_terminated or job.send_notification:
                need_email_notification = True
            
            if job.date_conditions == 1 and job.timezone:
                has_timezone = True
            
            if job.job_type.upper() == "SQL" or job.job_type.upper() == "DBPROC":
                has_sql = True
        
        # Add required operator imports
        if has_complex_conditions:
            imports.add("from airflow.operators.python import BranchPythonOperator")
        if has_ssh:
            imports.add("from airflow.providers.ssh.operators.ssh import SSHOperator")
        if has_k8s:
            imports.add("from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator")
        if has_file_sensor:
            #imports.add("from airflow.providers.sftp.sensors.sftp import SFTPSensor")
            imports.add("from custom_operators.profile_aware_sftp_sensor import ProfileAwareSftpSensor")
        if has_calendar:
            imports.add("from airflow.timetables.base import Timetable")
            imports.add("from calendars.calendars import *")
        if need_email_notification:
            imports.add("from airflow.utils.email import send_email")
        if has_timezone:
            imports.add("import pendulum")
        if has_sql:
            imports.add("from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator")
        if self.handle_ext_ref:
            imports.add("from airflow import Dataset")
            imports.add("from airflow.sensors.external_task import ExternalTaskSensor")
        return imports

    def _generate_imports(self) -> str:
        """Generate import statements"""
        return "\n".join(sorted(self._get_required_imports()))
    
    def _get_timezone_for_dag(self) -> str:
        timezone = None
        for job in self.jobs.values():
            if job.date_conditions == 1 and getattr(job, "timezone", None):
                raw_tz = job.timezone
                # Normalize using mapping
                mapped_tz = TIMEZONE_MAP.get(raw_tz, raw_tz)
                if timezone is None:
                    timezone = mapped_tz
                elif mapped_tz != timezone:
                    raise ValueError("JIL file contains jobs having different timezone schedules")
        return timezone

    def _generate_dag_definition(self, dag_id: str, schedule_interval: str) -> str:
        timezoneinfo = self._get_timezone_for_dag()
        if timezoneinfo:
            start_date_line = f"pendulum.datetime(2024, 1, 1, tz=\"{timezoneinfo}\")"
        else:
            start_date_line = "datetime(2024, 1, 1)"
        """Generate DAG definition"""
        return dedent(f"""
            default_args = {{
                'owner': 'airflow',
                'depends_on_past': False,
                'start_date': {start_date_line},
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=1),
            }}
            
            dag = DAG(
                '{dag_id}',
                default_args=default_args,
                description='Converted from Autosys JIL',
                schedule={schedule_interval},
                catchup=False,
                tags=['autosys-conversion'],
            )
        """).strip()
    
    ## -------------Generate callback functions if applicable-----------------------##
    def _generate_callbacks(self) -> str:
        is_failure_callback_generated = False
        #is_retry_callback_generated = False
        callback_functions = []
        for job in self.jobs.values():
            if not job.is_box_job(): 
                if not is_failure_callback_generated:
                    alarm_if_fail = getattr(job, "alarm_if_fail", 0)
                    alarm_if_terminated = getattr(job, "alarm_if_terminated", 0)
                    if alarm_if_fail == 1 or alarm_if_terminated == 1:
                        callback_functions.append(self._generate_callback_func_def(self.status_failure))
                        is_failure_callback_generated = True                  
                #if not is_retry_callback_generated:
                    #if alarm_if_terminated == 1:
                        #callback_functions.append(self._generate_callback_func_def(self.status_retry))
                        #is_retry_callback_generated = True

        return "\n\n".join(callback_functions)

    def _generate_callback_func_def(self, callback_for: str, indent:int = 0) -> str:
        indent_str = " " * indent
        func_def = f"""
{indent_str}def notify_{callback_for}(context):
{indent_str}    ti = context['task_instance']
{indent_str}    dag_id = ti.dag_id
{indent_str}    task_id = ti.task_id
{indent_str}    state = ti.state
{indent_str}    log_url = ti.log_url
{indent_str}    exception = context.get("exception")
{indent_str}    recipients = context['params'].get("emails")
{indent_str}    subject = f"{{state}} | Task {{task_id}} in DAG {{dag_id}} is :: {{state}}"
{indent_str}    body = f\"\"\" 
{indent_str}    <h3>DAG: {{dag_id}}</h3>
{indent_str}    <p>Task: {{task_id}}</p>
{indent_str}    <p>Execution Date: {{context['logical_date']}}</p>
{indent_str}    <p>exception :: {{exception}}</p>
{indent_str}    <p>Log: <a href="{{log_url}}">View Logs</a></p>
{indent_str}    \"\"\"
{indent_str}    send_email(to=recipients, subject=subject, html_content=body)
"""
        return func_def.strip("\n")
    

    def _generate_tasks(self) -> str:
        """Generate all task definitions with nested box support"""
        tasks = []
        
        # Get all root-level boxes (those not contained in other boxes)
        root_boxes = [name for name, info in self.box_hierarchy.items() 
                    if info['parent'] is None]
        
        # Generate task groups for root-level box jobs first
        for box_name in sorted(root_boxes):
            tasks.append(self._generate_task_group(box_name))
        
        # Then generate individual tasks (only for jobs NOT in any box)
        for job_name, job in self.jobs.items():
            if not job.is_box_job() and not job.box_name:
                # Check if this job has a complex condition that needs branching
                if job.condition:
                    trigger_rule, deps = ConditionParser.parse_condition(job.condition)
                    if trigger_rule == "complex":
                        # Generate task group for branch logic
                        tasks.append(self._generate_branch_task_group(job, deps))
                        continue
                
                # Generate regular tasks
                if job.is_file_watcher():
                    tasks.append(self._generate_file_watcher_task(job))
                else:
                    tasks.append(self._generate_command_task(job))
                    
        return "\n\n".join(tasks)

    def _generate_branch_task_group(self, job: AutosysJob, deps: List[str], indent: int = 0) -> str:
        """Generate task group containing branch operator, actual task, and skip task"""
        group_id = job.name
        tooltip = f"Branch logic for {job.name}"
        indent_str = " " * indent
        
        task_group_def = f"""
{indent_str}with TaskGroup(
{indent_str}    group_id='{group_id}',
{indent_str}    tooltip='{tooltip}',
{indent_str}    dag=dag,
{indent_str}) as {group_id}:
"""
        
        # Generate branch function and task
        branch_task = self._generate_branch_task_content(job, deps, indent = indent + 4)
        
        # Generate the actual task (file watcher or command)
        if job.is_file_watcher():
            actual_task = self._generate_file_watcher_task(job, indent= indent + 4)
        else:
            actual_task = self._generate_command_task(job, task_suffix="_execute", indent = indent + 4)
        
        # Generate skip task
        skip_task = f"""
{indent_str}    {job.name}_skip = EmptyOperator(
{indent_str}        task_id='{job.name}_skip',
{indent_str}        dag=dag,
{indent_str}    )
"""
        
        # Combine all parts
        task_group_def += "\n\n" + branch_task
        task_group_def += "\n\n" + actual_task
        task_group_def += "\n\n" + skip_task
        
        # Add internal dependencies
        dependencies = self._generate_branch_task_group_dependencies(job, indent = indent + 4)
        if dependencies:
            task_group_def += "\n\n" + dependencies
        
        return task_group_def
    
    def _generate_branch_task_content(self, job: AutosysJob, deps: List[str], indent: int = 0) -> str:
        """Generate branch function and operator content for task group"""
        condition_code = ConditionParser.generate_condition_code(job.condition, deps)
        
        func_name = f"check_condition_{job.name}"
        indent_str = " " * indent
        
        # Determine the actual task name based on job type
        actual_task_name = job.name
        if job.is_file_watcher() and job.has_command():
            actual_task_name = f"{job.name}_command"
        elif job.is_file_watcher():
            actual_task_name = f"{job.name}"
        
        return f"""
{indent_str}def {func_name}(task_instance, **context):
{indent_str}    if {condition_code}:
{indent_str}        return '{actual_task_name}'
{indent_str}    else:
{indent_str}        return '{job.name}_skip'
{indent_str}
{indent_str}{job.name}_branch = BranchPythonOperator(
{indent_str}    task_id='{job.name}_branch',
{indent_str}    python_callable={func_name},
{indent_str}    dag=dag,
{indent_str})
"""

    def _generate_branch_task_group_dependencies(self, job: AutosysJob, indent = 0) -> str:
        """Generate dependencies within the branch task group"""
        dependencies = []
        indent_str = " " * indent
        # Determine the actual task name based on job type
        actual_task_name = job.name
        if job.is_file_watcher() and job.has_command():
            actual_task_name = f"{job.name}_command"
            # Add file sensor to command dependency if it's a file watcher with command
            dependencies.append(f"    {job.name} >> {job.name}_command")
        elif job.is_file_watcher():
            actual_task_name = f"{job.name}"
        else:
            actual_task_name = f"{job.name}_execute"
        
        # Add dependency from branch task to actual task and skip task
        dependencies.append(f"{indent_str}{job.name}_branch >> [{actual_task_name}, {job.name}_skip]")
        
        return '\n'.join(dependencies) if dependencies else ""

    def _generate_task_group(self, box_name: str, indent: int = 0) -> str:
        """Generate task group for box job with nested support"""
        box_info = self.box_hierarchy[box_name]
        job = box_info['job']
        children = box_info['children']
        
        group_id = job.name
        tooltip = job.description if job.description else f"Task group for {job.name}"
        indent_str = " " * indent
        
        task_group_def = f"""
{indent_str}with TaskGroup(
{indent_str}    group_id='{group_id}',
{indent_str}    tooltip='{tooltip}',
{indent_str}    dag=dag,
{indent_str}) as {group_id}:
"""
        # Generate tasks and nested task groups within this group
        inner_tasks = []
        
        # First, generate nested box jobs (child boxes)
        child_boxes = [child for child in children if child in self.box_hierarchy]
        for child_box_name in child_boxes:
            nested_task_group = self._generate_task_group(child_box_name, indent + 4)
            inner_tasks.append(nested_task_group)
        
        # Then generate regular jobs within this box
        regular_jobs = [child for child in children if child not in self.box_hierarchy]
        
        # Generate branch tasks for jobs with complex conditions
        for child_job_name in regular_jobs:
            child_job = self.jobs[child_job_name]
            if child_job.condition:
                trigger_rule, deps = ConditionParser.parse_condition(child_job.condition)
                if trigger_rule == "complex":
                    branch_task = self._generate_branch_task_group(child_job, deps, indent + 4)
                    inner_tasks.append(branch_task)
        
        # Generate regular tasks for jobs without complex conditions
        for child_job_name in regular_jobs:
            child_job = self.jobs[child_job_name]
            
            # Skip jobs with complex conditions (already handled above)
            if child_job.condition:
                trigger_rule, deps = ConditionParser.parse_condition(child_job.condition)
                if trigger_rule == "complex":
                    continue
            
            if child_job.is_file_watcher():
                inner_tasks.append(self._generate_file_watcher_task(child_job, indent = indent + 4))
            else:
                inner_tasks.append(self._generate_command_task(child_job, indent = indent + 4))
        
        if inner_tasks:
            # Add proper indentation for all inner tasks
            # indented_tasks = []
            # for task in inner_tasks:
            #     # Add 4 spaces to each line to indent within the TaskGroup
            #     indented_task = '\n'.join('    ' + line if line.strip() else line 
            #                             for line in task.split('\n'))
            #     indented_tasks.append(indented_task)
            
            task_group_def += "\n".join(inner_tasks)
            
            # Add task dependencies within the task group
            dependencies = self._generate_task_group_dependencies(children, indent = indent + 4)
            if dependencies:
                task_group_def += "\n\n" + dependencies
        else:
            # If no child jobs, create a dummy task
            task_group_def += "\n\n" + f"""
{indent_str}    {group_id}_dummy = EmptyOperator(
{indent_str}        task_id='{group_id}_dummy',
{indent_str}        dag=dag,
{indent_str}    )
"""
        
        return task_group_def

    def _generate_task_group_dependencies(self, children: list, indent: int = 4) -> str:

        """Generate task dependencies within the task group with nested support"""
        dependencies = []
        indent_str = ""
        
        for child_name in children:
            # Skip if this child is a nested box (task group)
            if child_name in self.box_hierarchy:
                continue
                
            child_job = self.jobs[child_name]
            
            # Handle complex conditions with branching
            if child_job.condition:
                trigger_rule, deps = ConditionParser.parse_condition(child_job.condition)
                print(f"{trigger_rule} {deps}")
                if trigger_rule == "complex":
                    # Add dependencies from predecessor tasks to the branch task
                    for dep in deps:
                        if dep in children:
                            dep_task_ref = self._get_task_reference_within_group(dep, children)
                            dependencies.append(f"{indent_str}{dep_task_ref} >> {child_name}")
                    continue
            
            # Get dependencies for this job (non-complex conditions)
            job_deps = []
            if child_job.condition:
                trigger_rule, deps = ConditionParser.parse_condition(child_job.condition)
                
                # Set trigger rule for simple conditions
                if trigger_rule in ["all_success", "all_failed", "all_done", "one_success"]:
                    if not (trigger_rule == "all_success" and len(deps) == 1):
                        target_task = self._get_task_reference_within_group(child_name, children)
                        dependencies.append(f"{indent_str}{target_task}.trigger_rule = TriggerRule.{trigger_rule.upper()}")
                
                for dep in deps:
                    # Only include dependencies that are within the same task group
                    if dep in children:
                        dep_task_ref = self._get_task_reference_within_group(dep, children)
                        job_deps.append(dep_task_ref)
            print(f"job_deps: {job_deps}")
            # Generate dependency definition if there are any
            if job_deps:
                target_task = self._get_task_reference_within_group(child_name, children)
                
                if len(job_deps) == 1:
                    dep_line = f"{indent_str}{job_deps[0]} >> {target_task}"
                else:
                    deps_str = ', '.join(job_deps)
                    dep_line = f"{indent_str}[{deps_str}] >> {target_task}"
                dependencies.append(dep_line)
                    # for job_dep in job_deps:
                    #     dep_line = f"{indent_str}{job_dep} >> {target_task}"
                    #     dependencies.append(dep_line)
        
        # Handle file watcher with command dependencies within the task group
        for child_name in children:
            if child_name in self.box_hierarchy:
                continue
                
            child_job = self.jobs[child_name]
            
            # Handle file watcher with command dependencies
            if child_job.is_file_watcher() and child_job.has_command():
                # Check if it's a complex condition (wrapped in task group)
                if child_job.condition:
                    trigger_rule, _ = ConditionParser.parse_condition(child_job.condition)
                    if trigger_rule == "complex":
                        # Dependency is handled within the nested task group
                        continue
                
                dependencies.append(f"{indent_str}{child_name} >> {child_name}_command")
        normalized_deps = self._normalize_dependencies(dependencies, indent)
        return '\n'.join(normalized_deps)
        # return '\n'.join(dependencies)

    def _get_task_reference_within_group(self, job_name: str, group_children: list) -> str:
        """Get the proper task reference for a job within a task group"""
        # If the job is a nested box within this group
        if job_name in self.box_hierarchy:
            return job_name  # Reference the nested task group
        
        job = self.jobs.get(job_name)
        if not job:
            return job_name
        
        # Check if job has complex condition (wrapped in its own task group)
        if job.condition:
            trigger_rule, _ = ConditionParser.parse_condition(job.condition)
            if trigger_rule == "complex":
                return job_name  # Reference the task group wrapping the job
        
        # Regular job reference within the same task group - use simple task name
        if job.is_file_watcher() and job.has_command():
            return f"{job_name}_command"
        elif job.is_file_watcher():
            return f"{job_name}"
        else:
            return job_name
    
    def _generate_file_watcher_task(self, job: AutosysJob, indent: int = 0) -> str:
        """Generate file sensor task"""
        filepath = job.watch_file
        indent_str = " " * indent 
        fs_conn_id = job.fs_conn_id if job.fs_conn_id else job.machine
        poke_interval = job.watch_interval if job.watch_interval else 60
        ##------------------New Features---------------
        retries = getattr(job, "n_retrys", 0)
        term_run_time = getattr(job, "term_run_time", 0)
        alarm_if_fail = getattr(job, "alarm_if_fail", 0)
        alarm_if_terminated = getattr(job, "alarm_if_terminated", 0)
        send_notification = getattr(job, "send_notification", 0)
        email_addresses = [
            getattr(job, "notification_emailaddress", None),
            getattr(job, "notification_emailaddress_on_alarm", None),
            getattr(job, "notification_emailaddress_on_failure", None),
            getattr(job, "notification_emailaddress_on_success", None),
            getattr(job, "notification_emailaddress_on_terminated", None),
        ]  
        # Filter, deduplicate, and join
        emails_list = sorted(set([e.strip() for e in email_addresses if e and e.strip()]))
        emails = emails_list if emails_list else None
        attributes = [
            f"{indent_str}    task_id='{job.name}',",
            f"{indent_str}    path='{filepath}',",
            f"{indent_str}    sftp_conn_id='{fs_conn_id}',",
            f"{indent_str}    poke_interval={poke_interval},",
            f"{indent_str}    timeout=300,",
        ]
        if retries > 0:
            attributes.append(f"{indent_str}    retries={retries},")
            attributes.append(f"{indent_str}    retry_delay=timedelta(minutes=1),")
        if job.profile:
            attributes.append(f"{indent_str}    profile_file='{job.profile}',")
        if term_run_time > 0:
            attributes.append(f"{indent_str}    execution_timeout=timedelta(minutes={term_run_time}),")
        if alarm_if_fail == 1 or alarm_if_terminated == 1 or send_notification == 1:
            attributes.append(f"{indent_str}    params={{'emails': {emails}}},")
            attributes.append(f"{indent_str}    on_failure_callback=notify_{self.status_failure},")
        #if alarm_if_terminated == 1:
            #attributes.append(f"{indent_str}    on_retry_callback=notify_{self.status_retry},")
        attributes.append(f"{indent_str}    dag=dag,")

        task_def = f"""
{indent_str}{job.name} = ProfileAwareSftpSensor(
{chr(10).join([a for a in attributes if a])}
{indent_str})
"""   
        # If file watcher has a command, add command task
        if job.has_command():
            command_task = self._generate_command_task(job, task_suffix="_command", indent=indent)
            task_def += "\n\n" + command_task
            
        return task_def
    
    def _generate_command_task(self, job: AutosysJob, task_suffix: str = "", indent: int = 0) -> str:
        """Generate SSHOperator or KubernetesPodOperator for command execution"""
        task_id = f"{job.name}{task_suffix}"
        indent_str = " " * indent
        ##------------------New Features---------------
        retries = getattr(job, "n_retrys", 0)
        term_run_time = getattr(job, "term_run_time", 0)
        alarm_if_fail = getattr(job, "alarm_if_fail", 0)
        alarm_if_terminated = getattr(job, "alarm_if_terminated", 0)
        send_notification = getattr(job, "send_notification", 0)
        email_addresses = [
            getattr(job, "notification_emailaddress", None),
            getattr(job, "notification_emailaddress_on_alarm", None),
            getattr(job, "notification_emailaddress_on_failure", None),
            getattr(job, "notification_emailaddress_on_success", None),
            getattr(job, "notification_emailaddress_on_terminated", None),
        ]
        # Filter, deduplicate, and join
        emails_list = sorted(set([e.strip() for e in email_addresses if e and e.strip()]))
        emails = emails_list if emails_list else None
        common_attributes = []
        if retries > 0:
            common_attributes.append(f"{indent_str}    retries={retries},")
            common_attributes.append(f"{indent_str}    retry_delay=timedelta(minutes=1),")
        if alarm_if_fail == 1 or alarm_if_terminated == 1 or send_notification == 1:
            common_attributes.append(f"{indent_str}    params={{'emails': {emails}}},")
            common_attributes.append(f"{indent_str}    on_failure_callback=notify_{self.status_failure},")
        if term_run_time > 0:
            common_attributes.append(f"{indent_str}    execution_timeout=timedelta(minutes={term_run_time}),")
        print(f"external dependency list:: {self.ext_dep_list}")
        if self.handle_ext_ref and self.dep_seq_info == "upstream" and self.downstream_jil_schedule == "None":
            dataset_name = ""
            for ext_dep_task in self.ext_dep_list:
                if job.name == ext_dep_task:
                    dataset_name = ext_dep_task
                    common_attributes.append(f"{indent_str}    outlets=[Dataset('{dataset_name}')],")
        resolved_cmd = job.command
        # Append stdout/stderr redirection if present
        if getattr(job, "std_out_file", None):
            if job.std_out_file.startswith(">"):
                resolved_cmd += f" {job.std_out_file}"
            else:
                resolved_cmd += f" > {job.std_out_file}"
        if getattr(job, "std_err_file", None):
            if job.std_err_file.startswith(">"):
                resolved_cmd += f" 2{job.std_err_file}"
            else:
                resolved_cmd += f" 2> {job.std_err_file}"
        # Build environment variables
        env_vars = "{}"
        if job.envvars:
            env_vars = json.dumps(job.envvars)
     
        if getattr(job, "job_type", "SQL").upper() == "SQL":
            sql_command = job.sql_command
            if sql_command.startswith(("'", '"')) and sql_command.endswith(("'", '"')):
                sql_command = sql_command[1:-1].strip()
            sql_command = f'"{sql_command}"'
            attributes = [
                f"{indent_str}    task_id='{task_id}',",
                f"{indent_str}    conn_id='{job.db_conn_id or job.machine}',",
                f"{indent_str}    sql={sql_command},",
                f"{indent_str}    split_statements=True,",
                f"{indent_str}    return_last=False,",
            ]
            attributes.extend(common_attributes)
            attributes.append(f"{indent_str}    dag=dag,")
            return f"""
{indent_str}{task_id} = SQLExecuteQueryOperator(
{chr(10).join([a for a in attributes if a])}
{indent_str})
"""
        elif getattr(job, "job_type", "DBPROC").upper() == "DBPROC":
            sp_name = job.sp_name
            if sp_name.startswith(("'", '"')) and sp_name.endswith(("'", '"')):
                sp_name = sp_name[1:-1].strip()
            if job.has_sp_params:
                sql =self._get_sp_call_with_params(job, indent_str)
            else:
                sql = "sql=\"call {sp_name}()\",",
            attributes = [
                f"{indent_str}    task_id='{task_id}',",
                f"{indent_str}    conn_id='{job.db_conn_id or job.machine}',",
                f"{indent_str}    {sql}",
            ]
            attributes.extend(common_attributes)
            attributes.append(f"{indent_str}    dag=dag,")
            return f"""
{indent_str}{task_id} = SQLExecuteQueryOperator(
{chr(10).join([a for a in attributes if a])}
{indent_str})
"""
        elif getattr(job, "operator_type", "KubernetesPodOperator") == "KubernetesPodOperator":
            # Determine if command is bash or python
            is_python = (job.command.startswith('python') or 
                        job.command.endswith('.py') or 
                        'python' in job.command.lower())
            
            if job.command_image:
                image = job.command_image
            else:
                image = "python:3.9-slim" if is_python else "ubuntu:20.04"
            
            # Prepare command for execution
            if ">" in resolved_cmd or "|" in resolved_cmd:
                cmd = ["/bin/bash", "-c", resolved_cmd]
            elif is_python:
                cmd = job.command.split(" ")
            else:
                cmd = ["/bin/bash", "-c", resolved_cmd]

            attributes = [
                f"{indent_str}    task_id='{task_id}',",
                f"{indent_str}    name='{task_id}',",
                f"{indent_str}    namespace='default',",
                f"{indent_str}    image='{image}',",
                f"{indent_str}    cmds={cmd[0:1]},",
                f"{indent_str}    arguments={cmd[1:] if len(cmd) > 1 else []},",
                f"{indent_str}    env_vars={env_vars},",
                f"{indent_str}    is_delete_operator_pod=True,",
                f"{indent_str}    get_logs=True,",
            ]
            attributes.extend(common_attributes)
            attributes.append(f"{indent_str}    dag=dag,")

            return f"""
{indent_str}{task_id} = KubernetesPodOperator(
{chr(10).join([a for a in attributes if a])}
{indent_str})
"""        
        else:
            if job.profile:
                command = repr(f"bash -c '. {job.profile} && {resolved_cmd}'")
            else:
                command = repr(resolved_cmd)

            attributes = [
                f"{indent_str}    task_id='{task_id}',",
                f"{indent_str}    ssh_conn_id='{job.ssh_conn_id or job.machine}',",
                f"{indent_str}    command=literal({command}),",
                f"{indent_str}    environment={env_vars},",
            ]
            attributes.extend(common_attributes)
            attributes.append(f"{indent_str}    dag=dag,")

            return f"""
{indent_str}{task_id} = SSHOperator(
{chr(10).join([a for a in attributes if a])}
{indent_str})
"""
    def _get_sp_call_with_params(self, job: AutosysJob, indent_str: str) -> str:
        rendered_params = {}

        for param_name, meta in job.sp_params.items():
            value = meta["value"]
            # Dynamic parameter from JIL
            if isinstance(value, str) and value.startswith("$"):
                var_name = value[1:]
                # Jinja template: try Airflow Variable, fallback to environment variable
                rendered_params[param_name] = (
                    f"{{{{ var.value.{var_name} if var.value.{var_name} is not none "
                    f"else os.environ.get('{var_name}') }}}}"
                )
            else:
                # Static value already cast in parser
                rendered_params[param_name] = value
        return f"""
{indent_str}    sql=f"CALL {job.sp_name}({', '.join([f'%({p})s' for p in rendered_params.keys()])})",
{indent_str}    parameters={rendered_params},
        """
    def _get_task_reference(self, job_name: str) -> str:
        """Get the proper task reference for a job, considering nested task groups"""
        job = self.jobs.get(job_name)
        if not job:
            return job_name
        
        # If job is in a box, reference it within the task group hierarchy
        if job.box_name:
            # Build the full path for nested task groups
            path_parts = []
            current_box = job.box_name
            
            # Walk up the hierarchy to build the full path
            while current_box:
                path_parts.insert(0, current_box)
                box_info = self.box_hierarchy.get(current_box)
                if box_info and box_info['parent']:
                    current_box = box_info['parent']
                else:
                    break
            
            # Check if job has complex condition (wrapped in task group)
            if job.condition:
                trigger_rule, _ = ConditionParser.parse_condition(job.condition)
                if trigger_rule == "complex":
                    # Job is wrapped in its own task group
                    path_parts.append(job_name)
                    return '.'.join(path_parts)
            
            # Regular job reference within nested task groups
            if job.is_file_watcher() and job.has_command():
                task_name = f"{job_name}_command"
            elif job.is_file_watcher():
                task_name = f"{job_name}"
            else:
                task_name = job_name
            
            path_parts.append(task_name)
            return '.'.join(path_parts)
        else:
            # Job is not in a box
            # Check if job has complex condition (wrapped in task group)
            if job.condition:
                trigger_rule, _ = ConditionParser.parse_condition(job.condition)
                if trigger_rule == "complex":
                    return job_name  # Reference the task group itself
            
            # Regular job reference
            if job.is_file_watcher() and job.has_command():
                return f"{job_name}_command"
            elif job.is_file_watcher():
                return f"{job_name}"
            else:
                return job_name

    def _normalize_dependencies(self, dependencies, indent=0):
        """
        Normalize Airflow dependencies, preserving indentation.

        Args:
            dependencies (List[str]): List of dependency strings like "a >> b"
            indent (int): Number of spaces to prefix each normalized line

        Returns:
            List[str]: Normalized and indented dependency strings
        """
        indent_str = " " * indent

        graph = defaultdict(set)
        reverse_graph = defaultdict(set)
        all_edges = set()
        all_nodes = set()
        pre_grouped = []
        extra_lines = []

        dep_pattern = re.compile(r"^\s*(\w+(?:_\w+)?)\s*>>\s*(\w+(?:_\w+)?)")

        for line in dependencies:
            match = dep_pattern.match(line.strip())
            if match:
                src, dst = match.groups()
                graph[src].add(dst)
                reverse_graph[dst].add(src)
                all_edges.add((src, dst))
                all_nodes.update([src, dst])
            elif "[" in line and "]" in line:
                pre_grouped.append(f"{indent_str}{line}")
            else:
                extra_lines.append(f"{indent_str}{line}")

        visited_edges = set()
        result = []
        handled_targets = set()
        used_starts = set()

        def walk_chain(start):
            chain = [start]
            while (
                start in graph and
                len(graph[start]) == 1
            ):
                next_node = list(graph[start])[0]
                if (start, next_node) in visited_edges or len(reverse_graph[next_node]) != 1:
                    break
                visited_edges.add((start, next_node))
                chain.append(next_node)
                start = next_node
            return chain

        # Handle chains and diverging branches
        for node in all_nodes:
            if node not in reverse_graph and node not in used_starts:
                chain = walk_chain(node)
                used_starts.update(chain)
                if len(chain) > 1:
                    result.append(f"{indent_str}{' >> '.join(chain)}")
                    handled_targets.update(chain[1:])
                elif node in graph:
                    children = sorted(graph[node] - handled_targets)
                    if len(children) > 1:
                        result.append(f"{indent_str}{node} >> [{', '.join(children)}]")
                        for c in children:
                            visited_edges.add((node, c))
                            handled_targets.add(c)
                    elif len(children) == 1:
                        result.append(f"{indent_str}{node} >> {children[0]}")
                        visited_edges.add((node, children[0]))
                        handled_targets.add(children[0])

        # Handle converging branches
        for target in all_nodes:
            sources = reverse_graph.get(target, set())
            unvisited_sources = [s for s in sources if (s, target) not in visited_edges]
            if len(unvisited_sources) > 1:
                result.append(f"{indent_str}[{', '.join(sorted(unvisited_sources))}] >> {target}")
                for s in unvisited_sources:
                    visited_edges.add((s, target))
                handled_targets.add(target)

        # Remaining unvisited edges
        for src, dst in sorted(all_edges):
            if (src, dst) not in visited_edges:
                result.append(f"{indent_str}{src} >> {dst}")

        return  sorted(set(extra_lines)) + sorted(set(pre_grouped)) + sorted(set(result))
    
    def _generate_dependencies(self) -> str:
        """Generate task dependencies"""
        dependencies = []
        
        for job_name, job in self.jobs.items():
            if job.condition:
                trigger_rule, deps = ConditionParser.parse_condition(job.condition)
                print(f"Processing job '{job_name}' {trigger_rule} {deps}")
                # # Handle box job dependencies
                # if job.is_box_job():
                #     # Set trigger rule for box if not complex
                #     if trigger_rule != "complex" and trigger_rule != "none":
                #         dependencies.append(f"{job_name}.trigger_rule = TriggerRule.{trigger_rule.upper()}")
                #     # Add dependencies
                #     for dep in deps:
                #         dep_task = self._get_task_reference(dep)
                #         box_task = job.name  # Use box name directly for TaskGroup
                #         dependencies.append(f"{dep_task} >> {box_task}")
                #     continue
            
                # Skip dependency generation for tasks within task groups
                # Task group dependencies are handled in _generate_task_group_dependencies
                if job.box_name:
                    continue
                
                # For complex conditions, the task is now wrapped in a task group
                if trigger_rule == "complex":
                    # Add dependencies from predecessor tasks to the task group
                    for dep in deps:
                        dep_task = self._get_task_reference(dep)
                        dependencies.append(f"{dep_task} >> {job_name}")
                    continue
                    
                # For simple success conditions, use direct dependency without branching
                elif trigger_rule == "all_success" and len(deps) == 1:
                    # This is a simple success(job) condition, use direct dependency
                    task_name = self._get_task_reference(job_name)
                    dep_task = self._get_task_reference(deps[0])
                    
                    dependencies.append(f"{dep_task} >> {task_name}")
                    
                else:
                    # Other simple dependencies with trigger rules
                    task_name = self._get_task_reference(job_name)
                        
                    if trigger_rule != "none":
                        # Set trigger rule on the task
                        if job.is_file_watcher() and job.has_command():
                            dependencies.append(f"{job_name}_command.trigger_rule = TriggerRule.{trigger_rule.upper()}")
                        elif job.is_file_watcher():
                            dependencies.append(f"{job_name}.trigger_rule = TriggerRule.{trigger_rule.upper()}")
                        else:
                            dependencies.append(f"{job_name}.trigger_rule = TriggerRule.{trigger_rule.upper()}")
                    
                    for dep in deps:
                        dep_task = self._get_task_reference(dep)
                        dependencies.append(f"{dep_task} >> {task_name}")
            
            # Handle file watcher with command dependencies (only for jobs not in task groups)
            if job.is_file_watcher() and job.has_command() and not job.box_name:
                # Check if job has complex condition (would be in task group)
                if job.condition:
                    trigger_rule, _ = ConditionParser.parse_condition(job.condition)
                    if trigger_rule == "complex":
                        continue  # Skip, handled within task group
                
                dependencies.append(f"{job_name} >> {job_name}_command")
        
        # Generate dependencies between task groups and handle cross-group dependencies
        for job_name, job in self.jobs.items():
            if job.box_name and job.condition:
                # Check if any dependencies are outside the current task group context
                _, deps = ConditionParser.parse_condition(job.condition)
                for dep in deps:
                    dep_job = self.jobs.get(dep)
                    if dep_job and dep_job.box_name != job.box_name:
                        # Dependency crosses task group boundaries
                        dep_task = self._get_simple_task_reference(dep)
                        target_task = self._get_simple_task_reference(job_name)
                        dependencies.append(f"{dep_task} >> {target_task}")
        
        normalized_deps = self._normalize_dependencies(dependencies)
        return "\n".join(normalized_deps)
        # return "\n".join(dependencies)

    def _get_simple_task_reference(self, job_name: str) -> str:
        """Get simple task reference without nested paths for cross-group dependencies"""
        job = self.jobs.get(job_name)
        if not job:
            return job_name
        
        # Check if job has complex condition (wrapped in task group)
        if job.condition:
            trigger_rule, _ = ConditionParser.parse_condition(job.condition)
            if trigger_rule == "complex":
                return job_name  # Reference the task group itself
        
        # Regular job reference - use simple task name
        if job.is_file_watcher() and job.has_command():
            return f"{job_name}_command"
        elif job.is_file_watcher():
            return f"{job_name}"
        else:
            return job_name
      
    # def _determine_schedule_interval(self) -> str:
    #     """Determine schedule interval from job start times and date conditions"""
    #     # Look for scheduling information in jobs
    #     for job in self.jobs.values():
    #         print(f"Run Calendar: {job.run_calendar}, Start Times: {job.start_times}, Days of Week: {job.days_of_week}")
    #         if job.run_calendar:
    #             return f"{job.run_calendar}()"
    #         elif job.start_times:
    #             # Convert Autosys start time to cron format
    #             start_time = job.start_times[0]
    #             cron_schedule = self._convert_autosys_schedule_to_cron(start_time, job.days_of_week)
    #             if cron_schedule:
    #                 return f"'{cron_schedule}'"
        
    #     return "None"
    
    # def _convert_autosys_schedule_to_cron(self, start_time: str, days_of_week: List[str]) -> str:
    #     """Convert Autosys scheduling format to cron expression"""
    #     # Handle multiple start times (e.g., "02:00,14:00")
    #     start_times = [t.strip().strip('"') for t in start_time.split(',')]
        
    #     # Parse all times and extract hours/minutes
    #     hours = []
    #     minutes = []
        
    #     for time_str in start_times:
    #         hour, minute = self._parse_time(time_str)
    #         hours.append(str(hour))
    #         minutes.append(str(minute))
        
    #     # Check if all minutes are the same
    #     if len(set(minutes)) == 1:
    #         # All times have the same minute, combine hours
    #         minute_str = minutes[0]
    #         hour_str = ','.join(hours)
    #     else:
    #         # Different minutes - return first schedule for now
    #         # (Airflow doesn't support multiple schedules with different minutes in one expression)
    #         return self._convert_single_time_to_cron(start_times[0], days_of_week)
        
    #     # Convert days of week
    #     dow_str = self._convert_days_of_week(days_of_week)
        
    #     # Build cron expression: minute hour day month day_of_week
    #     return f"{minute_str} {hour_str} * * {dow_str}"

    # def _parse_time(self, start_time: str) -> tuple:
    #     """Parse time string and return (hour, minute) tuple"""
    #     if ':' in start_time:
    #         hour, minute = start_time.split(':')
    #         hour = int(hour)
    #         minute = int(minute)
    #     else:
    #         # Handle formats like "0200" or single hour
    #         if len(start_time) == 4:
    #             hour = int(start_time[:2])
    #             minute = int(start_time[2:])
    #         else:
    #             hour = int(start_time)
    #             minute = 0
    #     return hour, minute

    # def _convert_days_of_week(self, days_of_week: List[str]) -> str:
    #     """Convert days of week from Autosys format to cron"""
    #     dow_map = {
    #         'su': '0', 'mo': '1', 'tu': '2', 'we': '3', 
    #         'th': '4', 'fr': '5', 'sa': '6',
    #         'sun': '0', 'mon': '1', 'tue': '2', 'wed': '3',
    #         'thu': '4', 'fri': '5', 'sat': '6'
    #     }
        
    #     if days_of_week:
    #         # Convert day names to numbers
    #         cron_days = []
    #         for day in days_of_week:
    #             day_lower = day.lower().strip()
    #             if day_lower in dow_map:
    #                 cron_days.append(dow_map[day_lower])
            
    #         if cron_days:
    #             return ','.join(cron_days)
    #         else:
    #             return '*'
    #     else:
    #         return '*'

    # def _convert_single_time_to_cron(self, start_time: str, days_of_week: List[str]) -> str:
    #     """Convert single Autosys time to cron expression"""
    #     hour, minute = self._parse_time(start_time)
    #     dow_str = self._convert_days_of_week(days_of_week)
        
    #     # Build cron expression: minute hour day month day_of_week
    #     return f"{minute} {hour} * * {dow_str}"
