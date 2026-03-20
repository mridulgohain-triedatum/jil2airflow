# ---------------- refactored_streamlit_wizard_app.py ----------------
import streamlit as st
import json
import zipfile
import io
from streamlit_ace import st_ace
from jil_parser import JILParser
from airflow_dag_generator import AirflowDAGGenerator
import re
from summary_and_visualization import show_summary_and_dag
from dag_configurator import show_single_dag_configurator
import os
from typing import Dict
from autosys_job import AutosysJob
from  utils.external_dep_utils import ExternalDepUtils
from utils.converter_utils import Utils

# ----------------------------------------
# Session Init
# ----------------------------------------
def init_session():
    defaults = {
        "step": 0,
        "mode": None,
        "jil_files": [],
        "jil_content": "",
        "jobs_dict": None,
        "batch_jobs_dicts": {},
        "batch_dags": {},
        "dag_id": "autosys_converted_dag",
        "schedule": "DEFAULT",
        "dag_generated": False,
        "dag_code": "",
        "dag_code_version": 0,
        "selected_dag_to_view": None,
        "ext_dep_dict": {},
        "ext_option": None,
        "jil_files_full_name": [],
        "dep_info": {},
        "handle_ext_ref": False,
        "downstream_jil_schedule": "",
        "jobs_schedule_dict": {}
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v




init_session()
st.set_page_config(page_title="JIL to Airflow Wizard", layout="wide")
st.title("🧙‍♂️ Autosys JIL → Airflow DAG Wizard")

# ----------------------------------------
# Function to Substitute Environment Variables
# ----------------------------------------
def substitute_env_vars(command: str, env_vars: dict) -> str:
    # Substitute $var and ${var} with their values from env_vars
    def replacer(match):
        var_name = match.group(1) or match.group(2)
        return env_vars.get(var_name, match.group(0))
    return re.sub(r'\$(\w+)|\$\{(\w+)\}', replacer, command)

def get_prefixed_job_names(jobs: Dict[str, AutosysJob]) -> Dict[str, str]:
    """
    Return a dict mapping job_name -> fully prefixed name 
    including nested box hierarchy.
    """
    hierarchy = Utils.build_box_hierarchy(jobs)
    prefixed_names: Dict[str, str] = {}

    def build_prefix(job_name: str) -> str:
        job = jobs[job_name]
        if job.is_box_job() and job_name in hierarchy:
            # If it's a box, build prefix from parent chain
            parent = hierarchy[job_name]['parent']
            if parent:
                return f"{build_prefix(parent)}.{job_name}"
            else:
                return job_name
        else:
            # Regular job, include parent chain if exists
            if job.box_name and job.box_name in hierarchy:
                return f"{build_prefix(job.box_name)}.{job_name}"
            else:
                return job_name

    # Compute prefix for every job
    for job_name in jobs:
        prefixed_names[job_name] = build_prefix(job_name)

    return prefixed_names


# ----------------------------------------
# Sidebar for Environment Variables
# ----------------------------------------
with st.sidebar:
    st.header("Profile / Environment Variables")
    # Checkbox for substitution
    substitute_in_command = st.checkbox("Substitute in command", key="substitute_in_command")
    # Do NOT assign to st.session_state.substitute_in_command here
    if "env_vars" not in st.session_state:
        st.session_state.env_vars = {}
    if "env_vars_list" not in st.session_state:
        st.session_state.env_vars_list = list(st.session_state.env_vars.items())

    if st.button("➕ Add Variable"):
        # Only append if not both key and value are blank
        if not (len(st.session_state.env_vars_list) > 0 and st.session_state.env_vars_list[-1][0] == "" and st.session_state.env_vars_list[-1][1] == ""):
            st.session_state.env_vars_list.append(("", ""))

    for i, (key, val) in enumerate(st.session_state.env_vars_list):
        cols = st.columns([5, 8, 2])  # Adjusted column widths
        new_key = cols[0].text_input("Key", value=key, key=f"env_key_{i}")
        new_val = cols[1].text_input("Value", value=val, key=f"env_val_{i}")
        st.session_state.env_vars_list[i] = (new_key, new_val)  # Moved this line before delete button
        
        # Center the delete button using markdown with adjusted padding
        cols[2].markdown("<div style='text-align: center; padding-top: 28px'>", unsafe_allow_html=True)
        if cols[2].button("🗑️", key=f"del_env_{i}"):
            st.session_state.env_vars_list.pop(i)
            st.rerun()
        cols[2].markdown("</div>", unsafe_allow_html=True)

    # Only keep pairs where at least key or value is non-blank
    st.session_state.env_vars = {k: v for k, v in st.session_state.env_vars_list if k or v}
    #print(f"{st.session_state.env_vars_list} {st.session_state.env_vars}")

# ----------------------------------------
# Step 0: Mode Selection + Upload
# ----------------------------------------
if st.session_state.step == 0:
    st.header("Step 0: Select Mode and Upload File(s)")
    mode = st.radio("Choose Mode", ["Single File Mode", "Batch File Mode"])
    st.session_state.mode = "single" if "Single" in mode else "batch"
    multiple = st.session_state.mode == "batch"
    # files = st.file_uploader("Upload JIL files", type=["jil", "txt"], accept_multiple_files=multiple)
    files = st.file_uploader("Upload JIL files", type=["jil", "txt"], accept_multiple_files=True, key="step0_file_uploader")

    if files:
        st.session_state.jil_files = [os.path.splitext(file.name)[0] for file in files]
        st.session_state.jil_files_full_name = [file.name for file in files]
        if not isinstance(files, list):
            files = [files]
        parsed_files = {}
        ext_dep_dict = {}
        jobs_schedule_dict = {}
        parser = None
        if st.session_state.mode == "single":
            parser = JILParser()
        for f in files:
            try:
                if st.session_state.mode == "batch":
                    parser = JILParser()
                content = f.read().decode("utf-8")
                jobs = parser.parse_content(content)
                # Always pass env vars as job.envvars
                for job in jobs.values():
                    if hasattr(job, "command") and job.command:
                        if hasattr(job, "envvars") and isinstance(job.envvars, dict):
                            #print(f"current job.envvars={job.envvars}")
                            job.envvars.update(st.session_state.env_vars)
                            #print(f"update job.envvars={st.session_state.env_vars}")
                        else:
                            #print(f"current job.envvars={job.envvars}")
                            job.envvars = dict(st.session_state.env_vars)
                            #print(f"add job.envvars={st.session_state.env_vars}")
                # Substitute in command if checked
                if st.session_state.get("substitute_in_command", False):
                    for job in jobs.values():
                        if hasattr(job, "command") and job.command:
                            job.command = substitute_env_vars(job.command, st.session_state.env_vars)
                        if hasattr(job, "watch_file") and job.watch_file:
                            job.watch_file = substitute_env_vars(job.watch_file, st.session_state.env_vars)
                        if hasattr(job, "std_out_file") and job.std_out_file:
                            job.std_out_file = substitute_env_vars(job.std_out_file, st.session_state.env_vars)
                        if hasattr(job, "std_err_file") and job.std_err_file:
                            job.std_err_file = substitute_env_vars(job.std_err_file, st.session_state.env_vars)    
                if not jobs:
                    st.warning(f"⚠️ No jobs found in: {f.name}")
                    continue
                    
                parsed_files[f.name] = jobs
                #-----------------------------------------
                # Checking if jil has multiple schedules
                #-------------------------------------------
                job_name_to_schedule_map = Utils.check_for_schedules(jobs)
                if job_name_to_schedule_map:
                    jobs_schedule_dict[f.name] = job_name_to_schedule_map
                #-----------------End of job schedule check    
                #------------------------------------------
                # CHecking for external dependency
                #------------------------------------------
                ext_dep_to_job_map = ExternalDepUtils.extract_external_dependency_to_job_mapping(jobs)  
                if ext_dep_to_job_map:
                    #ext_dep_dict[f.name] = list(ext_dep_to_job_map.keys())
                    ext_dep_dict[f.name] = ext_dep_to_job_map
                #-----------External dependency check ends
            except Exception as e:
                st.error(f"❌ Error parsing {f.name}: {e}")	
        if parsed_files:
            if st.session_state.mode == "single":
                st.session_state.jil_content = content
                st.session_state.batch_jobs_dicts[st.session_state.jil_files_full_name[0]] = next(iter(parsed_files.values()))
                st.session_state.jobs_dict = next(iter(parsed_files.values()))
                st.session_state.dep_info[st.session_state.jil_files_full_name[0]]="downstream"                                       
            else:
                st.session_state.batch_jobs_dicts = parsed_files
            #------------------- Redirection if multiple schedules are found in a jil -----------
            if jobs_schedule_dict:
                st.session_state.jobs_schedule_dict = jobs_schedule_dict
                st.session_state.step = 50
                st.rerun()  
            #------------------- End -------------------------------------------------------
            
            #------------------- Redirecting if external dependency found-------   
            if ext_dep_dict:
                st.session_state.ext_dep_dict = ext_dep_dict
                if st.session_state.mode == "batch":
                    st.session_state.step = -1
                    st.rerun()
                else:
                    st.session_state.step = 9
                    st.rerun()
            #-------------------Redirection ends-------------
            cols = st.columns([3, 1, 3])
            with cols[0]:
                if st.button("Next ➡️", key="step0_next"):
                    st.session_state.step = 1
                    st.rerun()
    else:
        cols = st.columns([3, 1, 3])
        with cols[0]:
            st.button("Next ➡️", key="step0_next_nofile", disabled=True)

# ----------------------------------------
# Step 1 (Single): Summary + Visualization
# ----------------------------------------
elif st.session_state.step == 1 and st.session_state.mode == "single":
    show_summary_and_dag(st.session_state.jobs_dict)
    cols = st.columns([1, 1, 1])
    with cols[0]:
        if st.button("⬅️ Back", key="step1s_back"):
            st.session_state.step = 0
            st.rerun()
    with cols[1]:
        if st.button("Next ➡️", key="step1s_next"):
            st.session_state.step = 2
            st.rerun()

# ----------------------------------------
# Step 1 (Batch): Common Configuration
# ----------------------------------------
elif st.session_state.step == 1 and st.session_state.mode == "batch":
    st.header("Step 1: Common Configuration for All DAGs")
    st.session_state.schedule = st.text_input("Schedule Interval (cron/@daily etc.)", value=st.session_state.schedule)

    # Operator configuration
    operator_type = st.selectbox("Operator Type", ["SSHOperator", "KubernetesPodOperator", "SQLExecuteQueryOperator"])
    st.session_state.operator_type = operator_type
    if operator_type == "KubernetesPodOperator":
        image = st.text_input("Container Image", value="python:3.9")
        namespace = st.text_input("Namespace", value="default")
        envvars = st.text_input("Env Vars (JSON)", value=json.dumps(st.session_state.env_vars))
        resource_requests = st.text_input("Resource Requests", value="")
        resource_limits = st.text_input("Resource Limits", value="")
    elif operator_type == "SQLExecuteQueryOperator":
        db_conn_id = st.text_input("DB Connection ID (defaults to job's machine attribute if present)")
        envvars = st.text_input("Environment (JSON)", value=json.dumps(st.session_state.env_vars))
    else:
        # For SSHOperator, show SSH Connection ID but default to machine attribute if present
        ssh_conn_id = st.text_input("SSH Connection ID (defaults to job's machine attribute if present)")
        envvars = st.text_input("Environment (JSON)", value=json.dumps(st.session_state.env_vars))
    #st.session_state.dag_id = st.text_input("Base DAG ID", value=st.session_state.dag_id)
    if st.button("Generate All DAGs 🚀"):
        # Inject operator config into each job
        for fname, jobs_dict in st.session_state.batch_jobs_dicts.items():
            for job in jobs_dict.values():
                if job.has_command():
                    job.operator_type = operator_type
                    if operator_type == "KubernetesPodOperator":
                        job.command_image = image
                        job.namespace = namespace
                        job.envvars = json.loads(envvars)
                        job.resource_requests = resource_requests
                        job.resource_limits = resource_limits
                    elif operator_type == "SQLExecuteQueryOperator":
                        job.db_conn_id = db_conn_id
                        job.envvars = json.loads(envvars)
                    else:
                        job.ssh_conn_id = ssh_conn_id
                        job.envvars = json.loads(envvars)
        #extracting fully qualified job name
        external_job_to_dependent_map = next(iter(st.session_state.ext_dep_dict.values()), {})
        external_task_to_dag_id_map = {}
        dag_id_to_schedule_map = {}
        for fname, jobs_dict in st.session_state.batch_jobs_dicts.items():
            dag_id = f"{fname.split('.')[0]}"
            schedule = Utils.determine_schedule_interval(jobs_dict)
            dag_id_to_schedule_map[dag_id] = schedule
            if st.session_state.handle_ext_ref:
                full_job_names = get_prefixed_job_names(jobs_dict)
                for job_name, job in jobs_dict.items():
                    if job_name in external_job_to_dependent_map.keys():
                        external_task_to_dag_id_map[full_job_names.get(job_name, job_name)] = dag_id
                external_job_to_dependent_map = {
                    full_job_names.get(job, job): deps
                    for job, deps in external_job_to_dependent_map.items()
                    }     
        for fname, jobs_dict in st.session_state.batch_jobs_dicts.items():
            dag_id = f"{fname.split('.')[0]}"
            if st.session_state.handle_ext_ref:
                dag_code = AirflowDAGGenerator(jobs_dict, [job for sublist in st.session_state.ext_dep_dict.values() for job in sublist],
                                           st.session_state.dep_info.get(fname, None), st.session_state.schedule,
                                           external_job_to_dependent_map, external_task_to_dag_id_map, dag_id_to_schedule_map, st.session_state.downstream_jil_schedule,
                                           st.session_state.handle_ext_ref).generate_dag(dag_id, st.session_state.schedule)
            else:
                dag_code = AirflowDAGGenerator(jobs_dict, {}, None, st.session_state.schedule,
                                           {}, {}, {}, st.session_state.downstream_jil_schedule, False).generate_dag(dag_id, st.session_state.schedule)
            st.session_state.batch_dags[fname] = {
                "dag_id": dag_id,
                "code": dag_code,
                "jobs_dict": jobs_dict
            }
        st.session_state.step = 2
        st.rerun()
    cols = st.columns([1, 1, 1])
    with cols[0]:
        if st.button("⬅️ Back", key="step1b_back"):
            st.session_state.step = 0
            st.rerun()
    with cols[1]:
        if st.button("🔄 Restart Wizard", key="step2_restart"):
            env_vars = st.session_state.get("env_vars", {})
            env_vars_list = st.session_state.get("env_vars_list", [])
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.session_state.env_vars = env_vars
            st.session_state.env_vars_list = env_vars_list
            st.rerun()

# ----------------------------------------
# Step 2 (Single): Configure & Generate DAG
# ----------------------------------------
elif st.session_state.step == 2 and st.session_state.mode == "single":
    show_single_dag_configurator()
    cols = st.columns([1, 1, 1])
    with cols[0]:
        if st.button("⬅️ Back", key="step2_back"):
            st.session_state.step = 1
            st.rerun()
    with cols[1]:
        if st.button("🔄 Restart Wizard", key="step2_restart"):
            env_vars = st.session_state.get("env_vars", {})
            env_vars_list = st.session_state.get("env_vars_list", [])
            #print(f"env_vars={env_vars} env_vars_list={env_vars_list}")
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.session_state.env_vars = env_vars
            st.session_state.env_vars_list = env_vars_list
            st.rerun()

# ----------------------------------------
# Step 2 (Batch): View/Download Listing
# ----------------------------------------
elif st.session_state.step == 2 and st.session_state.mode == "batch":
    st.header("Step 2: DAG Listing")
    for fname, data in st.session_state.batch_dags.items():
        col1, col2, col3 = st.columns([4, 1, 1])
        col1.markdown(f"**{fname}**  →  `{data['dag_id']}`")
        with col2:
            if st.button("👁 View", key=f"view_{fname}"):
                st.session_state.selected_dag_to_view = fname
                st.session_state.step = 3
                st.rerun()
        with col3:
            st.download_button("⬇ Download", data=data["code"], file_name=f"{data['dag_id']}.py", mime="text/x-python")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for fname, data in st.session_state.batch_dags.items():
            zf.writestr(f"{data['dag_id']}.py", data["code"])
    st.download_button("📦 Download All DAGs as ZIP", data=buf.getvalue(), file_name="all_dags.zip", mime="application/zip")
    
    cols = st.columns([1, 1, 1])
    with cols[0]:
        if st.button("⬅️ Back", key="step2_back"):
            st.session_state.step = 0
            st.rerun()
    with cols[1]:
        if st.button("🔄 Restart Wizard", key="step2_restart"):
            env_vars = st.session_state.get("env_vars", {})
            env_vars_list = st.session_state.get("env_vars_list", [])
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.session_state.env_vars = env_vars
            st.session_state.env_vars_list = env_vars_list
            st.rerun()

# ----------------------------------------
# Step 3: View Selected DAG (Batch Mode)
# ----------------------------------------
elif st.session_state.step == 3 and st.session_state.mode == "batch":
    fname = st.session_state.selected_dag_to_view
    dag_info = st.session_state.batch_dags[fname]
    st.subheader(f"👁 Viewing DAG for: `{fname}`")
    show_summary_and_dag(dag_info["jobs_dict"])
    st.subheader("📝 DAG Code")
    st_ace(value=dag_info["code"], language="python", key=f"view_dag_{fname}")
    cols = st.columns([1, 1, 1])
    with cols[0]:
        if st.button("⬅️ Back", key="step3_back"):
            st.session_state.step = 2
            st.rerun()
    with cols[1]:
        if st.button("🔄 Restart Wizard", key="step3_restart"):
            env_vars = st.session_state.get("env_vars", {})
            env_vars_list = st.session_state.get("env_vars_list", [])
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.session_state.env_vars = env_vars
            st.session_state.env_vars_list = env_vars_list
            st.rerun()
# ----------------------------------------
# Step -1: External dependency handling in batch mode
# ----------------------------------------
elif st.session_state.step == -1 and st.session_state.mode == "batch":
    import pandas as pd
    st.header("⚠️ External dependency found!")
    ext_dep_dict = st.session_state.ext_dep_dict
    data = [{"File Name": fname, "External Dependencies": ", ".join(ext_dep_list) if ext_dep_list else "—"}
            for fname, ext_dep_list in ext_dep_dict.items()]
    df = pd.DataFrame(data)
    # Apply zebra-striping style
    def style_table(df):
        return df.style.set_properties(**{
            'background-color': '#f9f9f9',
            'color': '#000',
            'border-color': '#ddd',
            'border-width': '1px',
            'border-style': 'solid',
            'padding': '5px'
        }).apply(lambda x: ['background-color: #eef6ff' if i % 2 == 0 else '' for i in range(len(x))], axis=0)
    st.dataframe(style_table(df), use_container_width=True)
    st.error("⚠️ Remove files having external dependency!")
    if st.button("🔄 Restart Wizard", key="step_minus1_restart"):
        env_vars = st.session_state.get("env_vars", {})
        env_vars_list = st.session_state.get("env_vars_list", [])
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.session_state.env_vars = env_vars
        st.session_state.env_vars_list = env_vars_list
        st.rerun()
# ----------------------------------------
# Step 9: External dependency handling in single mode
# ----------------------------------------
elif st.session_state.step == 9 and st.session_state.mode == "single":
    import pandas as pd
    st.header("⚠️ External dependency found!")
    ext_dep_dict = st.session_state.ext_dep_dict
    data = [{"File Name": fname, "External Dependencies": ", ".join(ext_dep_list) if ext_dep_list else "—"}
            for fname, ext_dep_list in ext_dep_dict.items()]
    df = pd.DataFrame(data)
    # Apply zebra-striping style
    def style_table(df):
        return df.style.set_properties(**{
            'background-color': '#f9f9f9',
            'color': '#000',
            'border-color': '#ddd',
            'border-width': '1px',
            'border-style': 'solid',
            'padding': '5px'
        }).apply(lambda x: ['background-color: #eef6ff' if i % 2 == 0 else '' for i in range(len(x))], axis=0)
    st.dataframe(style_table(df), use_container_width=True)
    #extract schedule of the downstream job
    st.session_state.downstream_jil_schedule = Utils.determine_schedule_interval(st.session_state.jobs_dict)

    option = st.radio("Choose option to handle external dependency",
                      ["Separate - This will generate separate Dag for each uploaded JIL files",
                      "Merge - This will merge all the uploaded JIL files into a single Dag"])
    st.session_state.ext_option = "merge" if "Merge" in option else "separate"
    ext_files = st.file_uploader("Upload JIL files containing definition of external jobs", type=["jil", "txt"], accept_multiple_files=True, key="step9_file_uploader")
    st.subheader("Uploaded JIL files:")
    for fname in st.session_state.jil_files_full_name:
        st.write(f"- {fname}")
    if ext_files:
        st.session_state.jil_files.extend([os.path.splitext(file.name)[0] for file in ext_files]) #Appending new files
        st.session_state.jil_files_full_name.extend([file.name for file in ext_files])
        if not isinstance(ext_files, list):
            ext_files = [ext_files]
        parsed_files = {}
        parser = None
        if st.session_state.ext_option == "merge":
            parser = JILParser()
        for f in ext_files:
            try:
                if st.session_state.ext_option == "separate":
                    parser = JILParser()
                    st.session_state.dep_info[f.name] = "upstream"
                content = f.read().decode("utf-8")
                jobs = parser.parse_content(content)
                # Always pass env vars as job.envvars
                for job in jobs.values():
                    if hasattr(job, "command") and job.command:
                        if hasattr(job, "envvars") and isinstance(job.envvars, dict):
                            #print(f"current job.envvars={job.envvars}")
                            job.envvars.update(st.session_state.env_vars)
                            #print(f"update job.envvars={st.session_state.env_vars}")
                        else:
                            #print(f"current job.envvars={job.envvars}")
                            job.envvars = dict(st.session_state.env_vars)
                            #print(f"add job.envvars={st.session_state.env_vars}")
                # Substitute in command if checked
                if st.session_state.get("substitute_in_command", False):
                    for job in jobs.values():
                        if hasattr(job, "command") and job.command:
                            job.command = substitute_env_vars(job.command, st.session_state.env_vars)
                        if hasattr(job, "watch_file") and job.watch_file:
                            job.watch_file = substitute_env_vars(job.watch_file, st.session_state.env_vars)
                        if hasattr(job, "std_out_file") and job.std_out_file:
                            job.std_out_file = substitute_env_vars(job.std_out_file, st.session_state.env_vars)
                        if hasattr(job, "std_err_file") and job.std_err_file:
                            job.std_err_file = substitute_env_vars(job.std_err_file, st.session_state.env_vars)        
                if not jobs:
                    st.warning(f"⚠️ No jobs found in: {f.name}")
                    continue
                # ----------------------------------------
                # Start of validation
                # if mode is batch then check in each file for external dependency. If present mark as failure
                # ----------------------------------------
                #ext_dep_to_job_map = ExternalDepUtils.extract_external_dependency_to_job_mapping(jobs)
                #if st.session_state.ext_option == "separate" and ext_dep_to_job_map:
                    #st.session_state.validation_msg = f"⚠️ Job definition not found for '{' , '.join(ext_dep_to_job_map.keys())}' in JIL: '{f.name}'. \n\n Remove '{f.name}' from selection."
                    #st.session_state.step = -11
                    #st.rerun()
                # ----------------------------------------
                # End of Validation
                # ----------------------------------------                    
                parsed_files[f.name] = jobs
            except Exception as e:
                st.error(f"❌ Error parsing {f.name}: {e}")
        if parsed_files:
            if st.session_state.ext_option == "merge":
                st.session_state.jil_content = content
                for job_dict in parsed_files.values():
                    st.session_state.jobs_dict.update(job_dict)
                # ----------------------------------------
                # Start of validation
                # Check for external dependency. If present mark as failure
                # ----------------------------------------
                #ext_dep_to_job_map = ExternalDepUtils.extract_external_dependency_to_job_mapping(st.session_state.jobs_dict)
                #if ext_dep_to_job_map:
                    #st.session_state.validation_msg =f"⚠️ Job definition not found for '{' , '.join(ext_dep_to_job_map.keys())}' in JIL: '{' + '.join(parsed_files.keys())}'"
                    #st.session_state.step = 99
                    #st.rerun()                
            else:
                for file_key, jobs_dict in parsed_files.items():
                    if file_key in st.session_state.batch_jobs_dicts:
                        st.session_state.batch_jobs_dicts[file_key].update(jobs_dict)
                    else:
                        st.session_state.batch_jobs_dicts[file_key] = jobs_dict
            cols = st.columns([3, 1, 3])
            with cols[0]:
                if st.button("Next ➡️", key="step9_next"):
                    st.session_state.step = 1
                    if st.session_state.ext_option == "separate":
                        st.session_state.mode = "batch"
                        st.session_state.handle_ext_ref = True
                    st.rerun()
            with cols[1]:
                if st.button("🔄 Restart Wizard", key="step9_restart_after_upload"):
                    env_vars = st.session_state.get("env_vars", {})
                    env_vars_list = st.session_state.get("env_vars_list", [])
                    #print(f"env_vars={env_vars} env_vars_list={env_vars_list}")
                    for key in list(st.session_state.keys()):
                        del st.session_state[key]
                    st.session_state.env_vars = env_vars
                    st.session_state.env_vars_list = env_vars_list
                    st.rerun()
    else:
        cols = st.columns([3, 1, 3])
        with cols[0]:
            st.button("Next ➡️", key="step0_next_nofile", disabled=True)
        with cols[1]:
            if st.button("🔄 Restart Wizard", key="step9_restart_before_upload"):
                st.session_state.step = 1
                env_vars = st.session_state.get("env_vars", {})
                env_vars_list = st.session_state.get("env_vars_list", [])     
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                st.session_state.env_vars = env_vars
                st.session_state.env_vars_list = env_vars_list
                st.rerun()
elif st.session_state.step == 50:
    import pandas as pd
    st.header("⚠️ Multiple schedule or multiple timezone found!")
    jobs_schedule_dict = st.session_state.jobs_schedule_dict
    print(f"jobs_schedule_dict - {jobs_schedule_dict}")
    rows = []
    if jobs_schedule_dict:
        for fname, job_to_schedule_dict in jobs_schedule_dict.items():
            for job_name, schedule_list in job_to_schedule_dict.items():
                rows.append({
                    "File Name": fname,
                    "Job name": job_name,
                    "Schedule": jobs_schedule_dict.get(fname, {}).get(job_name, {}).get("schedule", "-"),
                    "Timezone": jobs_schedule_dict.get(fname, {}).get(job_name, {}).get("timezone", "-")
                })          
    df = pd.DataFrame(rows)
    # Apply zebra-striping style
    def style_table(df):
        return df.style.set_properties(**{
            'background-color': '#f9f9f9',
            'color': '#000',
            'border-color': '#ddd',
            'border-width': '1px',
            'border-style': 'solid',
            'padding': '5px'
        }).apply(lambda x: ['background-color: #eef6ff' if i % 2 == 0 else '' for i in range(len(x))], axis=0)
    st.dataframe(style_table(df), use_container_width=True)
    st.error("⚠️ Airflow does not support multiple schedule/timezone!")
    st.error("⚠️ Remove file/s having multiple schedule or multiple timezone!")
    if st.button("🔄 Restart Wizard", key="step_50_restart"):
        env_vars = st.session_state.get("env_vars", {})
        env_vars_list = st.session_state.get("env_vars_list", [])
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        st.session_state.env_vars = env_vars
        st.session_state.env_vars_list = env_vars_list
        st.rerun()     