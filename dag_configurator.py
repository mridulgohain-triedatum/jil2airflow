
import streamlit as st
from streamlit_ace import st_ace
from airflow_dag_generator import AirflowDAGGenerator
import json

def show_single_dag_configurator():
    st.header("Step 3: ⚙️ Configure Jobs and Generate DAG")
    jobs_dict = st.session_state.jobs_dict

    st.subheader("🛠️ Job Configuration")
    for job in jobs_dict.values():
        default_image = "python:3.9-slim" if "python" in (job.command or "").lower() else "ubuntu:20.04"
        job.envvars = job.envvars or {}
        job.fs_conn_id = getattr(job, "fs_conn_id", "fs_default")

        with st.expander(f"🔧 `{job.name}` [{job.job_type.upper()}]"):
            if job.has_command():

                if job.is_sql_job():
                    job.db_conn_id = st.text_input("DB Connection ID", value=job.db_conn_id or job.machine or "db_default", key=f"DB_{job.name}")
                    job.operator_type = "SQLExecuteQueryOperator"
                else:
                    job.operator_type = st.selectbox(
                                        "Operator Type",
                                        ["SSHOperator", "KubernetesPodOperator"],
                                        #index=1 if job.operator_type == "SSHOperator" else 0,
                                        key=f"operator_{job.name}"
                                    )
                    job.command = st.text_input("Command", value=job.command, key=f"cmd_{job.name}")                
                    if job.operator_type == "KubernetesPodOperator":
                        job.command_image = st.text_input("Container Image", value=default_image, key=f"img_{job.name}")
                        job.namespace = st.text_input("Namespace", value='default', key=f"ns_{job.name}")
                        envvars = st.text_input("Env Vars (JSON)", value=json.dumps(job.envvars), key=f"env_{job.name}")
                        job.envvars = json.loads(envvars) if envvars else {}
                        job.resource_requests = st.text_input("Resource Requests (json)", value=job.resource_requests, key=f"requests_{job.name}")
                        job.resource_limits = st.text_input("Resource Limits (json)", value=job.resource_limits, key=f"limits_{job.name}")
                    else:
                        job.ssh_conn_id = st.text_input("SSH Connection ID", value=job.ssh_conn_id or job.machine or "ssh_default", key=f"ssh_{job.name}")
                        envvars = st.text_input("Environment (JSON)", value=json.dumps(job.envvars), key=f"env_{job.name}")
                        job.envvars = json.loads(envvars) if envvars else {}
            if job.is_file_watcher():
                job.fs_conn_id = st.text_input("Filesystem Connection ID", value=job.fs_conn_id or job.machine or "fs_default", key=f"fsid_{job.name}")
                job.watch_interval = st.number_input("Watch Interval (seconds)", value=job.watch_interval, key=f"watch_interval_{job.name}")

    st.subheader("🧪 DAG Settings")
    st.session_state.dag_id = st.text_input("DAG ID", value=st.session_state.jil_files[0])
    st.session_state.schedule = st.text_input("Schedule Interval", value=st.session_state.schedule)

    if st.button("🚀 Generate DAG"):
        generator =  AirflowDAGGenerator(jobs_dict, {}, None, st.session_state.schedule, {}, {}, {},
                                         st.session_state.downstream_jil_schedule, st.session_state.handle_ext_ref)
        dag_code = generator.generate_dag(st.session_state.dag_id, st.session_state.schedule)
        st.session_state.dag_code = dag_code
        st.session_state.dag_generated = True
        st.session_state.dag_code_version += 1

    if st.session_state.dag_generated:
        st.subheader("📝 Generated DAG Code")
        dag_editor_key = f"dag_code_editor_v{st.session_state.dag_code_version}"
        updated_code = st_ace(
            value=st.session_state.dag_code,
            language="python",
            theme="github",
            keybinding="vscode",
            font_size=14,
            tab_size=4,
            show_gutter=True,
            show_print_margin=False,
            wrap=True,
            auto_update=True,
            key=dag_editor_key
        )
        st.download_button("⬇️ Download DAG", data=st.session_state.dag_code, file_name=f"{st.session_state.dag_id}.py", mime="text/x-python")
