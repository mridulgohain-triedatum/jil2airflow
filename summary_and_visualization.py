
import streamlit as st
import pandas as pd
import tempfile
from pyvis.network import Network
import streamlit.components.v1 as components
import re
import random
from collections import defaultdict

def show_summary_and_dag(jobs_dict):
    st.subheader("📋 Job Summary")
    def job_summary_table(jobs_dict):
        rows = []
        for job in jobs_dict.values():
            rows.append({
                "Job Name": job.name,
                "Job Type": job.job_type.upper(),
                "Command": job.command[:80] + "..." if job.command and len(job.command) > 80 else job.command,
                "Box Name": job.box_name,
                "Is File Watcher": job.is_file_watcher(),
                "Has Command": job.has_command(),
                "Condition": job.condition[:80] + "..." if job.condition and len(job.condition) > 80 else job.condition,
                "Start Times": ", ".join(job.start_times),
                "Days of Week": ", ".join(job.days_of_week),
                "Calendar": job.run_calendar,
                "Profile": job.profile,
            })
        return pd.DataFrame(rows)

    summary_df = job_summary_table(jobs_dict)
    st.dataframe(summary_df, use_container_width=True)

    def show_interactive_dag(jobs_dict):
        BOX_COLORS = [
            "#FFB6C1", "#ADD8E6", "#90EE90", "#FFFF99", "#FFA07A", "#DDA0DD", "#FFDEAD",
            "#87CEFA", "#F08080", "#66CDAA", "#E6E6FA", "#FFD700", "#B0E0E6", "#F5DEB3",
            "#8FBC8F", "#FA8072", "#00CED1", "#F4A460", "#BC8F8F", "#48D1CC"
        ]

        def assign_box_colors(jobs_dict):
            box_names = sorted({job.name for job in jobs_dict.values() if job.job_type == "b"})
            random.seed(42)
            return {box: BOX_COLORS[i % len(BOX_COLORS)] for i, box in enumerate(box_names)}

        def extract_dependency_types(condition, known_jobs):
            dependencies = defaultdict(set)
            if not condition:
                return dependencies
            
            # Support both full and shorthand forms, case-insensitive
            patterns = {
                "success": r"(?:success|s)\(([^\)]+)\)",
                "failure": r"(?:failure|f)\(([^\)]+)\)",
                "exitcode": r"(?:exitcode|e)\(([^\)]+)\)"
            }
            
            for cond_type, pattern in patterns.items():
                matches = re.findall(pattern, condition, flags=re.IGNORECASE)
                for job in matches:
                    if job in known_jobs:
                        dependencies[job].add(cond_type.lower())
            
            return dependencies

        def get_edge_color(label):
            return {"success": "green", "failure": "red", "exitcode": "orange"}.get(label, "gray")

        net = Network(height="600px", width="100%", directed=True)
        net.set_options("""
        {
            "physics": {
                "barnesHut": {
                    "gravitationalConstant": -8000,
                    "centralGravity": 0.3,
                    "springLength": 100,
                    "springConstant": 0.04,
                    "damping": 0.09,
                    "avoidOverlap": 1
                }
            },
            "edges": {
                "smooth": {
                    "type": "cubicBezier",
                    "forceDirection": "horizontal",
                    "roundness": 0.4
                },
                "arrows": {
                    "to": {"enabled": true}
                }
            }
        }
        """)

        known_jobs = set(jobs_dict.keys())
        box_color_map = assign_box_colors(jobs_dict)

        for job in jobs_dict.values():
            label = f"{job.name}\n[{job.job_type.upper()}]"
            if job.job_type.lower() == "b":
                color = box_color_map.get(job.name, "#A9A9A9")
            elif job.box_name:
                color = box_color_map.get(job.box_name, "#D3D3D3")
            else:
                color = "#D3D3D3"
            tooltip = f"Command: {job.command or 'No command'}\nCondition: {job.condition or 'None'}"
            net.add_node(job.name, label=label, title=tooltip, color=color)

        for job in jobs_dict.values():
            if job.condition:
                dep_map = extract_dependency_types(job.condition, known_jobs)
                for dep_job, types in dep_map.items():
                    for cond_type in types:
                        net.add_edge(dep_job, job.name, label=cond_type, color=get_edge_color(cond_type), arrows="to")

        with tempfile.NamedTemporaryFile(mode="w", suffix=".html", delete=False, encoding="utf-8") as f:
            net.save_graph(f.name)
            html_file = f.name

        with open(html_file, "r", encoding="utf-8") as f:
            html_content = f.read()
        components.html(html_content, height=650, scrolling=True)

    st.subheader("📈 DAG Visualization")
    show_interactive_dag(jobs_dict)
