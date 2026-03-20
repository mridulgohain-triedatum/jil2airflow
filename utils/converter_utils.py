from typing import Dict, List
from autosys_job import AutosysJob
from collections import defaultdict
class Utils:

    @staticmethod
    def check_for_schedules(jobs: Dict[str, AutosysJob])-> Dict:
        jobname_to_schedule_map = defaultdict(dict)
        for job in jobs.values():
            schedule = job.run_calendar or job.start_times or job.start_mins
            if job.run_calendar or job.start_times or job.start_mins:
                jobname_to_schedule_map[job.name]["schedule"] = schedule
                jobname_to_schedule_map[job.name]["timezone"] = (job.timezone if job.timezone else "")
            if job.timezone and not (job.run_calendar and job.start_times and job.start_mins):
                jobname_to_schedule_map[job.name]["schedule"] = schedule or ""
                jobname_to_schedule_map[job.name]["timezone"] = job.timezone
        if len(jobname_to_schedule_map) > 1:
            return jobname_to_schedule_map
        else:
            return {}
    @staticmethod    
    def build_box_hierarchy(jobs: Dict[str, AutosysJob]) -> Dict[str, Dict]:
        """Build hierarchy of box jobs and their contained jobs with nesting support"""
        hierarchy = {}
        
        # First pass: identify all box jobs and their direct children
        for job_name, job in jobs.items():
            if job.is_box_job():
                hierarchy[job_name] = {
                    'job': job,
                    'children': [],
                    'parent': None,
                    'level': 0
                }
        
        # Second pass: assign children to their parent boxes
        for job_name, job in jobs.items():
            if job.box_name and job.box_name in hierarchy:
                hierarchy[job.box_name]['children'].append(job_name)
                
                # If this child is also a box, set its parent
                if job.is_box_job() and job_name in hierarchy:
                    hierarchy[job_name]['parent'] = job.box_name
        
        # Third pass: calculate nesting levels
        def calculate_level(box_name, visited=None):
            if visited is None:
                visited = set()
            
            if box_name in visited:
                return 0  # Circular reference protection
            
            visited.add(box_name)
            
            if hierarchy[box_name]['parent'] is None:
                hierarchy[box_name]['level'] = 0
            else:
                parent_level = calculate_level(hierarchy[box_name]['parent'], visited)
                hierarchy[box_name]['level'] = parent_level + 1
            
            visited.remove(box_name)
            return hierarchy[box_name]['level']
        
        # Calculate levels for all boxes
        for box_name in hierarchy:
            calculate_level(box_name)
        
        return hierarchy    
    @staticmethod
    def determine_schedule_interval(jobs: Dict[str, AutosysJob]) -> str:
        """Determine schedule interval from job start times, start mins, and date conditions"""
        for job in jobs.values():
            print(
                f"Run Calendar: {job.run_calendar}, "
                f"Start Times: {job.start_times}, "
                f"Start Mins: {job.start_mins}, "
                f"Days of Week: {job.days_of_week}"
            )
            
            # Run calendar takes precedence
            if job.run_calendar:
                return f"{job.run_calendar}()"
            
            # Ensure start_times and start_mins are not both set
            if job.start_times and job.start_mins:
                raise ValueError(f"Job {job.job_name} has both start_times and start_mins defined, which is not allowed.")
            
            if job.start_times:
                cron_schedule = Utils.convert_autosys_schedule_to_cron(job.start_times, job.days_of_week)
                if cron_schedule:
                    return f"'{cron_schedule}'"
            
            elif job.start_mins:
                cron_schedule = Utils.convert_start_mins_to_cron(job.start_mins, job.days_of_week)
                if cron_schedule:
                    return f"'{cron_schedule}'"
        
        return "None"
    
    @staticmethod
    def convert_autosys_schedule_to_cron(start_times_str: str, days_of_week: List[str]) -> str:
        """Convert Autosys start_times string to cron expression"""
        # Clean and split string
        start_times = [t.strip().strip('"') for t in start_times_str.split(',') if t.strip()]
        
        hours = []
        minutes = []
        
        for time_str in start_times:
            hour, minute = Utils.parse_time(time_str)
            hours.append(str(hour))
            minutes.append(str(minute))
        
        if len(set(minutes)) == 1:
            minute_str = minutes[0]
            hour_str = ','.join(hours)
        else:
            # Different minutes — just take the first time
            return Utils.convert_single_time_to_cron(start_times[0], days_of_week)
        
        dow_str = Utils.convert_days_of_week(days_of_week)
        return f"{minute_str} {hour_str} * * {dow_str}"
    
    @staticmethod
    def convert_start_mins_to_cron(start_mins_str: str, days_of_week: List[str]) -> str:
        """Convert Autosys start_mins string to cron expression"""
        # Clean and split
        start_mins_list = [m.strip().strip('"') for m in start_mins_str.split(',') if m.strip()]
        
        # Validate 0–59
        minute_values = []
        for m in start_mins_list:
            try:
                val = int(m)
                if 0 <= val <= 59:
                    minute_values.append(str(val))
                else:
                    raise ValueError
            except ValueError:
                raise ValueError(f"Invalid minute value '{m}' in start_mins.")
        
        minute_str = ','.join(minute_values)
        hour_str = '*'
        dow_str = Utils.convert_days_of_week(days_of_week)
        return f"{minute_str} {hour_str} * * {dow_str}"
    
    @staticmethod
    def parse_time(start_time: str) -> tuple:
        """Parse time string and return (hour, minute) tuple"""
        start_time = start_time.strip().strip('"')
        if ':' in start_time:
            hour, minute = start_time.split(':')
            hour = int(hour)
            minute = int(minute)
        else:
            if len(start_time) == 4:
                hour = int(start_time[:2])
                minute = int(start_time[2:])
            else:
                hour = int(start_time)
                minute = 0
        return hour, minute

    @staticmethod
    def convert_days_of_week(days_of_week: List[str]) -> str:
        """Convert days of week from Autosys format to cron"""
        dow_map = {
            'su': '0', 'mo': '1', 'tu': '2', 'we': '3', 
            'th': '4', 'fr': '5', 'sa': '6',
            'sun': '0', 'mon': '1', 'tue': '2', 'wed': '3',
            'thu': '4', 'fri': '5', 'sat': '6'
        }
        
        if days_of_week:
            cron_days = []
            for day in days_of_week:
                day_lower = day.lower().strip()
                if day_lower in dow_map:
                    cron_days.append(dow_map[day_lower])
            return ','.join(cron_days) if cron_days else '*'
        return '*'

    @staticmethod
    def _convert_single_time_to_cron(start_time: str, days_of_week: List[str]) -> str:
        """Convert single Autosys time to cron expression"""
        hour, minute = Utils.parse_time(start_time)
        dow_str = Utils.convert_days_of_week(days_of_week)
        return f"{minute} {hour} * * {dow_str}"