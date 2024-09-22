"""This module contains the Job class that represents an S3 Batch Operations job."""

import datetime

INACTIVE_STATUSES = ["Complete", "Cancelled", "Failed"]


class Job:
    """Represents an S3 Batch Operations job"""

    def __init__(self, job):
        self._job = job

    @property
    def status(self):
        """Get the job status"""
        return self._job["Status"]

    @property
    def total_tasks(self):
        """Get the total number of tasks"""
        return self._job["ProgressSummary"]["TotalNumberOfTasks"]

    @property
    def completed_tasks(self):
        """Get the number of completed tasks"""
        return (
            self._job["ProgressSummary"]["NumberOfTasksSucceeded"]
            + self._job["ProgressSummary"]["NumberOfTasksFailed"]
        )

    @property
    def completed_ratio(self):
        """Get the percentage of completed tasks"""
        return self.total_tasks and self.completed_tasks / self.total_tasks or 0

    @property
    def failed_tasks(self):
        """Get the number of failed tasks"""
        return self._job["ProgressSummary"]["NumberOfTasksFailed"]

    @property
    def failure_ratio(self):
        """Get the percentage of failed tasks"""
        return self.total_tasks and self.failed_tasks / self.total_tasks or 0

    @property
    def successful_tasks(self):
        """Get the number of done tasks"""
        return self._job["ProgressSummary"]["NumberOfTasksSucceeded"]

    @property
    def success_ratio(self):
        """Get the percentage of successful tasks"""
        return self.total_tasks and self.successful_tasks / self.total_tasks or 0

    @property
    def is_active(self):
        """Check if the job is active"""
        return self._job["Status"] not in INACTIVE_STATUSES

    @property
    def id(self):
        """Get the job ID"""
        return self._job["JobId"]

    @property
    def description(self):
        """Get the job description"""
        return self._job["Description"]

    @property
    def creation_time(self):
        """Get the job creation time"""
        return self._job["CreationTime"].replace(tzinfo=None)

    @property
    def elapsed_time(self):
        """Get the elapsed time since the job started"""
        # When job is active, ElapsedTimeInActiveSeconds is always 0. Probably that's a bug.
        if self.is_active:
            return datetime.datetime.now() - self.creation_time
        return datetime.timedelta(
            seconds=self._job["ProgressSummary"]["Timers"][
                "ElapsedTimeInActiveSeconds"
            ]
        )

    @property
    def elapsed_seconds(self):
        """Get the elapsed time since the job started"""
        return self.elapsed_time.total_seconds()

    @property
    def elapsed_hours(self):
        """Get the elapsed time since the job started in hours"""
        return self.elapsed_seconds / 3600

    @property
    def eta(self):
        """Calculate the estimated time of completion"""
        if not self.is_active or self.total_tasks == 0:
            return None

        return self.creation_time + self.elapsed_time / (self.completed_ratio)

    @property
    def tasks_per_hour(self):
        """Calculate the number of tasks completed per hour"""
        return self.elapsed_hours and int(self.completed_tasks / self.elapsed_hours) or 0
