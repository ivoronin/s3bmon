#!/usr/bin/env -S uv run
#
# Run this script with: `uv run main.py` (pip3 install uv)
#

"""Display S3 Batch Operations jobs in a table"""

from rich.text import Text

from textual import work
from textual.containers import VerticalScroll, Container
from textual.app import App, ComposeResult
from textual.screen import ModalScreen
from textual.widgets import DataTable, Header, Pretty, Footer, Button, Static
from textual.reactive import reactive

import botocore.exceptions

from s3bmon import aws
from s3bmon.job import Job


def humanize_num(num) -> str:
    """Humanize a number to a string with a unit"""
    for unit in ("", "K", "M"):
        if abs(num) < 1000.0:
            return f"{num:3.1f}{unit}"
        num /= 1000.0
    return f"{num:.1f}B"


STATUS_COLOR = {
    "Active": "green",
    "Cancelled": "yellow",
    "Failed": "red",
}


class Alert(ModalScreen):
    """Alert screen"""

    DEFAULT_CSS = """
    Alert {
        align: center middle;
    }

    #alert {
        width: 40%;
        height: 30%;
        padding: 0 1;
        align: center middle;
        border: thick $background 80%;
        background: $surface;
    }

    #alert Static {
        text-align: center;
    }

    #alert Button {
        align-horizontal: center;
        dock: bottom;
    }
    """

    BINDINGS = [
        ("q", "app.quit", "Quit"),
    ]

    def __init__(self, message):
        super().__init__()
        self.content = Container(
            Static(message),
            Button("Quit", variant="error"),
            id="alert",
        )

    def compose(self) -> ComposeResult:
        """Compose the screen"""
        yield self.content

    def on_button_pressed(self, _):
        """Quit the app"""
        self.app.exit()


class JobDetails(ModalScreen):
    """Job details screen"""

    TITLE = "Job Details"

    DEFAULT_CSS = """
    JobDetails {
        align: center middle;
    }

    #details {
        width: 80%;
        height: 80%;
        padding: 0 1;
        border: thick $background 80%;
        background: $surface;
    }

    #details VerticalScroll {
        padding-top: 1;
    }
    """

    BINDINGS = [
        ("escape", "dismiss", "Back to list"),
        ("q", "app.quit", "Quit"),
    ]

    def __init__(self, job_id):
        super().__init__()
        self.job_id = job_id
        self.pretty = Pretty({})
        self.content = Container(
            Header(),
            VerticalScroll(self.pretty),
            id="details",
        )
        self.content.loading = True
        self.sub_title = f"Job ID: {job_id}"

    def compose(self) -> ComposeResult:
        """Compose the screen"""
        yield self.content
        yield Footer(show_command_palette=False)

    async def on_mount(self):
        """Mount the screen"""
        self.update_details()

    @work
    async def update_details(self):
        """Update the details"""
        try:
            account_id = await aws.get_account_id()
            job = await aws.describe_job(account_id, self.job_id)
        except botocore.exceptions.BotoCoreError as exc:
            return await self.app.push_screen_wait(Alert(str(exc)))
        self.pretty.update(job)
        self.content.loading = False


class Application(App):
    """S3 Batch Operations Monitor"""

    TITLE = "S3 Batch Operations Monitor"

    DEFAULT_CSS = """
    #jobs {
        height: 100%;
    }
    """

    BINDINGS = [
        ("a", "toggle_active", "Toggle active only jobs"),
        ("r", "refresh", "Force refresh"),
        ("q", "quit", "Quit"),
    ]

    active_only = reactive(False)
    jobs = reactive({})

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table = DataTable(zebra_stripes=True, cursor_type="row")
        self.content = Container(self.table, id="jobs")
        self.content.loading = True
        self.table.add_column("JOB ID", key="job_id")
        self.table.add_column("DESCRIPTION", key="description")
        self.table.add_column("STATUS", key="status")
        self.table.add_column("CREATION TIME", key="creation_time")
        self.table.add_column(Text("TOTAL", justify="right"), key="total")
        self.table.add_column(Text("%SUCC", justify="right"), key="success_percentage")
        self.table.add_column(Text("%FAIL", justify="right"), key="failure_percentage")
        self.table.add_column(Text("OBJ/HR", justify="right"), key="tasks_per_hour")
        self.table.add_column(Text("ACT", justify="right"), key="elapsed_hours")
        self.table.add_column("ESTIMATED DONE", key="eta")

    def compose(self) -> ComposeResult:
        """Compose the widgets"""
        yield Header()
        yield self.content
        yield Footer(show_command_palette=False)

    def watch_active_only(self, _: bool) -> None:
        """Watch for changes in active_only"""
        self.content.loading = True
        self.update_jobs()

    def action_refresh(self):
        """Refresh the table"""
        self.content.loading = True
        self.update_jobs()

    def action_toggle_active(self):
        """Toggle active only jobs"""
        self.active_only = not self.active_only

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Show job details"""
        self.push_screen(JobDetails(event.row_key.value))

    @work(exclusive=True)
    async def update_jobs(self) -> list[tuple]:
        """Get the rows for the table"""
        try:
            account_id = await aws.get_account_id()
            items = await aws.list_jobs(account_id)
        except botocore.exceptions.BotoCoreError as exc:
            return await self.push_screen_wait(Alert(str(exc)))

        self.sub_title = f"Account ID: {account_id}"

        new_jobs = {}
        for item in items:
            job = Job(item)
            if self.active_only and not job.is_active:
                continue
            eta = job.eta
            new_jobs[job.id] = (
                job.id[:8],
                job.description or "-",
                Text(job.status, style=STATUS_COLOR.get(job.status, "white")),
                job.creation_time.strftime("%d-%m-%y %H:%M"),
                Text(humanize_num(job.total_tasks), justify="right"),
                Text(f"{job.success_ratio*100:.2f}", justify="right"),
                Text(f"{job.failure_ratio*100:.2f}", justify="right"),
                Text(humanize_num(job.tasks_per_hour), justify="right"),
                Text(f"{round(job.elapsed_hours, 1)}H", justify="right"),
                eta and eta.strftime("%d-%m-%y %H:%M") or "-",
            )

        self.jobs = new_jobs
        self.content.loading = False
        self.table.refresh()

    async def watch_jobs(self):
        """Update the table"""
        for key, values in self.jobs.items():
            if key not in self.table.rows:
                self.table.add_row(*values, key=key)
            else:
                row_idx = self.table.get_row_index(key)
                for col_idx, value in enumerate(values):
                    self.table.update_cell_at((row_idx, col_idx), value)

        row_keys = list(self.table.rows.keys())
        for key in row_keys:
            if key not in self.jobs:
                self.table.remove_row(key)

        self.table.sort("creation_time", reverse=True)

    async def on_mount(self) -> None:
        """Mount the app"""
        self.update_jobs()
        self.set_interval(60, self.update_jobs)
