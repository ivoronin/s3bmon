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
from textual.widgets import DataTable, Header, Pretty, Footer, Button, Static, Input
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


class Filter(ModalScreen):
    """Alert screen"""

    DEFAULT_CSS = """
    Filter {
        align: center middle;
    }

    #filter {
        width: 40%;
        height: 5;
        padding: 0 1;
        align: center middle;
        border: thick $background 80%;
        background: $surface;
    }

    #filter Input {
        width: 100%;
    }
    """

    def __init__(self):
        super().__init__()
        self.content = Container(
            Input(self.app.filter, placeholder="Enter a description substring"),
            id="filter",
        )

    def compose(self) -> ComposeResult:
        """Compose the screen"""
        yield self.content

    def on_input_submitted(self, event: Input.Submitted):
        """Set the filter and dismiss"""
        self.app.filter = event.value  # pylint: disable=attribute-defined-outside-init
        self.dismiss()


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
        ("/", "push_screen('filter')", "Filter"),
        ("q", "quit", "Quit"),
    ]

    active_only = reactive(False)
    jobs = reactive([])
    filter = reactive("")

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

    async def watch_filter(self, _: str) -> None:
        """Watch for changes in filter"""
        return await self.watch_jobs()

    async def watch_active_only(self, _: bool) -> None:
        """Watch for changes in active_only"""
        return await self.watch_jobs()

    def action_refresh(self):
        """Refresh the table"""
        self.content.loading = True
        self.fetch_jobs()

    def action_toggle_active(self):
        """Toggle active only jobs"""
        self.active_only = not self.active_only

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Show job details"""
        self.push_screen(JobDetails(event.row_key.value))

    @work(exclusive=True)
    async def fetch_jobs(self) -> list[tuple]:
        """Get the rows for the table"""
        try:
            account_id = await aws.get_account_id()
            self.jobs = [Job(i) for i in await aws.list_jobs(account_id)]
        except botocore.exceptions.BotoCoreError as exc:
            return await self.push_screen_wait(Alert(str(exc)))
        await self.watch_jobs()
        self.content.loading = False

    async def watch_jobs(self):
        """Watch for changes in jobs"""
        self.sub_title = f"*{self.filter}* " if self.filter else ""
        self.sub_title += "Active only" if self.active_only else ""

        job_rows = {}
        for job in self.jobs:
            if self.active_only and not job.is_active:
                continue
            if self.filter and self.filter.lower() not in job.description.lower():
                continue
            eta = job.eta
            job_rows[job.id] = (
                job.id[:8],
                job.description or "-",
                Text(job.status, style=STATUS_COLOR.get(job.status, "white")),
                job.creation_time.strftime("%y-%m-%d %H:%M"),
                Text(humanize_num(job.total_tasks), justify="right"),
                Text(f"{job.success_ratio*100:.2f}", justify="right"),
                Text(f"{job.failure_ratio*100:.2f}", justify="right"),
                Text(humanize_num(job.tasks_per_hour), justify="right"),
                Text(f"{round(job.elapsed_hours, 1)}H", justify="right"),
                eta and eta.strftime("%y-%m-%d %H:%M") or "-",
            )

        for key, values in job_rows.items():
            if key not in self.table.rows:
                self.table.add_row(*values, key=key)
            else:
                row_idx = self.table.get_row_index(key)
                for col_idx, value in enumerate(values):
                    self.table.update_cell_at((row_idx, col_idx), value)

        row_keys = list(self.table.rows.keys())
        for key in row_keys:
            if key not in job_rows:
                self.table.remove_row(key)

        self.table.sort("creation_time", reverse=True)

    async def on_mount(self) -> None:
        """Mount the app"""
        self.install_screen(Filter, name="filter")
        self.fetch_jobs()
        self.set_interval(60, self.fetch_jobs)
