import sys

from qzcli import cli


def test_blame_alias_dispatches_to_task_dimensions(monkeypatch):
    captured = {}

    def fake_cmd_task_dimensions(args):
        captured.update(
            command=args.command,
            serve=args.serve,
            page_size=args.page_size,
            api_workers=args.api_workers,
        )
        return 0

    monkeypatch.setattr(cli, "cmd_task_dimensions", fake_cmd_task_dimensions)
    monkeypatch.setattr(sys, "argv", ["qzcli", "blame", "--no-serve"])

    assert cli.main() == 0
    assert captured == {
        "command": "blame",
        "serve": False,
        "page_size": 2000,
        "api_workers": 8,
    }


def test_tasks_and_jobs_aliases_dispatch_to_task_dimensions(monkeypatch):
    seen = []

    def fake_cmd_task_dimensions(args):
        seen.append(args.command)
        return 0

    monkeypatch.setattr(cli, "cmd_task_dimensions", fake_cmd_task_dimensions)
    for command in ("tasks", "jobs"):
        monkeypatch.setattr(sys, "argv", ["qzcli", command, "--no-serve"])
        assert cli.main() == 0

    assert seen == ["tasks", "jobs"]
