"""PyFlink environment adapter.

Declares the framework-specific steps needed to set up a PyFlink analysis
environment: installing PyFlink, starting a cluster, and verifying readiness.
"""

from __future__ import annotations

from typing import Any

from pyframework_pipeline.environment.planning import PlanStep


class PyFlinkEnvironmentAdapter:
    """Generates PyFlink-specific environment plan steps."""

    framework_id = "pyflink"

    ROLES = ("client", "jobmanager", "taskmanager")

    def get_plan_steps(
        self,
        platform: str,
        platform_config: dict[str, Any],
        software: dict[str, Any],
        host_refs: dict[str, Any],
    ) -> list[PlanStep]:
        """Return framework-specific plan steps for PyFlink."""
        steps: list[PlanStep] = []

        hosts_by_role = {}
        for host_entry in platform_config.get("hosts", []):
            hosts_by_role[host_entry["role"]] = host_entry["hostRef"]

        client_ref = hosts_by_role.get("client", "")
        jm_ref = hosts_by_role.get("jobmanager", client_ref)
        tm_ref = hosts_by_role.get("taskmanager", client_ref)

        # Install PyFlink on client
        steps.append(PlanStep(
            id="install-pyflink",
            kind="framework-install",
            hostRef=client_ref,
            command="pip install apache-flink",
            description="Install PyFlink on client host",
            mutatesHost=True,
            requiresApproval=True,
            rollbackHint="pip uninstall apache-flink",
        ))

        # Check Java on JM
        steps.append(PlanStep(
            id="check-java-jm",
            kind="check",
            hostRef=jm_ref,
            command="java -version",
            description="Check Java on JobManager",
        ))

        # Check Java on TM
        if tm_ref != jm_ref:
            steps.append(PlanStep(
                id="check-java-tm",
                kind="check",
                hostRef=tm_ref,
                command="java -version",
                description="Check Java on TaskManager",
            ))

        # Readiness: import PyFlink
        steps.append(PlanStep(
            id="readiness-pyflink-import",
            kind="framework-readiness",
            hostRef=client_ref,
            command='python3 -c "from pyflink.table import StreamTableEnvironment; print(\'OK\')"',
            description="Verify PyFlink can be imported",
        ))

        # Readiness: submit minimal batch job
        steps.append(PlanStep(
            id="readiness-mini-job",
            kind="framework-smoke-test",
            hostRef=client_ref,
            command="python3 -c \""
            "from pyflink.datastream import StreamExecutionEnvironment; "
            "from pyflink.table import StreamTableEnvironment; "
            "env = StreamExecutionEnvironment.get_execution_environment(); "
            "t_env = StreamTableEnvironment.create(env); "
            "t_env.execute_sql('SELECT 1').print(); "
            "print('SMOKE_TEST_OK')\"",
            description="Submit minimal PyFlink batch job",
        ))

        return steps
