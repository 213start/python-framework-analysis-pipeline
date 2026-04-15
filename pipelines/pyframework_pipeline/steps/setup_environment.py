"""Step 3: framework environment setup.

This step owns generating environment plans, validating environment records,
and producing readiness reports. The implementation lives in
``pyframework_pipeline.environment.planning`` and
``pyframework_pipeline.environment.records``.

CLI entry points:

    python -m pyframework_pipeline environment plan <project.yaml> --platform arm
    python -m pyframework_pipeline environment validate <run-dir>
"""
