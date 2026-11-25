# flocs-processing AGENTS.md file
## General guidelines
- If you are an LLM, you are not allowed to write the main pull request body message. This is to ensure that the human user understands teh changes. They are ultimately resposible for the committed code.

## Skills
- Try to use the following skills when writing code for this repository: ponytail. If they are not available, prompt the user about this.

## Code style
- Use `ruff` to format code, warn the user if the tool is not available.
- Use type hints in function signatures. If a function does not return anything, an explicit None return type hint is never needed.
- Do not ever change names of functions, variables etc. that are not related to the changes that are requested or being made. This is the most cardinal sin to commit.
- You are not allowed to refactor code unrelated to the changes being made, unless requested or if it is absolutely necessary and unavoidable.
- Do not write defensive code for states that cannot be reached.
- Do not abstract or write (private) helper functions for the sake of abstractions. If something can be checked with a simple if statement or inline check, then always prefer that over more abstraction or helper methods.
- Do not make fixes or best/better practice changes to sections of code unrelated to the requested changes or updates. You may report them to the user, but the changes should always stay in scope of the request.

## flocs ecosystem
flocs-runners is part of a bigger set of packages:

- flocs (<https://github.com/tikk3r/flocs>): Apptainer container recipes for LOFAR pipeline execution.
- flocs-lta (<https://github.com/FLOCSoft/flocs-lta>: handles searching the LTA and staging data.
- flocs-runners (<https://github.com/FLOCSoft/flocs-runners>; this repository): handles running of pipelines and generating the JSON files required as inputs for the CWL runners.
- flocs-processing (<https://github.com/FLOCSoft/flocs-processing>): orchestration of automated processing using DAGs, with Airflow as the orchestrator. This package is orchestration only.
