# Investment as a Service

This is a Python project designed to run in Databricks to automatically buy and sell securities, including cryptocurrencies and stocks/ETFs, through different brokerages.

[![Integration](https://github.com/daneisburgh/invaas/actions/workflows/integration.yml/badge.svg)](https://github.com/daneisburgh/invaas/actions/workflows/integration.yml)
[![Deployment](https://github.com/daneisburgh/invaas/actions/workflows/deployment.yml/badge.svg)](https://github.com/daneisburgh/invaas/actions/workflows/deployment.yml)
[![semantic-release](https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--release-e10079.svg)](https://github.com/semantic-release/semantic-release)
[![Commitizen friendly](https://img.shields.io/badge/commitizen-friendly-brightgreen.svg)](http://commitizen.github.io/cz-cli/)

## Getting started

1. Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html

2. Authenticate to your Databricks workspace:

   ```
   $ databricks configure
   ```

3. To deploy a development copy of this project, type:

   ```
   $ databricks bundle deploy --target dev
   ```

   (Note that "dev" is the default target, so the `--target` parameter
   is optional here.)

   This deploys everything that's defined for this project.
   For example, the default template would deploy a job called
   `[dev yourname] invaas_job` to your workspace.
   You can find that job by opening your workpace and clicking on **Workflows**.

4. Similarly, to deploy a production copy, type:

   ```
   $ databricks bundle deploy --target prod
   ```

5. To run a job or pipeline, use the "run" comand:

   ```
   $ databricks bundle run
   ```

6. Optionally, install developer tools such as the Databricks extension for Visual Studio Code from
   https://docs.databricks.com/dev-tools/vscode-ext.html. Or read the "getting started" documentation for
   **Databricks Connect** for instructions on running the included Python code from a different IDE.

7. For documentation on the Databricks asset bundles format used
   for this project, and for CI/CD configuration, see
   https://docs.databricks.com/dev-tools/bundles/index.html.
