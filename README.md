# databricks_intro_sds
Introduction to Databrick for ML on Superdatascience plaftorm.

Databricks Free Edition:
https://docs.databricks.com/aws/en/getting-started/free-edition

Install Databricks CLI Windows:
winget search databricks
winget install Databricks.DatabricksCLI

databricks -v

Install uv Windows Powershell:
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex" 

Activate environment:
uv venv -p 3.11 .venv
source .venv/Scripts/activate

Add this to your ~/.bashrc or run it manually for now:
export PATH="/c/Users/your_user/AppData/Roaming/Python/Scripts:$PATH"

Connect to Databricks host:
databricks auth login --host https://dbc-xxxxx-xxxx.cloud.databricks.com/