# Open-Data-Manager
Manages ETL of data 

# WIP
This is not quite ready for release yet, All major bugs known major bugs are now fixed. I would still say things are not yet ready for production. 
However, there's still plenty that you can learn from. The ETL scripts for Binance work just fine and you shouldn't have issues if you use the default database set up. 


# Features
* Pull OHLCV data, including data cleaning and ETL process
* Support for Binanace Exchange
* Support for PSQL and Timescale DB (you can connect any other SQL database you prefer)
* Load SQL querires directly into Pandas DataFrames 


# Feature Road Map
* Add more crytpo exchanges using CCXT
* Add Alpaca Markets for stock trading (support for other stock platfroms must be done individually)
* Add support for streaming data from websockets
* Add support for Kakfa for streaming
* Add support for Redis for caching
* Add Airflow integration for ETL and scheduling
* Add DBT support for data warehousing pipelines
* Add docker container
* Higher speed batch inserts 
* Dedciated Keys/password manager 


# Conventional Commit Types

## 🔧 Core Conventional Commit Types

| Type         | Description                                                                       |
|--------------|-----------------------------------------------------------------------------------|
| **feat**     | A new feature                                                                     |
| **fix**      | A bug fix                                                                         |
| **docs**     | Documentation only changes                                                        |
| **style**    | Changes that do not affect the meaning of the code (white-space, formatting, etc) |
| **refactor** | A code change that neither fixes a bug nor adds a feature                         |
| **perf**     | A code change that improves performance                                           |
| **test**     | Adding or correcting tests                                                        |
| **build**    | Changes that affect the build system or external dependencies (e.g., npm)         |
| **ci**       | Changes to CI configuration files and scripts (e.g., GitHub Actions, Travis)      |
| **chore**    | Other changes that don't modify src or test files (e.g., release notes, configs)  |
| **revert**   | Reverts a previous commit                                                         |

## 🧪 Extended/Optional Types

| Type         | Description                                                         |
|--------------|---------------------------------------------------------------------|
| **wip**      | Work in progress; not ready for production                          |
| **merge**    | A merge commit                                                      |
| **hotfix**   | A quick fix for a critical issue                                    |
| **security** | Security-related changes                                            |
| **deps**     | Updating or pinning dependencies                                    |
| **infra**    | Infrastructure-related changes (e.g., Terraform, Dockerfiles)       |
| **ux**       | Changes affecting user experience (not necessarily features)        |
| **i18n**     | Internationalization and localization changes                       |
| **release**  | Version bumps, changelog updates, tagging, etc.                     |
| **env**      | Environment-related changes (e.g., `.env` files, deployment configs)|

## 📚 Optional Scopes

You can add an optional scope in parentheses to clarify what part of the app is affected.

# Contact Me

If you'd like to get in touch, feel free to reach out via email or connect with me on LinkedIn:

- **Email:** [carljames1321@gmail.com](mailto:carljames1321@gmail.com)
- **LinkedIn:** [https://www.linkedin.com/in/jchanley/](https://www.linkedin.com/in/jchanley/)