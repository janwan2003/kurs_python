# Warsaw Public Transport Analysis

This project utilizes the Dagster framework to analyze public transport data in Warsaw, including bus speeds, punctuality, and speed violations at various stops. 

## Why Dagster?

Dagster was chosen for this project for several reasons:

1. **Workflow Orchestration**: Dagster allows us to define, schedule, and monitor data pipelines effectively, facilitating the management of complex data workflows. This capability ensures that our data processing tasks are executed in a timely and orderly manner, making the overall system more reliable and easier to manage.

2. **Asset Management**: Dagster's asset management system lets us define and track assets, which represent meaningful elements within our data ecosystem, such as datasets, models, or analysis results. By tracking the dependencies and states of these assets, we can maintain a clear and up-to-date view of our data pipeline's structure and its outputs.

3. **Development Productivity**: Dagster enhances development productivity by providing a framework to define jobs that actualize our assets periodically. This feature allows us to automate the refreshment of data and analyses, ensuring that our insights remain relevant and based on the most current data available.

4. **Type Checking and Error Handling**: Dagster's support for type checking at the pipeline level helps catch errors early in the development process, making pipelines more robust and easier to debug. Additionally, comprehensive error handling mechanisms ensure that any issues encountered during pipeline execution can be promptly addressed, minimizing downtime and data processing errors.


## Installation

To set up and run this project, follow these steps:

1. **Clone the Repository**: Clone this repository to your local machine using `git clone`.

2. **Set up a Python Environment**: It's recommended to use a virtual environment. Create one with Python 3.10 or later:

3. **Install dependencies**: Go to bus-analysis and run pip install .[dev]

4. **Export WARSAW_API_KEY to your env**: Run export WARSAW_API_KEY="*your_key*"

5. **Run dagster**: Run dagster dev

It is not recommended to run fetching assets data as it might take a lot of time.

## Some Project Decisions

Throughout the development of this project, several key decisions were made to ensure the robustness and accuracy of our data analysis pipeline. Here are some of the notable decisions:

1. **Bus Speed Anomalies**: We determined that any instances of buses traveling at speeds over 100 km/h are to be considered anomalies. 

2. **Speed Violations Measurement**: Speed violations are assessed for all buses based on proximity to the nearest stop. 

3. **Code Quality and Consistency**: To maintain high code quality and ensure consistency across the project, we utilized pylint and black for static code analysis and auto-formatting, respectively.