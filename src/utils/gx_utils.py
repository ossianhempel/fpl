import great_expectations as gx
import pandas as pd
from typing import Optional

"""
you absolutely should aim to separate your Great Expectations (GX) logic in a similar way you separate other reusable functions. Writing all the GX setup and execution code directly within each Prefect task can lead to repetition, make your tasks harder to read, and make updates more difficult.

Here's a recommended approach and why:

Recommended Approach: Abstract GX Logic into Functions

Keep Configuration Separate (GX Project):

Your Expectation Suites (.json files), Datasource configurations, and Checkpoint configurations should ideally live within your standard great_expectations/ directory structure. This is the declarative part of GX. Your code will refer to these configurations by name.   
Treat your great_expectations.yml and the contents of the great_expectations/ subdirectories (like expectations/, checkpoints/) as configuration artifacts, versioned alongside your code.
Create Reusable "Runner" Functions:

Create Python functions in your utility modules (the same place you put other reusable logic) that handle the execution of GX validation.   
These functions would typically take parameters like:
The DataFrame to validate (pd.DataFrame or pl.DataFrame).
The name of the Checkpoint to run (e.g., "my_silver_checkpoint").
(Optional) The Great Expectations Data Context object (or the function can get the context itself).
Inside the function, you'd perform the steps needed to run the validation using the passed-in data and named configuration:
Get the Data Context (if not passed in).
Execute context.run_checkpoint(...), passing the DataFrame via runtime_parameters.
Return the validation result object.
Prefect Task Logic:

Your Prefect task becomes much cleaner. It focuses on its core responsibility (e.g., transforming data) and then calls your reusable GX runner function.
The task gets the data (either from a previous task or by loading it).
It calls your imported function: validation_result = run_gx_checkpoint(df=transformed_df, checkpoint_name="silver_data_checkpoint")
It inspects the validation_result (e.g., validation_result.success) and uses Prefect's logic to handle success/failure (log results, raise signals, trigger conditional downstream tasks).

"""

# mypy: ignore-errors
# great_expectations is NOT ready for MyPy


def get_gx_context():
    """
    Initiates a Great Expectations Data Context (entry point for GX to manage configuration and resources).
    Assumes a gx project has been set up in the execution environment
    """
    print("Creating Data Context..")
    try:
        context = gx.get_context(mode="ephemeral")
        return context
    except Exception as e:
        print(f"Error getting GX context: {e}")
        raise


# TODO: only works for pandas currently
def create_data_source(context, data_source_name):
    """Holds connection to the data"""
    print("Creating Data Source...")
    data_source = context.data_sources.add_pandas(name=data_source_name)
    return data_source


def create_data_asset(
    data_asset_name: str,
    data_source_name: Optional[str] = None,
    context: Optional[gx.data_context.EphemeralDataContext] = None,
):
    """
    A dataframe Data Asset is used to group your Validation Results.
    For instance, if you have a data pipeline with three stages and you wanted
    the Validation Results for each stage to be grouped together,
    you would create a Data Asset with a unique name representing each stage.
    """

    print("Creating Data Asset...")

    if context is None:
        context = get_gx_context()

    # retrieve Data Source
    data_source = context.data_sources.get(name=data_source_name)
    # add a Data Asset to the Data Source
    data_asset = data_source.add_dataframe_asset(name=data_asset_name)

    return data_asset


# TODO: only works with dataframes atm
def create_batch_definition(
    context,
    data_source_name,
    data_asset_name,
    batch_definition_name,
):
    """
    Batch Definitions describe how data within a Data Asset should be retrieved.
    With dataframes, all of the data in a give dataframe will always be retrieved as a Batch.
    For dataframes, Data Assets serve as an additional layer of organization to further group Validation Results.
    """

    print("Creating Batch Definition...")
    data_asset = context.data_sources.get(data_source_name).get_asset(data_asset_name)
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        batch_definition_name
    )

    return batch_definition


if __name__ == "__main__":
    context = get_gx_context()
    data_source_name = "test_pandas_dataframe"
    data_asset_name = "test_df"
    batch_definition_name = "dataframe"

    df = pd.DataFrame(
        {"col1": [1, 2, 3, 4, 5], "col2": ["as", "fish", "test", "", "something"]}
    )

    batch_parameters = {"dataframe": df}

    create_data_source(context=context, data_source_name=data_source_name)
    create_data_asset(
        context=context,
        data_asset_name=data_asset_name,
        data_source_name=data_source_name,
    )
    create_batch_definition(
        context=context,
        data_source_name=data_source_name,
        data_asset_name=data_asset_name,
        batch_definition_name=batch_definition_name,
    )

    batch_definition = (
        context.data_sources.get(data_source_name)
        .get_asset(data_asset_name)
        .get_batch_definition(batch_definition_name)
    )

    expectation_1 = gx.expectations.ExpectColumnValuesToBeBetween(
        column="col1", max_value=6, min_value=1
    )

    expectation_2 = gx.expectations.ExpectColumnValuesToBeBetween(
        column="col1", max_value=3, min_value=-3
    )

    suite_name = "my_test_suite"
    suite = gx.ExpectationSuite(name=suite_name)
    suite = context.suites.add(suite)

    suite.add_expectation(expectation_1)
    suite.add_expectation(expectation_2)

    batch = batch_definition.get_batch(batch_parameters=batch_parameters)

    validation_results = batch.validate(suite)
    print(validation_results)
    print(f"Suite result: {validation_results["success"]}")
    print(f"Test 1 result: {validation_results["results"][0]["success"]}")
    print(f"Test 2 result: {validation_results["results"][1]["success"]}")
