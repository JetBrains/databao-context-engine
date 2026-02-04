import dataclasses
import json
import shutil
from pathlib import Path

import pytest
from pydantic import ValidationError

from databao_context_engine import DatasourceType
from databao_context_engine.pluginlib.plugin_utils import execute_datasource_plugin
from databao_context_engine.plugins.dbt.dbt_plugin import DbtPlugin
from databao_context_engine.plugins.dbt.types import DbtColumn, DbtContext, DbtMaterialization, DbtModel


@pytest.fixture
def dbt_target_folder_path(tmp_path):
    dbt_target_folder_path = tmp_path.joinpath("dbt_target")
    shutil.copytree(Path(__file__).parent.joinpath("data"), dbt_target_folder_path)

    return dbt_target_folder_path


def test_dbt_plugin__build_context_fails_with_wrong_target_folder(tmp_path):
    under_test = DbtPlugin()

    with pytest.raises(ValueError) as e:
        execute_datasource_plugin(
            under_test,
            DatasourceType(full_type="dbt"),
            {
                "name": "test_config",
                "type": "dbt",
                "dbt_target_folder_path": str(tmp_path.joinpath("invalid_folder_path").resolve()),
            },
            "test_config",
        )

    assert 'Invalid "dbt_target_folder_path": not a directory' in str(e.value)


def test_dbt_plugin__build_context_fails_with_missing_manifest_file(tmp_path):
    target_folder = tmp_path.joinpath("folder_without_manifest")
    target_folder.mkdir()
    target_folder.joinpath("catalog.json").touch()

    under_test = DbtPlugin()

    with pytest.raises(ValueError) as e:
        execute_datasource_plugin(
            under_test,
            DatasourceType(full_type="dbt"),
            {
                "name": "test_config",
                "type": "dbt",
                "dbt_target_folder_path": str(target_folder.resolve()),
            },
            "test_config",
        )

    assert 'Invalid "dbt_target_folder_path": missing manifest.json file' in str(e.value)


def test_dbt_plugin__build_context_fails_with_invalid_manifest_file(tmp_path):
    target_folder = tmp_path.joinpath("folder_with_invalid_manifest")
    target_folder.mkdir()
    manifest_file = target_folder.joinpath("manifest.json")
    manifest_file.write_text(
        json.dumps({"nodes": {"my_invalid_model": {"resource_type": "model", "name": "my_invalid_model"}}})
    )

    under_test = DbtPlugin()

    with pytest.raises(ValidationError):
        execute_datasource_plugin(
            under_test,
            DatasourceType(full_type="dbt"),
            {
                "name": "test_config",
                "type": "dbt",
                "dbt_target_folder_path": str(target_folder.resolve()),
            },
            "test_config",
        )


def test_dbt_plugin__build_context(dbt_target_folder_path, expected_customers_model):
    under_test = DbtPlugin()

    result = execute_datasource_plugin(
        under_test,
        DatasourceType(full_type="dbt"),
        {"name": "test_config", "type": "dbt", "dbt_target_folder_path": str(dbt_target_folder_path.resolve())},
        "test_config",
    )

    assert isinstance(result, DbtContext)
    assert len(result.models) == 5
    assert {model.id for model in result.models} == {
        "model.toastie_winkel.customers",
        "model.toastie_winkel.orders",
        "model.toastie_winkel.stg_customers",
        "model.toastie_winkel.stg_payments",
        "model.toastie_winkel.stg_orders",
    }

    customers_model = next(model for model in result.models if model.id == "model.toastie_winkel.customers")
    assert customers_model == expected_customers_model


def test_dbt_plugin__build_context_without_catalog(dbt_target_folder_path, expected_customers_model_without_catalog):
    # Deletes the catalog file
    dbt_target_folder_path.joinpath("catalog.json").unlink()

    under_test = DbtPlugin()

    result = execute_datasource_plugin(
        under_test,
        DatasourceType(full_type="dbt"),
        {"name": "test_config", "type": "dbt", "dbt_target_folder_path": str(dbt_target_folder_path.resolve())},
        "test_config",
    )

    assert isinstance(result, DbtContext)
    assert len(result.models) == 5
    assert {model.id for model in result.models} == {
        "model.toastie_winkel.customers",
        "model.toastie_winkel.orders",
        "model.toastie_winkel.stg_customers",
        "model.toastie_winkel.stg_payments",
        "model.toastie_winkel.stg_orders",
    }

    customers_model = next(model for model in result.models if model.id == "model.toastie_winkel.customers")
    assert customers_model == expected_customers_model_without_catalog


@pytest.fixture
def expected_customers_model() -> DbtModel:
    return DbtModel(
        id="model.toastie_winkel.customers",
        name="customers",
        database="toastie_winkel",
        schema="main",
        materialization=DbtMaterialization.TABLE,
        primary_key=["customer_id"],
        depends_on_nodes=[
            "model.toastie_winkel.stg_customers",
            "model.toastie_winkel.stg_orders",
            "model.toastie_winkel.stg_payments",
        ],
        description="This table has basic information about a customer, as well as some derived facts based on a customer's orders",
        columns=[
            DbtColumn(name="customer_id", type="INTEGER", description="This is a unique identifier for a customer"),
            DbtColumn(name="first_name", type="VARCHAR", description="Customer's first name. PII."),
            DbtColumn(name="last_name", type="VARCHAR", description="Customer's last name. PII."),
            DbtColumn(name="first_order", type="DATE", description="Date (UTC) of a customer's first order"),
            DbtColumn(
                name="most_recent_order", type="DATE", description="Date (UTC) of a customer's most recent order"
            ),
            DbtColumn(
                name="number_of_orders",
                type="BIGINT",
                description="Count of the number of orders a customer has placed",
            ),
            DbtColumn(name="total_order_amount", type=None, description="Total value (AUD) of a customer's orders"),
            DbtColumn(
                name="customer_source",
                type="VARCHAR",
                description="""Customer acquisition channels indicate how customers discovered or were referred to the business:

| source        | description                                                                                        |
|---------------|----------------------------------------------------------------------------------------------------|
| organic       | Customers who found the business through unpaid/natural search results or direct navigation        |
| paid_search   | Customers acquired through paid search advertising campaigns (Google Ads, Bing Ads, etc.)          |
| paid_social   | Customers acquired through paid social media advertising (Facebook, Instagram, LinkedIn ads, etc.) |
| referral      | Customers who were referred by existing customers, partners, or other external sources             |
| null or empty | Customer acquisition source is unknown, should be considered as organic                            |""",
            ),
        ],
    )


@pytest.fixture
def expected_customers_model_without_catalog(expected_customers_model) -> DbtModel:
    return dataclasses.replace(
        expected_customers_model,
        columns=[dataclasses.replace(column, type=None) for column in expected_customers_model.columns],
    )
