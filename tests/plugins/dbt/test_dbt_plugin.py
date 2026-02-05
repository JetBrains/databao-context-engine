import dataclasses
import json
import shutil
from pathlib import Path

import pytest
from pydantic import ValidationError

from databao_context_engine import DatasourceType
from databao_context_engine.pluginlib.plugin_utils import execute_datasource_plugin
from databao_context_engine.plugins.dbt.dbt_plugin import DbtPlugin
from databao_context_engine.plugins.dbt.types import (
    DbtAcceptedValuesConstraint,
    DbtColumn,
    DbtContext,
    DbtMaterialization,
    DbtModel,
    DbtRelationshipConstraint,
    DbtSimpleConstraint,
)


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


def test_dbt_plugin__build_context(dbt_target_folder_path, expected_orders_model):
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

    orders_model = next(model for model in result.models if model.id == "model.toastie_winkel.orders")
    assert orders_model == expected_orders_model


def test_dbt_plugin__build_context_without_catalog(dbt_target_folder_path, expected_orders_model_without_catalog):
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

    orders_model = next(model for model in result.models if model.id == "model.toastie_winkel.orders")
    assert orders_model == expected_orders_model_without_catalog


@pytest.fixture
def expected_orders_model() -> DbtModel:
    return DbtModel(
        id="model.toastie_winkel.orders",
        name="orders",
        database="toastie_winkel",
        schema="main",
        materialization=DbtMaterialization.TABLE,
        primary_key=["order_id"],
        depends_on_nodes=["model.toastie_winkel.stg_orders", "model.toastie_winkel.stg_payments"],
        description="This table has basic information about orders, as well as some derived facts based on payments",
        columns=[
            DbtColumn(
                name="order_id",
                type="INTEGER",
                description="This is a unique identifier for an order",
                constraints=[
                    DbtSimpleConstraint(type="unique", is_enforced=True, description=""),
                    DbtSimpleConstraint(type="not_null", is_enforced=True, description=""),
                ],
            ),
            DbtColumn(
                name="customer_id",
                type="INTEGER",
                description="Foreign key to the customers table",
                constraints=[
                    DbtSimpleConstraint(type="not_null", is_enforced=True, description=""),
                    DbtRelationshipConstraint(
                        type="relationships",
                        is_enforced=True,
                        description="",
                        target_model="customers",
                        target_column="customer_id",
                    ),
                ],
            ),
            DbtColumn(
                name="order_date", type="DATE", description="Date (UTC) that the order was placed", constraints=[]
            ),
            DbtColumn(
                name="order_status",
                type="VARCHAR",
                description="""Orders can be one of the following statuses:

| status         | description                                                                                                            |
|----------------|------------------------------------------------------------------------------------------------------------------------|
| placed         | The order has been placed but has not yet left the warehouse                                                           |
| shipped        | The order has ben shipped to the customer and is currently in transit                                                  |
| completed      | The order has been received by the customer                                                                            |
| return_pending | The customer has indicated that they would like to return the order, but it has not yet been received at the warehouse |
| returned       | The order has been returned by the customer and received at the warehouse                                              |""",
                constraints=[
                    DbtAcceptedValuesConstraint(
                        type="accepted_values",
                        is_enforced=True,
                        description="",
                        accepted_values=["placed", "shipped", "completed", "return_pending", "returned"],
                    )
                ],
            ),
            DbtColumn(
                name="amount",
                type="DOUBLE",
                description="Total amount (AUD) of the order",
                constraints=[DbtSimpleConstraint(type="not_null", is_enforced=True, description="")],
            ),
            DbtColumn(
                name="credit_card_amount",
                type="DOUBLE",
                description="Amount of the order (AUD) paid for by credit card",
                constraints=[DbtSimpleConstraint(type="not_null", is_enforced=True, description="")],
            ),
            DbtColumn(
                name="coupon_amount",
                type="DOUBLE",
                description="Amount of the order (AUD) paid for by coupon",
                constraints=[DbtSimpleConstraint(type="not_null", is_enforced=True, description="")],
            ),
            DbtColumn(
                name="bank_transfer_amount",
                type="DOUBLE",
                description="Amount of the order (AUD) paid for by bank transfer",
                constraints=[DbtSimpleConstraint(type="not_null", is_enforced=True, description="")],
            ),
            DbtColumn(
                name="gift_card_amount",
                type="DOUBLE",
                description="Amount of the order (AUD) paid for by gift card",
                constraints=[DbtSimpleConstraint(type="not_null", is_enforced=True, description="")],
            ),
        ],
    )


@pytest.fixture
def expected_orders_model_without_catalog(expected_orders_model) -> DbtModel:
    return dataclasses.replace(
        expected_orders_model,
        columns=[dataclasses.replace(column, type=None) for column in expected_orders_model.columns],
    )
