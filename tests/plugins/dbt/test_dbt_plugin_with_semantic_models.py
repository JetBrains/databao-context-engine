import shutil
from pathlib import Path

import pytest

from databao_context_engine import DatasourceType
from databao_context_engine.pluginlib.plugin_utils import execute_datasource_plugin
from databao_context_engine.plugins.dbt.dbt_plugin import DbtPlugin
from databao_context_engine.plugins.dbt.types import (
    DbtConfigFile,
    DbtContext,
    DbtSemanticDimension,
    DbtSemanticEntity,
    DbtSemanticMeasure,
    DbtSemanticModel,
)


@pytest.fixture
def dbt_target_folder_path(tmp_path):
    dbt_target_folder_path = tmp_path.joinpath("dbt_target")
    shutil.copytree(Path(__file__).parent.joinpath("data").joinpath("with_semantic_models"), dbt_target_folder_path)

    return dbt_target_folder_path


def test_dbt_plugin_with_semantic_models__build_context(dbt_target_folder_path, expected_order_payments_semantic_model):
    under_test = DbtPlugin()

    result = execute_datasource_plugin(
        under_test,
        DatasourceType(full_type="dbt"),
        {"name": "test_config", "type": "dbt", "dbt_target_folder_path": str(dbt_target_folder_path.resolve())},
        "test_config",
    )

    assert isinstance(result, DbtContext)
    assert len(result.semantic_models) == 9
    assert {model.id for model in result.semantic_models} == {
        "semantic_model.web_shop_orders.dim_customers",
        "semantic_model.web_shop_orders.dim_order_items",
        "semantic_model.web_shop_orders.dim_order_payments",
        "semantic_model.web_shop_orders.dim_order_reviews",
        "semantic_model.web_shop_orders.dim_orders",
        "semantic_model.web_shop_orders.dim_products",
        "semantic_model.web_shop_orders.dim_sellers",
        "semantic_model.web_shop_orders.fct_order_payments",
        "semantic_model.web_shop_orders.fct_sales",
    }

    order_payments_semantic_model = next(
        semantic_model
        for semantic_model in result.semantic_models
        if semantic_model.id == expected_order_payments_semantic_model.id
    )
    assert order_payments_semantic_model == expected_order_payments_semantic_model


def test_dbt_plugin_with_semantic_models__divide_context_into_chunks(dbt_target_folder_path):
    under_test = DbtPlugin()

    context = under_test.build_context(
        "dbt",
        "test_config",
        DbtConfigFile(name="test_config", type="dbt", dbt_target_folder_path=dbt_target_folder_path),
    )

    result = under_test.divide_context_into_chunks(context)

    expected_number_of_chunks = (
        len(context.semantic_models) + len(context.models) + sum(len(model.columns) for model in context.models)
    )
    assert len(result) == expected_number_of_chunks


@pytest.fixture
def expected_order_payments_semantic_model() -> DbtSemanticModel:
    return DbtSemanticModel(
        id="semantic_model.web_shop_orders.fct_order_payments",
        name="fct_order_payments",
        model="fct_order_payments",
        description="Items contained in each order. Grain is one row per order item.\n",
        entities=[
            DbtSemanticEntity(name="payment", type="primary", description=None),
            DbtSemanticEntity(name="order", type="foreign", description="Adding a description"),
        ],
        measures=[
            DbtSemanticMeasure(name="payment_count", agg="sum", description="Count of payments."),
            DbtSemanticMeasure(
                name="sum_payment", agg="sum", description="Sum of payment value for each order payment."
            ),
            DbtSemanticMeasure(
                name="avg_payment", agg="average", description="Average payment value for each order payment."
            ),
        ],
        dimensions=[DbtSemanticDimension(name="purchased_at", type="time", description=None)],
    )
