import datahub.emitter.mce_builder as builder
from datahub.emitter.rest_emitter import DatahubRestEmitter
from demo.configs import GMS_SERVER

# Construct a lineage object.
lineage_mce = builder.make_lineage_mce(
    [
        builder.make_dataset_urn("snowflake", "bst.demo.orders"), # Upstream
    ],
    builder.make_dataset_urn("snowflake", "bst.demo.orders_summary"), # Downstream
)

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter(GMS_SERVER)

# Emit metadata!
emitter.emit_mce(lineage_mce)