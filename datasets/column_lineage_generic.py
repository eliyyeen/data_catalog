import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    FineGrainedLineage,
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
    Upstream,
    UpstreamLineage,
)
from demo.configs import GMS_SERVER

def datasetUrn(tbl):
    res = builder.make_dataset_urn("snowflake", tbl, "PROD")
    print(res)
    return res

def fldUrn(tbl, fld):
    res = builder.make_schema_field_urn(datasetUrn(tbl), fld)
    print(res)
    return res

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter(GMS_SERVER)

# Test the connection
emitter.test_connection()

fineGrainedLineages = [
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
        upstreams=[fldUrn("bst.demo.products", "id"), fldUrn("bst.demo.products", "name"), fldUrn("bst.demo.products", "category")],
        downstreamType=FineGrainedLineageDownstreamType.FIELD_SET,
        downstreams=[fldUrn("bst.demo.best_selling_item", "product_id"), fldUrn("bst.demo.best_selling_item", "product_name"), fldUrn("bst.demo.best_selling_item", "product_category")],
        confidenceScore=0.8,
    ),
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
        upstreams=[fldUrn("bst.demo.order_items", "product_id")],
        downstreamType=FineGrainedLineageDownstreamType.FIELD,
        downstreams=[fldUrn("bst.demo.best_selling_item", "product_id")],
    )
]

upstream = Upstream(dataset=datasetUrn("bst.demo.products"), type=DatasetLineageType.COPY)

fieldLineages = UpstreamLineage(
    upstreams=[upstream], fineGrainedLineages=fineGrainedLineages
)

lineageMcp = MetadataChangeProposalWrapper(
    entityType="dataset",
    entityUrn=datasetUrn("bst.demo.best_selling_item"),
    aspectName="upstreamLineage",
    aspect=fieldLineages
)


# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter(GMS_SERVER)

# Emit metadata!
emitter.emit_mcp(lineageMcp)





#emitter.emit_mcp( get_mcp("bst.demo.products", ["id"], "bst.demo.best_selling_item", ["product_id"]) )
#emitter.emit_mcp( get_mcp("bst.demo.products", ["name", "category"], "bst.demo.best_selling_item", ["product_name", "product_category"]) )
#emitter.emit_mcp( get_mcp( "bst.demo.best_selling_item", ["product_id"], "bst.demo.order_items", ["product_id"] ))

#emitter.emit_mcp( get_mcp("bst.demo.users", ["id", "first_name", "last_name"], "bst.demo.top_10_spenders", ["user_id", "first_name", "last_name"]) )
#emitter.emit_mcp( get_mcp( "bst.demo.top_10_spenders", ["avg_sale_price"], "bst.demo.order_items", ["sale_price"]) )
