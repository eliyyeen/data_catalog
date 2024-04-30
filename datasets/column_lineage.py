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
from datahub.metadata.schema_classes import ChangeTypeClass
from demo.configs import GMS_SERVER

def datasetUrn(tbl):
    res = builder.make_dataset_urn("snowflake", tbl, "PROD")
    print(res)
    return res

def fldUrn(tbl, fld):
    res = builder.make_schema_field_urn(datasetUrn(tbl), fld)
    print(res)
    return res



fineGrainedLineages = [
    FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
        upstreams=[fldUrn("bst.demo.products", "id")],
        downstreamType=FineGrainedLineageDownstreamType.FIELD,
        downstreams=[fldUrn("bst.demo.best_selling_item", "product_id")]
    ),

]


upstream = Upstream(dataset=datasetUrn("bst.demo.products"), type=DatasetLineageType.TRANSFORMED)

fieldLineages = UpstreamLineage(
    upstreams=[upstream], fineGrainedLineages=fineGrainedLineages
)

lineageMcp = MetadataChangeProposalWrapper(
    entityType="dataset",
    changeType=ChangeTypeClass.UPSERT,
    entityUrn=datasetUrn("bst.demo.best_selling_item"),
    aspectName="upstreamLineage",
    aspect=fieldLineages
)


# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter(GMS_SERVER)

# Test the connection
emitter.test_connection()

# Emit metadata!
emitter.emit_mcp(lineageMcp)

