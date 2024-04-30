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

# Create an emitter to the GMS REST API.
emitter = DatahubRestEmitter(GMS_SERVER)

# Test the connection
emitter.test_connection()

def get_fine_grained_lineage_connection2(upstream_table, upstream_field, downstream_table, downstream_field):
    return FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
        upstreams=[fldUrn(upstream_table, upstream_field)],
        downstreamType=FineGrainedLineageDownstreamType.FIELD,
        downstreams=[fldUrn(downstream_table, downstream_field)]
    )



def get_mcp2(upstream_table, upstream_fields, downstream_table, downstream_fields):

    fineGrainedLineages = [
        get_fine_grained_lineage_connection(upstream_table, up_field, downstream_table, down_field) for up_field, down_field in zip(upstream_fields, downstream_fields)
    ]
    upstream = Upstream(dataset=datasetUrn(upstream_table), type=DatasetLineageType.COPY)
    fieldLineages = UpstreamLineage(
        upstreams=[upstream], fineGrainedLineages=fineGrainedLineages
    )
    lineageMcp = MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=datasetUrn(downstream_table),
        aspectName="upstreamLineage",
        aspect=fieldLineages
    )
    return lineageMcp

def get_mcp3(connections, upstream_table, downstream_table):
    fineGrainedLineages = [
        get_fine_grained_lineage_connection(upstream_table, up_field, downstream_table, down_field) for up_field, down_field, upstream_table in connections
    ]

    upstream = Upstream(dataset=datasetUrn(upstream_table), type=DatasetLineageType.COPY)
    fieldLineages = UpstreamLineage(
        upstreams=[upstream], fineGrainedLineages=fineGrainedLineages
    )

    lineageMcp = MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=datasetUrn(downstream_table),
        aspectName="upstreamLineage",
        aspect=fieldLineages
    )
    return lineageMcp


def get_fine_grained_lineage_connection_one_field(upstream_table, upstream_field, downstream_table, downstream_field):
    return FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
        upstreams=[fldUrn(upstream_table, upstream_field)],
        downstreamType=FineGrainedLineageDownstreamType.FIELD,
        downstreams=[fldUrn(downstream_table, downstream_field)]
    )


def get_fine_grained_lineage_connection_multiple_fields(upstream_table, upstream_fields, downstream_table, downstream_fields):
    return FineGrainedLineage(
        upstreamType=FineGrainedLineageUpstreamType.FIELD_SET,
        upstreams=[fldUrn(upstream_table, upstream_field) for upstream_field in upstream_fields],
        downstreamType=FineGrainedLineageDownstreamType.FIELD_SET,
        downstreams=[fldUrn(downstream_table, downstream_field) for downstream_field in downstream_fields]
    )


def get_mcp_one_field(upstream_table, upstream_field, downstream_table, downstream_field):
    fineGrainedLineages = [
        get_fine_grained_lineage_connection_one_field(upstream_table, upstream_field, downstream_table, downstream_field)
    ]

    upstream = Upstream(dataset=datasetUrn(upstream_table), type=DatasetLineageType.TRANSFORMED)
    fieldLineages = UpstreamLineage(
        upstreams=[upstream], fineGrainedLineages=fineGrainedLineages
    )

    lineageMcp = MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=datasetUrn(downstream_table),
        aspectName="upstreamLineage",
        aspect=fieldLineages
    )
    return lineageMcp

def get_mcp_multiple_fields(connections, downstream_table):
    fineGrainedLineages = [
        get_fine_grained_lineage_connection_one_field(upstream_table, up_field, downstream_table, down_field) for up_field, down_field, upstream_table in connections
    ]

    upstream = Upstream(dataset=datasetUrn(upstream_table), type=DatasetLineageType.COPY)
    fieldLineages = UpstreamLineage(
        upstreams=[upstream], fineGrainedLineages=fineGrainedLineages
    )

    fineGrainedLineages = [
        get_fine_grained_lineage_connection_one_field(upstream_table, upstream_field, downstream_table, downstream_field) for upstream_field, downstream_field in zip(upstream_fields, downstream_fields)
    ]

    upstream = Upstream(dataset=datasetUrn(upstream_table), type=DatasetLineageType.TRANSFORMED)
    fieldLineages = UpstreamLineage(
        upstreams=[upstream], fineGrainedLineages=fineGrainedLineages
    )

    lineageMcp = MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=datasetUrn(downstream_table),
        aspectName="upstreamLineage",
        aspect=fieldLineages
    )
    return lineageMcp


emitter.emit_mcp( get_mcp(
        [
            ["id", "product_id", "bst.demo.products"],
            ["name", "product_name", "bst.demo.products"],
            ["category", "product_category", "bst.demo.products"],
            ["product_id", "product_category", "bst.demo.order_items"]
        ], "bst.demo.products", "bst.demo.best_selling_item"
    )
)




#emitter.emit_mcp( get_mcp("bst.demo.products", ["id"], "bst.demo.best_selling_item", ["product_id"]) )
#emitter.emit_mcp( get_mcp("bst.demo.products", ["name", "category"], "bst.demo.best_selling_item", ["product_name", "product_category"]) )
#emitter.emit_mcp( get_mcp( "bst.demo.best_selling_item", ["product_id"], "bst.demo.order_items", ["product_id"] ))

#emitter.emit_mcp( get_mcp("bst.demo.users", ["id", "first_name", "last_name"], "bst.demo.top_10_spenders", ["user_id", "first_name", "last_name"]) )
#emitter.emit_mcp( get_mcp( "bst.demo.top_10_spenders", ["avg_sale_price"], "bst.demo.order_items", ["sale_price"]) )

emitter.emit_mcp( get_mcp(
        [
            ["id", "user_id", "bst.demo.users"],
            ["first_name", "first_name", "bst.demo.users"],
            ["last_name", "last_name", "bst.demo.users"],
            ["sale_price", "avg_sale_price", "bst.demo.order_items"]
        ], "bst.demo.users", "bst.demo.top_10_spenders"
    )
)