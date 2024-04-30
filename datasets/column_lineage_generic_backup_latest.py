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


def get_mcp(connections, upstream_table_global, downstream_table):
    fineGrainedLineages = []
    for upstream_fields, downstream_fields, upstream_table in connections:
        if len(upstream_fields) == 1 and len(downstream_fields) == 1:
            fineGrainedLineages.append( get_fine_grained_lineage_connection_one_field(upstream_table, upstream_fields[0], downstream_table, downstream_fields[0]) )
        else:
            fineGrainedLineages.append(get_fine_grained_lineage_connection_multiple_fields(upstream_table, upstream_fields, downstream_table, downstream_fields))

    upstream = Upstream(dataset=datasetUrn(upstream_table_global), type=DatasetLineageType.COPY)


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
            [["id", "name", "category"], ["product_id", "product_name", "product_category"], "bst.demo.products"],
            [["product_id"], ["product_id"], "bst.demo.order_items"]
        ], "bst.demo.products", "bst.demo.best_selling_item"
    )
)




#emitter.emit_mcp( get_mcp("bst.demo.products", ["id"], "bst.demo.best_selling_item", ["product_id"]) )
#emitter.emit_mcp( get_mcp("bst.demo.products", ["name", "category"], "bst.demo.best_selling_item", ["product_name", "product_category"]) )
#emitter.emit_mcp( get_mcp( "bst.demo.best_selling_item", ["product_id"], "bst.demo.order_items", ["product_id"] ))

#emitter.emit_mcp( get_mcp("bst.demo.users", ["id", "first_name", "last_name"], "bst.demo.top_10_spenders", ["user_id", "first_name", "last_name"]) )
#emitter.emit_mcp( get_mcp( "bst.demo.top_10_spenders", ["avg_sale_price"], "bst.demo.order_items", ["sale_price"]) )
