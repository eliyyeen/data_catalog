# inlined from metadata-ingestion/examples/library/dataset_schema.py
# Imports for urn construction utility methods
from datahub.emitter.mce_builder import make_data_platform_urn, make_dataset_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from demo.configs import GMS_SERVER
# Imports for metadata model classes
from datahub.metadata.schema_classes import (
    AuditStampClass,
    OtherSchemaClass,
    SchemaMetadataClass,
)

load_tables_airflow: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=make_dataset_urn(platform="airflow", name="load_events", env="PROD"),
    aspect=SchemaMetadataClass(
        schemaName="",  # not used
        platform=make_data_platform_urn("airflow"),
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        lastModified=AuditStampClass(
            time=1640692800000, actor="urn:li:corpuser:ingestion"
        ),
        fields=[

        ],
    ),
)

load_distribution_tables_airflow: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=make_dataset_urn(platform="airflow", name="load_distributions", env="PROD"),
    aspect=SchemaMetadataClass(
        schemaName="",  # not used
        platform=make_data_platform_urn("airflow"),
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        lastModified=AuditStampClass(
            time=1640692800000, actor="urn:li:corpuser:ingestion"
        ),
        fields=[

        ],
    ),
)


kafka_events: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=make_dataset_urn(platform="kafka", name="web_events", env="PROD"),
    aspect=SchemaMetadataClass(
        schemaName="web_events",  # not used
        platform=make_data_platform_urn("kafka"),
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        lastModified=AuditStampClass(
            time=1640692800000, actor="urn:li:corpuser:ingestion"
        ),
        fields=[

        ],
    ),
)


sparks_events: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=make_dataset_urn(platform="airflow", name="train", env="PROD"),
    aspect=SchemaMetadataClass(
        schemaName="train",  # not used
        platform=make_data_platform_urn("airflow"),
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        lastModified=AuditStampClass(
            time=1640692800000, actor="urn:li:corpuser:ingestion"
        ),
        fields=[

        ],
    ),
)

science_model: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
    entityUrn=make_dataset_urn(platform="mlModel", name="recommendations", env="PROD"),
    aspect=SchemaMetadataClass(
        schemaName="recommendations",  # not used
        platform=make_data_platform_urn("mlModel"),
        version=0,
        hash="",
        platformSchema=OtherSchemaClass(rawSchema=""),
        lastModified=AuditStampClass(
            time=1640692800000, actor="urn:li:corpuser:ingestion"
        ),
        fields=[

        ],
    ),
)




# Create rest emitter
rest_emitter = DatahubRestEmitter(gms_server=GMS_SERVER)
rest_emitter.emit(load_tables_airflow)
rest_emitter.emit(kafka_events)
rest_emitter.emit(sparks_events)
rest_emitter.emit(science_model)
rest_emitter.emit(load_distribution_tables_airflow)
