import logging

from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from demo.configs import GMS_SERVER

# Imports for metadata model classes
from datahub.metadata.schema_classes import TagPropertiesClass

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Create rest emitter
rest_emitter = DatahubRestEmitter(gms_server=GMS_SERVER)

def create_tag(t_name, desc):
    tag_urn = make_tag_urn(t_name)
    tag_properties_aspect = TagPropertiesClass(
        name=t_name,
        description=desc,
    )
    event: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
        entityUrn=tag_urn,
        aspect=tag_properties_aspect,
    )
    rest_emitter.emit(event)
    log.info(f"Created tag {tag_urn}")

create_tag("PII", "Personal Identifiable Information")
create_tag("CustomerID", "Customer ID")
create_tag("First Name", "First name")
create_tag("Last Name", "Last name")

create_tag("Identity", "Identity info")
create_tag("Top Users", "top spending users")
create_tag("Best Seller", "best selling product")