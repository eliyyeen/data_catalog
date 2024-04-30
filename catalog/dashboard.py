from typing import List

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.dashboard import DashboardInfoClass
from datahub.metadata.schema_classes import ChangeAuditStampsClass
from demo.configs import GMS_SERVER

emitter = DatahubRestEmitter(GMS_SERVER)


def create_dashboard(d_name, desc):
    charts_in_dashboard: List[str] = [
        builder.make_chart_urn(platform="looker", name=d_name)
    ]
    last_modified = ChangeAuditStampsClass()
    dashboard_info = DashboardInfoClass(
        title=d_name,
        description=desc,
        lastModified=last_modified,
        charts=charts_in_dashboard,
    )
    chart_info_mcp = MetadataChangeProposalWrapper(
        entityUrn=builder.make_dashboard_urn(platform="looker", name=d_name),
        aspect=dashboard_info,
    )
    emitter.emit_mcp(chart_info_mcp)


create_dashboard("Dashboard", "Dashboard")
create_dashboard("Recommendations", "Recommendations dashboard")