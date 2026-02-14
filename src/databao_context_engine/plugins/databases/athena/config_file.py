from __future__ import annotations

from typing import Annotated, Any

from pydantic import BaseModel, Field

from databao_context_engine.pluginlib.config import ConfigPropertyAnnotation
from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabaseConfigFile


class AwsProfileAuth(BaseModel):
    profile_name: str


class AwsIamAuth(BaseModel):
    aws_access_key_id: Annotated[str, ConfigPropertyAnnotation(secret=True)]
    aws_secret_access_key: Annotated[str, ConfigPropertyAnnotation(secret=True)]
    session_token: str | None = None


class AwsAssumeRoleAuth(BaseModel):
    role_arn: str | None = None
    role_session_name: str | None = None
    source_profile: str | None = None


class AwsDefaultAuth(BaseModel):
    # Uses environment variables, instance profile, ECS task role
    pass


class AthenaConnectionProperties(BaseModel):
    region_name: str
    schema_name: str = "default"
    catalog: str | None = "awsdatacatalog"
    work_group: str | None = None
    s3_staging_dir: str | None = None
    auth: AwsIamAuth | AwsProfileAuth | AwsDefaultAuth | AwsAssumeRoleAuth
    additional_properties: dict[str, Any] = {}

    def to_athena_kwargs(self) -> dict[str, Any]:
        kwargs = self.model_dump(
            exclude={
                "additional_properties": True,
            },
            exclude_none=True,
        )
        auth_fields = kwargs.pop("auth", {})
        kwargs.update(auth_fields)
        kwargs.update(self.additional_properties)
        return kwargs


class AthenaConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="athena")
    connection: AthenaConnectionProperties
