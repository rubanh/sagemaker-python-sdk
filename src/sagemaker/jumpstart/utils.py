# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
"""This module contains utilities related to SageMaker JumpStart."""
from __future__ import absolute_import
import logging
import os
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse
from packaging.version import Version
import sagemaker
from sagemaker.config.config_schema import (
    MODEL_ENABLE_NETWORK_ISOLATION_PATH,
    MODEL_EXECUTION_ROLE_ARN_PATH,
    TRAINING_JOB_ENABLE_NETWORK_ISOLATION_PATH,
    TRAINING_JOB_INTER_CONTAINER_ENCRYPTION_PATH,
    TRAINING_JOB_ROLE_ARN_PATH,
)
from sagemaker.jumpstart import constants, enums
from sagemaker.jumpstart import accessors
from sagemaker.s3 import parse_s3_url
from sagemaker.jumpstart.exceptions import (
    DeprecatedJumpStartModelError,
    VulnerableJumpStartModelError,
)
from sagemaker.jumpstart.types import (
    JumpStartModelHeader,
    JumpStartModelSpecs,
    JumpStartVersionedModelId,
)
from sagemaker.session import Session
from sagemaker.config import load_sagemaker_config
from sagemaker.utils import resolve_value_from_config
from sagemaker.workflow import is_pipeline_variable

LOGGER = logging.getLogger(__name__)


def get_jumpstart_launched_regions_message() -> str:
    """Returns formatted string indicating where JumpStart is launched."""
    if len(constants.JUMPSTART_REGION_NAME_SET) == 0:
        return "JumpStart is not available in any region."
    if len(constants.JUMPSTART_REGION_NAME_SET) == 1:
        region = list(constants.JUMPSTART_REGION_NAME_SET)[0]
        return f"JumpStart is available in {region} region."

    sorted_regions = sorted(list(constants.JUMPSTART_REGION_NAME_SET))
    if len(constants.JUMPSTART_REGION_NAME_SET) == 2:
        return f"JumpStart is available in {sorted_regions[0]} and {sorted_regions[1]} regions."

    formatted_launched_regions_list = []
    for i, region in enumerate(sorted_regions):
        region_prefix = "" if i < len(sorted_regions) - 1 else "and "
        formatted_launched_regions_list.append(region_prefix + region)
    formatted_launched_regions_str = ", ".join(formatted_launched_regions_list)
    return f"JumpStart is available in {formatted_launched_regions_str} regions."


def get_jumpstart_content_bucket(
    region: str = constants.JUMPSTART_DEFAULT_REGION_NAME,
) -> str:
    """Returns regionalized content bucket name for JumpStart.

    Raises:
        RuntimeError: If JumpStart is not launched in ``region``.
    """

    if (
        constants.ENV_VARIABLE_JUMPSTART_CONTENT_BUCKET_OVERRIDE in os.environ
        and len(os.environ[constants.ENV_VARIABLE_JUMPSTART_CONTENT_BUCKET_OVERRIDE]) > 0
    ):
        bucket_override = os.environ[constants.ENV_VARIABLE_JUMPSTART_CONTENT_BUCKET_OVERRIDE]
        LOGGER.info("Using JumpStart bucket override: '%s'", bucket_override)
        return bucket_override
    try:
        return constants.JUMPSTART_REGION_NAME_TO_LAUNCHED_REGION_DICT[region].content_bucket
    except KeyError:
        formatted_launched_regions_str = get_jumpstart_launched_regions_message()
        raise ValueError(
            f"Unable to get content bucket for JumpStart in {region} region. "
            f"{formatted_launched_regions_str}"
        )


def get_formatted_manifest(
    manifest: List[Dict],
) -> Dict[JumpStartVersionedModelId, JumpStartModelHeader]:
    """Returns formatted manifest dictionary from raw manifest.

    Keys are JumpStartVersionedModelId objects, values are
    ``JumpStartModelHeader`` objects.
    """
    manifest_dict = {}
    for header in manifest:
        header_obj = JumpStartModelHeader(header)
        manifest_dict[
            JumpStartVersionedModelId(header_obj.model_id, header_obj.version)
        ] = header_obj
    return manifest_dict


def get_sagemaker_version() -> str:
    """Returns sagemaker library version.

    If the sagemaker library version has not been set, this function
    calls ``parse_sagemaker_version`` to retrieve the version and set
    the constant.
    """
    if accessors.SageMakerSettings.get_sagemaker_version() == "":
        accessors.SageMakerSettings.set_sagemaker_version(parse_sagemaker_version())
    return accessors.SageMakerSettings.get_sagemaker_version()


def parse_sagemaker_version() -> str:
    """Returns sagemaker library version. This should only be called once.

    Function reads ``__version__`` variable in ``sagemaker`` module.
    In order to maintain compatibility with the ``packaging.version``
    library, versions with fewer than 2, or more than 3, periods are rejected.
    All versions that cannot be parsed with ``packaging.version`` are also
    rejected.

    Raises:
        RuntimeError: If the SageMaker version is not readable. An exception is also raised if
        the version cannot be parsed by ``packaging.version``.
    """
    version = sagemaker.__version__
    parsed_version = None

    num_periods = version.count(".")
    if num_periods == 2:
        parsed_version = version
    elif num_periods == 3:
        trailing_period_index = version.rfind(".")
        parsed_version = version[:trailing_period_index]
    else:
        raise RuntimeError(f"Bad value for SageMaker version: {sagemaker.__version__}")

    Version(parsed_version)

    return parsed_version


def is_jumpstart_model_input(model_id: Optional[str], version: Optional[str]) -> bool:
    """Determines if `model_id` and `version` input are for JumpStart.

    This method returns True if both arguments are not None, false if both arguments
    are None, and raises an exception if one argument is None but the other isn't.

    Args:
        model_id (str): Optional. Model ID of the JumpStart model.
        version (str): Optional. Version of the JumpStart model.

    Raises:
        ValueError: If only one of the two arguments is None.
    """
    if model_id is not None or version is not None:
        if model_id is None or version is None:
            raise ValueError(
                "Must specify JumpStart `model_id` and `model_version` when getting specs for "
                "JumpStart models."
            )
        return True
    return False


def is_jumpstart_model_uri(uri: Optional[str]) -> bool:
    """Returns True if URI corresponds to a JumpStart-hosted model.

    Args:
        uri (Optional[str]): uri for inference/training job.
    """

    bucket = None
    if urlparse(uri).scheme == "s3":
        bucket, _ = parse_s3_url(uri)

    return bucket in constants.JUMPSTART_BUCKET_NAME_SET


def tag_key_in_array(tag_key: str, tag_array: List[Dict[str, str]]) -> bool:
    """Returns True if ``tag_key`` is in the ``tag_array``.

    Args:
        tag_key (str): the tag key to check if it's already in the ``tag_array``.
        tag_array (List[Dict[str, str]]): array of tags to check for ``tag_key``.
    """
    for tag in tag_array:
        if tag_key == tag["Key"]:
            return True
    return False


def get_tag_value(tag_key: str, tag_array: List[Dict[str, str]]) -> str:
    """Return the value of a tag whose key matches the given ``tag_key``.

    Args:
        tag_key (str): AWS tag for which to search.
        tag_array (List[Dict[str, str]]): List of AWS tags, each formatted as dicts.

    Raises:
        KeyError: If the number of matches for the ``tag_key`` is not equal to 1.
    """
    tag_values = [tag["Value"] for tag in tag_array if tag_key == tag["Key"]]
    if len(tag_values) != 1:
        raise KeyError(
            f"Cannot get value of tag for tag key '{tag_key}' -- found {len(tag_values)} "
            f"number of matches in the tag list."
        )

    return tag_values[0]


def add_single_jumpstart_tag(
    uri: str, tag_key: enums.JumpStartTag, curr_tags: Optional[List[Dict[str, str]]]
) -> Optional[List]:
    """Adds ``tag_key`` to ``curr_tags`` if ``uri`` corresponds to a JumpStart model.

    Args:
        uri (str): URI which may correspond to a JumpStart model.
        tag_key (enums.JumpStartTag): Custom tag to apply to current tags if the URI
            corresponds to a JumpStart model.
        curr_tags (Optional[List]): Current tags associated with ``Estimator`` or ``Model``.
    """
    if is_jumpstart_model_uri(uri):
        if curr_tags is None:
            curr_tags = []
        if not tag_key_in_array(tag_key, curr_tags):
            curr_tags.append(
                {
                    "Key": tag_key,
                    "Value": uri,
                }
            )
    return curr_tags


def get_jumpstart_base_name_if_jumpstart_model(
    *uris: Optional[str],
) -> Optional[str]:
    """Return default JumpStart base name if a URI belongs to JumpStart.

    If no URIs belong to JumpStart, return None.

    Args:
        *uris (Optional[str]): URI to test for association with JumpStart.
    """
    for uri in uris:
        if is_jumpstart_model_uri(uri):
            return constants.JUMPSTART_RESOURCE_BASE_NAME
    return None


def add_jumpstart_tags(
    tags: Optional[List[Dict[str, str]]] = None,
    inference_model_uri: Optional[str] = None,
    inference_script_uri: Optional[str] = None,
    training_model_uri: Optional[str] = None,
    training_script_uri: Optional[str] = None,
) -> Optional[List[Dict[str, str]]]:
    """Add custom tags to JumpStart models, return the updated tags.

    No-op if this is not a JumpStart model related resource.

    Args:
        tags (Optional[List[Dict[str,str]]): Current tags for JumpStart inference
            or training job. (Default: None).
        inference_model_uri (Optional[str]): S3 URI for inference model artifact.
            (Default: None).
        inference_script_uri (Optional[str]): S3 URI for inference script tarball.
            (Default: None).
        training_model_uri (Optional[str]): S3 URI for training model artifact.
            (Default: None).
        training_script_uri (Optional[str]): S3 URI for training script tarball.
            (Default: None).
    """
    warn_msg = (
        "The URI (%s) is a pipeline variable which is only interpreted at execution time. "
        "As a result, the JumpStart resources will not be tagged."
    )
    if inference_model_uri:
        if is_pipeline_variable(inference_model_uri):
            logging.warning(warn_msg, "inference_model_uri")
        else:
            tags = add_single_jumpstart_tag(
                inference_model_uri, enums.JumpStartTag.INFERENCE_MODEL_URI, tags
            )

    if inference_script_uri:
        if is_pipeline_variable(inference_script_uri):
            logging.warning(warn_msg, "inference_script_uri")
        else:
            tags = add_single_jumpstart_tag(
                inference_script_uri, enums.JumpStartTag.INFERENCE_SCRIPT_URI, tags
            )

    if training_model_uri:
        if is_pipeline_variable(training_model_uri):
            logging.warning(warn_msg, "training_model_uri")
        else:
            tags = add_single_jumpstart_tag(
                training_model_uri, enums.JumpStartTag.TRAINING_MODEL_URI, tags
            )

    if training_script_uri:
        if is_pipeline_variable(training_script_uri):
            logging.warning(warn_msg, "training_script_uri")
        else:
            tags = add_single_jumpstart_tag(
                training_script_uri, enums.JumpStartTag.TRAINING_SCRIPT_URI, tags
            )

    return tags


def update_inference_tags_with_jumpstart_training_tags(
    inference_tags: Optional[List[Dict[str, str]]], training_tags: Optional[List[Dict[str, str]]]
) -> Optional[List[Dict[str, str]]]:
    """Updates the tags for the ``sagemaker.model.Model.deploy`` command with any JumpStart tags.

    Args:
        inference_tags (Optional[List[Dict[str, str]]]): Custom tags to appy to inference job.
        training_tags (Optional[List[Dict[str, str]]]): Tags from training job.
    """
    if training_tags:
        for tag_key in enums.JumpStartTag:
            if tag_key_in_array(tag_key, training_tags):
                tag_value = get_tag_value(tag_key, training_tags)
                if inference_tags is None:
                    inference_tags = []
                if not tag_key_in_array(tag_key, inference_tags):
                    inference_tags.append({"Key": tag_key, "Value": tag_value})

    return inference_tags


def verify_model_region_and_return_specs(
    model_id: Optional[str],
    version: Optional[str],
    scope: Optional[str],
    region: str,
    tolerate_vulnerable_model: bool = False,
    tolerate_deprecated_model: bool = False,
) -> JumpStartModelSpecs:
    """Verifies that an acceptable model_id, version, scope, and region combination is provided.

    Args:
        model_id (Optional[str]): model ID of the JumpStart model to verify and
            obtains specs.
        version (Optional[str]): version of the JumpStart model to verify and
            obtains specs.
        scope (Optional[str]): scope of the JumpStart model to verify.
        region (Optional[str]): region of the JumpStart model to verify and
            obtains specs.
        tolerate_vulnerable_model (bool): True if vulnerable versions of model
            specifications should be tolerated (exception not raised). If False, raises an
            exception if the script used by this version of the model has dependencies with known
            security vulnerabilities. (Default: False).
        tolerate_deprecated_model (bool): True if deprecated models should be tolerated
            (exception not raised). False if these models should raise an exception.
            (Default: False).


    Raises:
        NotImplementedError: If the scope is not supported.
        ValueError: If the combination of arguments specified is not supported.
        VulnerableJumpStartModelError: If any of the dependencies required by the script have
            known security vulnerabilities.
        DeprecatedJumpStartModelError: If the version of the model is deprecated.
    """

    if scope is None:
        raise ValueError(
            "Must specify `model_scope` argument to retrieve model "
            "artifact uri for JumpStart models."
        )

    if scope not in constants.SUPPORTED_JUMPSTART_SCOPES:
        raise NotImplementedError(
            "JumpStart models only support scopes: "
            f"{', '.join(constants.SUPPORTED_JUMPSTART_SCOPES)}."
        )

    model_specs = accessors.JumpStartModelsAccessor.get_model_specs(
        region=region, model_id=model_id, version=version  # type: ignore
    )

    if (
        scope == constants.JumpStartScriptScope.TRAINING.value
        and not model_specs.training_supported
    ):
        raise ValueError(
            f"JumpStart model ID '{model_id}' and version '{version}' " "does not support training."
        )

    if model_specs.deprecated:
        if not tolerate_deprecated_model:
            raise DeprecatedJumpStartModelError(model_id=model_id, version=version)
        LOGGER.warning("Using deprecated JumpStart model '%s' and version '%s'.", model_id, version)

    if scope == constants.JumpStartScriptScope.INFERENCE.value and model_specs.inference_vulnerable:
        if not tolerate_vulnerable_model:
            raise VulnerableJumpStartModelError(
                model_id=model_id,
                version=version,
                vulnerabilities=model_specs.inference_vulnerabilities,
                scope=constants.JumpStartScriptScope.INFERENCE,
            )
        LOGGER.warning(
            "Using vulnerable JumpStart model '%s' and version '%s' (inference).", model_id, version
        )

    if scope == constants.JumpStartScriptScope.TRAINING.value and model_specs.training_vulnerable:
        if not tolerate_vulnerable_model:
            raise VulnerableJumpStartModelError(
                model_id=model_id,
                version=version,
                vulnerabilities=model_specs.training_vulnerabilities,
                scope=constants.JumpStartScriptScope.TRAINING,
            )
        LOGGER.warning(
            "Using vulnerable JumpStart model '%s' and version '%s' (training).", model_id, version
        )

    return model_specs


def update_dict_if_key_not_present(
    dict_to_update: dict, key_to_add: Any, value_to_add: Any
) -> dict:
    """If a key is not present in the dict, add the new (key, value) pair, and return dict."""
    if key_to_add not in dict_to_update:
        dict_to_update[key_to_add] = value_to_add

    return dict_to_update


def resolve_model_intelligent_default_field(
    field_name: str,
    field_val: Optional[Any],
    sagemaker_session: Session,
    default_value: Optional[str] = None,
) -> Any:
    """Given a field name, checks if there are intelligent defaults to set.

    For the role field, which is customer-supplied, we allow ``field_val`` to take precedence
    over intelligent defaults. For all other fields, intelligent defaults takes precedence
    over the JumpStart default fields.
    """
    # In case, sagemaker_session is None, get sagemaker_config from load_sagemaker_config()
    # to resolve value from config for the respective field_name parameter
    _sagemaker_config = load_sagemaker_config() if (sagemaker_session is None) else None

    # We allow customers to define a role which takes precedence
    # over intelligent defaults
    if field_name == "role":
        return resolve_value_from_config(
            direct_input=field_val,
            config_path=MODEL_EXECUTION_ROLE_ARN_PATH,
            default_value=default_value or sagemaker_session.get_caller_identity_arn(),
            sagemaker_session=sagemaker_session,
            sagemaker_config=_sagemaker_config,
        )

    # JumpStart Models have certain default field values. We want
    # intelligent defaults to take priority over the model-specific defaults.
    if field_name == "enable_network_isolation":
        resolved_val = resolve_value_from_config(
            direct_input=None,
            config_path=MODEL_ENABLE_NETWORK_ISOLATION_PATH,
            sagemaker_session=sagemaker_session,
            default_value=default_value,
            sagemaker_config=_sagemaker_config,
        )
        return resolved_val if resolved_val is not None else field_val

    # field is not covered by intelligent defaults so return as is
    return field_val


def resolve_estimator_intelligent_default_field(
    field_name: str,
    field_val: Optional[Any],
    sagemaker_session: Session,
    default_value: Optional[str] = None,
) -> Any:
    """Given a field name, checks if there are intelligent defaults to set.

    For the role field, which is customer-supplied, we allow ``field_val`` to take precedence
    over intelligent defaults. For all other fields, intelligent defaults takes precedence
    over the JumpStart default fields.
    """

    # Workaround for config injection if sagemaker_session is None, since in
    # that case sagemaker_session will not be initialized until
    # `_init_sagemaker_session_if_does_not_exist` is called later
    _sagemaker_config = load_sagemaker_config() if (sagemaker_session is None) else None

    # We allow customers to define a role which takes precedence
    # over intelligent defaults
    if field_name == "role":
        return resolve_value_from_config(
            direct_input=field_val,
            config_path=TRAINING_JOB_ROLE_ARN_PATH,
            default_value=default_value or sagemaker_session.get_caller_identity_arn(),
            sagemaker_session=sagemaker_session,
            sagemaker_config=_sagemaker_config,
        )

    # JumpStart Estimators have certain default field values. We want
    # intelligent defaults to take priority over the model-specific defaults.
    if field_name == "enable_network_isolation":

        resolved_val = resolve_value_from_config(
            direct_input=None,
            config_path=TRAINING_JOB_ENABLE_NETWORK_ISOLATION_PATH,
            sagemaker_session=sagemaker_session,
            default_value=default_value,
            sagemaker_config=_sagemaker_config,
        )
        return resolved_val if resolved_val is not None else field_val

    if field_name == "encrypt_inter_container_traffic":

        resolved_val = resolve_value_from_config(
            direct_input=None,
            config_path=TRAINING_JOB_INTER_CONTAINER_ENCRYPTION_PATH,
            sagemaker_session=sagemaker_session,
            default_value=default_value,
            sagemaker_config=_sagemaker_config,
        )
        return resolved_val if resolved_val is not None else field_val

    # field is not covered by intelligent defaults so return as is
    return field_val


def is_valid_model_id(
    model_id: Optional[str],
    region: Optional[str] = None,
    model_version: Optional[str] = None,
    script: enums.JumpStartScriptScope = enums.JumpStartScriptScope.INFERENCE,
) -> bool:
    """Returns True if the model ID is supported for the given script.

    Raises:
        ValueError: If the script is not supported by JumpStart.
    """
    if model_id in {None, ""}:
        return False
    if not isinstance(model_id, str):
        return False

    region = region or constants.JUMPSTART_DEFAULT_REGION_NAME
    model_version = model_version or "*"

    models_manifest_list = accessors.JumpStartModelsAccessor._get_manifest(region=region)
    model_id_set = {model.model_id for model in models_manifest_list}
    if script == enums.JumpStartScriptScope.INFERENCE:
        return model_id in model_id_set
    if script == enums.JumpStartScriptScope.TRAINING:
        return (
            model_id in model_id_set
            and accessors.JumpStartModelsAccessor.get_model_specs(
                region=region,
                model_id=model_id,
                version=model_version,
            ).training_supported
        )
    raise ValueError(f"Unsupported script: {script}")
