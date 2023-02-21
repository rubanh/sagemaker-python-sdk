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
from __future__ import absolute_import

import os
import pytest
from mock import MagicMock


@pytest.fixture(scope="module")
def base_config_with_schema():
    return {"SchemaVersion": "1.0"}


@pytest.fixture(scope="module")
def valid_vpc_config():
    return {"SecurityGroupIds": ["sg123"], "Subnets": ["subnet-1234"]}


@pytest.fixture(scope="module")
def valid_iam_role_arn():
    return "arn:aws:iam::366666666666:role/IMRole"


@pytest.fixture(scope="module")
def valid_feature_group_config(valid_iam_role_arn):
    s3_storage_config = {"KmsKeyId": "somekmskeyid"}
    security_storage_config = {"KmsKeyId": "someotherkmskeyid"}
    online_store_config = {"SecurityConfig": security_storage_config}
    offline_store_config = {"S3StorageConfig": s3_storage_config}
    return {
        "OnlineStoreConfig": online_store_config,
        "OfflineStoreConfig": offline_store_config,
        "RoleArn": valid_iam_role_arn,
    }


@pytest.fixture(scope="module")
def valid_edge_packaging_config(valid_iam_role_arn):
    return {
        "OutputConfig": {"KmsKeyId": "somekeyid"},
        "RoleArn": valid_iam_role_arn,
    }


@pytest.fixture(scope="module")
def valid_model_config(valid_iam_role_arn, valid_vpc_config):
    return {
        "EnableNetworkIsolation": True,
        "ExecutionRoleArn": valid_iam_role_arn,
        "VpcConfig": valid_vpc_config,
    }


@pytest.fixture(scope="module")
def valid_model_package_config(valid_iam_role_arn):
    transform_job_definition = {
        "TransformOutput": {"KmsKeyId": "somerandomkmskeyid"},
        "TransformResources": {"VolumeKmsKeyId": "somerandomkmskeyid"},
    }
    validation_specification = {
        "ValidationProfiles": [{"TransformJobDefinition": transform_job_definition}],
        "ValidationRole": valid_iam_role_arn,
    }
    return {"ValidationSpecification": validation_specification}


@pytest.fixture(scope="module")
def valid_processing_job_config(valid_iam_role_arn, valid_vpc_config):
    network_config = {"EnableNetworkIsolation": True, "VpcConfig": valid_vpc_config}
    dataset_definition = {
        "AthenaDatasetDefinition": {"KmsKeyId": "somekmskeyid"},
        "RedshiftDatasetDefinition": {
            "KmsKeyId": "someotherkmskeyid",
            "ClusterRoleArn": valid_iam_role_arn,
        },
    }
    return {
        "NetworkConfig": network_config,
        "ProcessingInputs": [{"DatasetDefinition": dataset_definition}],
        "ProcessingOutputConfig": {"KmsKeyId": "somerandomkmskeyid"},
        "ProcessingResources": {"ClusterConfig": {"VolumeKmsKeyId": "somerandomkmskeyid"}},
        "RoleArn": valid_iam_role_arn,
    }


@pytest.fixture(scope="module")
def valid_training_job_config(valid_iam_role_arn, valid_vpc_config):
    return {
        "EnableNetworkIsolation": True,
        "OutputDataConfig": {"KmsKeyId": "somekmskey"},
        "ResourceConfig": {"VolumeKmsKeyId": "somevolumekmskey"},
        "RoleArn": valid_iam_role_arn,
        "VpcConfig": valid_vpc_config,
    }


@pytest.fixture(scope="module")
def valid_pipeline_config(valid_iam_role_arn):
    return {"RoleArn": valid_iam_role_arn}


@pytest.fixture(scope="module")
def valid_compilation_job_config(valid_iam_role_arn, valid_vpc_config):
    return {
        "OutputConfig": {"KmsKeyId": "somekmskey"},
        "RoleArn": valid_iam_role_arn,
        "VpcConfig": valid_vpc_config,
    }


@pytest.fixture(scope="module")
def valid_transform_job_config():
    return {
        "DataCaptureConfig": {"KmsKeyId": "somekmskey"},
        "TransformOutput": {"KmsKeyId": "somekmskey2"},
        "TransformResources": {"VolumeKmsKeyId": "somevolumekmskey"},
    }


@pytest.fixture(scope="module")
def valid_automl_config(valid_iam_role_arn, valid_vpc_config):
    return {
        "AutoMLJobConfig": {
            "SecurityConfig": {"VolumeKmsKeyId": "somevolumekmskey", "VpcConfig": valid_vpc_config}
        },
        "OutputDataConfig": {"KmsKeyId": "somekmskey"},
        "RoleArn": valid_iam_role_arn,
    }


@pytest.fixture(scope="module")
def valid_endpointconfig_config():
    return {
        "AsyncInferenceConfig": {"OutputConfig": {"KmsKeyId": "somekmskey"}},
        "DataCaptureConfig": {"KmsKeyId": "somekmskey2"},
        "KmsKeyId": "somekmskey3",
        "ProductionVariants": [{"CoreDumpConfig": {"KmsKeyId": "somekmskey4"}}],
        "ShadowProductionVariants": [{"CoreDumpConfig": {"KmsKeyId": "somekmskey5"}}],
    }


@pytest.fixture(scope="module")
def valid_monitoring_schedule_config(valid_iam_role_arn, valid_vpc_config):
    network_config = {"EnableNetworkIsolation": True, "VpcConfig": valid_vpc_config}
    return {
        "MonitoringScheduleConfig": {
            "MonitoringJobDefinition": {
                "MonitoringOutputConfig": {"KmsKeyId": "somekmskey"},
                "MonitoringResources": {"ClusterConfig": {"VolumeKmsKeyId": "somevolumekmskey"}},
                "NetworkConfig": network_config,
                "RoleArn": valid_iam_role_arn,
            }
        }
    }


@pytest.fixture(scope="module")
def valid_config_with_all_the_scopes(
    valid_feature_group_config,
    valid_monitoring_schedule_config,
    valid_endpointconfig_config,
    valid_automl_config,
    valid_transform_job_config,
    valid_compilation_job_config,
    valid_pipeline_config,
    valid_model_config,
    valid_model_package_config,
    valid_processing_job_config,
    valid_training_job_config,
    valid_edge_packaging_config,
):
    return {
        "FeatureGroup": valid_feature_group_config,
        "MonitoringSchedule": valid_monitoring_schedule_config,
        "EndpointConfig": valid_endpointconfig_config,
        "AutoML": valid_automl_config,
        "TransformJob": valid_transform_job_config,
        "CompilationJob": valid_compilation_job_config,
        "Pipeline": valid_pipeline_config,
        "Model": valid_model_config,
        "ModelPackage": valid_model_package_config,
        "ProcessingJob": valid_processing_job_config,
        "TrainingJob": valid_training_job_config,
        "EdgePackagingJob": valid_edge_packaging_config,
    }


@pytest.fixture(scope="module")
def s3_resource_mock():
    return MagicMock(name="s3")


@pytest.fixture(scope="module")
def get_data_dir():
    return os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "config")
