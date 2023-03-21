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
"""This module contains/maintains the schema of the Config file."""
from __future__ import absolute_import, print_function

SECURITY_GROUP_IDS = "SecurityGroupIds"
SUBNETS = "Subnets"
ENABLE_NETWORK_ISOLATION = "EnableNetworkIsolation"
VOLUME_KMS_KEY_ID = "VolumeKmsKeyId"
KMS_KEY_ID = "KmsKeyId"
ROLE_ARN = "RoleArn"
TAGS = "Tags"
KEY = "Key"
VALUE = "Value"
EXECUTION_ROLE_ARN = "ExecutionRoleArn"
CLUSTER_ROLE_ARN = "ClusterRoleArn"
VPC_CONFIG = "VpcConfig"
OUTPUT_DATA_CONFIG = "OutputDataConfig"
AUTO_ML_JOB_CONFIG = "AutoMLJobConfig"
ASYNC_INFERENCE_CONFIG = "AsyncInferenceConfig"
OUTPUT_CONFIG = "OutputConfig"
PROCESSING_OUTPUT_CONFIG = "ProcessingOutputConfig"
CLUSTER_CONFIG = "ClusterConfig"
NETWORK_CONFIG = "NetworkConfig"
CORE_DUMP_CONFIG = "CoreDumpConfig"
DATA_CAPTURE_CONFIG = "DataCaptureConfig"
MONITORING_OUTPUT_CONFIG = "MonitoringOutputConfig"
RESOURCE_CONFIG = "ResourceConfig"
SCHEMA_VERSION = "SchemaVersion"
DATASET_DEFINITION = "DatasetDefinition"
ATHENA_DATASET_DEFINITION = "AthenaDatasetDefinition"
REDSHIFT_DATASET_DEFINITION = "RedshiftDatasetDefinition"
MONITORING_JOB_DEFINITION = "MonitoringJobDefinition"
SAGEMAKER = "SageMaker"
PYTHON_SDK = "PythonSDK"
MODULES = "Modules"
OFFLINE_STORE_CONFIG = "OfflineStoreConfig"
ONLINE_STORE_CONFIG = "OnlineStoreConfig"
S3_STORAGE_CONFIG = "S3StorageConfig"
SECURITY_CONFIG = "SecurityConfig"
TRANSFORM_JOB_DEFINITION = "TransformJobDefinition"
MONITORING_SCHEDULE_CONFIG = "MonitoringScheduleConfig"
MONITORING_RESOURCES = "MonitoringResources"
PROCESSING_RESOURCES = "ProcessingResources"
PRODUCTION_VARIANTS = "ProductionVariants"
TRANSFORM_OUTPUT = "TransformOutput"
TRANSFORM_RESOURCES = "TransformResources"
VALIDATION_ROLE = "ValidationRole"
VALIDATION_SPECIFICATION = "ValidationSpecification"
VALIDATION_PROFILES = "ValidationProfiles"
PROCESSING_INPUTS = "ProcessingInputs"
FEATURE_GROUP = "FeatureGroup"
EDGE_PACKAGING_JOB = "EdgePackagingJob"
TRAINING_JOB = "TrainingJob"
PROCESSING_JOB = "ProcessingJob"
MODEL_PACKAGE = "ModelPackage"
MODEL = "Model"
MONITORING_SCHEDULE = "MonitoringSchedule"
ENDPOINT_CONFIG = "EndpointConfig"
AUTO_ML = "AutoML"
COMPILATION_JOB = "CompilationJob"
CUSTOM_PARAMETERS = "CustomParameters"
PIPELINE = "Pipeline"
TRANSFORM_JOB = "TransformJob"
PROPERTIES = "properties"
PATTERN_PROPERTIES = "patternProperties"
TYPE = "type"
OBJECT = "object"
ADDITIONAL_PROPERTIES = "additionalProperties"
ENABLE_INTER_CONTAINER_TRAFFIC_ENCRYPTION = "EnableInterContainerTrafficEncryption"


def _simple_path(*args: str):
    """Appends an arbitrary number of strings to use as path constants"""
    return ".".join(args)


COMPILATION_JOB_VPC_CONFIG_PATH = _simple_path(SAGEMAKER, COMPILATION_JOB, VPC_CONFIG)
COMPILATION_JOB_KMS_KEY_ID_PATH = _simple_path(
    SAGEMAKER, COMPILATION_JOB, OUTPUT_CONFIG, KMS_KEY_ID
)
COMPILATION_JOB_OUTPUT_CONFIG_PATH = _simple_path(SAGEMAKER, COMPILATION_JOB, OUTPUT_CONFIG)
COMPILATION_JOB_ROLE_ARN_PATH = _simple_path(SAGEMAKER, COMPILATION_JOB, ROLE_ARN)
TRAINING_JOB_ENABLE_NETWORK_ISOLATION_PATH = _simple_path(
    SAGEMAKER, TRAINING_JOB, ENABLE_NETWORK_ISOLATION
)
TRAINING_JOB_KMS_KEY_ID_PATH = _simple_path(SAGEMAKER, TRAINING_JOB, OUTPUT_DATA_CONFIG, KMS_KEY_ID)
TRAINING_JOB_RESOURCE_CONFIG_PATH = _simple_path(SAGEMAKER, TRAINING_JOB, RESOURCE_CONFIG)
TRAINING_JOB_OUTPUT_DATA_CONFIG_PATH = _simple_path(SAGEMAKER, TRAINING_JOB, OUTPUT_DATA_CONFIG)
TRAINING_JOB_VOLUME_KMS_KEY_ID_PATH = _simple_path(
    SAGEMAKER, TRAINING_JOB, RESOURCE_CONFIG, VOLUME_KMS_KEY_ID
)
TRAINING_JOB_ROLE_ARN_PATH = _simple_path(SAGEMAKER, TRAINING_JOB, ROLE_ARN)
TRAINING_JOB_VPC_CONFIG_PATH = _simple_path(SAGEMAKER, TRAINING_JOB, VPC_CONFIG)
TRAINING_JOB_SECURITY_GROUP_IDS_PATH = _simple_path(
    TRAINING_JOB_VPC_CONFIG_PATH, SECURITY_GROUP_IDS
)
TRAINING_JOB_SUBNETS_PATH = _simple_path(TRAINING_JOB_VPC_CONFIG_PATH, SUBNETS)
EDGE_PACKAGING_KMS_KEY_ID_PATH = _simple_path(
    SAGEMAKER, EDGE_PACKAGING_JOB, OUTPUT_CONFIG, KMS_KEY_ID
)
EDGE_PACKAGING_OUTPUT_CONFIG_PATH = _simple_path(SAGEMAKER, EDGE_PACKAGING_JOB, OUTPUT_CONFIG)
EDGE_PACKAGING_ROLE_ARN_PATH = _simple_path(SAGEMAKER, EDGE_PACKAGING_JOB, ROLE_ARN)
ENDPOINT_CONFIG_DATA_CAPTURE_KMS_KEY_ID_PATH = _simple_path(
    SAGEMAKER, ENDPOINT_CONFIG, DATA_CAPTURE_CONFIG, KMS_KEY_ID
)
ENDPOINT_CONFIG_DATA_CAPTURE_PATH = _simple_path(SAGEMAKER, ENDPOINT_CONFIG, DATA_CAPTURE_CONFIG)
ENDPOINT_CONFIG_ASYNC_INFERENCE_PATH = _simple_path(
    SAGEMAKER, ENDPOINT_CONFIG, ASYNC_INFERENCE_CONFIG
)
ENDPOINT_CONFIG_PRODUCTION_VARIANTS_PATH = _simple_path(
    SAGEMAKER, ENDPOINT_CONFIG, PRODUCTION_VARIANTS
)
ENDPOINT_CONFIG_ASYNC_KMS_KEY_ID_PATH = _simple_path(
    SAGEMAKER, ENDPOINT_CONFIG, ASYNC_INFERENCE_CONFIG, OUTPUT_CONFIG, KMS_KEY_ID
)
ENDPOINT_CONFIG_KMS_KEY_ID_PATH = _simple_path(SAGEMAKER, ENDPOINT_CONFIG, KMS_KEY_ID)
FEATURE_GROUP_ONLINE_STORE_CONFIG_PATH = _simple_path(SAGEMAKER, FEATURE_GROUP, ONLINE_STORE_CONFIG)
FEATURE_GROUP_OFFLINE_STORE_CONFIG_PATH = _simple_path(
    SAGEMAKER, FEATURE_GROUP, OFFLINE_STORE_CONFIG
)
FEATURE_GROUP_ROLE_ARN_PATH = _simple_path(SAGEMAKER, FEATURE_GROUP, ROLE_ARN)
FEATURE_GROUP_OFFLINE_STORE_KMS_KEY_ID_PATH = _simple_path(
    FEATURE_GROUP_OFFLINE_STORE_CONFIG_PATH, S3_STORAGE_CONFIG, KMS_KEY_ID
)
FEATURE_GROUP_ONLINE_STORE_KMS_KEY_ID_PATH = _simple_path(
    FEATURE_GROUP_ONLINE_STORE_CONFIG_PATH, SECURITY_CONFIG, KMS_KEY_ID
)
AUTO_ML_OUTPUT_CONFIG_PATH = _simple_path(SAGEMAKER, AUTO_ML, OUTPUT_DATA_CONFIG)
AUTO_ML_KMS_KEY_ID_PATH = _simple_path(SAGEMAKER, AUTO_ML, OUTPUT_DATA_CONFIG, KMS_KEY_ID)
AUTO_ML_VOLUME_KMS_KEY_ID_PATH = _simple_path(
    SAGEMAKER, AUTO_ML, AUTO_ML_JOB_CONFIG, SECURITY_CONFIG, VOLUME_KMS_KEY_ID
)
AUTO_ML_ROLE_ARN_PATH = _simple_path(SAGEMAKER, AUTO_ML, ROLE_ARN)
AUTO_ML_VPC_CONFIG_PATH = _simple_path(
    SAGEMAKER, AUTO_ML, AUTO_ML_JOB_CONFIG, SECURITY_CONFIG, VPC_CONFIG
)
AUTO_ML_JOB_CONFIG_PATH = _simple_path(SAGEMAKER, AUTO_ML, AUTO_ML_JOB_CONFIG)
MONITORING_JOB_DEFINITION_PREFIX = _simple_path(
    SAGEMAKER, MONITORING_SCHEDULE, MONITORING_SCHEDULE_CONFIG, MONITORING_JOB_DEFINITION
)
MONITORING_JOB_OUTPUT_KMS_KEY_ID_PATH = _simple_path(
    MONITORING_JOB_DEFINITION_PREFIX, MONITORING_OUTPUT_CONFIG, KMS_KEY_ID
)
MONITORING_JOB_VOLUME_KMS_KEY_ID_PATH = _simple_path(
    MONITORING_JOB_DEFINITION_PREFIX, MONITORING_RESOURCES, CLUSTER_CONFIG, VOLUME_KMS_KEY_ID
)
MONITORING_JOB_NETWORK_CONFIG_PATH = _simple_path(MONITORING_JOB_DEFINITION_PREFIX, NETWORK_CONFIG)
MONITORING_JOB_ENABLE_NETWORK_ISOLATION_PATH = _simple_path(
    MONITORING_JOB_DEFINITION_PREFIX, NETWORK_CONFIG, ENABLE_NETWORK_ISOLATION
)
MONITORING_JOB_VPC_CONFIG_PATH = _simple_path(
    MONITORING_JOB_DEFINITION_PREFIX, NETWORK_CONFIG, VPC_CONFIG
)
MONITORING_JOB_SECURITY_GROUP_IDS_PATH = _simple_path(
    MONITORING_JOB_VPC_CONFIG_PATH, SECURITY_GROUP_IDS
)
MONITORING_JOB_SUBNETS_PATH = _simple_path(MONITORING_JOB_VPC_CONFIG_PATH, SUBNETS)
MONITORING_JOB_ROLE_ARN_PATH = _simple_path(MONITORING_JOB_DEFINITION_PREFIX, ROLE_ARN)
PIPELINE_ROLE_ARN_PATH = _simple_path(SAGEMAKER, PIPELINE, ROLE_ARN)
PIPELINE_TAGS_PATH = _simple_path(SAGEMAKER, PIPELINE, TAGS)
TRANSFORM_OUTPUT_KMS_KEY_ID_PATH = _simple_path(
    SAGEMAKER, TRANSFORM_JOB, TRANSFORM_OUTPUT, KMS_KEY_ID
)
TRANSFORM_RESOURCES_VOLUME_KMS_KEY_ID_PATH = _simple_path(
    SAGEMAKER, TRANSFORM_JOB, TRANSFORM_RESOURCES, VOLUME_KMS_KEY_ID
)
TRANSFORM_JOB_KMS_KEY_ID_PATH = _simple_path(
    SAGEMAKER, TRANSFORM_JOB, DATA_CAPTURE_CONFIG, KMS_KEY_ID
)
TRANSFORM_JOB_VOLUME_KMS_KEY_ID_PATH = _simple_path(
    SAGEMAKER, TRANSFORM_JOB, TRANSFORM_RESOURCES, VOLUME_KMS_KEY_ID
)
MODEL_VPC_CONFIG_PATH = _simple_path(SAGEMAKER, MODEL, VPC_CONFIG)
MODEL_ENABLE_NETWORK_ISOLATION_PATH = _simple_path(SAGEMAKER, MODEL, ENABLE_NETWORK_ISOLATION)
MODEL_EXECUTION_ROLE_ARN_PATH = _simple_path(SAGEMAKER, MODEL, EXECUTION_ROLE_ARN)
PROCESSING_JOB_ENABLE_NETWORK_ISOLATION_PATH = _simple_path(
    SAGEMAKER, PROCESSING_JOB, NETWORK_CONFIG, ENABLE_NETWORK_ISOLATION
)
PROCESSING_JOB_INPUTS_PATH = _simple_path(SAGEMAKER, PROCESSING_JOB, PROCESSING_INPUTS)
REDSHIFT_DATASET_DEFINITION_KMS_KEY_ID_PATH = _simple_path(
    DATASET_DEFINITION, REDSHIFT_DATASET_DEFINITION, KMS_KEY_ID
)
ATHENA_DATASET_DEFINITION_KMS_KEY_ID_PATH = _simple_path(
    DATASET_DEFINITION, ATHENA_DATASET_DEFINITION, KMS_KEY_ID
)
REDSHIFT_DATASET_DEFINITION_CLUSTER_ROLE_ARN_PATH = _simple_path(
    DATASET_DEFINITION, REDSHIFT_DATASET_DEFINITION, CLUSTER_ROLE_ARN
)
PROCESSING_JOB_NETWORK_CONFIG_PATH = _simple_path(SAGEMAKER, PROCESSING_JOB, NETWORK_CONFIG)
PROCESSING_JOB_VPC_CONFIG_PATH = _simple_path(SAGEMAKER, PROCESSING_JOB, NETWORK_CONFIG, VPC_CONFIG)
PROCESSING_JOB_SUBNETS_PATH = _simple_path(PROCESSING_JOB_VPC_CONFIG_PATH, SUBNETS)
PROCESSING_JOB_SECURITY_GROUP_IDS_PATH = _simple_path(
    PROCESSING_JOB_VPC_CONFIG_PATH, SECURITY_GROUP_IDS
)
PROCESSING_OUTPUT_CONFIG_PATH = _simple_path(SAGEMAKER, PROCESSING_JOB, PROCESSING_OUTPUT_CONFIG)
PROCESSING_JOB_KMS_KEY_ID_PATH = _simple_path(
    SAGEMAKER, PROCESSING_JOB, PROCESSING_OUTPUT_CONFIG, KMS_KEY_ID
)
PROCESSING_JOB_PROCESSING_RESOURCES_PATH = _simple_path(
    SAGEMAKER, PROCESSING_JOB, PROCESSING_RESOURCES
)
PROCESSING_JOB_VOLUME_KMS_KEY_ID_PATH = _simple_path(
    SAGEMAKER, PROCESSING_JOB, PROCESSING_RESOURCES, CLUSTER_CONFIG, VOLUME_KMS_KEY_ID
)
PROCESSING_JOB_ROLE_ARN_PATH = _simple_path(SAGEMAKER, PROCESSING_JOB, ROLE_ARN)
MODEL_PACKAGE_VALIDATION_ROLE_PATH = _simple_path(
    SAGEMAKER, MODEL_PACKAGE, VALIDATION_SPECIFICATION, VALIDATION_ROLE
)
MODEL_PACKAGE_VALIDATION_PROFILES_PATH = _simple_path(
    SAGEMAKER, MODEL_PACKAGE, VALIDATION_SPECIFICATION, VALIDATION_PROFILES
)

# Paths for reference elsewhere in the SDK.
# Names include the schema version since the paths could change with other schema versions
MONITORING_SCHEDULE_INTER_CONTAINER_ENCRYPTION_PATH = _simple_path(
    SAGEMAKER,
    MONITORING_SCHEDULE,
    MONITORING_SCHEDULE_CONFIG,
    MONITORING_JOB_DEFINITION,
    NETWORK_CONFIG,
    ENABLE_INTER_CONTAINER_TRAFFIC_ENCRYPTION,
)
AUTO_ML_INTER_CONTAINER_ENCRYPTION_PATH = _simple_path(
    SAGEMAKER,
    AUTO_ML,
    AUTO_ML_JOB_CONFIG,
    SECURITY_CONFIG,
    ENABLE_INTER_CONTAINER_TRAFFIC_ENCRYPTION,
)
PROCESSING_JOB_INTER_CONTAINER_ENCRYPTION_PATH = _simple_path(
    SAGEMAKER, PROCESSING_JOB, NETWORK_CONFIG, ENABLE_INTER_CONTAINER_TRAFFIC_ENCRYPTION
)
TRAINING_JOB_INTER_CONTAINER_ENCRYPTION_PATH = _simple_path(
    SAGEMAKER, TRAINING_JOB, ENABLE_INTER_CONTAINER_TRAFFIC_ENCRYPTION
)


SAGEMAKER_PYTHON_SDK_CONFIG_SCHEMA = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    TYPE: OBJECT,
    "required": [SCHEMA_VERSION],
    ADDITIONAL_PROPERTIES: False,
    "definitions": {
        "roleArn": {
            # Schema for IAM Role. This includes a Regex validator.
            TYPE: "string",
            "pattern": r"^arn:aws[a-z\-]*:iam::\d{12}:role/?[a-zA-Z_0-9+=,.@\-_/]+$",
        },
        "securityGroupId": {TYPE: "string", "pattern": r"[-0-9a-zA-Z]+"},
        "subnet": {TYPE: "string", "pattern": r"[-0-9a-zA-Z]+"},
        "vpcConfig": {
            # Schema for VPC Configs.
            # Regex is taken from https://docs.aws.amazon.com/sagemaker/latest/APIReference
            # /API_VpcConfig.html
            TYPE: OBJECT,
            ADDITIONAL_PROPERTIES: False,
            PROPERTIES: {
                SECURITY_GROUP_IDS: {
                    TYPE: "array",
                    "items": {"$ref": "#/definitions/securityGroupId"},
                },
                SUBNETS: {TYPE: "array", "items": {"$ref": "#/definitions/subnet"}},
            },
        },
        "productionVariant": {
            TYPE: OBJECT,
            ADDITIONAL_PROPERTIES: False,
            PROPERTIES: {
                CORE_DUMP_CONFIG: {
                    TYPE: OBJECT,
                    ADDITIONAL_PROPERTIES: False,
                    PROPERTIES: {KMS_KEY_ID: {TYPE: "string"}},
                }
            },
        },
        "validationProfile": {
            TYPE: OBJECT,
            ADDITIONAL_PROPERTIES: False,
            PROPERTIES: {
                TRANSFORM_JOB_DEFINITION: {
                    TYPE: OBJECT,
                    ADDITIONAL_PROPERTIES: False,
                    PROPERTIES: {
                        TRANSFORM_OUTPUT: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {KMS_KEY_ID: {TYPE: "string"}},
                        },
                        TRANSFORM_RESOURCES: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {VOLUME_KMS_KEY_ID: {TYPE: "string"}},
                        },
                    },
                }
            },
        },
        "processingInput": {
            TYPE: OBJECT,
            ADDITIONAL_PROPERTIES: False,
            PROPERTIES: {
                DATASET_DEFINITION: {
                    TYPE: OBJECT,
                    ADDITIONAL_PROPERTIES: False,
                    PROPERTIES: {
                        ATHENA_DATASET_DEFINITION: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {
                                KMS_KEY_ID: {
                                    TYPE: "string",
                                }
                            },
                        },
                        REDSHIFT_DATASET_DEFINITION: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {
                                KMS_KEY_ID: {
                                    TYPE: "string",
                                },
                                CLUSTER_ROLE_ARN: {"$ref": "#/definitions/roleArn"},
                            },
                        },
                    },
                }
            },
        },
        # https://docs.aws.amazon.com/sagemaker/latest/APIReference/API_Tag.html
        "tags": {
            TYPE: "array",
            "items": {
                TYPE: OBJECT,
                ADDITIONAL_PROPERTIES: False,
                PROPERTIES: {
                    KEY: {
                        TYPE: "string",
                        "pattern": r"^[\w\s\d_.:/=+\-@]*$",
                        "minLength": 1,
                        "maxLength": 128,
                    },
                    VALUE: {
                        TYPE: "string",
                        "pattern": r"^[\w\s\d_.:/=+\-@]*$",
                        "minLength": 0,
                        "maxLength": 256,
                    },
                },
                "required": [KEY, VALUE],
            },
            "minItems": 0,
            "maxItems": 50,
        },
        SUBNETS: {TYPE: "array", "items": {"$ref": "#/definitions/subnet"}},
    },
    PROPERTIES: {
        SCHEMA_VERSION: {
            TYPE: "string",
            # Currently we support only one schema version (1.0).
            # In the future this might change if we introduce any breaking changes.
            # So added an enum as a validator.
            "enum": ["1.0"],
            "description": "The schema version of the document.",
        },
        CUSTOM_PARAMETERS: {
            TYPE: OBJECT,
            ADDITIONAL_PROPERTIES: False,
            PATTERN_PROPERTIES: {
                r"^[\w\s\d_.:/=+\-@]+$": {TYPE: "string"},
            },
        },
        SAGEMAKER: {
            TYPE: OBJECT,
            ADDITIONAL_PROPERTIES: False,
            PROPERTIES: {
                PYTHON_SDK: {
                    TYPE: OBJECT,
                    ADDITIONAL_PROPERTIES: False,
                    PROPERTIES: {
                        MODULES: {
                            # Any SageMaker Python SDK specific configuration will be added here.
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                        }
                    },
                },
                # Feature Group
                # https://docs.aws.amazon.com/sagemaker/latest/APIReference/API_CreateFeatureGroup
                # .html
                FEATURE_GROUP: {
                    TYPE: OBJECT,
                    ADDITIONAL_PROPERTIES: False,
                    PROPERTIES: {
                        OFFLINE_STORE_CONFIG: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {
                                S3_STORAGE_CONFIG: {
                                    TYPE: OBJECT,
                                    ADDITIONAL_PROPERTIES: False,
                                    PROPERTIES: {KMS_KEY_ID: {TYPE: "string"}},
                                }
                            },
                        },
                        ONLINE_STORE_CONFIG: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {
                                SECURITY_CONFIG: {
                                    TYPE: OBJECT,
                                    ADDITIONAL_PROPERTIES: False,
                                    PROPERTIES: {KMS_KEY_ID: {TYPE: "string"}},
                                }
                            },
                        },
                        ROLE_ARN: {"$ref": "#/definitions/roleArn"},
                        TAGS: {"$ref": "#/definitions/tags"},
                    },
                },
                # Monitoring Schedule
                # https://docs.aws.amazon.com/sagemaker/latest/APIReference/API_CreateMonitoringSchedule.html
                MONITORING_SCHEDULE: {
                    TYPE: OBJECT,
                    ADDITIONAL_PROPERTIES: False,
                    PROPERTIES: {
                        MONITORING_SCHEDULE_CONFIG: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {
                                MONITORING_JOB_DEFINITION: {
                                    TYPE: OBJECT,
                                    ADDITIONAL_PROPERTIES: False,
                                    PROPERTIES: {
                                        MONITORING_OUTPUT_CONFIG: {
                                            TYPE: OBJECT,
                                            ADDITIONAL_PROPERTIES: False,
                                            PROPERTIES: {KMS_KEY_ID: {TYPE: "string"}},
                                        },
                                        MONITORING_RESOURCES: {
                                            TYPE: OBJECT,
                                            ADDITIONAL_PROPERTIES: False,
                                            PROPERTIES: {
                                                CLUSTER_CONFIG: {
                                                    TYPE: OBJECT,
                                                    ADDITIONAL_PROPERTIES: False,
                                                    PROPERTIES: {
                                                        VOLUME_KMS_KEY_ID: {TYPE: "string"}
                                                    },
                                                }
                                            },
                                        },
                                        NETWORK_CONFIG: {
                                            TYPE: OBJECT,
                                            ADDITIONAL_PROPERTIES: False,
                                            PROPERTIES: {
                                                ENABLE_INTER_CONTAINER_TRAFFIC_ENCRYPTION: {
                                                    TYPE: "boolean"
                                                },
                                                ENABLE_NETWORK_ISOLATION: {TYPE: "boolean"},
                                                VPC_CONFIG: {"$ref": "#/definitions/vpcConfig"},
                                            },
                                        },
                                        ROLE_ARN: {"$ref": "#/definitions/roleArn"},
                                    },
                                }
                            },
                        },
                        TAGS: {"$ref": "#/definitions/tags"},
                    },
                },
                # Endpoint Config
                # https://docs.aws.amazon.com/sagemaker/latest/APIReference/API_CreateEndpointConfig.html
                # Note: there is a separate API for creating Endpoints.
                # That will be added later to schema once we start
                # supporting other parameters such as Tags
                ENDPOINT_CONFIG: {
                    TYPE: OBJECT,
                    ADDITIONAL_PROPERTIES: False,
                    PROPERTIES: {
                        ASYNC_INFERENCE_CONFIG: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {
                                OUTPUT_CONFIG: {
                                    TYPE: OBJECT,
                                    ADDITIONAL_PROPERTIES: False,
                                    PROPERTIES: {KMS_KEY_ID: {TYPE: "string"}},
                                }
                            },
                        },
                        DATA_CAPTURE_CONFIG: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {KMS_KEY_ID: {TYPE: "string"}},
                        },
                        KMS_KEY_ID: {TYPE: "string"},
                        PRODUCTION_VARIANTS: {
                            TYPE: "array",
                            "items": {"$ref": "#/definitions/productionVariant"},
                        },
                        TAGS: {"$ref": "#/definitions/tags"},
                    },
                },
                # Auto ML
                # https://docs.aws.amazon.com/sagemaker/latest/APIReference/API_CreateAutoMLJob.html
                AUTO_ML: {
                    TYPE: OBJECT,
                    ADDITIONAL_PROPERTIES: False,
                    PROPERTIES: {
                        AUTO_ML_JOB_CONFIG: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {
                                SECURITY_CONFIG: {
                                    TYPE: OBJECT,
                                    ADDITIONAL_PROPERTIES: False,
                                    PROPERTIES: {
                                        ENABLE_INTER_CONTAINER_TRAFFIC_ENCRYPTION: {
                                            TYPE: "boolean"
                                        },
                                        VOLUME_KMS_KEY_ID: {
                                            TYPE: "string",
                                        },
                                        VPC_CONFIG: {"$ref": "#/definitions/vpcConfig"},
                                    },
                                }
                            },
                        },
                        OUTPUT_DATA_CONFIG: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {KMS_KEY_ID: {TYPE: "string"}},
                        },
                        ROLE_ARN: {"$ref": "#/definitions/roleArn"},
                        TAGS: {"$ref": "#/definitions/tags"},
                    },
                },
                # Transform Job
                # https://docs.aws.amazon.com/sagemaker/latest/APIReference/API_CreateTransformJob.html
                TRANSFORM_JOB: {
                    TYPE: OBJECT,
                    ADDITIONAL_PROPERTIES: False,
                    PROPERTIES: {
                        DATA_CAPTURE_CONFIG: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {KMS_KEY_ID: {TYPE: "string"}},
                        },
                        TRANSFORM_OUTPUT: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {KMS_KEY_ID: {TYPE: "string"}},
                        },
                        TRANSFORM_RESOURCES: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {VOLUME_KMS_KEY_ID: {TYPE: "string"}},
                        },
                        TAGS: {"$ref": "#/definitions/tags"},
                    },
                },
                # Compilation Job
                # https://docs.aws.amazon.com/sagemaker/latest/APIReference/API_CreateCompilationJob
                # .html
                COMPILATION_JOB: {
                    TYPE: OBJECT,
                    ADDITIONAL_PROPERTIES: False,
                    PROPERTIES: {
                        OUTPUT_CONFIG: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {KMS_KEY_ID: {TYPE: "string"}},
                        },
                        ROLE_ARN: {"$ref": "#/definitions/roleArn"},
                        VPC_CONFIG: {"$ref": "#/definitions/vpcConfig"},
                        TAGS: {"$ref": "#/definitions/tags"},
                    },
                },
                # Pipeline
                # https://docs.aws.amazon.com/sagemaker/latest/APIReference/API_CreatePipeline.html
                PIPELINE: {
                    TYPE: OBJECT,
                    ADDITIONAL_PROPERTIES: False,
                    PROPERTIES: {
                        ROLE_ARN: {"$ref": "#/definitions/roleArn"},
                        TAGS: {"$ref": "#/definitions/tags"},
                    },
                },
                # Model
                # https://docs.aws.amazon.com/sagemaker/latest/APIReference/API_CreateModel.html
                MODEL: {
                    TYPE: OBJECT,
                    ADDITIONAL_PROPERTIES: False,
                    PROPERTIES: {
                        ENABLE_NETWORK_ISOLATION: {TYPE: "boolean"},
                        EXECUTION_ROLE_ARN: {"$ref": "#/definitions/roleArn"},
                        VPC_CONFIG: {"$ref": "#/definitions/vpcConfig"},
                        TAGS: {"$ref": "#/definitions/tags"},
                    },
                },
                # Model Package
                # https://docs.aws.amazon.com/sagemaker/latest/APIReference/API_CreateModelPackage.html
                MODEL_PACKAGE: {
                    TYPE: OBJECT,
                    ADDITIONAL_PROPERTIES: False,
                    PROPERTIES: {
                        VALIDATION_SPECIFICATION: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {
                                VALIDATION_PROFILES: {
                                    TYPE: "array",
                                    "items": {"$ref": "#/definitions/validationProfile"},
                                    # https://docs.aws.amazon.com/sagemaker/latest/APIReference/API_ModelPackageValidationSpecification.html
                                    # According to the API docs, This array should have exactly 1
                                    # item.
                                    "minItems": 1,
                                    "maxItems": 1,
                                },
                                VALIDATION_ROLE: {"$ref": "#/definitions/roleArn"},
                            },
                        },
                    },
                },
                # Processing Job
                # https://docs.aws.amazon.com/sagemaker/latest/APIReference/API_CreateProcessingJob.html
                PROCESSING_JOB: {
                    TYPE: OBJECT,
                    ADDITIONAL_PROPERTIES: False,
                    PROPERTIES: {
                        NETWORK_CONFIG: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {
                                ENABLE_INTER_CONTAINER_TRAFFIC_ENCRYPTION: {TYPE: "boolean"},
                                ENABLE_NETWORK_ISOLATION: {TYPE: "boolean"},
                                VPC_CONFIG: {"$ref": "#/definitions/vpcConfig"},
                            },
                        },
                        PROCESSING_INPUTS: {
                            TYPE: "array",
                            "items": {"$ref": "#/definitions/processingInput"},
                        },
                        PROCESSING_OUTPUT_CONFIG: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {KMS_KEY_ID: {TYPE: "string"}},
                        },
                        PROCESSING_RESOURCES: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {
                                CLUSTER_CONFIG: {
                                    TYPE: OBJECT,
                                    ADDITIONAL_PROPERTIES: False,
                                    PROPERTIES: {VOLUME_KMS_KEY_ID: {TYPE: "string"}},
                                }
                            },
                        },
                        ROLE_ARN: {"$ref": "#/definitions/roleArn"},
                        TAGS: {"$ref": "#/definitions/tags"},
                    },
                },
                # Training Job
                # https://docs.aws.amazon.com/sagemaker/latest/APIReference/API_CreateTrainingJob.html
                TRAINING_JOB: {
                    TYPE: OBJECT,
                    ADDITIONAL_PROPERTIES: False,
                    PROPERTIES: {
                        ENABLE_INTER_CONTAINER_TRAFFIC_ENCRYPTION: {TYPE: "boolean"},
                        ENABLE_NETWORK_ISOLATION: {TYPE: "boolean"},
                        OUTPUT_DATA_CONFIG: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {KMS_KEY_ID: {TYPE: "string"}},
                        },
                        RESOURCE_CONFIG: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {VOLUME_KMS_KEY_ID: {TYPE: "string"}},
                        },
                        ROLE_ARN: {"$ref": "#/definitions/roleArn"},
                        VPC_CONFIG: {"$ref": "#/definitions/vpcConfig"},
                        TAGS: {"$ref": "#/definitions/tags"},
                    },
                },
                # Edge Packaging Job
                # https://docs.aws.amazon.com/sagemaker/latest/APIReference/API_CreateEdgePackagingJob.html
                EDGE_PACKAGING_JOB: {
                    TYPE: OBJECT,
                    ADDITIONAL_PROPERTIES: False,
                    PROPERTIES: {
                        OUTPUT_CONFIG: {
                            TYPE: OBJECT,
                            ADDITIONAL_PROPERTIES: False,
                            PROPERTIES: {KMS_KEY_ID: {TYPE: "string"}},
                        },
                        ROLE_ARN: {"$ref": "#/definitions/roleArn"},
                        TAGS: {"$ref": "#/definitions/tags"},
                    },
                },
            },
        },
    },
}
