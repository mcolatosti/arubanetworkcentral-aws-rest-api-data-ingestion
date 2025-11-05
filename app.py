#!/usr/bin/env python3
import os
import aws_cdk as cdk
from aws_cdk import (
    App, Stack, Duration, RemovalPolicy, CfnOutput,
    aws_ec2 as ec2, aws_rds as rds, aws_secretsmanager as secretsmanager,
    aws_lambda as _lambda, aws_iam as iam, aws_events as events,
    aws_events_targets as targets, aws_logs as logs,
    Environment, SecretValue
)

def _require_context(app: App, key: str, description: str) -> str:
    val = app.node.try_get_context(key)
    if not val:
        raise ValueError(
            f"Missing required context '{key}'. Provide with: cdk deploy "
            f"-c {key}=<value>. Expected: {description}"
        )
    return val

def _require_csv_context(app: App, key: str, description: str) -> list[str]:
    raw = _require_context(app, key, description)
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    if not parts:
        raise ValueError(f"Context '{key}' must list at least one subnet id.")
    return parts

class ArubaCentralIngestionStack(Stack):
    def __init__(self, scope: App, construct_id: str, *, environment_name: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        is_prod = environment_name == "prod"

        # Required context
        app_node = self.node.root  # type: ignore
        vpc_id = _require_context(app_node, "vpcId", "Existing VPC ID (e.g. vpc-1234567890abcdef0)")
        lambda_subnet_ids = _require_csv_context(
            app_node, "lambdaSubnetIds",
            "Comma separated subnet IDs for Lambda (e.g. subnet-aaa,subnet-bbb)"
        )
        db_subnet_ids = _require_csv_context(
            app_node, "dbSubnetIds",
            "Comma separated subnet IDs for RDS (e.g. subnet-ccc,subnet-ddd)"
        )
        mysql_version_str = _require_context(
            app_node, "mysqlVersion",
            "MySQL engine version string (e.g. 8.0.34). Use 'aws rds describe-db-engine-versions --engine mysql'"
        )

        # Guard rails
        if len(db_subnet_ids) < 2:
            raise ValueError("RDS requires at least two subnet IDs (different AZs).")
        if not lambda_subnet_ids:
            raise ValueError("At least one Lambda subnet ID must be provided.")
        # Basic format check
        if not mysql_version_str[0].isdigit():
            raise ValueError("mysqlVersion must start with a digit (e.g. 8.0.34).")

        # Existing VPC + subnets
        vpc = ec2.Vpc.from_lookup(self, "ExistingVpc", vpc_id=vpc_id)
        lambda_subnets = [ec2.Subnet.from_subnet_id(self, f"LambdaSubnet{i}", sid) for i, sid in enumerate(lambda_subnet_ids)]
        db_subnets = [ec2.Subnet.from_subnet_id(self, f"DbSubnet{i}", sid) for i, sid in enumerate(db_subnet_ids)]
        lambda_subnet_selection = ec2.SubnetSelection(subnets=lambda_subnets)
        db_subnet_selection = ec2.SubnetSelection(subnets=db_subnets)

        # Secrets
        db_secret = secretsmanager.Secret(
            self,
            "DbCredentialsSecret",
            description="MySQL credentials for Aruba Central ingestion",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username":"aruba_admin"}',
                generate_string_key="password",
                password_length=16,
                exclude_punctuation=True,
            ),
        )

        aruba_api_secret = secretsmanager.Secret(
            self,
            "ArubaApiCredentialsSecret",
            description="Aruba Central API OAuth2 client credentials",
            secret_object_value={
                "clientId": SecretValue.unsafe_plain_text("REPLACE_CLIENT_ID"),
                "clientSecret": SecretValue.unsafe_plain_text("REPLACE_CLIENT_SECRET"),
                "customerId": SecretValue.unsafe_plain_text("REPLACE_CUSTOMER_ID"),
                "baseUrl": SecretValue.unsafe_plain_text("https://apigw-prod2.central.arubanetworks.com"),
            },
        )

        # Security Groups
        db_sg = ec2.SecurityGroup(self, "DbSg", vpc=vpc, description="MySQL RDS security group", allow_all_outbound=False)
        lambda_sg = ec2.SecurityGroup(self, "LambdaSg", vpc=vpc, description="Lambda SG", allow_all_outbound=True)
        db_sg.add_ingress_rule(lambda_sg, ec2.Port.tcp(3306), "Lambda access to MySQL")

        # RDS (use generic version constructor to avoid missing constant issues)
        selected_mysql_version = rds.MysqlEngineVersion.of(mysql_version_str, "8.0")
        db_instance = rds.DatabaseInstance(
            self,
            "ArubaMySql",
            engine=rds.DatabaseInstanceEngine.mysql(version=selected_mysql_version),
            instance_type=ec2.InstanceType.of(ec2.InstanceClass.BURSTABLE3, ec2.InstanceSize.MICRO),
            vpc=vpc,
            vpc_subnets=db_subnet_selection,
            security_groups=[db_sg],
            credentials=rds.Credentials.from_secret(db_secret),
            allocated_storage=20,
            storage_encrypted=True,
            multi_az=False,
            database_name="aruba_central",
            backup_retention=Duration.days(7 if is_prod else 1),
            deletion_protection=is_prod,
            removal_policy=RemovalPolicy.SNAPSHOT if is_prod else RemovalPolicy.DESTROY,
            monitoring_interval=Duration.seconds(0),
            publicly_accessible=False,
        )

        # IAM Role
        ingestion_role = iam.Role(
            self,
            "IngestionLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaVPCAccessExecutionRole"),
            ],
        )
        db_secret.grant_read(ingestion_role)
        aruba_api_secret.grant_read(ingestion_role)

        # Lambda (clients + legacy devices mixed)
        ingestion_fn = _lambda.Function(
            self,
            "clientsFn",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="ingestion_handler.lambda_handler",
            code=_lambda.Code.from_asset("lambda_py"),
            role=ingestion_role,
            memory_size=512,
            timeout=Duration.minutes(15),
            vpc=vpc,
            security_groups=[lambda_sg],
            vpc_subnets=lambda_subnet_selection,
            environment={
                "ENVIRONMENT": environment_name,
                "LOG_LEVEL": "INFO" if is_prod else "DEBUG",
                "DB_SECRET_ARN": db_secret.secret_arn,
                "ARUBA_API_SECRET_ARN": aruba_api_secret.secret_arn,
                "DB_HOST": db_instance.instance_endpoint.hostname,
                "DB_PORT": str(db_instance.instance_endpoint.port),
                "PAGE_LIMIT": "100",
                "SITE_PAGE_DELAY_MS": "400",
                "CLIENT_PAGE_DELAY_MS": "250",
                "DEVICE_PAGE_DELAY_MS": "250",
                "ARUBA_CLIENTS_EXCLUDE_STATUS": "disconnected",
                "ARUBA_PAGE_SIZE": "100",
                "ARUBA_PAGE_DELAY_SECONDS": "2.0",
                "COLLECT_CLIENTS": "true",
                "COLLECT_DEVICES": "true",
            },
        )

        logs.LogGroup(
            self,
            "IngestionLogGroup",
            log_group_name=f"/aws/lambda/{ingestion_fn.function_name}",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # Removed 4hr EventBridge rule (IngestionSchedule)

        events.Rule(
            self, "ArubaIngestionEvery15Min",
            schedule=events.Schedule.rate(Duration.minutes(30)),
            targets=[targets.LambdaFunction(ingestion_fn)]
        )

        # Dedicated Switch Interfaces Ingestion Lambda
        switch_interfacedetails_fn = _lambda.Function(
            self,
            "switch-interfacesFn",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="switch_interfaces_ingestion_handler.lambda_handler",
            code=_lambda.Code.from_asset("lambda_py"),
            role=ingestion_role,
            memory_size=512,
            timeout=Duration.minutes(15),
            vpc=vpc,
            security_groups=[lambda_sg],
            vpc_subnets=lambda_subnet_selection,
            environment={
                "ENVIRONMENT": environment_name,
                "LOG_LEVEL": "INFO" if is_prod else "DEBUG",
                "DB_SECRET_ARN": db_secret.secret_arn,
                "ARUBA_API_SECRET_ARN": aruba_api_secret.secret_arn,
                "DB_HOST": db_instance.instance_endpoint.hostname,
                "DB_PORT": str(db_instance.instance_endpoint.port),
                "ARUBA_PAGE_SIZE": "100",
                "ARUBA_PAGE_DELAY_SECONDS": "2.0",
                "COLLECT_SWITCH_INTERFACEDETAILS": "true",
            },
        )

        logs.LogGroup(
            self,
            "SwitchInterfacesLogGroup",
            log_group_name=f"/aws/lambda/{switch_interfacedetails_fn.function_name}",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        events.Rule(
            self,
            "SwitchInterfacesEvery15Min",
            schedule=events.Schedule.rate(Duration.minutes(30)),
            description="Periodic Aruba Central switch interfaces ingestion",
            targets=[targets.LambdaFunction(switch_interfacedetails_fn)],
        )

        # Dedicated Device Status (v2) Lambda (decoupled from clients)
        device_status_v2_fn = _lambda.Function(
            self,
            "devicestatusFn",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="device_status_v2_ingestion_handler.lambda_handler",
            code=_lambda.Code.from_asset("lambda_py"),
            role=ingestion_role,
            memory_size=512,
            timeout=Duration.minutes(15),
            vpc=vpc,
            security_groups=[lambda_sg],
            vpc_subnets=lambda_subnet_selection,
            environment={
                "ENVIRONMENT": environment_name,
                "LOG_LEVEL": "INFO" if is_prod else "DEBUG",
                "DB_SECRET_ARN": db_secret.secret_arn,
                "ARUBA_API_SECRET_ARN": aruba_api_secret.secret_arn,
                "DB_HOST": db_instance.instance_endpoint.hostname,
                "DB_PORT": str(db_instance.instance_endpoint.port),
                "ARUBA_PAGE_SIZE": "100",
                "ARUBA_PAGE_DELAY_SECONDS": "2.0",
                "ARUBA_DEVICE_STATUS_ENDPOINT": "/network-monitoring/v2/devices/status",
            },
        )

        logs.LogGroup(
            self,
            "DeviceStatusV2LogGroup",
            log_group_name=f"/aws/lambda/{device_status_v2_fn.function_name}",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        events.Rule(
            self,
            "DeviceStatusV2Every15Min",
            schedule=events.Schedule.rate(Duration.minutes(30)),
            description="Periodic Aruba Central device status v2 ingestion",
            targets=[targets.LambdaFunction(device_status_v2_fn)],
        )

        # Dedicated AP Ingestion Lambda
        ap_ingestion_fn = _lambda.Function(
            self,
            "apIngestionFn",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="ap_ingestion_lambda_handler.lambda_handler",
            code=_lambda.Code.from_asset("lambda_py"),
            role=ingestion_role,
            memory_size=512,
            timeout=Duration.minutes(15),
            vpc=vpc,
            security_groups=[lambda_sg],
            vpc_subnets=lambda_subnet_selection,
            environment={
                "ENVIRONMENT": environment_name,
                "LOG_LEVEL": "INFO" if is_prod else "DEBUG",
                "DB_SECRET_ARN": db_secret.secret_arn,
                "ARUBA_API_SECRET_ARN": aruba_api_secret.secret_arn,
                "DB_HOST": db_instance.instance_endpoint.hostname,
                "DB_PORT": str(db_instance.instance_endpoint.port),
                "ARUBA_PAGE_SIZE": "100",
                "ARUBA_PAGE_DELAY_SECONDS": "2.0",
                "ARUBA_APS_ENDPOINT": "/monitoring/v2/aps",
                "ARUBA_AP_DETAIL_ENDPOINT": "/monitoring/v2/aps/{serial}",
            },
        )

        logs.LogGroup(
            self,
            "ApIngestionLogGroup",
            log_group_name=f"/aws/lambda/{ap_ingestion_fn.function_name}",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        events.Rule(
            self,
            "ApIngestionEvery15Min",
            schedule=events.Schedule.rate(Duration.minutes(30)),
            description="Periodic Aruba Central AP ingestion",
            targets=[targets.LambdaFunction(ap_ingestion_fn)],
        )

        # Outputs
        CfnOutput(self, "RdsEndpoint", value=db_instance.instance_endpoint.hostname)
        CfnOutput(self, "LambdaName", value=ingestion_fn.function_name)
        try:
            CfnOutput(self, "DeviceStatusV2LambdaName", value=device_status_v2_fn.function_name)
        except Exception:
            pass
        CfnOutput(self, "SwitchInterfaceDetailsLambdaName", value=switch_interfacedetails_fn.function_name)
        CfnOutput(self, "ArubaApiSecretArn", value=aruba_api_secret.secret_arn)
        CfnOutput(self, "DbSecretArn", value=db_secret.secret_arn)
        CfnOutput(self, "LambdaRuntime", value=ingestion_fn.runtime.to_string())
        CfnOutput(self, "VpcId", value=vpc_id)
        CfnOutput(self, "LambdaSubnetIds", value=",".join(lambda_subnet_ids))
        CfnOutput(self, "DbSubnetIds", value=",".join(db_subnet_ids))
        CfnOutput(self, "MySqlVersion", value=mysql_version_str)

app = App()
env_name = app.node.try_get_context("environment") or os.environ.get("ENVIRONMENT", "dev")

ArubaCentralIngestionStack(
    app,
    f"aruba-restapiv2-ingestion",
    environment_name=env_name,
    env=Environment(
        account=os.environ.get("CDK_DEFAULT_ACCOUNT"),
        region=os.environ.get("CDK_DEFAULT_REGION", "us-east-1"),
    ),
)

app.synth()