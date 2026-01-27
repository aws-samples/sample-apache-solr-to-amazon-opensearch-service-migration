import * as cdk from "aws-cdk-lib";
import {CfnOutput} from "aws-cdk-lib";
import {Construct} from "constructs";
import {IVpc, Vpc} from "aws-cdk-lib/aws-ec2";
import {VpcConstruct} from "./constructs/vpc";
import {PipelineIamRole} from "./constructs/iam";
import {OpenSearchConstruct} from "./constructs/opensearch";
import {S3Construct} from "./constructs/s3";
import {OpenSearchRoleMapper} from "./constructs/opensearch_role_custom_resource";
import {OpensearchPipelineConstruct} from "./constructs/opensearch_pipeline";
import {Ec2workbench} from "./constructs/ec2workbench";

export interface Solr2OsStackProps extends cdk.StackProps {
    cidr?: string;
    namePrefix?: string;
    domainName?: string;
    indexName?: string;
    enable_pipeline: boolean;
}

export class Solr2OsStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: Solr2OsStackProps) {
        super(scope, id, props);

        const namePrefix = props?.namePrefix || "solr2os";
        let vpcName = namePrefix + "-vpc";
        let domainName = props?.domainName || namePrefix + "-domain";
        let migrationBucketName = namePrefix + "-migration-bucket"
        let pipelineName = namePrefix + "-pipeline"
        let indexName = props?.indexName || "solr-migration";
        let cidr = props?.cidr  || "10.0.0.0/16"
        let vpc: IVpc;

        vpc = new VpcConstruct(this, 'MigrationVPC', {
                vpcCidr: cidr,
                vpcName: vpcName
            }).vpc;

        const pipeline_iam = new PipelineIamRole(this, "PipelineIamUser", {
            vpc: vpc,
            pipelineName: pipelineName,
            migrationBucketName: migrationBucketName,
            domainName: domainName
        })

        const opensearch = new OpenSearchConstruct(this, "MigrationDomain", {
            vpc: vpc,
            subnets: vpc.privateSubnets,
            domainName: domainName,
            pipelineRoleArn: pipeline_iam.pipelineRole.roleArn
        });

        const ec2_host = new Ec2workbench(this, "Ec2WorkBench", {
            vpc: vpc as Vpc,
            domainName: domainName,
            migrationBucketName: migrationBucketName,
            opensearchEndpoint: opensearch.domain.domainEndpoint,
            indexName: indexName,
            opensearchSecretName: opensearch.secret.secretName,
            pipelineName: pipelineName
        });

        opensearch.domain.grantReadWrite(ec2_host.host.role);

        const s3 = new S3Construct(this, "MigrationS3", {
            migrationBucketName: migrationBucketName
        })

        const openSearchRoleMapper = new OpenSearchRoleMapper(this, 'RoleMapper', {
                vpc: vpc,
                domainName: domainName,
                roleName: "all_access",
                iamRoleArns: `${pipeline_iam.pipelineRole.roleArn},${ec2_host.host.role.roleArn}`,
                domainEndpoint: opensearch.domain.domainEndpoint,
                secretName: opensearch.secret.secretName
        })
        openSearchRoleMapper.node.addDependency(opensearch.domain)
        openSearchRoleMapper.node.addDependency(opensearch.secret)
        openSearchRoleMapper.node.addDependency(pipeline_iam.pipelineRole)
        openSearchRoleMapper.node.addDependency(ec2_host.host)

        if (props?.enable_pipeline === true) {
            console.log("Creating pipeline");
            const pipeline = new OpensearchPipelineConstruct(this, "MigrationPipeline", {
                vpc: vpc,
                pipelineName: pipelineName,
                migrationBucketName: migrationBucketName,
                opensearchEndpoint: opensearch.domain.domainEndpoint,
                pipelineRoleArn: pipeline_iam.pipelineRole.roleArn,
                indexName: indexName
            })

            pipeline.node.addDependency(pipeline_iam.pipelineRole)
            pipeline.node.addDependency(pipeline_iam.pipelineRole)
            pipeline.node.addDependency(s3.migrationBucket)
            // pipeline.node.addDependency(pipeline_mapper.cr)

            new CfnOutput(this, "PipelineName", {
                value: pipelineName,
                exportName: "PipelineName"
            });
        }

        new CfnOutput(this, "OpensearchEndpoint", {
            value: opensearch.domain.domainEndpoint,
            exportName: "OpensearchEndpoint"
        })

        new CfnOutput(this, "OpensearchSecretName", {
            value: opensearch.secret.secretName,
            exportName: "OpensearchSecretName"
        })

        new CfnOutput(this, 'WorkBenchInstanceID', {
            value: ec2_host.host.instanceId,
            exportName: "WorkBenchInstanceID"
        });

        new CfnOutput(this, 'WorkBenchPrivateIP', {
            value: ec2_host.host.instancePrivateIp,
            exportName: "WorkBenchPrivateIP"
        });

        new CfnOutput(this,"PackageBucketName", {
            value: `${s3.migrationBucket.bucketName}`,
            exportName: "PackageBucketName"
        })

        new CfnOutput(this,"DataBucketName", {
            value: `${s3.migrationBucket.bucketName}/migration_data`,
            exportName: "DataBucketName"
        })

        new CfnOutput(this, "OpenSearchDashboardSSMSessionCommand", {
            value: ` aws ssm start-session --target ${ec2_host.host.instanceId} \\
                --document-name AWS-StartPortForwardingSessionToRemoteHost \\
                --parameters '{"host":["${opensearch.domain.domainEndpoint}"],"portNumber":["443"], "localPortNumber":["8200"]}'`,
            exportName: "OpenSearchDashboardSSMSessionCommand"
        })

        new CfnOutput(this, "MigrationToolPath", {
            value: "/home/ec2-user/sample-apache-solr-to-amazon-opensearch-service-migration",
            exportName: "MigrationToolPath"
        });

        new CfnOutput(this, "RunMigrationCommand", {
            value: "cd /home/ec2-user/sample-apache-solr-to-amazon-opensearch-service-migration && python3 main.py",
            exportName: "RunMigrationCommand"
        });

        new CfnOutput(this, "SSMSessionCommand", {
            value: `aws ssm start-session --target ${ec2_host.host.instanceId}`,
            exportName: "SSMSessionCommand"
        });
    }
}