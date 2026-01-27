import {Construct} from "constructs";
import {CfnPipeline} from "aws-cdk-lib/aws-osis";
import {LogGroup, RetentionDays} from "aws-cdk-lib/aws-logs";
import {Fn, RemovalPolicy} from "aws-cdk-lib";
import {IVpc, SecurityGroup} from "aws-cdk-lib/aws-ec2";
import * as fs from "fs";

export interface OpensearchPipelineProps {
    readonly vpc: IVpc;
    readonly pipelineName?: string;
    readonly migrationBucketName: string;
    readonly opensearchEndpoint: string;
    readonly pipelineRoleArn: string;
    readonly indexName: string;
}

export class OpensearchPipelineConstruct extends Construct {
    readonly pipeline: CfnPipeline;
    constructor(scope: Construct, id: string, props: OpensearchPipelineProps) {
        super(scope, id);

        const pipeline_name = props.pipelineName || 'solr2os-migration-pipeline';
        const fileContents = fs.readFileSync('lib/pipeline/pipeline.yaml', 'utf8');

        const cloudwatchLogsGroup = new LogGroup(scope, 'LogGroup', {
            logGroupName: `/aws/vendedlogs/OpenSearchService/${pipeline_name}`,
            retention: RetentionDays.ONE_MONTH,
            removalPolicy: RemovalPolicy.DESTROY
        });

        const securityGroup = new SecurityGroup(this, "SecurityGroup", {
            vpc: props.vpc, description: 'Security group for Pipeline domain', allowAllOutbound: true
        });

        const cfnPipeline = new CfnPipeline(scope, "Pipeline", {
            maxUnits: 1,
            minUnits: 1,
            pipelineConfigurationBody: Fn.sub(fileContents, {
                region: process.env["CDK_DEFAULT_REGION"] || "",
                accountId: process.env["CDK_DEFAULT_ACCOUNT"] || "",
                pipelineRoleArn: props.pipelineRoleArn,
                openSearchDomainVPCEndpoint: props.opensearchEndpoint,
                bucketName: props.migrationBucketName,
                indexName: props.indexName
            }),
            pipelineName: pipeline_name,
            vpcOptions: {
                subnetIds: props.vpc.privateSubnets.map((subnet) => subnet.subnetId),
                securityGroupIds: [securityGroup.securityGroupId],
                vpcEndpointManagement: "SERVICE"
            },
            logPublishingOptions: {
                cloudWatchLogDestination: {
                    logGroup: cloudwatchLogsGroup.logGroupName,
                },
                isLoggingEnabled: true,
            },
        });
        this.pipeline = cfnPipeline;
    }
}


